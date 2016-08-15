/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.controller.repository;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.controller.repository.claim.StandardContentClaim;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wali.MinimalLockingWriteAheadLog;
import org.wali.SerDe;
import org.wali.SyncListener;
import org.wali.UpdateType;
import org.wali.WriteAheadRepository;

/**
 * <p>
 * Implements FlowFile Repository using WALI as the backing store.
 * </p>
 *
 * <p>
 * We expose a property named <code>nifi.flowfile.repository.always.sync</code> that is a boolean value indicating whether or not to force WALI to sync with disk on each update. By default, the value
 * is <code>false</code>. This is needed only in situations in which power loss is expected and not mitigated by Uninterruptable Power Sources (UPS) or when running in an unstable Virtual Machine for
 * instance. Otherwise, we will flush the data that is written to the Operating System and the Operating System will be responsible to flush its buffers when appropriate. The Operating System can be
 * configured to hold only a certain buffer size or not to buffer at all, as well. When using a UPS, this is generally not an issue, as the machine is typically notified before dying, in which case
 * the Operating System will flush the data to disk. Additionally, most disks on enterprise servers also have battery backups that can power the disks long enough to flush their buffers. For this
 * reason, we choose instead to not sync to disk for every write but instead sync only when we checkpoint.
 * </p>
 */
public class WriteAheadFlowFileRepository implements FlowFileRepository, SyncListener {

    private final AtomicLong flowFileSequenceGenerator = new AtomicLong(0L);
    private final boolean alwaysSync;

    private static final Logger logger = LoggerFactory.getLogger(WriteAheadFlowFileRepository.class);
    private volatile ScheduledFuture<?> checkpointFuture;

    private final long checkpointDelayMillis;
    private final Path flowFileRepositoryPath;
    private final int numPartitions;
    private final ScheduledExecutorService checkpointExecutor;

    // effectively final
    private WriteAheadRepository<RepositoryRecord> wal;
    private WriteAheadRecordSerde serde;
    private ResourceClaimManager claimManager;

    // WALI Provides the ability to register callbacks for when a Partition or the entire Repository is sync'ed with the underlying disk.
    // We keep track of this because we need to ensure that the ContentClaims are destroyed only after the FlowFile Repository has been
    // synced with disk.
    //
    // This is required due to the following scenario, which could exist if we did not do this:
    //
    // A Processor modifies a FlowFile (whose content is in ContentClaim A), writing the new content to ContentClaim B.
    // The processor removes ContentClaim A, which deletes the backing file.
    // The FlowFile Repository writes out this change but has not yet synced the update to disk.
    // Power is lost.
    // On recovery of the FlowFile Repository, we recover the FlowFile which is pointing to ContentClaim A because the update to the repo
    //      has not been flushed to disk.
    // ContentClaim A does not exist anymore because the Session Commit destroyed the data.
    // This results in Data Loss!
    // However, the comment in the class's JavaDocs regarding sync'ing should also be considered.
    //
    // In order to avoid this, instead of destroying ContentClaim A, the ProcessSession puts the claim on the Claim Destruction Queue.
    // We periodically force a sync of the FlowFile Repository to the backing storage mechanism.
    // We can then destroy the data. If we end up syncing the FlowFile Repository to the backing storage mechanism and then restart
    // before the data is destroyed, it's okay because the data will be unknown to the Content Repository, so it will be destroyed
    // on restart.
    private final ConcurrentMap<Integer, BlockingQueue<ResourceClaim>> claimsAwaitingDestruction = new ConcurrentHashMap<>();

    public WriteAheadFlowFileRepository() {
        final NiFiProperties properties = NiFiProperties.getInstance();

        alwaysSync = Boolean.parseBoolean(properties.getProperty(NiFiProperties.FLOWFILE_REPOSITORY_ALWAYS_SYNC, "false"));

        // determine the database file path and ensure it exists
        flowFileRepositoryPath = properties.getFlowFileRepositoryPath();
        numPartitions = properties.getFlowFileRepositoryPartitions();
        checkpointDelayMillis = FormatUtils.getTimeDuration(properties.getFlowFileRepositoryCheckpointInterval(), TimeUnit.MILLISECONDS);

        checkpointExecutor = Executors.newSingleThreadScheduledExecutor();
    }

    @Override
    public void initialize(final ResourceClaimManager claimManager) throws IOException {
        this.claimManager = claimManager;

        Files.createDirectories(flowFileRepositoryPath);

        // TODO: Should ensure that only 1 instance running and pointing at a particular path
        // TODO: Allow for backup path that can be used if disk out of space?? Would allow a snapshot to be stored on
        // backup and then the data deleted from the normal location; then can move backup to normal location and
        // delete backup. On restore, if no files exist in partition's directory, would have to check backup directory
        serde = new WriteAheadRecordSerde(claimManager);
        wal = new MinimalLockingWriteAheadLog<>(flowFileRepositoryPath, numPartitions, serde, this);
    }

    @Override
    public void close() throws IOException {
        if (checkpointFuture != null) {
            checkpointFuture.cancel(false);
        }

        checkpointExecutor.shutdown();
        wal.shutdown();
    }

    @Override
    public boolean isVolatile() {
        return false;
    }

    @Override
    public long getStorageCapacity() throws IOException {
        return Files.getFileStore(flowFileRepositoryPath).getTotalSpace();
    }

    @Override
    public long getUsableStorageSpace() throws IOException {
        return Files.getFileStore(flowFileRepositoryPath).getUsableSpace();
    }

    @Override
    public void updateRepository(final Collection<RepositoryRecord> records) throws IOException {
        updateRepository(records, alwaysSync);
    }

    private void markDestructable(final ResourceClaim resourceClaim) {
        if (resourceClaim == null) {
            return;
        }

        claimManager.markDestructable(resourceClaim);
    }

    private boolean isDestructable(final ContentClaim claim) {
        if (claim == null) {
            return false;
        }

        final ResourceClaim resourceClaim = claim.getResourceClaim();
        if (resourceClaim == null) {
            return false;
        }

        return !resourceClaim.isInUse();
    }

    private void updateRepository(final Collection<RepositoryRecord> records, final boolean sync) throws IOException {
        for (final RepositoryRecord record : records) {
            if (record.getType() != RepositoryRecordType.DELETE && record.getType() != RepositoryRecordType.CONTENTMISSING && record.getDestination() == null) {
                throw new IllegalArgumentException("Record " + record + " has no destination and Type is " + record.getType());
            }
        }

        // update the repository.
        final int partitionIndex = wal.update(records, sync);

        // The below code is not entirely thread-safe, but we are OK with that because the results aren't really harmful.
        // Specifically, if two different threads call updateRepository with DELETE records for the same Content Claim,
        // it's quite possible for claimant count to be 0 below, which results in two different threads adding the Content
        // Claim to the 'claimsAwaitDestruction' map. As a result, we can call #markDestructable with the same ContentClaim
        // multiple times, and the #markDestructable method is not necessarily idempotent.
        // However, the result of this is that the FileSystem Repository may end up trying to remove the content multiple times.
        // This does not, however, cause problems, as ContentRepository should handle this
        // This does indicate that some refactoring should probably be performed, though, as this is not a very clean interface.
        final Set<ResourceClaim> claimsToAdd = new HashSet<>();
        for (final RepositoryRecord record : records) {
            if (record.getType() == RepositoryRecordType.DELETE) {
                // For any DELETE record that we have, if claim is destructible, mark it so
                if (record.getCurrentClaim() != null && isDestructable(record.getCurrentClaim())) {
                    claimsToAdd.add(record.getCurrentClaim().getResourceClaim());
                }

                // If the original claim is different than the current claim and the original claim is destructible, mark it so
                if (record.getOriginalClaim() != null && !record.getOriginalClaim().equals(record.getCurrentClaim()) && isDestructable(record.getOriginalClaim())) {
                    claimsToAdd.add(record.getOriginalClaim().getResourceClaim());
                }
            } else if (record.getType() == RepositoryRecordType.UPDATE) {
                // if we have an update, and the original is no longer needed, mark original as destructible
                if (record.getOriginalClaim() != null && record.getCurrentClaim() != record.getOriginalClaim() && isDestructable(record.getOriginalClaim())) {
                    claimsToAdd.add(record.getOriginalClaim().getResourceClaim());
                }
            }

            final List<ContentClaim> transientClaims = record.getTransientClaims();
            if (transientClaims != null) {
                for (final ContentClaim transientClaim : transientClaims) {
                    if (isDestructable(transientClaim)) {
                        claimsToAdd.add(transientClaim.getResourceClaim());
                    }
                }
            }
        }

        if (!claimsToAdd.isEmpty()) {
            // Get / Register a Set<ContentClaim> for the given Partiton Index
            final Integer partitionKey = Integer.valueOf(partitionIndex);
            BlockingQueue<ResourceClaim> claimQueue = claimsAwaitingDestruction.get(partitionKey);
            if (claimQueue == null) {
                claimQueue = new LinkedBlockingQueue<>();
                final BlockingQueue<ResourceClaim> existingClaimQueue = claimsAwaitingDestruction.putIfAbsent(partitionKey, claimQueue);
                if (existingClaimQueue != null) {
                    claimQueue = existingClaimQueue;
                }
            }

            claimQueue.addAll(claimsToAdd);
        }
    }


    @Override
    public void onSync(final int partitionIndex) {
        final BlockingQueue<ResourceClaim> claimQueue = claimsAwaitingDestruction.get(Integer.valueOf(partitionIndex));
        if (claimQueue == null) {
            return;
        }

        final Set<ResourceClaim> claimsToDestroy = new HashSet<>();
        claimQueue.drainTo(claimsToDestroy);

        for (final ResourceClaim claim : claimsToDestroy) {
            markDestructable(claim);
        }
    }

    @Override
    public void onGlobalSync() {
        for (final BlockingQueue<ResourceClaim> claimQueue : claimsAwaitingDestruction.values()) {
            final Set<ResourceClaim> claimsToDestroy = new HashSet<>();
            claimQueue.drainTo(claimsToDestroy);

            for (final ResourceClaim claim : claimsToDestroy) {
                markDestructable(claim);
            }
        }
    }

    /**
     * Swaps the FlowFiles that live on the given Connection out to disk, using the specified Swap File and returns the number of FlowFiles that were persisted.
     *
     * @param queue queue to swap out
     * @param swapLocation location to swap to
     * @throws IOException ioe
     */
    @Override
    public void swapFlowFilesOut(final List<FlowFileRecord> swappedOut, final FlowFileQueue queue, final String swapLocation) throws IOException {
        final List<RepositoryRecord> repoRecords = new ArrayList<>();
        if (swappedOut == null || swappedOut.isEmpty()) {
            return;
        }

        for (final FlowFileRecord swapRecord : swappedOut) {
            final RepositoryRecord repoRecord = new StandardRepositoryRecord(queue, swapRecord, swapLocation);
            repoRecords.add(repoRecord);
        }

        // update WALI to indicate that the records were swapped out.
        wal.update(repoRecords, true);

        logger.info("Successfully swapped out {} FlowFiles from {} to Swap File {}", new Object[]{swappedOut.size(), queue, swapLocation});
    }

    @Override
    public void swapFlowFilesIn(final String swapLocation, final List<FlowFileRecord> swapRecords, final FlowFileQueue queue) throws IOException {
        final List<RepositoryRecord> repoRecords = new ArrayList<>();

        for (final FlowFileRecord swapRecord : swapRecords) {
            final StandardRepositoryRecord repoRecord = new StandardRepositoryRecord(queue, swapRecord);
            repoRecord.setSwapLocation(swapLocation);   // set the swap file to indicate that it's being swapped in.
            repoRecord.setDestination(queue);

            repoRecords.add(repoRecord);
        }

        updateRepository(repoRecords, true);
        logger.info("Repository updated to reflect that {} FlowFiles were swapped in to {}", new Object[]{swapRecords.size(), queue});
    }

    @Override
    public long loadFlowFiles(final QueueProvider queueProvider, final long minimumSequenceNumber) throws IOException {
        final Map<String, FlowFileQueue> queueMap = new HashMap<>();
        for (final FlowFileQueue queue : queueProvider.getAllQueues()) {
            queueMap.put(queue.getIdentifier(), queue);
        }
        serde.setQueueMap(queueMap);
        final Collection<RepositoryRecord> recordList = wal.recoverRecords();
        serde.setQueueMap(null);

        for (final RepositoryRecord record : recordList) {
            final ContentClaim claim = record.getCurrentClaim();
            if (claim != null) {
                claimManager.incrementClaimantCount(claim.getResourceClaim());
            }
        }

        // Determine the next sequence number for FlowFiles
        long maxId = minimumSequenceNumber;
        for (final RepositoryRecord record : recordList) {
            final long recordId = serde.getRecordIdentifier(record);
            if (recordId > maxId) {
                maxId = recordId;
            }

            final FlowFileRecord flowFile = record.getCurrent();
            final FlowFileQueue queue = record.getOriginalQueue();
            if (queue != null) {
                queue.put(flowFile);
            }
        }

        // Set the AtomicLong to 1 more than the max ID so that calls to #getNextFlowFileSequence() will
        // return the appropriate number.
        flowFileSequenceGenerator.set(maxId + 1);
        logger.info("Successfully restored {} FlowFiles", recordList.size());

        final Runnable checkpointRunnable = new Runnable() {
            @Override
            public void run() {
                try {
                    logger.info("Initiating checkpoint of FlowFile Repository");
                    final long start = System.nanoTime();
                    final int numRecordsCheckpointed = checkpoint();
                    final long end = System.nanoTime();
                    final long millis = TimeUnit.MILLISECONDS.convert(end - start, TimeUnit.NANOSECONDS);
                    logger.info("Successfully checkpointed FlowFile Repository with {} records in {} milliseconds",
                            new Object[]{numRecordsCheckpointed, millis});
                } catch (final IOException e) {
                    logger.error("Unable to checkpoint FlowFile Repository due to " + e.toString(), e);
                }
            }
        };

        checkpointFuture = checkpointExecutor.scheduleWithFixedDelay(checkpointRunnable, checkpointDelayMillis, checkpointDelayMillis, TimeUnit.MILLISECONDS);

        return maxId;
    }

    @Override
    public long getNextFlowFileSequence() {
        return flowFileSequenceGenerator.getAndIncrement();
    }

    @Override
    public long getMaxFlowFileIdentifier() throws IOException {
        // flowFileSequenceGenerator is 1 more than the MAX so that we can call #getAndIncrement on the AtomicLong
        return flowFileSequenceGenerator.get() - 1;
    }

    public int checkpoint() throws IOException {
        return wal.checkpoint();
    }

    private static class WriteAheadRecordSerde implements SerDe<RepositoryRecord> {
        private static final int CURRENT_ENCODING_VERSION = 9;

        public static final byte ACTION_CREATE = 0;
        public static final byte ACTION_UPDATE = 1;
        public static final byte ACTION_DELETE = 2;
        public static final byte ACTION_SWAPPED_OUT = 3;
        public static final byte ACTION_SWAPPED_IN = 4;

        private Map<String, FlowFileQueue> flowFileQueueMap = null;
        private long recordsRestored = 0L;
        private final ResourceClaimManager claimManager;

        public WriteAheadRecordSerde(final ResourceClaimManager claimManager) {
            this.claimManager = claimManager;
        }

        private void setQueueMap(final Map<String, FlowFileQueue> queueMap) {
            this.flowFileQueueMap = queueMap;
        }

        @Override
        public void serializeEdit(final RepositoryRecord previousRecordState, final RepositoryRecord record, final DataOutputStream out) throws IOException {
            serializeEdit(previousRecordState, record, out, false);
        }

        public void serializeEdit(final RepositoryRecord previousRecordState, final RepositoryRecord record, final DataOutputStream out, final boolean forceAttributesWritten) throws IOException {
            if (record.isMarkedForAbort()) {
                logger.warn("Repository Record {} is marked to be aborted; it will be persisted in the FlowFileRepository as a DELETE record", record);
                out.write(ACTION_DELETE);
                out.writeLong(getRecordIdentifier(record));
                serializeContentClaim(record.getCurrentClaim(), record.getCurrentClaimOffset(), out);
                return;
            }

            final UpdateType updateType = getUpdateType(record);

            if (updateType.equals(UpdateType.DELETE)) {
                out.write(ACTION_DELETE);
                out.writeLong(getRecordIdentifier(record));
                serializeContentClaim(record.getCurrentClaim(), record.getCurrentClaimOffset(), out);
                return;
            }

            // If there's a Destination Connection, that's the one that we want to associated with this record.
            // However, on restart, we will restore the FlowFile and set this connection to its "originalConnection".
            // If we then serialize the FlowFile again before it's transferred, it's important to allow this to happen,
            // so we use the originalConnection instead
            FlowFileQueue associatedQueue = record.getDestination();
            if (associatedQueue == null) {
                associatedQueue = record.getOriginalQueue();
            }

            if (updateType.equals(UpdateType.SWAP_OUT)) {
                out.write(ACTION_SWAPPED_OUT);
                out.writeLong(getRecordIdentifier(record));
                out.writeUTF(associatedQueue.getIdentifier());
                out.writeUTF(getLocation(record));
                return;
            }

            final FlowFile flowFile = record.getCurrent();
            final ContentClaim claim = record.getCurrentClaim();

            switch (updateType) {
                case UPDATE:
                    out.write(ACTION_UPDATE);
                    break;
                case CREATE:
                    out.write(ACTION_CREATE);
                    break;
                case SWAP_IN:
                    out.write(ACTION_SWAPPED_IN);
                    break;
                default:
                    throw new AssertionError();
            }

            out.writeLong(getRecordIdentifier(record));
            out.writeLong(flowFile.getEntryDate());
            out.writeLong(flowFile.getLineageStartDate());
            out.writeLong(flowFile.getLineageStartIndex());

            final Long queueDate = flowFile.getLastQueueDate();
            out.writeLong(queueDate == null ? System.currentTimeMillis() : queueDate);
            out.writeLong(flowFile.getQueueDateIndex());
            out.writeLong(flowFile.getSize());

            if (associatedQueue == null) {
                logger.warn("{} Repository Record {} has no Connection associated with it; it will be destroyed on restart",
                        new Object[]{this, record});
                writeString("", out);
            } else {
                writeString(associatedQueue.getIdentifier(), out);
            }

            serializeContentClaim(claim, record.getCurrentClaimOffset(), out);

            if (forceAttributesWritten || record.isAttributesChanged() || updateType == UpdateType.CREATE || updateType == UpdateType.SWAP_IN) {
                out.write(1);   // indicate attributes changed
                final Map<String, String> attributes = flowFile.getAttributes();
                out.writeInt(attributes.size());
                for (final Map.Entry<String, String> entry : attributes.entrySet()) {
                    writeString(entry.getKey(), out);
                    writeString(entry.getValue(), out);
                }
            } else {
                out.write(0);   // indicate attributes did not change
            }

            if (updateType == UpdateType.SWAP_IN) {
                out.writeUTF(record.getSwapLocation());
            }
        }

        @Override
        public RepositoryRecord deserializeEdit(final DataInputStream in, final Map<Object, RepositoryRecord> currentRecordStates, final int version) throws IOException {
            final int action = in.read();
            final long recordId = in.readLong();
            if (action == ACTION_DELETE) {
                final StandardFlowFileRecord.Builder ffBuilder = new StandardFlowFileRecord.Builder().id(recordId);

                if (version > 4) {
                    deserializeClaim(in, version, ffBuilder);
                }

                final FlowFileRecord flowFileRecord = ffBuilder.build();
                final StandardRepositoryRecord record = new StandardRepositoryRecord((FlowFileQueue) null, flowFileRecord);
                record.markForDelete();

                return record;
            }

            if (action == ACTION_SWAPPED_OUT) {
                final String queueId = in.readUTF();
                final String location = in.readUTF();
                final FlowFileQueue queue = flowFileQueueMap.get(queueId);

                final FlowFileRecord flowFileRecord = new StandardFlowFileRecord.Builder()
                        .id(recordId)
                        .build();

                return new StandardRepositoryRecord(queue, flowFileRecord, location);
            }

            final StandardFlowFileRecord.Builder ffBuilder = new StandardFlowFileRecord.Builder();
            final RepositoryRecord record = currentRecordStates.get(recordId);
            ffBuilder.id(recordId);
            if (record != null) {
                ffBuilder.fromFlowFile(record.getCurrent());
            }
            ffBuilder.entryDate(in.readLong());

            if (version > 1) {
                // read the lineage identifiers and lineage start date, which were added in version 2.
                if(version < 9){
                    final int numLineageIds = in.readInt();
                    for (int i = 0; i < numLineageIds; i++) {
                        in.readUTF(); //skip identifiers
                    }
                }
                final long lineageStartDate = in.readLong();
                final long lineageStartIndex;
                if (version > 7) {
                    lineageStartIndex = in.readLong();
                } else {
                    lineageStartIndex = 0L;
                }
                ffBuilder.lineageStart(lineageStartDate, lineageStartIndex);

                if (version > 5) {
                    final long lastQueueDate = in.readLong();
                    final long queueDateIndex;
                    if (version > 7) {
                        queueDateIndex = in.readLong();
                    } else {
                        queueDateIndex = 0L;
                    }

                    ffBuilder.lastQueued(lastQueueDate, queueDateIndex);
                }
            }

            ffBuilder.size(in.readLong());
            final String connectionId = readString(in);

            logger.debug("{} -> {}", new Object[]{recordId, connectionId});

            deserializeClaim(in, version, ffBuilder);

            // recover new attributes, if they changed
            final int attributesChanged = in.read();
            if (attributesChanged == -1) {
                throw new EOFException();
            } else if (attributesChanged == 1) {
                final int numAttributes = in.readInt();
                final Map<String, String> attributes = new HashMap<>();
                for (int i = 0; i < numAttributes; i++) {
                    final String key = readString(in);
                    final String value = readString(in);
                    attributes.put(key, value);
                }

                ffBuilder.addAttributes(attributes);
            } else if (attributesChanged != 0) {
                throw new IOException("Attribute Change Qualifier not found in stream; found value: "
                        + attributesChanged + " after successfully restoring " + recordsRestored + " records. The FlowFile Repository appears to be corrupt!");
            }

            final FlowFileRecord flowFile = ffBuilder.build();
            String swapLocation = null;
            if (action == ACTION_SWAPPED_IN) {
                swapLocation = in.readUTF();
            }

            final StandardRepositoryRecord standardRepoRecord;

            if (flowFileQueueMap == null) {
                standardRepoRecord = new StandardRepositoryRecord(null, flowFile);
            } else {
                final FlowFileQueue queue = flowFileQueueMap.get(connectionId);
                standardRepoRecord = new StandardRepositoryRecord(queue, flowFile);
                if (swapLocation != null) {
                    standardRepoRecord.setSwapLocation(swapLocation);
                }

                if (connectionId.isEmpty()) {
                    logger.warn("{} does not have a Queue associated with it; this record will be discarded", flowFile);
                    standardRepoRecord.markForAbort();
                } else if (queue == null) {
                    logger.warn("{} maps to unknown Queue {}; this record will be discarded", flowFile, connectionId);
                    standardRepoRecord.markForAbort();
                }
            }

            recordsRestored++;
            return standardRepoRecord;
        }

        @Override
        public StandardRepositoryRecord deserializeRecord(final DataInputStream in, final int version) throws IOException {
            final int action = in.read();
            if (action == -1) {
                return null;
            }

            final long recordId = in.readLong();
            if (action == ACTION_DELETE) {
                final StandardFlowFileRecord.Builder ffBuilder = new StandardFlowFileRecord.Builder().id(recordId);

                if (version > 4) {
                    deserializeClaim(in, version, ffBuilder);
                }

                final FlowFileRecord flowFileRecord = ffBuilder.build();
                final StandardRepositoryRecord record = new StandardRepositoryRecord((FlowFileQueue) null, flowFileRecord);
                record.markForDelete();
                return record;
            }

            // if action was not delete, it must be create/swap in
            final StandardFlowFileRecord.Builder ffBuilder = new StandardFlowFileRecord.Builder();
            final long entryDate = in.readLong();

            if (version > 1) {
                // read the lineage identifiers and lineage start date, which were added in version 2.
                if(version < 9) {
                    final int numLineageIds = in.readInt();
                    for (int i = 0; i < numLineageIds; i++) {
                        in.readUTF(); //skip identifiers
                    }
                }

                final long lineageStartDate = in.readLong();
                final long lineageStartIndex;
                if (version > 7) {
                    lineageStartIndex = in.readLong();
                } else {
                    lineageStartIndex = 0L;
                }
                ffBuilder.lineageStart(lineageStartDate, lineageStartIndex);

                if (version > 5) {
                    final long lastQueueDate = in.readLong();
                    final long queueDateIndex;
                    if (version > 7) {
                        queueDateIndex = in.readLong();
                    } else {
                        queueDateIndex = 0L;
                    }

                    ffBuilder.lastQueued(lastQueueDate, queueDateIndex);
                }
            }

            final long size = in.readLong();
            final String connectionId = readString(in);

            logger.debug("{} -> {}", new Object[]{recordId, connectionId});

            ffBuilder.id(recordId);
            ffBuilder.entryDate(entryDate);
            ffBuilder.size(size);

            deserializeClaim(in, version, ffBuilder);

            final int attributesChanged = in.read();
            if (attributesChanged == 1) {
                final int numAttributes = in.readInt();
                final Map<String, String> attributes = new HashMap<>();
                for (int i = 0; i < numAttributes; i++) {
                    final String key = readString(in);
                    final String value = readString(in);
                    attributes.put(key, value);
                }

                ffBuilder.addAttributes(attributes);
            } else if (attributesChanged == -1) {
                throw new EOFException();
            } else if (attributesChanged != 0) {
                throw new IOException("Attribute Change Qualifier not found in stream; found value: "
                        + attributesChanged + " after successfully restoring " + recordsRestored + " records");
            }

            final FlowFileRecord flowFile = ffBuilder.build();
            String swapLocation = null;
            if (action == ACTION_SWAPPED_IN) {
                swapLocation = in.readUTF();
            }

            final StandardRepositoryRecord record;

            if (flowFileQueueMap == null) {
                record = new StandardRepositoryRecord(null, flowFile);
            } else {
                final FlowFileQueue queue = flowFileQueueMap.get(connectionId);
                record = new StandardRepositoryRecord(queue, flowFile);
                if (swapLocation != null) {
                    record.setSwapLocation(swapLocation);
                }

                if (connectionId.isEmpty()) {
                    logger.warn("{} does not have a FlowFile Queue associated with it; this record will be discarded", flowFile);
                    record.markForAbort();
                } else if (queue == null) {
                    logger.warn("{} maps to unknown FlowFile Queue {}; this record will be discarded", flowFile, connectionId);
                    record.markForAbort();
                }
            }

            recordsRestored++;
            return record;
        }

        @Override
        public void serializeRecord(final RepositoryRecord record, final DataOutputStream out) throws IOException {
            serializeEdit(null, record, out, true);
        }

        private void serializeContentClaim(final ContentClaim claim, final long offset, final DataOutputStream out) throws IOException {
            if (claim == null) {
                out.write(0);
            } else {
                out.write(1);

                final ResourceClaim resourceClaim = claim.getResourceClaim();
                writeString(resourceClaim.getId(), out);
                writeString(resourceClaim.getContainer(), out);
                writeString(resourceClaim.getSection(), out);
                out.writeLong(claim.getOffset());
                out.writeLong(claim.getLength());

                out.writeLong(offset);
                out.writeBoolean(resourceClaim.isLossTolerant());
            }
        }

        private void deserializeClaim(final DataInputStream in, final int serializationVersion, final StandardFlowFileRecord.Builder ffBuilder) throws IOException {
            // determine current Content Claim.
            final int claimExists = in.read();
            if (claimExists == 1) {
                final String claimId;
                if (serializationVersion < 4) {
                    claimId = String.valueOf(in.readLong());
                } else {
                    claimId = readString(in);
                }

                final String container = readString(in);
                final String section = readString(in);

                final long resourceOffset;
                final long resourceLength;
                if (serializationVersion < 7) {
                    resourceOffset = 0L;
                    resourceLength = -1L;
                } else {
                    resourceOffset = in.readLong();
                    resourceLength = in.readLong();
                }

                final long claimOffset = in.readLong();

                final boolean lossTolerant;
                if (serializationVersion >= 3) {
                    lossTolerant = in.readBoolean();
                } else {
                    lossTolerant = false;
                }

                final ResourceClaim resourceClaim = claimManager.newResourceClaim(container, section, claimId, lossTolerant);
                final StandardContentClaim contentClaim = new StandardContentClaim(resourceClaim, resourceOffset);
                contentClaim.setLength(resourceLength);

                ffBuilder.contentClaim(contentClaim);
                ffBuilder.contentClaimOffset(claimOffset);
            } else if (claimExists == -1) {
                throw new EOFException();
            } else if (claimExists != 0) {
                throw new IOException("Claim Existence Qualifier not found in stream; found value: "
                        + claimExists + " after successfully restoring " + recordsRestored + " records");
            }
        }

        private void writeString(final String toWrite, final OutputStream out) throws IOException {
            final byte[] bytes = toWrite.getBytes("UTF-8");
            final int utflen = bytes.length;

            if (utflen < 65535) {
                out.write(utflen >>> 8);
                out.write(utflen);
                out.write(bytes);
            } else {
                out.write(255);
                out.write(255);
                out.write(utflen >>> 24);
                out.write(utflen >>> 16);
                out.write(utflen >>> 8);
                out.write(utflen);
                out.write(bytes);
            }
        }

        private String readString(final InputStream in) throws IOException {
            final Integer numBytes = readFieldLength(in);
            if (numBytes == null) {
                throw new EOFException();
            }
            final byte[] bytes = new byte[numBytes];
            fillBuffer(in, bytes, numBytes);
            return new String(bytes, "UTF-8");
        }

        private Integer readFieldLength(final InputStream in) throws IOException {
            final int firstValue = in.read();
            final int secondValue = in.read();
            if (firstValue < 0) {
                return null;
            }
            if (secondValue < 0) {
                throw new EOFException();
            }
            if (firstValue == 0xff && secondValue == 0xff) {
                final int ch1 = in.read();
                final int ch2 = in.read();
                final int ch3 = in.read();
                final int ch4 = in.read();
                if ((ch1 | ch2 | ch3 | ch4) < 0) {
                    throw new EOFException();
                }
                return (ch1 << 24) + (ch2 << 16) + (ch3 << 8) + ch4;
            } else {
                return (firstValue << 8) + secondValue;
            }
        }

        private void fillBuffer(final InputStream in, final byte[] buffer, final int length) throws IOException {
            int bytesRead;
            int totalBytesRead = 0;
            while ((bytesRead = in.read(buffer, totalBytesRead, length - totalBytesRead)) > 0) {
                totalBytesRead += bytesRead;
            }
            if (totalBytesRead != length) {
                throw new EOFException();
            }
        }

        @Override
        public Long getRecordIdentifier(final RepositoryRecord record) {
            return record.getCurrent().getId();
        }

        @Override
        public UpdateType getUpdateType(final RepositoryRecord record) {
            switch (record.getType()) {
                case CONTENTMISSING:
                case DELETE:
                    return UpdateType.DELETE;
                case CREATE:
                    return UpdateType.CREATE;
                case UPDATE:
                    return UpdateType.UPDATE;
                case SWAP_OUT:
                    return UpdateType.SWAP_OUT;
                case SWAP_IN:
                    return UpdateType.SWAP_IN;
            }
            return null;
        }

        @Override
        public int getVersion() {
            return CURRENT_ENCODING_VERSION;
        }

        @Override
        public String getLocation(final RepositoryRecord record) {
            return record.getSwapLocation();
        }
    }
}
