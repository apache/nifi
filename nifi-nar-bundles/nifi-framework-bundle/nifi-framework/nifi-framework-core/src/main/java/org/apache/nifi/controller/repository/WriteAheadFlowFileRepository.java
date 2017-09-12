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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wali.MinimalLockingWriteAheadLog;
import org.wali.SyncListener;
import org.wali.WriteAheadRepository;

/**
 * <p>
 * Implements FlowFile Repository using WALI as the backing store.
 * </p>
 *
 * <p>
 * We expose a property named <code>nifi.flowfile.repository.always.sync</code>
 * that is a boolean value indicating whether or not to force WALI to sync with
 * disk on each update. By default, the value is <code>false</code>. This is
 * needed only in situations in which power loss is expected and not mitigated
 * by Uninterruptable Power Sources (UPS) or when running in an unstable Virtual
 * Machine for instance. Otherwise, we will flush the data that is written to
 * the Operating System and the Operating System will be responsible to flush
 * its buffers when appropriate. The Operating System can be configured to hold
 * only a certain buffer size or not to buffer at all, as well. When using a
 * UPS, this is generally not an issue, as the machine is typically notified
 * before dying, in which case the Operating System will flush the data to disk.
 * Additionally, most disks on enterprise servers also have battery backups that
 * can power the disks long enough to flush their buffers. For this reason, we
 * choose instead to not sync to disk for every write but instead sync only when
 * we checkpoint.
 * </p>
 */
public class WriteAheadFlowFileRepository implements FlowFileRepository, SyncListener {
    private static final String FLOWFILE_REPOSITORY_DIRECTORY_PREFIX = "nifi.flowfile.repository.directory";

    private final AtomicLong flowFileSequenceGenerator = new AtomicLong(0L);
    private final boolean alwaysSync;

    private static final Logger logger = LoggerFactory.getLogger(WriteAheadFlowFileRepository.class);
    private volatile ScheduledFuture<?> checkpointFuture;

    private final long checkpointDelayMillis;
    private final SortedSet<Path> flowFileRepositoryPaths = new TreeSet<>();
    private final int numPartitions;
    private final ScheduledExecutorService checkpointExecutor;

    // effectively final
    private WriteAheadRepository<RepositoryRecord> wal;
    private RepositoryRecordSerdeFactory serdeFactory;
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

    /**
     * default no args constructor for service loading only.
     */
    public WriteAheadFlowFileRepository() {
        alwaysSync = false;
        checkpointDelayMillis = 0l;
        numPartitions = 0;
        checkpointExecutor = null;
    }

    public WriteAheadFlowFileRepository(final NiFiProperties nifiProperties) {
        alwaysSync = Boolean.parseBoolean(nifiProperties.getProperty(NiFiProperties.FLOWFILE_REPOSITORY_ALWAYS_SYNC, "false"));

        // determine the database file path and ensure it exists
        for (final String propertyName : nifiProperties.getPropertyKeys()) {
            if (propertyName.startsWith(FLOWFILE_REPOSITORY_DIRECTORY_PREFIX)) {
                final String directoryName = nifiProperties.getProperty(propertyName);
                flowFileRepositoryPaths.add(Paths.get(directoryName));
            }
        }

        numPartitions = nifiProperties.getFlowFileRepositoryPartitions();
        checkpointDelayMillis = FormatUtils.getTimeDuration(nifiProperties.getFlowFileRepositoryCheckpointInterval(), TimeUnit.MILLISECONDS);

        checkpointExecutor = Executors.newSingleThreadScheduledExecutor();
    }

    @Override
    public void initialize(final ResourceClaimManager claimManager) throws IOException {
        this.claimManager = claimManager;

        for (final Path path : flowFileRepositoryPaths) {
            Files.createDirectories(path);
        }

        // TODO: Should ensure that only 1 instance running and pointing at a particular path
        // TODO: Allow for backup path that can be used if disk out of space?? Would allow a snapshot to be stored on
        // backup and then the data deleted from the normal location; then can move backup to normal location and
        // delete backup. On restore, if no files exist in partition's directory, would have to check backup directory
        serdeFactory = new RepositoryRecordSerdeFactory(claimManager);
        wal = new MinimalLockingWriteAheadLog<>(flowFileRepositoryPaths, numPartitions, serdeFactory, this);
        logger.info("Initialized FlowFile Repository using {} partitions", numPartitions);
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
        long capacity = 0L;
        for (final Path path : flowFileRepositoryPaths) {
            capacity += Files.getFileStore(path).getTotalSpace();
        }

        return capacity;
    }

    @Override
    public long getUsableStorageSpace() throws IOException {
        long usableSpace = 0L;
        for (final Path path : flowFileRepositoryPaths) {
            usableSpace += Files.getFileStore(path).getUsableSpace();
        }

        return usableSpace;
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
            if (record.getType() != RepositoryRecordType.DELETE && record.getType() != RepositoryRecordType.CONTENTMISSING
                && record.getType() != RepositoryRecordType.CLEANUP_TRANSIENT_CLAIMS && record.getDestination() == null) {
                throw new IllegalArgumentException("Record " + record + " has no destination and Type is " + record.getType());
            }
        }

        // Partition records by whether or not their type is 'CLEANUP_TRANSIENT_CLAIMS'. We do this because we don't want to send
        // these types of records to the Write-Ahead Log.
        final Map<Boolean, List<RepositoryRecord>> partitionedRecords = records.stream()
            .collect(Collectors.partitioningBy(record -> record.getType() == RepositoryRecordType.CLEANUP_TRANSIENT_CLAIMS));

        List<RepositoryRecord> recordsForWal = partitionedRecords.get(Boolean.FALSE);
        if (recordsForWal == null) {
            recordsForWal = Collections.emptyList();
        }

        // update the repository.
        final int partitionIndex = wal.update(recordsForWal, sync);

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
     * Swaps the FlowFiles that live on the given Connection out to disk, using
     * the specified Swap File and returns the number of FlowFiles that were
     * persisted.
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

        // TODO: We should probably update this to support bulk 'SWAP OUT' records. As-is, we have to write out a
        // 'SWAP OUT' record for each FlowFile, which includes the Update Type, FlowFile ID, swap file location, and Queue ID.
        // We could instead have a single record with Update Type of 'SWAP OUT' and just include swap file location, Queue ID,
        // and all FlowFile ID's.
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
        serdeFactory.setQueueMap(queueMap);
        final Collection<RepositoryRecord> recordList = wal.recoverRecords();
        serdeFactory.setQueueMap(null);

        for (final RepositoryRecord record : recordList) {
            final ContentClaim claim = record.getCurrentClaim();
            if (claim != null) {
                claimManager.incrementClaimantCount(claim.getResourceClaim());
            }
        }

        // Determine the next sequence number for FlowFiles
        int numFlowFilesMissingQueue = 0;
        long maxId = minimumSequenceNumber;
        for (final RepositoryRecord record : recordList) {
            final long recordId = serdeFactory.getRecordIdentifier(record);
            if (recordId > maxId) {
                maxId = recordId;
            }

            final FlowFileRecord flowFile = record.getCurrent();
            final FlowFileQueue queue = record.getOriginalQueue();
            if (queue == null) {
                numFlowFilesMissingQueue++;
            } else {
                queue.put(flowFile);
            }
        }

        // Set the AtomicLong to 1 more than the max ID so that calls to #getNextFlowFileSequence() will
        // return the appropriate number.
        flowFileSequenceGenerator.set(maxId + 1);
        logger.info("Successfully restored {} FlowFiles", recordList.size() - numFlowFilesMissingQueue);
        if (numFlowFilesMissingQueue > 0) {
            logger.warn("On recovery, found {} FlowFiles whose queue no longer exists. These FlowFiles will be dropped.", numFlowFilesMissingQueue);
        }

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
                } catch (final Throwable t) {
                    logger.error("Unable to checkpoint FlowFile Repository due to " + t.toString(), t);
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
}
