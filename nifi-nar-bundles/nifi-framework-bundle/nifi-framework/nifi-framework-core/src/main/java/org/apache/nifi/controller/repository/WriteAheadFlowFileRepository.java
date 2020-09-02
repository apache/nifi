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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.repository.schema.FieldCache;
import org.apache.nifi.security.kms.EncryptionException;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.wali.EncryptedSequentialAccessWriteAheadLog;
import org.apache.nifi.wali.SequentialAccessWriteAheadLog;
import org.apache.nifi.wali.SnapshotCapture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wali.MinimalLockingWriteAheadLog;
import org.wali.SyncListener;
import org.wali.WriteAheadRepository;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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
    static final String FLOWFILE_REPOSITORY_DIRECTORY_PREFIX = "nifi.flowfile.repository.directory";
    private static final String WRITE_AHEAD_LOG_IMPL = "nifi.flowfile.repository.wal.implementation";
    private static final String RETAIN_ORPHANED_FLOWFILES = "nifi.flowfile.repository.retain.orphaned.flowfiles";
    private static final String FLOWFILE_REPO_CACHE_SIZE = "nifi.flowfile.repository.wal.cache.characters";

    static final String SEQUENTIAL_ACCESS_WAL = "org.apache.nifi.wali.SequentialAccessWriteAheadLog";
    static final String ENCRYPTED_SEQUENTIAL_ACCESS_WAL = "org.apache.nifi.wali.EncryptedSequentialAccessWriteAheadLog";
    private static final String MINIMAL_LOCKING_WALI = "org.wali.MinimalLockingWriteAheadLog";
    private static final String DEFAULT_WAL_IMPLEMENTATION = SEQUENTIAL_ACCESS_WAL;
    private static final int DEFAULT_CACHE_SIZE = 10_000_000;

    private final String walImplementation;
    protected final NiFiProperties nifiProperties;

    private final AtomicLong flowFileSequenceGenerator = new AtomicLong(0L);
    private final boolean alwaysSync;
    private final boolean retainOrphanedFlowFiles;

    private static final Logger logger = LoggerFactory.getLogger(WriteAheadFlowFileRepository.class);
    volatile ScheduledFuture<?> checkpointFuture;

    private final long checkpointDelayMillis;
    private final List<File> flowFileRepositoryPaths = new ArrayList<>();
    private final List<File> recoveryFiles = new ArrayList<>();
    private final ScheduledExecutorService checkpointExecutor;
    private final int maxCharactersToCache;

    private volatile Collection<SerializedRepositoryRecord> recoveredRecords = null;
    private final Set<ResourceClaim> orphanedResourceClaims = Collections.synchronizedSet(new HashSet<>());

    private final Set<String> swapLocationSuffixes = new HashSet<>(); // guarded by synchronizing on object itself

    // effectively final
    private WriteAheadRepository<SerializedRepositoryRecord> wal;
    private RepositoryRecordSerdeFactory serdeFactory;
    private ResourceClaimManager claimManager;
    private FieldCache fieldCache;

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
        checkpointDelayMillis = 0L;
        checkpointExecutor = null;
        walImplementation = null;
        nifiProperties = null;
        retainOrphanedFlowFiles = true;
        maxCharactersToCache = 0;
    }

    public WriteAheadFlowFileRepository(final NiFiProperties nifiProperties) {
        alwaysSync = Boolean.parseBoolean(nifiProperties.getProperty(NiFiProperties.FLOWFILE_REPOSITORY_ALWAYS_SYNC, "false"));
        this.nifiProperties = nifiProperties;

        final String orphanedFlowFileProperty = nifiProperties.getProperty(RETAIN_ORPHANED_FLOWFILES);
        retainOrphanedFlowFiles = orphanedFlowFileProperty == null || Boolean.parseBoolean(orphanedFlowFileProperty);

        // determine the database file path and ensure it exists
        String writeAheadLogImpl = nifiProperties.getProperty(WRITE_AHEAD_LOG_IMPL);
        if (writeAheadLogImpl == null) {
            writeAheadLogImpl = DEFAULT_WAL_IMPLEMENTATION;
        }
        this.walImplementation = writeAheadLogImpl;
        this.maxCharactersToCache = nifiProperties.getIntegerProperty(FLOWFILE_REPO_CACHE_SIZE, DEFAULT_CACHE_SIZE);

        // We used to use one implementation (minimal locking) of the write-ahead log, but we now want to use the other
        // (sequential access), we must address this. Since the MinimalLockingWriteAheadLog supports multiple partitions,
        // we need to ensure that we recover records from all partitions, so we build up a List of Files for the
        // recovery files.
        for (final String propertyName : nifiProperties.getPropertyKeys()) {
            if (propertyName.startsWith(FLOWFILE_REPOSITORY_DIRECTORY_PREFIX)) {
                final String dirName = nifiProperties.getProperty(propertyName);
                recoveryFiles.add(new File(dirName));
            }
        }

        if (isSequentialAccessWAL(walImplementation)) {
            final String directoryName = nifiProperties.getProperty(FLOWFILE_REPOSITORY_DIRECTORY_PREFIX);
            flowFileRepositoryPaths.add(new File(directoryName));
        } else {
            flowFileRepositoryPaths.addAll(recoveryFiles);
        }


        checkpointDelayMillis = FormatUtils.getTimeDuration(nifiProperties.getFlowFileRepositoryCheckpointInterval(), TimeUnit.MILLISECONDS);

        checkpointExecutor = Executors.newSingleThreadScheduledExecutor();
    }

    /**
     * Returns true if the provided implementation is a sequential access write ahead log (plaintext or encrypted).
     *
     * @param walImplementation the implementation to check
     * @return true if this implementation is sequential access
     */
    private static boolean isSequentialAccessWAL(String walImplementation) {
        return walImplementation.equals(SEQUENTIAL_ACCESS_WAL) || walImplementation.equals(ENCRYPTED_SEQUENTIAL_ACCESS_WAL);
    }

    @Override
    public void initialize(final ResourceClaimManager claimManager) throws IOException {
        final FieldCache fieldCache = new CaffeineFieldCache(maxCharactersToCache);
        initialize(claimManager, createSerdeFactory(claimManager, fieldCache), fieldCache);
    }

    protected RepositoryRecordSerdeFactory createSerdeFactory(final ResourceClaimManager claimManager, final FieldCache fieldCache) {
        if (EncryptedSequentialAccessWriteAheadLog.class.getName().equals(nifiProperties.getProperty(NiFiProperties.FLOWFILE_REPOSITORY_WAL_IMPLEMENTATION))) {
            try {
                return new EncryptedRepositoryRecordSerdeFactory(claimManager, nifiProperties, fieldCache);
            } catch (final EncryptionException e) {
                throw new RuntimeException(e);
            }
        } else {
            return new StandardRepositoryRecordSerdeFactory(claimManager, fieldCache);
        }
    }

    public void initialize(final ResourceClaimManager claimManager, final RepositoryRecordSerdeFactory serdeFactory, final FieldCache fieldCache) throws IOException {
        this.claimManager = claimManager;
        this.fieldCache = fieldCache;

        for (final File file : flowFileRepositoryPaths) {
            Files.createDirectories(file.toPath());
        }

        // TODO: Should ensure that only 1 instance running and pointing at a particular path
        // TODO: Allow for backup path that can be used if disk out of space?? Would allow a snapshot to be stored on
        // backup and then the data deleted from the normal location; then can move backup to normal location and
        // delete backup. On restore, if no files exist in partition's directory, would have to check backup directory
        this.serdeFactory = serdeFactory;

        // The specified implementation can be plaintext or encrypted; the only difference is the serde factory
        if (isSequentialAccessWAL(walImplementation)) {
            // TODO: May need to instantiate ESAWAL for clarity?
            wal = new SequentialAccessWriteAheadLog<>(flowFileRepositoryPaths.get(0), serdeFactory, this);
        } else if (walImplementation.equals(MINIMAL_LOCKING_WALI)) {
            final SortedSet<Path> paths = flowFileRepositoryPaths.stream()
                    .map(File::toPath)
                    .collect(Collectors.toCollection(TreeSet::new));

            wal = new MinimalLockingWriteAheadLog<>(paths, 1, serdeFactory, this);
        } else {
            throw new IllegalStateException("Cannot create Write-Ahead Log because the configured property '" + WRITE_AHEAD_LOG_IMPL + "' has an invalid value of '" + walImplementation
                    + "'. Please update nifi.properties to indicate a valid value for this property.");
        }

        logger.info("Initialized FlowFile Repository");
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
    public Map<ResourceClaim, Set<ResourceClaimReference>> findResourceClaimReferences(final Set<ResourceClaim> resourceClaims, final FlowFileSwapManager swapManager) {
        if (!(isSequentialAccessWAL(walImplementation))) {
            return null;
        }

        final Map<ResourceClaim, Set<ResourceClaimReference>> references = new HashMap<>();

        final SnapshotCapture<SerializedRepositoryRecord> snapshot = ((SequentialAccessWriteAheadLog<SerializedRepositoryRecord>) wal).captureSnapshot();
        for (final SerializedRepositoryRecord repositoryRecord : snapshot.getRecords().values()) {
            final ContentClaim contentClaim = repositoryRecord.getContentClaim();
            if (contentClaim == null) {
                continue;
            }

            final ResourceClaim resourceClaim = contentClaim.getResourceClaim();
            if (resourceClaims.contains(resourceClaim)) {
                final Set<ResourceClaimReference> claimReferences = references.computeIfAbsent(resourceClaim, key -> new HashSet<>());
                claimReferences.add(createResourceClaimReference(repositoryRecord));
            }
        }


        for (final String swapLocation : snapshot.getSwapLocations()) {
            final String queueIdentifier = swapManager.getQueueIdentifier(swapLocation);
            final ResourceClaimReference swapReference = createResourceClaimReference(swapLocation, queueIdentifier);

            try {
                final SwapSummary swapSummary = swapManager.getSwapSummary(swapLocation);

                for (final ResourceClaim resourceClaim : swapSummary.getResourceClaims()) {
                    if (resourceClaims.contains(resourceClaim)) {
                        final Set<ResourceClaimReference> claimReferences = references.computeIfAbsent(resourceClaim, key -> new HashSet<>());
                        claimReferences.add(swapReference);
                    }
                }
            } catch (final Exception e) {
                logger.warn("Failed to read swap file " + swapLocation + " when attempting to find resource claim references", e);
            }
        }

        return references;
    }

    private ResourceClaimReference createResourceClaimReference(final String swapLocation, final String queueIdentifier) {
        return new ResourceClaimReference() {
            @Override
            public String getQueueIdentifier() {
                return queueIdentifier;
            }

            @Override
            public boolean isSwappedOut() {
                return true;
            }

            @Override
            public String getFlowFileUuid() {
                return null;
            }

            @Override
            public String getSwapLocation() {
                return swapLocation;
            }

            @Override
            public String toString() {
                return "Swap File[location=" + getSwapLocation() + ", queue=" + getQueueIdentifier() + "]";
            }

            @Override
            public int hashCode() {
                return Objects.hash(queueIdentifier, swapLocation);
            }

            @Override
            public boolean equals(final Object obj) {
                if (obj == null) {
                    return false;
                }
                if (obj == this) {
                    return true;
                }
                if (obj.getClass() != getClass()) {
                    return false;
                }

                final ResourceClaimReference other = (ResourceClaimReference) obj;
                return Objects.equals(queueIdentifier, other.getQueueIdentifier()) && Objects.equals(swapLocation, other.getSwapLocation());
            }
        };
    }

    private ResourceClaimReference createResourceClaimReference(final SerializedRepositoryRecord repositoryRecord) {
        final String queueIdentifier = repositoryRecord.getQueueIdentifier();
        final String flowFileUuid = repositoryRecord.getFlowFileRecord().getAttribute(CoreAttributes.UUID.key());

        return new ResourceClaimReference() {
            @Override
            public String getQueueIdentifier() {
                return queueIdentifier;
            }

            @Override
            public boolean isSwappedOut() {
                return false;
            }

            @Override
            public String getFlowFileUuid() {
                return flowFileUuid;
            }

            @Override
            public String getSwapLocation() {
                return null;
            }

            @Override
            public String toString() {
                return "FlowFile[uuid=" + getFlowFileUuid() + ", queue=" + getQueueIdentifier() + "]";
            }

            @Override
            public int hashCode() {
                return Objects.hash(queueIdentifier, flowFileUuid);
            }

            @Override
            public boolean equals(final Object obj) {
                if (obj == null) {
                    return false;
                }
                if (obj == this) {
                    return true;
                }
                if (obj.getClass() != getClass()) {
                    return false;
                }

                final ResourceClaimReference other = (ResourceClaimReference) obj;
                return Objects.equals(queueIdentifier, other.getQueueIdentifier()) && Objects.equals(flowFileUuid, other.getFlowFileUuid());
            }
        };
    }

    @Override
    public long getStorageCapacity() throws IOException {
        long capacity = 0L;
        for (final File file : flowFileRepositoryPaths) {
            capacity += Files.getFileStore(file.toPath()).getTotalSpace();
        }

        return capacity;
    }

    @Override
    public long getUsableStorageSpace() throws IOException {
        long usableSpace = 0L;
        for (final File file : flowFileRepositoryPaths) {
            usableSpace += Files.getFileStore(file.toPath()).getUsableSpace();
        }

        return usableSpace;
    }

    @Override
    public String getFileStoreName() {
        final Path path = flowFileRepositoryPaths.iterator().next().toPath();

        try {
            return Files.getFileStore(path).name();
        } catch (IOException e) {
            return null;
        }
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

    @Override
    public boolean isValidSwapLocationSuffix(final String swapLocationSuffix) {
        synchronized (swapLocationSuffixes) {
            return swapLocationSuffixes.contains(normalizeSwapLocation(swapLocationSuffix));
        }
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

        final List<SerializedRepositoryRecord> serializedRecords = new ArrayList<>(recordsForWal.size());
        recordsForWal.forEach(record -> serializedRecords.add(new LiveSerializedRepositoryRecord(record)));

        // update the repository.
        final int partitionIndex = wal.update(serializedRecords, sync);
        updateContentClaims(records, partitionIndex);
    }

    protected void updateContentClaims(Collection<RepositoryRecord> repositoryRecords, final int partitionIndex) {
        // The below code is not entirely thread-safe, but we are OK with that because the results aren't really harmful.
        // Specifically, if two different threads call updateRepository with DELETE records for the same Content Claim,
        // it's quite possible for claimant count to be 0 below, which results in two different threads adding the Content
        // Claim to the 'claimsAwaitDestruction' map. As a result, we can call #markDestructable with the same ContentClaim
        // multiple times, and the #markDestructable method is not necessarily idempotent.
        // However, the result of this is that the FileSystem Repository may end up trying to remove the content multiple times.
        // This does not, however, cause problems, as ContentRepository should handle this
        // This does indicate that some refactoring should probably be performed, though, as this is not a very clean interface.
        final Set<ResourceClaim> claimsToAdd = new HashSet<>();

        final Set<String> swapLocationsAdded = new HashSet<>();
        final Set<String> swapLocationsRemoved = new HashSet<>();

        for (final RepositoryRecord record : repositoryRecords) {
            updateClaimCounts(record);

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
            } else if (record.getType() == RepositoryRecordType.SWAP_OUT) {
                final String swapLocation = record.getSwapLocation();
                swapLocationsAdded.add(swapLocation);
                swapLocationsRemoved.remove(swapLocation);
            } else if (record.getType() == RepositoryRecordType.SWAP_IN) {
                final String swapLocation = record.getSwapLocation();
                swapLocationsRemoved.add(swapLocation);
                swapLocationsAdded.remove(swapLocation);
            }
        }

        // Once the content claim counts have been updated for all records, collect any transient claims that are eligible for destruction
        for (final RepositoryRecord record : repositoryRecords) {
            final List<ContentClaim> transientClaims = record.getTransientClaims();
            if (transientClaims != null) {
                for (final ContentClaim transientClaim : transientClaims) {
                    if (isDestructable(transientClaim)) {
                        claimsToAdd.add(transientClaim.getResourceClaim());
                    }
                }
            }
        }

        // If we have swapped files in or out, we need to ensure that we update our swapLocationSuffixes.
        if (!swapLocationsAdded.isEmpty() || !swapLocationsRemoved.isEmpty()) {
            synchronized (swapLocationSuffixes) {
                swapLocationsRemoved.forEach(loc -> swapLocationSuffixes.remove(normalizeSwapLocation(loc)));
                swapLocationsAdded.forEach(loc -> swapLocationSuffixes.add(normalizeSwapLocation(loc)));
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

    private void updateClaimCounts(final RepositoryRecord record) {
        final ContentClaim currentClaim = record.getCurrentClaim();
        final ContentClaim originalClaim = record.getOriginalClaim();

        if (record.getType() == RepositoryRecordType.DELETE || record.getType() == RepositoryRecordType.CONTENTMISSING) {
            decrementClaimCount(currentClaim);
        }

        if (record.isContentModified()) {
            // records which have been updated - remove original if exists
            decrementClaimCount(originalClaim);
        }
    }

    private void decrementClaimCount(final ContentClaim claim) {
        if (claim == null) {
            return;
        }

        claimManager.decrementClaimantCount(claim.getResourceClaim());
    }


    protected static String normalizeSwapLocation(final String swapLocation) {
        if (swapLocation == null) {
            return null;
        }

        final String normalizedPath = swapLocation.replace("\\", "/");
        final String withoutTrailing = (normalizedPath.endsWith("/") && normalizedPath.length() > 1) ? normalizedPath.substring(0, normalizedPath.length() - 1) : normalizedPath;
        final String pathRemoved = getLocationSuffix(withoutTrailing);

        final String normalized = StringUtils.substringBefore(pathRemoved, ".");
        return normalized;
    }

    private static String getLocationSuffix(final String swapLocation) {
        final int lastIndex = swapLocation.lastIndexOf("/");
        if (lastIndex < 0 || lastIndex >= swapLocation.length() - 1) {
            return swapLocation;
        }

        return swapLocation.substring(lastIndex + 1);
    }

    @Override
    public void onSync(final int partitionIndex) {
        final BlockingQueue<ResourceClaim> claimQueue = claimsAwaitingDestruction.get(partitionIndex);
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
     * @param queue        queue to swap out
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
        final List<SerializedRepositoryRecord> serializedRepositoryRecords = new ArrayList<>(repoRecords.size());
        repoRecords.forEach(record -> serializedRepositoryRecords.add(new LiveSerializedRepositoryRecord(record)));

        wal.update(serializedRepositoryRecords, true);

        synchronized (this.swapLocationSuffixes) {
            this.swapLocationSuffixes.add(normalizeSwapLocation(swapLocation));
        }

        logger.info("Successfully swapped out {} FlowFiles from {} to Swap File {}", swappedOut.size(), queue, swapLocation);
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

        synchronized (this.swapLocationSuffixes) {
            this.swapLocationSuffixes.remove(normalizeSwapLocation(swapLocation));
        }

        logger.info("Repository updated to reflect that {} FlowFiles were swapped in to {}", new Object[]{swapRecords.size(), queue});
    }

    void deleteRecursively(final File dir) {
        final File[] children = dir.listFiles();

        if (children != null) {
            for (final File child : children) {
                final boolean deleted = child.delete();
                if (!deleted) {
                    logger.warn("Failed to delete old file {}; this file should be cleaned up manually", child);
                }
            }
        }

        if (!dir.delete()) {
            logger.warn("Failed to delete old directory {}; this directory should be cleaned up manually", dir);
        }
    }

    private Optional<Collection<SerializedRepositoryRecord>> migrateFromSequentialAccessLog(final WriteAheadRepository<SerializedRepositoryRecord> toUpdate) throws IOException {
        final String recoveryDirName = nifiProperties.getProperty(FLOWFILE_REPOSITORY_DIRECTORY_PREFIX);
        final File recoveryDir = new File(recoveryDirName);
        if (!recoveryDir.exists()) {
            return Optional.empty();
        }

        final WriteAheadRepository<SerializedRepositoryRecord> recoveryWal = new SequentialAccessWriteAheadLog<>(recoveryDir, serdeFactory, this);
        logger.info("Encountered FlowFile Repository that was written using the Sequential Access Write Ahead Log. Will recover from this version.");

        final Collection<SerializedRepositoryRecord> recordList;
        try {
            recordList = recoveryWal.recoverRecords();
        } finally {
            recoveryWal.shutdown();
        }

        toUpdate.update(recordList, true);

        logger.info("Successfully recovered files from existing Write-Ahead Log and transitioned to new Write-Ahead Log. Will not delete old files.");

        final File journalsDir = new File(recoveryDir, "journals");
        deleteRecursively(journalsDir);

        final File checkpointFile = new File(recoveryDir, "checkpoint");
        if (!checkpointFile.delete() && checkpointFile.exists()) {
            logger.warn("Failed to delete old file {}; this file should be cleaned up manually", checkpointFile);
        }

        final File partialFile = new File(recoveryDir, "checkpoint.partial");
        if (!partialFile.delete() && partialFile.exists()) {
            logger.warn("Failed to delete old file {}; this file should be cleaned up manually", partialFile);
        }

        return Optional.of(recordList);
    }

    @SuppressWarnings("deprecation")
    private Optional<Collection<SerializedRepositoryRecord>> migrateFromMinimalLockingLog(final WriteAheadRepository<SerializedRepositoryRecord> toUpdate) throws IOException {
        final List<File> partitionDirs = new ArrayList<>();
        for (final File recoveryFile : recoveryFiles) {
            final File[] partitions = recoveryFile.listFiles(file -> file.getName().startsWith("partition-"));
            for (final File partition : partitions) {
                partitionDirs.add(partition);
            }
        }

        if (partitionDirs == null || partitionDirs.isEmpty()) {
            return Optional.empty();
        }

        logger.info("Encountered FlowFile Repository that was written using the 'Minimal Locking Write-Ahead Log'. "
                + "Will recover from this version and re-write the repository using the new version of the Write-Ahead Log.");

        final SortedSet<Path> paths = recoveryFiles.stream()
                .map(File::toPath)
                .collect(Collectors.toCollection(TreeSet::new));

        final Collection<SerializedRepositoryRecord> recordList;
        final MinimalLockingWriteAheadLog<SerializedRepositoryRecord> minimalLockingWal = new MinimalLockingWriteAheadLog<>(paths, partitionDirs.size(), serdeFactory, null);
        try {
            recordList = minimalLockingWal.recoverRecords();
        } finally {
            minimalLockingWal.shutdown();
        }

        toUpdate.update(recordList, true);

        // Delete the old repository
        logger.info("Successfully recovered files from existing Write-Ahead Log and transitioned to new implementation. Will now delete old files.");
        for (final File partitionDir : partitionDirs) {
            deleteRecursively(partitionDir);
        }

        for (final File recoveryFile : recoveryFiles) {
            final File snapshotFile = new File(recoveryFile, "snapshot");
            if (!snapshotFile.delete() && snapshotFile.exists()) {
                logger.warn("Failed to delete old file {}; this file should be cleaned up manually", snapshotFile);
            }

            final File partialFile = new File(recoveryFile, "snapshot.partial");
            if (!partialFile.delete() && partialFile.exists()) {
                logger.warn("Failed to delete old file {}; this file should be cleaned up manually", partialFile);
            }
        }

        return Optional.of(recordList);
    }

    @Override
    public Set<String> findQueuesWithFlowFiles(final FlowFileSwapManager swapManager) throws IOException {
        recoveredRecords = wal.recoverRecords();

        final Set<String> queueIds = new HashSet<>();
        for (final SerializedRepositoryRecord record : recoveredRecords) {
            final RepositoryRecordType recordType = record.getType();

            if (recordType != RepositoryRecordType.CREATE && recordType != RepositoryRecordType.UPDATE) {
                continue;
            }

            final String queueId = record.getQueueIdentifier();
            if (queueId != null) {
                queueIds.add(queueId);
            }
        }

        final Set<String> recoveredSwapLocations = wal.getRecoveredSwapLocations();
        for (final String swapLocation : recoveredSwapLocations) {
            final String queueId = swapManager.getQueueIdentifier(swapLocation);
            queueIds.add(queueId);
        }

        return queueIds;
    }

    @Override
    public long loadFlowFiles(final QueueProvider queueProvider) throws IOException {
        // If we have already loaded the records from the write-ahead logs, use them. Otherwise, recover the records now.
        // We do this because a call to #findQueuesWithFlowFiles will recover the records, and we don't want to have to re-read
        // the entire repository, so that method will stash the records away.
        Collection<SerializedRepositoryRecord> recordList;
        if (recoveredRecords == null) {
            // Since we used to use the MinimalLockingWriteAheadRepository, we need to ensure that if the FlowFile
            // Repo was written using that impl, that we properly recover from the implementation.
            recordList = wal.recoverRecords();
        } else {
            recordList = recoveredRecords;
        }

        final Set<String> recoveredSwapLocations = wal.getRecoveredSwapLocations();
        synchronized (this.swapLocationSuffixes) {
            recoveredSwapLocations.forEach(loc -> this.swapLocationSuffixes.add(normalizeSwapLocation(loc)));
            logger.debug("Recovered {} Swap Files: {}", swapLocationSuffixes.size(), swapLocationSuffixes);
        }

        // If we didn't recover any records from our write-ahead log, attempt to recover records from the other implementation
        // of the write-ahead log. We do this in case the user changed the "nifi.flowfile.repository.wal.impl" property.
        // In such a case, we still want to recover the records from the previous FlowFile Repository and write them into the new one.
        // Since these implementations do not write to the same files, they will not interfere with one another. If we do recover records,
        // then we will update the new WAL (with fsync()) and delete the old repository so that we won't recover it again.
        if (recordList == null || recordList.isEmpty()) {
            if (isSequentialAccessWAL(walImplementation)) {
                // Configured to use Sequential Access WAL but it has no records. Check if there are records in
                // a MinimalLockingWriteAheadLog that we can recover.
                recordList = migrateFromMinimalLockingLog(wal).orElse(new ArrayList<>());
            } else {
                // Configured to use Minimal Locking WAL but it has no records. Check if there are records in
                // a SequentialAccess Log that we can recover.
                recordList = migrateFromSequentialAccessLog(wal).orElse(new ArrayList<>());
            }
        }

        fieldCache.clear();

        final Map<String, FlowFileQueue> queueMap = new HashMap<>();
        for (final FlowFileQueue queue : queueProvider.getAllQueues()) {
            queueMap.put(queue.getIdentifier(), queue);
        }

        final List<SerializedRepositoryRecord> dropRecords = new ArrayList<>();
        int numFlowFilesMissingQueue = 0;
        long maxId = 0;
        for (final SerializedRepositoryRecord record : recordList) {
            final long recordId = serdeFactory.getRecordIdentifier(record);
            if (recordId > maxId) {
                maxId = recordId;
            }

            final String queueId = record.getQueueIdentifier();
            if (queueId == null) {
                numFlowFilesMissingQueue++;
                logger.warn("Encountered Repository Record (id={}) with no Queue Identifier. Dropping this FlowFile", recordId);

                // Add a drop record so that the record is not retained
                dropRecords.add(new ReconstitutedSerializedRepositoryRecord.Builder()
                    .flowFileRecord(record.getFlowFileRecord())
                    .swapLocation(record.getSwapLocation())
                    .type(RepositoryRecordType.DELETE)
                    .build());

                continue;
            }

            final ContentClaim claim = record.getContentClaim();
            final FlowFileQueue flowFileQueue = queueMap.get(queueId);
            final boolean orphaned = flowFileQueue == null;
            if (orphaned) {
                numFlowFilesMissingQueue++;

                if (isRetainOrphanedFlowFiles()) {
                    if (claim == null) {
                        logger.warn("Encountered Repository Record (id={}) with Queue identifier {} but no Queue exists with that ID. This FlowFile will not be restored to any "
                            + "FlowFile Queue in the flow. However, it will remain in the FlowFile Repository in case the flow containing this queue is later restored.", recordId, queueId);
                    } else {
                        claimManager.incrementClaimantCount(claim.getResourceClaim());
                        orphanedResourceClaims.add(claim.getResourceClaim());
                        logger.warn("Encountered Repository Record (id={}) with Queue identifier {} but no Queue exists with that ID. "
                                + "This FlowFile will not be restored to any FlowFile Queue in the flow. However, it will remain in the FlowFile Repository in "
                                + "case the flow containing this queue is later restored. This may result in the following Content Claim not being cleaned "
                                + "up by the Content Repository: {}", recordId, queueId, claim);
                    }
                } else {
                    dropRecords.add(new ReconstitutedSerializedRepositoryRecord.Builder()
                        .flowFileRecord(record.getFlowFileRecord())
                        .swapLocation(record.getSwapLocation())
                        .type(RepositoryRecordType.DELETE)
                        .build());

                    logger.warn("Encountered Repository Record (id={}) with Queue identifier {} but no Queue exists with that ID. This FlowFile will be dropped.", recordId, queueId);
                }

                continue;
            } else if (claim != null) {
                claimManager.incrementClaimantCount(claim.getResourceClaim());
            }

            flowFileQueue.put(record.getFlowFileRecord());
        }

        // If recoveredRecords has been populated it need to be nulled out now because it is no longer useful and can be garbage collected.
        recoveredRecords = null;

        // Set the AtomicLong to 1 more than the max ID so that calls to #getNextFlowFileSequence() will
        // return the appropriate number.
        flowFileSequenceGenerator.set(maxId + 1);
        logger.info("Successfully restored {} FlowFiles and {} Swap Files", recordList.size() - numFlowFilesMissingQueue, recoveredSwapLocations.size());
        if (numFlowFilesMissingQueue > 0) {
            logger.warn("On recovery, found {} FlowFiles whose queues no longer exists.", numFlowFilesMissingQueue);
        }

        if (dropRecords.isEmpty()) {
            logger.debug("No Drop Records to update Repository with");
        } else {
            final long updateStart = System.nanoTime();
            wal.update(dropRecords, true);
            final long updateEnd = System.nanoTime();
            final long updateMillis = TimeUnit.MILLISECONDS.convert(updateEnd - updateStart, TimeUnit.NANOSECONDS);
            logger.info("Successfully updated FlowFile Repository with {} Drop Records due to missing queues in {} milliseconds", dropRecords.size(), updateMillis);
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
                    logger.info("Successfully checkpointed FlowFile Repository with {} records in {} milliseconds", numRecordsCheckpointed, millis);
                } catch (final Throwable t) {
                    logger.error("Unable to checkpoint FlowFile Repository due to " + t.toString(), t);
                }
            }
        };

        checkpointFuture = checkpointExecutor.scheduleWithFixedDelay(checkpointRunnable, checkpointDelayMillis, checkpointDelayMillis, TimeUnit.MILLISECONDS);

        return maxId;
    }

    private boolean isRetainOrphanedFlowFiles() {
        return retainOrphanedFlowFiles;
    }

    @Override
    public Set<ResourceClaim> findOrphanedResourceClaims() {
        return Collections.unmodifiableSet(orphanedResourceClaims);
    }

    @Override
    public void updateMaxFlowFileIdentifier(final long maxId) {
        while (true) {
            final long currentId = flowFileSequenceGenerator.get();
            if (currentId >= maxId) {
                return;
            }

            final boolean updated = flowFileSequenceGenerator.compareAndSet(currentId, maxId);
            if (updated) {
                return;
            }
        }
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
