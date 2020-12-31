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
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.rocksdb.RocksDBMetronome;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wali.SerDe;
import org.wali.UpdateType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * <p>
 * Implements FlowFile Repository using RocksDB as the backing store.
 * </p>
 */
public class RocksDBFlowFileRepository implements FlowFileRepository {

    private static final Logger logger = LoggerFactory.getLogger(RocksDBFlowFileRepository.class);

    private static final String FLOWFILE_PROPERTY_PREFIX = "nifi.flowfile.repository.";
    private static final String FLOWFILE_REPOSITORY_DIRECTORY_PREFIX = FLOWFILE_PROPERTY_PREFIX + "directory";

    private static final byte[] SWAP_LOCATION_SUFFIX_KEY = "swap.location.sufixes".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SERIALIZATION_ENCODING_KEY = "serial.encoding".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SERIALIZATION_HEADER_KEY = "serial.header".getBytes(StandardCharsets.UTF_8);
    static final byte[] REPOSITORY_VERSION_KEY = "repository.version".getBytes(StandardCharsets.UTF_8);
    static final byte[] VERSION_ONE_BYTES = "1.0".getBytes(StandardCharsets.UTF_8);
    private static final IllegalStateException NO_NEW_FLOWFILES = new IllegalStateException("Repository is not currently accepting new FlowFiles");
    private static final Runtime runtime = Runtime.getRuntime();
    private static final NumberFormat percentFormat = NumberFormat.getPercentInstance();

    private final Map<String, FlowFileQueue> queueMap = new HashMap<>();

    /**
     * Each property is defined by its name in the file and its default value
     */
    enum RocksDbProperty {
        //FlowFileRepo Configuration Parameters
        SYNC_WARNING_PERIOD("rocksdb.sync.warning.period", "30 seconds"),
        CLAIM_CLEANUP_PERIOD("rocksdb.claim.cleanup.period", "30 seconds"),
        DESERIALIZATION_THREADS("rocksdb.deserialization.threads", "16"),
        DESERIALIZATION_BUFFER_SIZE("rocksdb.deserialization.buffer.size", "1000"),
        SYNC_PERIOD("rocksdb.sync.period", "10 milliseconds"),
        ACCEPT_DATA_LOSS("rocksdb.accept.data.loss", "false"),
        ENABLE_STALL_STOP("rocksdb.enable.stall.stop", "false"),
        STALL_PERIOD("rocksdb.stall.period", "100 milliseconds"),
        STALL_FLOWFILE_COUNT("rocksdb.stall.flowfile.count", "800000"),
        STALL_HEAP_USAGE_PERCENT("rocksdb.stall.heap.usage.percent", "95%"),
        STOP_FLOWFILE_COUNT("rocksdb.stop.flowfile.count", "1100000"),
        STOP_HEAP_USAGE_PERCENT("rocksdb.stop.heap.usage.percent", "99.9%"),
        REMOVE_ORPHANED_FLOWFILES("rocksdb.remove.orphaned.flowfiles.on.startup", "false"),
        ENABLE_RECOVERY_MODE("rocksdb.enable.recovery.mode", "false"),
        RECOVERY_MODE_FLOWFILE_LIMIT("rocksdb.recovery.mode.flowfile.count", "5000"),

        //RocksDB Configuration Parameters
        DB_PARALLEL_THREADS("rocksdb.parallel.threads", "8"),
        MAX_WRITE_BUFFER_NUMBER("rocksdb.max.write.buffer.number", "4"),
        WRITE_BUFFER_SIZE("rocksdb.write.buffer.size", "256 MB"),
        LEVEL_O_SLOWDOWN_WRITES_TRIGGER("rocksdb.level.0.slowdown.writes.trigger", "20"),
        LEVEL_O_STOP_WRITES_TRIGGER("rocksdb.level.0.stop.writes.trigger", "40"),
        DELAYED_WRITE_RATE("rocksdb.delayed.write.bytes.per.second", "16 MB"),
        MAX_BACKGROUND_FLUSHES("rocksdb.max.background.flushes", "1"),
        MAX_BACKGROUND_COMPACTIONS("rocksdb.max.background.compactions", "1"),
        MIN_WRITE_BUFFER_NUMBER_TO_MERGE("rocksdb.min.write.buffer.number.to.merge", "1"),
        STAT_DUMP_PERIOD("rocksdb.stat.dump.period", "600 sec"),
        ;

        final String propertyName;
        final String defaultValue;

        RocksDbProperty(String propertyName, String defaultValue) {
            this.propertyName = FLOWFILE_PROPERTY_PREFIX + propertyName;
            this.defaultValue = defaultValue;
        }

        /**
         * @param niFiProperties The Properties file
         * @param timeUnit       The desired time unit
         * @return The property Value in the desired units
         */
        long getTimeValue(NiFiProperties niFiProperties, TimeUnit timeUnit) {
            String propertyValue = niFiProperties.getProperty(this.propertyName, this.defaultValue);
            long timeValue = 0L;
            try {
                timeValue = Math.round(FormatUtils.getPreciseTimeDuration(propertyValue, timeUnit));
            } catch (IllegalArgumentException e) {
                this.generateIllegalArgumentException(propertyValue, e);
            }
            return timeValue;
        }

        /**
         * @param niFiProperties The Properties file
         * @return The property value as a boolean
         */
        boolean getBooleanValue(NiFiProperties niFiProperties) {
            String propertyValue = niFiProperties.getProperty(this.propertyName, this.defaultValue);
            return Boolean.parseBoolean(propertyValue);
        }

        /**
         * @param niFiProperties The Properties file
         * @return The property value as an int
         */
        int getIntValue(NiFiProperties niFiProperties) {
            String propertyValue = niFiProperties.getProperty(this.propertyName, this.defaultValue);
            int returnValue = 0;
            try {
                returnValue = Integer.parseInt(propertyValue);
            } catch (NumberFormatException e) {
                this.generateIllegalArgumentException(propertyValue, e);
            }
            return returnValue;
        }

        /**
         * @param niFiProperties The Properties file
         * @return The property value as a number of bytes
         */
        long getByteCountValue(NiFiProperties niFiProperties) {
            long returnValue = 0L;
            String propertyValue = niFiProperties.getProperty(this.propertyName, this.defaultValue);
            try {
                double writeBufferDouble = DataUnit.parseDataSize(propertyValue, DataUnit.B);
                returnValue = (long) (writeBufferDouble < Long.MAX_VALUE ? writeBufferDouble : Long.MAX_VALUE);
            } catch (IllegalArgumentException e) {
                this.generateIllegalArgumentException(propertyValue, e);
            }
            return returnValue;
        }

        /**
         * @param niFiProperties The Properties file
         * @return The property value as a percent
         */
        double getPercentValue(NiFiProperties niFiProperties) {
            String propertyValue = niFiProperties.getProperty(this.propertyName, this.defaultValue).replace('%', ' ');
            double returnValue = 0.0D;
            try {
                returnValue = Double.parseDouble(propertyValue) / 100D;
                if (returnValue > 1.0D) {
                    this.generateIllegalArgumentException(propertyValue, null);
                }
            } catch (NumberFormatException e) {
                this.generateIllegalArgumentException(propertyValue, e);
            }
            return returnValue;
        }

        /**
         * @param niFiProperties The Properties file
         * @return The property value as a long
         */
        long getLongValue(NiFiProperties niFiProperties) {
            String propertyValue = niFiProperties.getProperty(this.propertyName, this.defaultValue);
            long returnValue = 0L;
            try {
                returnValue = Long.parseLong(propertyValue);
            } catch (NumberFormatException e) {
                this.generateIllegalArgumentException(propertyValue, e);
            }
            return returnValue;
        }

        void generateIllegalArgumentException(String badValue, Throwable t) {
            throw new IllegalArgumentException("The NiFi Property: [" + this.propertyName + "] with value: [" + badValue + "] is not valid", t);
        }

    }


    private final AtomicLong flowFileSequenceGenerator = new AtomicLong(0L);
    private final int deserializationThreads;
    private final int deserializationBufferSize;
    private final long claimCleanupMillis;
    private final ScheduledExecutorService housekeepingExecutor;
    private final AtomicReference<Collection<ResourceClaim>> claimsAwaitingDestruction = new AtomicReference<>(new ArrayList<>());
    private final RocksDBMetronome db;
    private ResourceClaimManager claimManager;
    private RepositoryRecordSerdeFactory serdeFactory;
    private SerDe<SerializedRepositoryRecord> serializer;
    private String serializationEncodingName;
    private byte[] serializationHeader;

    private final boolean acceptDataLoss;
    private final boolean enableStallStop;
    private final boolean removeOrphanedFlowFiles;
    private final boolean enableRecoveryMode;
    private final long recoveryModeFlowFileLimit;
    private final AtomicReference<SerDe<SerializedRepositoryRecord>> recordDeserializer = new AtomicReference<>();
    private final List<byte[]> recordsToRestore = Collections.synchronizedList(new LinkedList<>());

    private final ReentrantLock stallStopLock = new ReentrantLock();
    private final AtomicLong inMemoryFlowFiles = new AtomicLong(0L);
    volatile boolean stallNewFlowFiles = false;
    volatile boolean stopNewFlowFiles = false;
    private final long stallMillis;
    private final long stallCount;
    private final long stopCount;
    private final double stallPercentage;
    private final double stopPercentage;

    private final Set<String> swapLocationSuffixes = new HashSet<>(); // guarded by synchronizing on object itself

    /**
     * default no args constructor for service loading only.
     */
    public RocksDBFlowFileRepository() {
        deserializationThreads = 0;
        deserializationBufferSize = 0;
        claimCleanupMillis = 0;
        housekeepingExecutor = null;
        db = null;
        acceptDataLoss = false;
        enableStallStop = false;
        removeOrphanedFlowFiles = false;
        stallMillis = 0;
        stallCount = 0;
        stopCount = 0;
        stallPercentage = 0;
        stopPercentage = 0;
        enableRecoveryMode = false;
        recoveryModeFlowFileLimit = 0;
    }

    public RocksDBFlowFileRepository(final NiFiProperties niFiProperties) {
        deserializationThreads = RocksDbProperty.DESERIALIZATION_THREADS.getIntValue(niFiProperties);
        deserializationBufferSize = RocksDbProperty.DESERIALIZATION_BUFFER_SIZE.getIntValue(niFiProperties);

        claimCleanupMillis = RocksDbProperty.CLAIM_CLEANUP_PERIOD.getTimeValue(niFiProperties, TimeUnit.MILLISECONDS);
        housekeepingExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread thread = Executors.defaultThreadFactory().newThread(r);
            thread.setDaemon(true);
            return thread;
        });

        acceptDataLoss = RocksDbProperty.ACCEPT_DATA_LOSS.getBooleanValue(niFiProperties);
        enableStallStop = RocksDbProperty.ENABLE_STALL_STOP.getBooleanValue(niFiProperties);

        removeOrphanedFlowFiles = RocksDbProperty.REMOVE_ORPHANED_FLOWFILES.getBooleanValue(niFiProperties);
        if (removeOrphanedFlowFiles) {
            logger.warn("The property \"{}\" is currently set to \"true\".  " +
                            "This can potentially lead to data loss, and should only be set if you are absolutely certain it is necessary.  " +
                            "Even then, it should be removed as soon as possible.",
                    RocksDbProperty.REMOVE_ORPHANED_FLOWFILES.propertyName);
        }
        stallMillis = RocksDbProperty.STALL_PERIOD.getTimeValue(niFiProperties, TimeUnit.MILLISECONDS);
        stallCount = RocksDbProperty.STALL_FLOWFILE_COUNT.getLongValue(niFiProperties);
        stopCount = RocksDbProperty.STOP_FLOWFILE_COUNT.getLongValue(niFiProperties);
        stallPercentage = RocksDbProperty.STALL_HEAP_USAGE_PERCENT.getPercentValue(niFiProperties);
        stopPercentage = RocksDbProperty.STOP_HEAP_USAGE_PERCENT.getPercentValue(niFiProperties);

        enableRecoveryMode = RocksDbProperty.ENABLE_RECOVERY_MODE.getBooleanValue(niFiProperties);
        recoveryModeFlowFileLimit = RocksDbProperty.RECOVERY_MODE_FLOWFILE_LIMIT.getLongValue(niFiProperties);
        if (enableRecoveryMode) {
            logger.warn("The property \"{}\" is currently set to \"true\" and  \"{}\" is set to  \"{}\".  " +
                            "This means that only {} FlowFiles will be loaded in to memory from the FlowFile repo at a time, " +
                            "allowing for recovery of a system encountering OutOfMemory errors (or similar).  " +
                            "This setting should be reset to \"false\" as soon as recovery is complete.",
                    RocksDbProperty.ENABLE_RECOVERY_MODE.propertyName, RocksDbProperty.RECOVERY_MODE_FLOWFILE_LIMIT.propertyName, recoveryModeFlowFileLimit, recoveryModeFlowFileLimit);
        }
        db = new RocksDBMetronome.Builder()
                .setStatDumpSeconds((int) (Math.min(RocksDbProperty.STAT_DUMP_PERIOD.getTimeValue(niFiProperties, TimeUnit.SECONDS), Integer.MAX_VALUE)))
                .setParallelThreads(RocksDbProperty.DB_PARALLEL_THREADS.getIntValue(niFiProperties))
                .setMaxWriteBufferNumber(RocksDbProperty.MAX_WRITE_BUFFER_NUMBER.getIntValue(niFiProperties))
                .setMinWriteBufferNumberToMerge(RocksDbProperty.MIN_WRITE_BUFFER_NUMBER_TO_MERGE.getIntValue(niFiProperties))
                .setWriteBufferSize(RocksDbProperty.WRITE_BUFFER_SIZE.getByteCountValue(niFiProperties))
                .setDelayedWriteRate(RocksDbProperty.DELAYED_WRITE_RATE.getByteCountValue(niFiProperties))
                .setLevel0SlowdownWritesTrigger(RocksDbProperty.LEVEL_O_SLOWDOWN_WRITES_TRIGGER.getIntValue(niFiProperties))
                .setLevel0StopWritesTrigger(RocksDbProperty.LEVEL_O_STOP_WRITES_TRIGGER.getIntValue(niFiProperties))
                .setMaxBackgroundFlushes(RocksDbProperty.MAX_BACKGROUND_FLUSHES.getIntValue(niFiProperties))
                .setMaxBackgroundCompactions(RocksDbProperty.MAX_BACKGROUND_COMPACTIONS.getIntValue(niFiProperties))
                .setSyncMillis(RocksDbProperty.SYNC_PERIOD.getTimeValue(niFiProperties, TimeUnit.MILLISECONDS))
                .setSyncWarningNanos(RocksDbProperty.SYNC_WARNING_PERIOD.getTimeValue(niFiProperties, TimeUnit.NANOSECONDS))
                .setStoragePath(getFlowFileRepoPath(niFiProperties))
                .setAdviseRandomOnOpen(false)
                .setCreateMissingColumnFamilies(true)
                .setCreateIfMissing(true)
                .setPeriodicSyncEnabled(!acceptDataLoss)
                .build();
    }

    /**
     * @param niFiProperties The Properties file
     * @return The path of the repo
     */
    static Path getFlowFileRepoPath(NiFiProperties niFiProperties) {
        for (final String propertyName : niFiProperties.getPropertyKeys()) {
            if (propertyName.startsWith(FLOWFILE_REPOSITORY_DIRECTORY_PREFIX)) {
                final String dirName = niFiProperties.getProperty(propertyName);
                return Paths.get(dirName);
            }
        }
        return null;
    }

    /**
     * @return The name of the File Store
     */
    @Override
    public String getFileStoreName() {
        try {
            return Files.getFileStore(db.getStoragePath()).name();
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public void initialize(final ResourceClaimManager claimManager) throws IOException {
        this.db.initialize();
        this.claimManager = claimManager;
        this.serdeFactory = new StandardRepositoryRecordSerdeFactory(claimManager);

        try {
            byte[] versionBytes = db.getConfiguration(REPOSITORY_VERSION_KEY);
            if (versionBytes == null) {
                db.putConfiguration(REPOSITORY_VERSION_KEY, VERSION_ONE_BYTES);
            } else if (!Arrays.equals(versionBytes, VERSION_ONE_BYTES)) {
                throw new IllegalStateException("Unknown repository version: " + new String(versionBytes, StandardCharsets.UTF_8));
            }

            byte[] serializationEncodingBytes = db.getConfiguration(SERIALIZATION_ENCODING_KEY);

            if (serializationEncodingBytes == null) {
                serializer = serdeFactory.createSerDe(null);
                serializationEncodingName = serializer.getClass().getName();
                db.putConfiguration(SERIALIZATION_ENCODING_KEY, serializationEncodingName.getBytes(StandardCharsets.UTF_8));
            } else {
                serializationEncodingName = new String(serializationEncodingBytes, StandardCharsets.UTF_8);
                serializer = serdeFactory.createSerDe(serializationEncodingName);
            }

            serializationHeader = db.getConfiguration(SERIALIZATION_HEADER_KEY);

            if (serializationHeader == null) {
                try (
                        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                        DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)
                ) {
                    serializer.writeHeader(dataOutputStream);
                    serializationHeader = byteArrayOutputStream.toByteArray();
                    db.putConfiguration(SERIALIZATION_HEADER_KEY, serializationHeader);
                }
            }


            byte[] swapLocationSuffixBytes = db.getConfiguration(SWAP_LOCATION_SUFFIX_KEY);
            if (swapLocationSuffixBytes != null) {
                try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(swapLocationSuffixBytes);
                     ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream)) {
                    Object o = objectInputStream.readObject();
                    if (o instanceof Collection) {
                        ((Collection<?>) o).forEach(obj -> swapLocationSuffixes.add(obj.toString()));
                    }
                }
            }
        } catch (RocksDBException | ClassNotFoundException e) {
            throw new IOException(e);
        }

        housekeepingExecutor.scheduleWithFixedDelay(this::doHousekeeping, 0, claimCleanupMillis, TimeUnit.MILLISECONDS);

        logger.info("Initialized FlowFile Repository at {}", db.getStoragePath());
    }

    @Override
    public void close() throws IOException {
        if (housekeepingExecutor != null) {
            housekeepingExecutor.shutdownNow();
        }

        if (db != null) {
            db.close();
        }
    }

    /**
     * This method is scheduled by the housekeepingExecutor at the specified interval.
     * Catches Throwable so as not to suppress future executions of the ScheduledExecutorService
     */
    private void doHousekeeping() {
        try {
            doClaimCleanup();
            updateStallStop();
            doRecovery();
        } catch (Throwable t) {
            // catching Throwable so as not to suppress subsequent executions
            logger.error("Encountered problem during housekeeping", t);
        }
    }

    /**
     * Marks as destructible any claims that were held by records which have now been deleted.
     */
    private void doClaimCleanup() {
        Collection<ResourceClaim> claimsToDestroy;
        synchronized (claimsAwaitingDestruction) {
            if (Thread.currentThread().isInterrupted()) {
                return;
            }
            claimsToDestroy = claimsAwaitingDestruction.getAndSet(new ArrayList<>());
        }
        if (claimsToDestroy != null) {
            Collection<ResourceClaim> uniqueClaimsToDestroy = new HashSet<>(claimsToDestroy);

            try {
                if (!acceptDataLoss) {
                    db.waitForSync();
                } else {
                    db.forceSync();
                }
            } catch (InterruptedException | RocksDBException e) {
                synchronized (claimsAwaitingDestruction) {
                    // if there was an exception, put back the claims we were attempting to destroy
                    claimsAwaitingDestruction.get().addAll(uniqueClaimsToDestroy);
                    return;
                }
            }
            for (final ResourceClaim claim : uniqueClaimsToDestroy) {
                claimManager.markDestructable(claim);
            }
        }
    }

    /**
     * Updates the stalled and stopped status of the repository
     */
    void updateStallStop() {
        // if stall.stop logic is not enabled, return
        if (!enableStallStop) return;

        if (stallStopLock.tryLock()) {
            try {
                final long inMemoryFlowFiles = getInMemoryFlowFiles();

                if (inMemoryFlowFiles >= stopCount) {
                    stopNewFlowFiles = true;
                    stallNewFlowFiles = true;
                    logger.warn("Halting new FlowFiles because maximum FlowFile count ({}) has been exceeded.  Current count: {}",
                            new Object[]{stopCount, inMemoryFlowFiles});
                    return;
                }

                // calculate usage percentage
                final double freeMemory = runtime.freeMemory();
                final double maxMemory = runtime.maxMemory();
                final double usedPercentage = 1.0d - (freeMemory / maxMemory);

                if (usedPercentage >= stopPercentage) {
                    stopNewFlowFiles = true;
                    stallNewFlowFiles = true;
                    logger.warn("Halting new FlowFiles because maximum heap usage percentage ({}) has been exceeded.  Current usage: {}",
                            new Object[]{percentFormat.format(stopPercentage), percentFormat.format(usedPercentage)});
                    return;
                }

                if (inMemoryFlowFiles >= stallCount) {
                    stopNewFlowFiles = false;
                    stallNewFlowFiles = true;
                    logger.warn("Stalling new FlowFiles because FlowFile count stall threshold ({}) has been exceeded.  Current count: {}",
                            new Object[]{stallCount, inMemoryFlowFiles});
                    return;
                }

                if (usedPercentage >= stallPercentage) {
                    stopNewFlowFiles = false;
                    stallNewFlowFiles = true;
                    logger.warn("Stalling new FlowFiles because heap usage percentage threshold ({}) has been exceeded.  Current count: {}",
                            new Object[]{percentFormat.format(stallPercentage), percentFormat.format(usedPercentage)});
                    return;
                }

                if (stopNewFlowFiles || stallNewFlowFiles) {
                    logger.info("Resuming acceptance of new FlowFiles");
                    stopNewFlowFiles = false;
                    stallNewFlowFiles = false;
                }
            } finally {
                stallStopLock.unlock();
            }
        }
    }

    /**
     * If in recovery mode, restore more FlowFile if under the configured limit
     */
    synchronized void doRecovery() {

        // if we are not in recovery mode, return
        if (!enableRecoveryMode) return;

        SerDe<SerializedRepositoryRecord> deserializer = recordDeserializer.get();
        if (deserializer == null) {
            return; // initial load hasn't completed
        }

        if (recordsToRestore.isEmpty()) {
            logger.warn("Recovery has been completed.  " +
                            "The property \"{}\" is currently set to \"true\", but should be reset to \"false\" as soon as possible.",
                    RocksDbProperty.ENABLE_RECOVERY_MODE.propertyName);
            return;
        }

        logger.warn("The property \"{}\" is currently set to \"true\" and \"{}\" is set to \"{}\".  " +
                        "This means that only {} FlowFiles will be loaded into memory from the FlowFile repo at a time, " +
                        "allowing for recovery of a system encountering OutOfMemory errors (or similar).  " +
                        "This setting should be reset to \"false\" as soon as recovery is complete.  " +
                        "There are {} records remaining to be recovered.",
                RocksDbProperty.ENABLE_RECOVERY_MODE.propertyName, RocksDbProperty.RECOVERY_MODE_FLOWFILE_LIMIT.propertyName,
                recoveryModeFlowFileLimit, recoveryModeFlowFileLimit, getRecordsToRestoreCount());

        while (!recordsToRestore.isEmpty() && inMemoryFlowFiles.get() < recoveryModeFlowFileLimit) {
            try {
                byte[] key = recordsToRestore.get(0);
                byte[] recordBytes = db.get(key);
                if (recordBytes != null) {
                    try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(recordBytes);
                         DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream)) {
                        SerializedRepositoryRecord record = deserializer.deserializeRecord(dataInputStream, deserializer.getVersion());
                        final FlowFileRecord flowFile = record.getFlowFileRecord();

                        final FlowFileQueue queue = queueMap.get(record.getQueueIdentifier());
                        if (queue != null) {
                            queue.put(flowFile);
                            inMemoryFlowFiles.incrementAndGet();
                        }
                    }
                }
                recordsToRestore.remove(0);
            } catch (IOException | RocksDBException e) {
                logger.warn("Encountered exception during recovery", e);
            }
        }
    }

    long getInMemoryFlowFiles() {
        return inMemoryFlowFiles.get();
    }

    long getRecordsToRestoreCount() {
        return recordsToRestore.size();
    }

    /**
     * Updates the FlowFile repository with the given SerializedRepositoryRecords
     *
     * @param records the records to update the repository with
     * @throws IOException if update fails or a required sync is interrupted
     */
    @Override
    public void updateRepository(final Collection<RepositoryRecord> records) throws IOException {

        // verify records are valid
        int netIncrease = countAndValidateRecords(records);

        final boolean causeIncrease = netIncrease > 0;
        if (causeIncrease && stopNewFlowFiles) {
            updateStallStop();
            throw NO_NEW_FLOWFILES;
        }

        //update the db with the new records
        int syncCounterValue = updateRocksDB(records);
        inMemoryFlowFiles.addAndGet(netIncrease);

        try {
            // if we created data, but are at a threshold, delay and allow other data to try to get through
            if (causeIncrease && (stallNewFlowFiles || stopNewFlowFiles)) {
                Thread.sleep(stallMillis);
                updateStallStop();
            }

            // if we got a record indicating data creation, wait for it to be synced to disk before proceeding
            if (!acceptDataLoss && syncCounterValue > 0) {
                db.waitForSync(syncCounterValue);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }

        // determine any content claims that can be destroyed
        determineDestructibleClaims(records);
    }

    /**
     * Check that each record has the required elements
     *
     * @param records to be validated
     * @return the change in in-memory FlowFile count represented by this Collection
     */
    private int countAndValidateRecords(Collection<RepositoryRecord> records) {
        int inMemoryDelta = 0;
        for (RepositoryRecord record : records) {
            validateRecord(record);
            if (record.getType() == RepositoryRecordType.CREATE || record.getType() == RepositoryRecordType.SWAP_IN) {
                inMemoryDelta++;
            } else if (record.getType() == RepositoryRecordType.DELETE || record.getType() == RepositoryRecordType.SWAP_OUT) {
                inMemoryDelta--;
            }
        }
        return inMemoryDelta;
    }

    /**
     * Check that a record has the required elements
     *
     * @param record to be validated
     */
    private void validateRecord(RepositoryRecord record) {
        if (record.getType() != RepositoryRecordType.DELETE
                && record.getType() != RepositoryRecordType.CONTENTMISSING
                && record.getType() != RepositoryRecordType.CLEANUP_TRANSIENT_CLAIMS
                && record.getType() != RepositoryRecordType.SWAP_OUT
                && record.getDestination() == null) {
            throw new IllegalArgumentException("Record " + record + " has no destination and Type is " + record.getType());
        }
    }

    /**
     * Update the records in the RocksDB database
     *
     * @param records to be persisted
     * @return the value of the sync counter immediately after the last update which requires a sync
     */
    private int updateRocksDB(Collection<RepositoryRecord> records) throws IOException {

        // Partition records by UpdateType.
        // We do this because we want to ensure that records creating data are persisted first.
        // Additionally, remove records of type 'CLEANUP_TRANSIENT_CLAIMS' so they aren't sent to the db

        final Map<UpdateType, List<RepositoryRecord>> partitionedRecords = new HashMap<>();
        for (RepositoryRecord repositoryRecord : records) {
            if (repositoryRecord.getType() == RepositoryRecordType.CLEANUP_TRANSIENT_CLAIMS) {
                continue;
            }
            final UpdateType updateType = serdeFactory.getUpdateType(new LiveSerializedRepositoryRecord(repositoryRecord));
            partitionedRecords.computeIfAbsent(updateType, ut -> new ArrayList<>()).add(repositoryRecord);
        }

        int counterValue;

        try {
            // handle CREATE records
            putAll(partitionedRecords.get(UpdateType.CREATE));

            // handle SWAP_OUT records
            List<RepositoryRecord> swapOutRecords = partitionedRecords.get(UpdateType.SWAP_OUT);
            if (swapOutRecords != null) {
                for (final RepositoryRecord record : swapOutRecords) {
                    final SerializedRepositoryRecord serializedRecord = new LiveSerializedRepositoryRecord(record);
                    final String newLocation = serdeFactory.getLocation(serializedRecord);
                    final Long recordIdentifier = serdeFactory.getRecordIdentifier(serializedRecord);

                    if (newLocation == null) {
                        logger.error("Received Record (ID=" + recordIdentifier + ") with UpdateType of SWAP_OUT but " +
                                "no indicator of where the Record is to be Swapped Out to; these records may be " +
                                "lost when the repository is restored!");
                    } else {
                        delete(recordIdentifier);
                    }
                }
            }

            // handle SWAP_IN records
            List<RepositoryRecord> swapInRecords = partitionedRecords.get(UpdateType.SWAP_IN);
            if (swapInRecords != null) {
                for (final RepositoryRecord record : swapInRecords) {
                    final SerializedRepositoryRecord serialized = new LiveSerializedRepositoryRecord(record);

                    final String newLocation = serdeFactory.getLocation(serialized);
                    if (newLocation == null) {
                        final Long recordIdentifier = serdeFactory.getRecordIdentifier(serialized);
                        logger.error("Received Record (ID=" + recordIdentifier + ") with UpdateType of SWAP_IN but " +
                                "no indicator of where the Record is to be Swapped In from; these records may be " +
                                "duplicated when the repository is restored!");
                    }
                    put(record);
                }
            }

            // if a sync is required, get the current value of the sync counter
            counterValue = syncRequired(partitionedRecords) ? db.getSyncCounterValue() : -1;

            // handle UPDATE records
            putAll(partitionedRecords.get(UpdateType.UPDATE));

            // handle DELETE records
            deleteAll(partitionedRecords.get(UpdateType.DELETE));

        } catch (RocksDBException e) {
            throw new IOException(e);
        }

        return counterValue;
    }

    private boolean syncRequired(Map<UpdateType, List<RepositoryRecord>> recordMap) {
        for (UpdateType updateType : recordMap.keySet()) {
            if (updateType == UpdateType.CREATE || updateType == UpdateType.SWAP_OUT || updateType == UpdateType.SWAP_IN) {
                return true;
            }
        }
        return false;
    }

    private void deleteAll(List<RepositoryRecord> repositoryRecords) throws RocksDBException {
        if (repositoryRecords != null) {
            for (final RepositoryRecord record : repositoryRecords) {
                final SerializedRepositoryRecord serialized = new LiveSerializedRepositoryRecord(record);
                final Long id = serdeFactory.getRecordIdentifier(serialized);
                delete(id);
            }
        }
    }

    private void delete(Long recordId) throws RocksDBException {
        byte[] key = RocksDBMetronome.getBytes(recordId);
        db.delete(key);
    }

    private void putAll(List<RepositoryRecord> repositoryRecords) throws IOException, RocksDBException {
        if (repositoryRecords != null) {
            for (final RepositoryRecord record : repositoryRecords) {
                put(record);
            }
        }
    }

    private void put(RepositoryRecord record) throws IOException, RocksDBException {
        final Long recordIdentifier = serdeFactory.getRecordIdentifier(new LiveSerializedRepositoryRecord(record));
        byte[] key = RocksDBMetronome.getBytes(recordIdentifier);
        final byte[] serializedRecord = serialize(record);
        db.put(key, serializedRecord);
    }

    private byte[] serialize(RepositoryRecord record) throws IOException {
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
             DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
            serializer.serializeRecord(new LiveSerializedRepositoryRecord(record), dataOutputStream);
            return byteArrayOutputStream.toByteArray();
        }
    }

    private void determineDestructibleClaims(Collection<RepositoryRecord> records) throws IOException {

        final Set<ResourceClaim> claimsToAdd = new HashSet<>();
        final Set<String> swapLocationsAdded = new HashSet<>();
        final Set<String> swapLocationsRemoved = new HashSet<>();
        for (final RepositoryRecord record : records) {
            updateClaimCounts(record);

            if (record.getType() == RepositoryRecordType.DELETE) {
                // For any DELETE record that we have, if claim is destructible, mark it so
                if (isDestructible(record.getCurrentClaim())) {
                    claimsToAdd.add(record.getCurrentClaim().getResourceClaim());
                }

                // If the original claim is different than the current claim and the original claim is destructible, mark it so
                if (shouldDestroyOriginal(record)) {
                    claimsToAdd.add(record.getOriginalClaim().getResourceClaim());
                }
            } else if (record.getType() == RepositoryRecordType.UPDATE) {
                // if we have an update, and the original is no longer needed, mark original as destructible
                if (shouldDestroyOriginal(record)) {
                    claimsToAdd.add(record.getOriginalClaim().getResourceClaim());
                }
            } else if (record.getType() == RepositoryRecordType.SWAP_OUT) {
                final String swapLocation = record.getSwapLocation();
                final String normalizedSwapLocation = normalizeSwapLocation(swapLocation);
                swapLocationsAdded.add(normalizedSwapLocation);
                swapLocationsRemoved.remove(normalizedSwapLocation);
            } else if (record.getType() == RepositoryRecordType.SWAP_IN) {
                final String swapLocation = record.getSwapLocation();
                final String normalizedSwapLocation = normalizeSwapLocation(swapLocation);
                swapLocationsRemoved.add(normalizedSwapLocation);
                swapLocationsAdded.remove(normalizedSwapLocation);
            }

            final List<ContentClaim> transientClaims = record.getTransientClaims();
            if (transientClaims != null) {
                for (final ContentClaim transientClaim : transientClaims) {
                    if (isDestructible(transientClaim)) {
                        claimsToAdd.add(transientClaim.getResourceClaim());
                    }
                }
            }
        }

        // If we have swapped files in or out, we need to ensure that we update our swapLocationSuffixes.
        if (!swapLocationsAdded.isEmpty() || !swapLocationsRemoved.isEmpty()) {
            synchronized (swapLocationSuffixes) {
                removeNormalizedSwapLocations(swapLocationsRemoved);
                addNormalizedSwapLocations(swapLocationsAdded);
            }
        }

        // add any new claims that can be destroyed to the list
        if (!claimsToAdd.isEmpty()) {
            synchronized (claimsAwaitingDestruction) {
                claimsAwaitingDestruction.get().addAll(claimsToAdd);
            }
        }
    }


    private void updateClaimCounts(final RepositoryRecord record) {
        final ContentClaim currentClaim = record.getCurrentClaim();
        final ContentClaim originalClaim = record.getOriginalClaim();
        final boolean claimChanged = !Objects.equals(currentClaim, originalClaim);

        if (record.getType() == RepositoryRecordType.DELETE || record.getType() == RepositoryRecordType.CONTENTMISSING) {
            decrementClaimCount(currentClaim);
        }

        if (claimChanged) {
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


    /**
     * @param claim to be evaluated
     * @return true if the claim can be destroyed
     */
    private boolean isDestructible(final ContentClaim claim) {
        if (claim == null) {
            return false;
        }

        final ResourceClaim resourceClaim = claim.getResourceClaim();
        if (resourceClaim == null) {
            return false;
        }

        return !resourceClaim.isInUse();
    }

    /**
     * @param record to be evaluated
     * @return true if the original claim can be destroyed
     */
    private boolean shouldDestroyOriginal(RepositoryRecord record) {
        final ContentClaim originalClaim = record.getOriginalClaim();
        return isDestructible(originalClaim) && !originalClaim.equals(record.getCurrentClaim());
    }

    @Override
    public boolean isVolatile() {
        return false;
    }

    @Override
    public long getStorageCapacity() throws IOException {
        return db.getStorageCapacity();
    }

    @Override
    public long getUsableStorageSpace() throws IOException {
        return db.getUsableStorageSpace();
    }


    @Override
    public boolean isValidSwapLocationSuffix(final String swapLocationSuffix) {
        String normalizedSwapLocation = normalizeSwapLocation(swapLocationSuffix);
        synchronized (swapLocationSuffixes) {
            return swapLocationSuffixes.contains(normalizedSwapLocation);
        }
    }


    static String normalizeSwapLocation(final String swapLocation) {
        if (swapLocation == null) {
            return null;
        }

        final String normalizedPath = swapLocation.replace("\\", "/");
        final String withoutTrailing = (normalizedPath.endsWith("/") && normalizedPath.length() > 1) ? normalizedPath.substring(0, normalizedPath.length() - 1) : normalizedPath;
        final String pathRemoved = getLocationSuffix(withoutTrailing);

        return StringUtils.substringBefore(pathRemoved, ".");
    }

    private static String getLocationSuffix(final String swapLocation) {
        final int lastIndex = swapLocation.lastIndexOf("/");
        if (lastIndex < 0 || lastIndex >= swapLocation.length() - 1) {
            return swapLocation;
        }

        return swapLocation.substring(lastIndex + 1);
    }

    @Override
    public void swapFlowFilesOut(final List<FlowFileRecord> swappedOut, final FlowFileQueue queue,
                                 final String swapLocation) throws IOException {
        final List<RepositoryRecord> repoRecords = new ArrayList<>();
        if (swappedOut == null || swappedOut.isEmpty()) {
            return;
        }

        for (final FlowFileRecord swapRecord : swappedOut) {
            final RepositoryRecord repoRecord = new StandardRepositoryRecord(queue, swapRecord, swapLocation);
            repoRecords.add(repoRecord);
        }

        updateRepository(repoRecords);
        addRawSwapLocation(swapLocation);
        logger.info("Successfully swapped out {} FlowFiles from {} to Swap File {}", swappedOut.size(), queue, swapLocation);
    }

    @Override
    public void swapFlowFilesIn(final String swapLocation, final List<FlowFileRecord> swapRecords,
                                final FlowFileQueue queue) throws IOException {
        final List<RepositoryRecord> repoRecords = new ArrayList<>();

        for (final FlowFileRecord swapRecord : swapRecords) {
            final StandardRepositoryRecord repoRecord = new StandardRepositoryRecord(queue, swapRecord);
            repoRecord.setSwapLocation(swapLocation);   // set the swap file to indicate that it's being swapped in.
            repoRecord.setDestination(queue);

            repoRecords.add(repoRecord);
        }

        updateRepository(repoRecords);
        removeRawSwapLocation(swapLocation);
        logger.info("Repository updated to reflect that {} FlowFiles were swapped in to {}", new Object[]{swapRecords.size(), queue});
    }


    @Override
    public long loadFlowFiles(final QueueProvider queueProvider) throws IOException {

        final long startTime = System.nanoTime();

        queueMap.clear();
        for (final FlowFileQueue queue : queueProvider.getAllQueues()) {
            queueMap.put(queue.getIdentifier(), queue);
        }

        final ExecutorService recordDeserializationExecutor = Executors.newFixedThreadPool(deserializationThreads, r -> {
            Thread thread = Executors.defaultThreadFactory().newThread(r);
            thread.setDaemon(true);
            return thread;
        });

        // Create a queue which will hold the bytes of the records that have been read from disk and are awaiting deserialization
        final BlockingQueue<byte[]> recordBytesQueue = new ArrayBlockingQueue<>(deserializationBufferSize);

        final AtomicBoolean doneReading = new AtomicBoolean(false);
        final List<Future<Long>> futures = new ArrayList<>(deserializationThreads);

        RepositoryRecordSerdeFactory factory = new StandardRepositoryRecordSerdeFactory(claimManager);

        final AtomicInteger numFlowFilesMissingQueue = new AtomicInteger(0);
        final AtomicInteger recordCount = new AtomicInteger(0);
        final AtomicInteger recoveryModeRecordCount = new AtomicInteger(0);

        for (int i = 0; i < deserializationThreads; i++) {
            futures.add(recordDeserializationExecutor.submit(() -> {
                long localMaxId = 0;
                int localRecordCount = 0;
                final Set<String> localRecoveredSwapLocations = new HashSet<>();

                // Create deserializer in each thread
                final SerDe<SerializedRepositoryRecord> localDeserializer = factory.createSerDe(serializationEncodingName);
                try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(serializationHeader);
                     DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream)) {
                    localDeserializer.readHeader(dataInputStream);
                }

                while (!doneReading.get() || !recordBytesQueue.isEmpty()) {
                    byte[] value = recordBytesQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (value != null) {
                        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(value);
                             DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream)) {
                            SerializedRepositoryRecord record = localDeserializer.deserializeRecord(dataInputStream, localDeserializer.getVersion());

                            localRecordCount++;

                            // increment the count for the record
                            final ContentClaim claim = record.getContentClaim();
                            if (claim != null) {
                                claimManager.incrementClaimantCount(claim.getResourceClaim());
                            }

                            final long recordId = record.getFlowFileRecord().getId();
                            if (recordId > localMaxId) {
                                localMaxId = recordId;
                            }

                            if (record.getType().equals(RepositoryRecordType.SWAP_OUT)) {
                                localRecoveredSwapLocations.add(normalizeSwapLocation(record.getSwapLocation()));
                            }

                            final FlowFileRecord flowFile = record.getFlowFileRecord();
                            final FlowFileQueue queue = queueMap.get(record.getQueueIdentifier());
                            if (queue == null) {
                                if (!removeOrphanedFlowFiles) {
                                    throw new IOException("Found FlowFile in repository without a corresponding queue.  " +
                                            "This may indicate an issue syncing the flow.xml in a cluster.  " +
                                            "To resolve this issue you should restore the flow.xml.  " +
                                            "Alternatively, if removing data is acceptable, you can add the following to nifi.properties: \n\n" +
                                            "\t\t" + RocksDbProperty.REMOVE_ORPHANED_FLOWFILES.propertyName + "=true\n\n" +
                                            "...once this has allowed you to restart nifi, you should remove it from nifi.properties to prevent inadvertent future data loss.");
                                }

                                numFlowFilesMissingQueue.incrementAndGet();
                                try {
                                    final Long recordIdentifier = factory.getRecordIdentifier(record);
                                    byte[] key = RocksDBMetronome.getBytes(recordIdentifier);
                                    db.delete(key);
                                } catch (RocksDBException e) {
                                    logger.warn("Could not clean up repository", e);
                                }
                            } else {
                                // verify we're supposed to enqueue the FlowFile
                                if (!enableRecoveryMode) {
                                    queue.put(flowFile);
                                } else if (recoveryModeRecordCount.incrementAndGet() <= recoveryModeFlowFileLimit) {
                                    queue.put(flowFile);
                                } else {
                                    final Long recordIdentifier = factory.getRecordIdentifier(record);
                                    byte[] key = RocksDBMetronome.getBytes(recordIdentifier);
                                    recordsToRestore.add(key);
                                }
                            }
                        }
                    }
                }
                recordCount.addAndGet(localRecordCount);
                addNormalizedSwapLocations(localRecoveredSwapLocations);
                return localMaxId;

            }));
        }

        long maxId = 0;
        RocksIterator rocksIterator = db.getIterator();
        rocksIterator.seekToFirst();
        long counter = 0;
        long totalRecords = 0;
        try {
            while (rocksIterator.isValid()) {
                if (recordBytesQueue.offer(rocksIterator.value(), 10, TimeUnit.SECONDS)) {
                    rocksIterator.next();
                    if (++counter == 5_000) { // periodically report progress
                        totalRecords += counter;
                        counter = 0;
                        logger.info("Read {} records from disk", totalRecords);
                    }
                } else {
                    // couldn't add to the queue in a timely fashion... make sure there are no exceptions from the consumers
                    for (Future<Long> f : futures) {
                        // the only way it could be done at this point is through an exception
                        // (because we haven't yet set doneReading = true)
                        if (f.isDone()) {
                            f.get(); // this will throw the exception
                        }
                    }
                    logger.warn("Failed to add record bytes to queue.  Will keep trying...");
                }
            }

            doneReading.set(true);
            totalRecords += counter;
            logger.info("Finished reading from rocksDB.  Read {} records from disk", totalRecords);

            for (Future<Long> f : futures) {
                long futureMax = f.get(); // will wait for completion (or exception)
                if (futureMax > maxId) {
                    maxId = futureMax;
                }
            }

            logger.info("Finished deserializing {} records", recordCount.get());

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        } catch (ExecutionException e) {
            throw new IOException(e);
        } finally {
            recordDeserializationExecutor.shutdownNow();
        }

        // Set the AtomicLong to 1 more than the max ID so that calls to #getNextFlowFileSequence() will
        // return the appropriate number.
        flowFileSequenceGenerator.set(maxId + 1);

        int flowFilesInQueues = recordCount.get() - numFlowFilesMissingQueue.get();
        inMemoryFlowFiles.set(!enableRecoveryMode ? flowFilesInQueues
                : Math.min(flowFilesInQueues, recoveryModeFlowFileLimit));
        logger.info("Successfully restored {} FlowFiles in {} milliseconds using {} threads",
                getInMemoryFlowFiles(),
                TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime),
                deserializationThreads);
        if (logger.isDebugEnabled()) {
            synchronized (this.swapLocationSuffixes) {
                logger.debug("Recovered {} Swap Files: {}", swapLocationSuffixes.size(), swapLocationSuffixes);
            }
        }
        if (numFlowFilesMissingQueue.get() > 0) {
            logger.warn("On recovery, found {} FlowFiles whose queue no longer exists.  These FlowFiles have been dropped.", numFlowFilesMissingQueue);
        }

        final SerDe<SerializedRepositoryRecord> deserializer = factory.createSerDe(serializationEncodingName);

        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(serializationHeader);
             DataInputStream dataInputStream = new DataInputStream(byteArrayInputStream)) {
            deserializer.readHeader(dataInputStream);
        }

        if (enableRecoveryMode) {
            recordDeserializer.set(deserializer);
        }

        return maxId;
    }

    @Override
    public Set<String> findQueuesWithFlowFiles(final FlowFileSwapManager flowFileSwapManager) throws IOException {
        return null;
    }

    private void addRawSwapLocation(String rawSwapLocation) throws IOException {
        addRawSwapLocations(Collections.singleton(rawSwapLocation));
    }

    private void addRawSwapLocations(Collection<String> rawSwapLocations) throws IOException {
        addNormalizedSwapLocations(rawSwapLocations.stream().map(RocksDBFlowFileRepository::normalizeSwapLocation).collect(Collectors.toSet()));
    }

    private void addNormalizedSwapLocations(Collection<String> normalizedSwapLocations) throws IOException {
        synchronized (this.swapLocationSuffixes) {
            this.swapLocationSuffixes.addAll(normalizedSwapLocations);
            persistSwapLocationSuffixes();
        }
    }

    private void removeRawSwapLocation(String rawSwapLocation) throws IOException {
        removeRawSwapLocations(Collections.singleton(rawSwapLocation));
    }

    private void removeRawSwapLocations(Collection<String> rawSwapLocations) throws IOException {
        removeNormalizedSwapLocations(rawSwapLocations.stream().map(RocksDBFlowFileRepository::normalizeSwapLocation).collect(Collectors.toSet()));
    }

    private void removeNormalizedSwapLocations(Collection<String> normalizedSwapLocations) throws IOException {
        synchronized (this.swapLocationSuffixes) {
            this.swapLocationSuffixes.removeAll(normalizedSwapLocations);
            persistSwapLocationSuffixes();
        }
    }

    private void persistSwapLocationSuffixes() throws IOException {
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
             ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream)
        ) {
            objectOutputStream.writeObject(swapLocationSuffixes);
            db.putConfiguration(SWAP_LOCATION_SUFFIX_KEY, byteArrayOutputStream.toByteArray());
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
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
    public long getMaxFlowFileIdentifier() {
        // flowFileSequenceGenerator is 1 more than the MAX so that we can call #getAndIncrement on the AtomicLong
        return flowFileSequenceGenerator.get() - 1;
    }
}
