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
package org.apache.nifi.controller.status.history;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.rocksdb.RocksDBMetronome;
import org.apache.nifi.rocksdb.RocksDBProperty;
import org.apache.nifi.util.ComponentMetrics;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.RingBuffer;
import org.apache.nifi.util.StopWatch;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class RocksDBComponentStatusRepository implements ComponentStatusRepository {
    private static Logger logger = LoggerFactory.getLogger(RocksDBComponentStatusRepository.class);

    private static final Set<MetricDescriptor<?>> DEFAULT_CONNECTION_METRICS = Arrays.stream(ConnectionStatusDescriptor.values())
            .map(ConnectionStatusDescriptor::getDescriptor)
            .collect(Collectors.toSet());
    private static final Set<MetricDescriptor<?>> DEFAULT_PROCESSOR_METRICS = Arrays.stream(ProcessorStatusDescriptor.values())
            .map(ProcessorStatusDescriptor::getDescriptor)
            .collect(Collectors.toSet());
    private static final Set<MetricDescriptor<?>> DEFAULT_GROUP_METRICS = Arrays.stream(ProcessGroupStatusDescriptor.values())
            .map(ProcessGroupStatusDescriptor::getDescriptor)
            .collect(Collectors.toSet());
    private static final Set<MetricDescriptor<?>> DEFAULT_RPG_METRICS = Arrays.stream(RemoteProcessGroupStatusDescriptor.values())
            .map(RemoteProcessGroupStatusDescriptor::getDescriptor)
            .collect(Collectors.toSet());

    private static final Gson gson;

    static {
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.registerTypeAdapter(MetricDescriptor.class, new MetricDescriptorSerde());
        gson = gsonBuilder.create();
    }

    private static final Charset UTF8 = StandardCharsets.UTF_8;
    private static final byte[] GARBAGE_COLLECTION_KEY = "garbage".getBytes();
    static final byte[] ID_KEY = "id.key".getBytes(UTF8);
    private static final byte[] VERSION_KEY = "version.key".getBytes(UTF8);
    private static final byte[] CURRENT_VERSION = RocksDBMetronome.getBytes(1);

    static final String NUM_DATA_POINTS_PROPERTY = "nifi.components.status.repository.buffer.size";
    static final String STORAGE_LOCATION_PROPERTY = "nifi.components.status.repository.storage.location";
    static final int DEFAULT_NUM_DATA_POINTS = 288;

    private static final String timestampsName = "timestamps";
    private static final String componentsName = "components";
    private static final String componentDetailsName = "componentDetails";

    private final RingBuffer<Long> timestamps;
    private final AtomicLong lastCaptureTime = new AtomicLong(0L);
    private volatile Set<String> currentComponents;
    private final int numDataPoints;
    private final Path storageLocation;

    private static final String PROPERTY_PREFIX = "nifi.components.status.";

    /* the "default" column family of the DB:
        - keyed by ID
        - contains serialized StatusSnapshots and garbage collection info
     */
    final RocksDBMetronome db;

    /* "timestamp" column family:
        - keyed by the timestamp of the collection
        - contains the list of all StatusSnapshot Ids for that timestamp
            > enables age-off of stats
     */
    ColumnFamilyHandle timestampsHandle;

    /* "components" column family:
        - keyed by component ID
        - contains list of all StatusSnapshot Ids for that component
     */
    ColumnFamilyHandle componentsHandle;

    /* "componentDetails" columnFamily:
        - keyed by component ID
        - contains the serialized ComponentDetails for each component
     */
    ColumnFamilyHandle componentDetailsHandle;

    private final ExecutorService executorService = Executors.newScheduledThreadPool(10, new ThreadFactory() {
        private final AtomicInteger threadNumber = new AtomicInteger(1);

        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = Executors.defaultThreadFactory().newThread(runnable);
            thread.setDaemon(true);
            thread.setName("Component Status Query Thread " + threadNumber.getAndIncrement());
            return thread;
        }
    });

    private final ExecutorService singleThreadExecutor = Executors.newSingleThreadExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable runnable) {
            Thread thread = Executors.defaultThreadFactory().newThread(runnable);
            thread.setDaemon(true);
            thread.setName("Component Status Capture Thread");
            return thread;
        }
    });

    public RocksDBComponentStatusRepository() {
        numDataPoints = DEFAULT_NUM_DATA_POINTS;
        timestamps = null;
        storageLocation = null;
        db = null;
    }

    public RocksDBComponentStatusRepository(final NiFiProperties niFiProperties) {
        numDataPoints = niFiProperties.getIntegerProperty(NUM_DATA_POINTS_PROPERTY, DEFAULT_NUM_DATA_POINTS);
        timestamps = new RingBuffer<>(numDataPoints);
        storageLocation = Paths.get(niFiProperties.getProperty(STORAGE_LOCATION_PROPERTY, "./stats_history"));
        db = new RocksDBMetronome.Builder()
                .setStoragePath(storageLocation)
                .setStatDumpSeconds((int) (Math.min(RocksDBProperty.STAT_DUMP_PERIOD.getTimeValue(niFiProperties, PROPERTY_PREFIX, TimeUnit.SECONDS), Integer.MAX_VALUE)))
                .setParallelThreads(RocksDBProperty.DB_PARALLEL_THREADS.getIntValue(niFiProperties, PROPERTY_PREFIX))
                .setMinWriteBufferNumberToMerge(RocksDBProperty.MIN_WRITE_BUFFER_NUMBER_TO_MERGE.getIntValue(niFiProperties, PROPERTY_PREFIX))
                .setMaxWriteBufferNumber(RocksDBProperty.MAX_WRITE_BUFFER_NUMBER.getIntValue(niFiProperties, PROPERTY_PREFIX))
                .setWriteBufferSize(RocksDBProperty.WRITE_BUFFER_SIZE.getByteCountValue(niFiProperties, PROPERTY_PREFIX))
                .setDelayedWriteRate(RocksDBProperty.DELAYED_WRITE_RATE.getByteCountValue(niFiProperties, PROPERTY_PREFIX))
                .setLevel0SlowdownWritesTrigger(RocksDBProperty.LEVEL_O_SLOWDOWN_WRITES_TRIGGER.getIntValue(niFiProperties, PROPERTY_PREFIX))
                .setLevel0StopWritesTrigger(RocksDBProperty.LEVEL_O_STOP_WRITES_TRIGGER.getIntValue(niFiProperties, PROPERTY_PREFIX))
                .setMaxBackgroundFlushes(RocksDBProperty.MAX_BACKGROUND_FLUSHES.getIntValue(niFiProperties, PROPERTY_PREFIX))
                .setMaxBackgroundCompactions(RocksDBProperty.MAX_BACKGROUND_COMPACTIONS.getIntValue(niFiProperties, PROPERTY_PREFIX))
                .setSyncMillis(RocksDBProperty.SYNC_PERIOD.getTimeValue(niFiProperties, PROPERTY_PREFIX, TimeUnit.MILLISECONDS))
                .setSyncWarningNanos(RocksDBProperty.SYNC_WARNING_PERIOD.getTimeValue(niFiProperties, PROPERTY_PREFIX, TimeUnit.NANOSECONDS))
                .setAdviseRandomOnOpen(true)
                .setPeriodicSyncEnabled(false)
                .addColumnFamily(timestampsName)
                .addColumnFamily(componentsName)
                .addColumnFamily(componentDetailsName)
                .build();

        try {
            db.initialize();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        timestampsHandle = db.getColumnFamilyHandle(timestampsName);
        componentsHandle = db.getColumnFamilyHandle(componentsName);
        componentDetailsHandle = db.getColumnFamilyHandle(componentDetailsName);

        // check/set serialization version to ease future possible upgrades
        try {
            byte[] versionBytes = db.getConfiguration(VERSION_KEY);
            if(versionBytes == null) {
                db.putConfiguration(VERSION_KEY, CURRENT_VERSION);
            } else if(!Arrays.equals(versionBytes, CURRENT_VERSION)) {
                throw new IllegalStateException("Unknown version");
            }
        } catch (RocksDBException e) {
            logger.warn("Could not verify correct version.", e);
        }

        // read timestmaps, sort, populate ring buffer, expire as needed
        Set<Long> sortedTimestamps = new TreeSet<>();

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        RocksIterator timestampIterator = db.getIterator(timestampsHandle);
        timestampIterator.seekToFirst();

        while(timestampIterator.isValid()) {
            final byte[] key = timestampIterator.key();
            try {
                sortedTimestamps.add(RocksDBMetronome.readLong(key));
            } catch (IOException e) {
                logger.warn("Bad timestamp: {}", key, e);
            }
            timestampIterator.next();
        }

        sortedTimestamps.forEach(timestamp -> {
            addTimestamp(timestamp);
            lastCaptureTime.set(timestamp);
        });

        // Build set of "current" component Ids
        RocksIterator componentIterator = db.getIterator(componentDetailsHandle);
        componentIterator.seekToFirst();
        Set<String> components = new HashSet<>();
        while(componentIterator.isValid()) {
            final byte[] key = componentIterator.key();
            components.add(new String(key, UTF8));
            componentIterator.next();
        }
        currentComponents = components;

        stopWatch.stop();
        logger.info("Recovered {} timestamps and {} components in {} milliseconds", timestamps.getSize(), components.size(), stopWatch.getDuration(TimeUnit.MILLISECONDS));
    }


    /**
     * Add a timestamp to the ring buffer. If this results in the eviction of a timestamp, delete the status snapshots for that timestamp
     *
     * @param timestamp the timestamp to add
     */
    void addTimestamp(long timestamp) {
        final Long evicted = timestamps.add(timestamp);
        if(evicted != null) {
            final byte[] timestampKey = RocksDBMetronome.getBytes(evicted);
            try {
                final byte[] timestampedRecords = db.get(timestampsHandle, timestampKey);
                try(ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(timestampedRecords)) {
                    byte[] key = new byte[8];
                    // drop each status that was associated with the evicted timestamp
                    while(byteArrayInputStream.read(key, 0, 8) == 8) {
                        db.delete(key);
                    }
                }
            } catch (RocksDBException | IOException e) {
                logger.warn("Failed to remove expired statuses for timestamp {}", evicted, e);
            }
        }
    }

    /**
     * Adds the byte representation of the given long to the end of the given array, removing bytes from the beginning of the array if it has exceeded the max length
     *
     * @param l the long to add
     * @param bytes the byte representation of some number of longs
     * @return array representing the addition of the given long to the end of the list
     */
    byte[] appendLong(long l, byte[] bytes) {
        if(bytes == null || bytes.length == 0) {
            return RocksDBMetronome.getBytes(l);
        }

        int maxArrayLength = numDataPoints * 8;;
        if(bytes.length == maxArrayLength) {
            System.arraycopy(bytes, 8, bytes, 0, maxArrayLength - 8);
            RocksDBMetronome.writeLong(l, bytes, maxArrayLength - 8);
            return bytes;
        }

        byte[] newSnapshots = new byte[bytes.length + 8];
        System.arraycopy(bytes, 0, newSnapshots, 0, bytes.length);
        RocksDBMetronome.writeLong(l, newSnapshots, bytes.length);
        return newSnapshots;
    }

    /**
     * Prepares the given ProcessGroupStatuses for insertion into the databsae by populating status and detail maps
     *
     * @param groupStatus   the statuses to save
     * @param timestamp     the timestamp of the statuses
     * @param statusMap     a map to store the statuses
     * @param detailMap     a map to store the component details
     */
    private void captureComponentStatuses(ProcessGroupStatus groupStatus, Date timestamp, Map<String, byte[]> statusMap, Map<String, byte[]> detailMap) {
        // Capture status for the ProcessGroup
        final ComponentDetails groupDetails = ComponentDetails.forProcessGroup(groupStatus);
        final StatusSnapshot groupSnapshot = ComponentMetrics.createSnapshot(groupStatus, timestamp);
        updateStatusHistory(groupSnapshot, groupDetails, statusMap, detailMap);

        // Capture statuses for the Processors
        groupStatus.getProcessorStatus().forEach(processorStatus -> {
            ComponentDetails componentDetails = ComponentDetails.forProcessor(processorStatus);
            StatusSnapshot statusSnapshot = ComponentMetrics.createSnapshot(processorStatus, timestamp);
            updateStatusHistory(statusSnapshot, componentDetails, statusMap, detailMap);
        });

        // Capture statuses for the Connections
        groupStatus.getConnectionStatus().forEach(connectionStatus -> {
            ComponentDetails componentDetails = ComponentDetails.forConnection(connectionStatus);
            StatusSnapshot statusSnapshot = ComponentMetrics.createSnapshot(connectionStatus, timestamp);
            updateStatusHistory(statusSnapshot, componentDetails, statusMap, detailMap);
        });

        // Capture statuses for the Remote Process Groups
        groupStatus.getRemoteProcessGroupStatus().forEach(remoteProcessGroupStatus -> {
            ComponentDetails componentDetails = ComponentDetails.forRemoteProcessGroup(remoteProcessGroupStatus);
            StatusSnapshot statusSnapshot = ComponentMetrics.createSnapshot(remoteProcessGroupStatus, timestamp);
            updateStatusHistory(statusSnapshot, componentDetails, statusMap, detailMap);
        });

        // Capture statuses for the child groups
        groupStatus.getProcessGroupStatus().forEach(childGroup -> captureComponentStatuses(childGroup, timestamp, statusMap, detailMap));
    }

    /**
     * @param statusSnapshot    the status of the component
     * @param componentDetails  the component details of the component
     * @param statusMap         a map to store the statuses
     * @param detailMap         a map to store the component details
     */
    private void updateStatusHistory(StatusSnapshot statusSnapshot, ComponentDetails componentDetails, Map<String, byte[]> statusMap, Map<String, byte[]> detailMap) {
        String componentId = componentDetails.getComponentId();
        statusMap.put(componentId, marshall(statusSnapshot));
        detailMap.put(componentId, marshall(componentDetails));
    }

    byte[] marshall(Object obj) {
        return gson.toJson(obj).getBytes(UTF8);
    }

    private <T> T unmarshall(byte[] status, Class<T> returnType) {
        return gson.fromJson(new String(status, UTF8), returnType);
    }

    /**
     * This method will remove all of the stats and details for the specified component
     *
     * @param componentId the component to remove
     */
    boolean removeComponent(String componentId) {
        logger.info("Removing component id: {}", componentId);
        byte[] componentKey = componentId.getBytes(UTF8);
        try {
            byte[] historyIds = db.get(componentsHandle, componentKey);

            try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(historyIds)) {
                byte[] key = new byte[8];

                while(byteArrayInputStream.read(key, 0, 8) == 8) {
                    db.delete(key);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            // lastly, remove from details and components
            db.delete(componentDetailsHandle, componentKey);
            db.delete(componentsHandle, componentKey);

            // sync the db
            db.forceSync();
        } catch (RocksDBException e) {
            logger.warn("Could not remove status for component id: {}", componentId, e);
            return false;
        }
        return true;
    }

    @Override
    public void capture(ProcessGroupStatus rootGroupStatus, List<GarbageCollectionStatus> garbageCollectionStatus) {
        capture(rootGroupStatus, garbageCollectionStatus, new Date());
    }

    @Override
    public void capture(ProcessGroupStatus rootGroupStatus, List<GarbageCollectionStatus> garbageCollectionStatus, Date timestamp) {
        // Execute asynchronously to minimize timestamp drift (due to when capture() is called)
        // single thread means stats will be collected in order (important for our implementation)
        singleThreadExecutor.execute(() -> {
            Map<String, byte[]> statusMap = new HashMap<>();
            Map<String, byte[]> detailsMap = new HashMap<>();

            try {
                captureComponentStatuses(rootGroupStatus, timestamp, statusMap, detailsMap);
                persistUpdates(statusMap, detailsMap, garbageCollectionStatus, timestamp);
            } catch(Exception e) {
                logger.error("Failed to capture component stats for Stats History", e);
                return;
            }

            logger.debug("Capture metrics for {}", this);
            lastCaptureTime.accumulateAndGet(timestamp.getTime(), Math::max);

            // remove stats for components that have been removed
            Set<String> oldComponents = currentComponents;
            Set<String> newComponents = new HashSet<>(detailsMap.keySet());
            oldComponents.removeAll(newComponents);

            oldComponents.forEach(oldComponent -> {
                boolean removed = removeComponent(oldComponent);
                if(!removed) {
                    // Put it back on the list so we have another shot at removing it later
                    newComponents.add(oldComponent);
                }
            });

            currentComponents = newComponents;
        });
    }

    /**
     * Persists the stats to the underlying DB
     *
     * @param statusMap statuses to persist
     * @param detailMap details to persist
     * @param gcStatus  garbage collection details to persist
     * @param timestamp the timestamp of these stats
     * @throws IOException  if encountered
     * @throws RocksDBException if encountered
     */
    void persistUpdates(Map<String, byte[]> statusMap, Map<String, byte[]> detailMap, List<GarbageCollectionStatus> gcStatus, Date timestamp) throws IOException, RocksDBException {
        // Map of generated Ids to serialized records
        Map<Long, byte[]> recordMap = new HashMap<>();
        // Map of component Ids to record Ids
        Map<String, Long> componentUpdateMap = new HashMap<>();

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();

        long newGarbageCollectionId = prepareAndPersistRecords(statusMap, gcStatus, recordMap, componentUpdateMap);
        associateRecordsWithTimestamp(recordMap, timestamp);
        // add the timestamp to the ring buffer, and evict expired records
        addTimestamp(timestamp.getTime());
        associateRecordsWithColumnFamilies(componentUpdateMap, detailMap);
        updateGarbageCollectionKey(newGarbageCollectionId);

        stopWatch.stop();
        logger.info("Persisted {} items in {} milliseconds", recordMap.size(), stopWatch.getDuration(TimeUnit.MILLISECONDS));
    }

    /**
     * Parses data from incoming statuses and garbage collection details into other data objects for further processing
     * as well as persists records for each incoming status and GC detail to the underlying repository
     *
     * @param statusMap                 statuses to parse into the recordMap and componentUpdateMap
     * @param garbageCollectionStatuses garbage collection details to parse into the recordMap and componentUpdateMap
     * @param recordMap                 Will map newly generated ids to values parsed from the statusMap and gcStatus list and will
     *                                  be persisted
     * @param componentUpdateMap        maps entry keys to record ids for future processing and persisting
     * @return  Each incoming garbage collection status will be stored with a unique id. A record will be persisted that
     * contains a composite of all of those ids. This return value is the id of that record
     */
    long prepareAndPersistRecords(Map<String, byte[]> statusMap, List<GarbageCollectionStatus> garbageCollectionStatuses,
                                  Map<Long, byte[]> recordMap, Map<String, Long> componentUpdateMap) throws IOException, RocksDBException {
        // prep entries
        long totalIdsRequired = statusMap.entrySet().size() + garbageCollectionStatuses.size() + 1;
        AtomicLong idGenerator = new AtomicLong(reserveIds(totalIdsRequired));

        // For each record in the status map, generate an Id and prep for storage
        statusMap.forEach((key, value) -> {
            long newId = idGenerator.incrementAndGet();
            recordMap.put(newId, value);
            componentUpdateMap.put(key, newId);
        });

        // prep garbage collection for storage
        // The following id will be associated with a composite key made of one id per GarbageCollectionStatus
        long newGarbageCollectionStatusId = idGenerator.incrementAndGet();
        try(ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
            for (GarbageCollectionStatus garbageCollectionStatus : garbageCollectionStatuses) {
                long newId = idGenerator.incrementAndGet();
                recordMap.put(newId, marshall(garbageCollectionStatus));
                dataOutputStream.writeLong(newId);
            }
            recordMap.put(newGarbageCollectionStatusId, byteArrayOutputStream.toByteArray());
        }

        // persist recordMap
        for (Map.Entry<Long, byte[]> entry : recordMap.entrySet()) {
            db.put(RocksDBMetronome.getBytes(entry.getKey()), entry.getValue());
        }

        return newGarbageCollectionStatusId;
    }

    /**
     * Associates the provided records with a timestamp in the repository by aggregating their keys into a single byte array
     *
     * @param recordMap A map of records whose keys will be aggregated into a single byte array and then associated with
     *                  a given timestamp within the repository
     * @param timestamp The timestamp
     */
    void associateRecordsWithTimestamp(Map<Long, byte[]> recordMap, Date timestamp) throws IOException, RocksDBException {
        // collect all Ids for the specified timestamp
        byte[] recordIdsForTimestamp;
        try(ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
            for (Long key : recordMap.keySet()) {
                dataOutputStream.writeLong(key);
            }
            recordIdsForTimestamp = byteArrayOutputStream.toByteArray();
        }

        // force persist recordIdsForTimestamp
        // this is done first to ensure that records are aged off properly
        db.put(timestampsHandle, RocksDBMetronome.getBytes(timestamp.getTime()), recordIdsForTimestamp, true);
    }

    void associateRecordsWithColumnFamilies(Map<String, Long> componentUpdateMap, Map<String, byte[]> detailMap) throws RocksDBException {
        // iterate through componentUpdateMap, adding the new statuses for each component
        for (Map.Entry<String, Long> entry : componentUpdateMap.entrySet()) {
            byte[] key = entry.getKey().getBytes(UTF8);
            byte[] componentSnapshots = db.get(componentsHandle, key);
            byte[] newSnapshots = appendLong(entry.getValue(), componentSnapshots);
            db.put(componentsHandle, key, newSnapshots, false);
        }

        // iterate through detailMap, updating the componentDetails for each component
        for(Map.Entry<String, byte[]> entry : detailMap.entrySet()) {
            byte[] key = entry.getKey().getBytes(UTF8);
            db.put(componentDetailsHandle, key, entry.getValue(), false);
        }
    }

    /**
     * Takes the provided id and will add it to the record of all garbage collection status ids, potentially
     * expiring the oldest id if the configurable number of data points to keep has been exceeded
     *
     * @param newGarbageCollectionStatusId A new garbage collection status id to add to the existing id
     */
    private void updateGarbageCollectionKey(long newGarbageCollectionStatusId) throws RocksDBException {
        // update GarbageCollection with newGarbageCollectionStatusId, and forceSync
        byte[] garbageCollection = db.get(GARBAGE_COLLECTION_KEY);
        byte[] newGarbageCollection = appendLong(newGarbageCollectionStatusId, garbageCollection);
        db.put(GARBAGE_COLLECTION_KEY, newGarbageCollection, true);
    }

    /**
     * This method reserves a requested number of Ids and updates the information in the DB
     *
     * @param totalIdsRequired the quantity of Ids to reserve
     * @return the starting id
     * @throws RocksDBException if encountered
     * @throws IOException      if encountered
     */
    synchronized long reserveIds(long totalIdsRequired) throws RocksDBException, IOException {
        byte[] idBytes = db.getConfiguration(ID_KEY);
        long currentId = idBytes == null ? 0 : RocksDBMetronome.readLong(idBytes);
        long newId = currentId + totalIdsRequired;
        db.putConfiguration(ID_KEY, RocksDBMetronome.getBytes(newId));
        return currentId;
    }

    @Override
    public Date getLastCaptureDate() {
        return new Date(lastCaptureTime.get());
    }

    @Override
    public StatusHistory getConnectionStatusHistory(String connectionId, Date start, Date end, int preferredDataPoints) {
        return getStatusHistory(connectionId, true, DEFAULT_CONNECTION_METRICS);
    }

    @Override
    public StatusHistory getProcessGroupStatusHistory(String processGroupId, Date start, Date end, int preferredDataPoints) {
        return getStatusHistory(processGroupId, true, DEFAULT_GROUP_METRICS);
    }

    @Override
    public StatusHistory getProcessorStatusHistory(String processorId, Date start, Date end, int preferredDataPoints, boolean includeCounters) {
        return getStatusHistory(processorId, includeCounters, DEFAULT_PROCESSOR_METRICS);
    }

    @Override
    public StatusHistory getRemoteProcessGroupStatusHistory(String remoteGroupId, Date start, Date end, int preferredDataPoints) {
        return getStatusHistory(remoteGroupId, true, DEFAULT_RPG_METRICS);
    }

    @Override
    public GarbageCollectionHistory getGarbageCollectionHistory(Date start, Date end) {
        try {
            StandardGarbageCollectionHistory history = new StandardGarbageCollectionHistory();

            byte[] garbageCollectionKeysRecordKey = db.get(GARBAGE_COLLECTION_KEY);
            if(garbageCollectionKeysRecordKey != null) {
                byte[] garbageCollectionKeys = db.get(garbageCollectionKeysRecordKey);
                try(ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(garbageCollectionKeys)) {
                    byte[] key = new byte[8];
                    while(byteArrayInputStream.read(key, 0, 8) == 8) {
                        byte[] gc = db.get(key);
                        GarbageCollectionStatus garbageCollectionStatus = unmarshall(gc, StandardGarbageCollectionStatus.class);
                        if(garbageCollectionStatus == null || garbageCollectionStatus.getTimestamp().before(start) || garbageCollectionStatus.getTimestamp().after(end)) {
                            continue;
                        }
                        history.addGarbageCollectionStatus(garbageCollectionStatus);
                    }
                }
            }

            return history;
        } catch (RocksDBException | IOException e) {
            logger.error("Could not get Garbage Collection History", e);
            return null;
        }
    }

    /**
     * Get the Status history for the requested component
     *
     * @param componentId               the component to get the history of
     * @param includeCounters           whether to include counters
     * @param defaultMetricDescriptors  descriptors for the metrics
     * @return the status history
     */
    private StatusHistory getStatusHistory(String componentId, boolean includeCounters, Set<MetricDescriptor<?>> defaultMetricDescriptors) {
        try {
            byte[] componentKey = componentId.getBytes(UTF8);

            byte[] historyIds = db.get(componentsHandle, componentKey);

            if(historyIds == null || historyIds.length == 0) {
                return createEmptyStatusHistory();
            }

            Map<Long, StatusSnapshot> snapshotMap = new HashMap<>();
            List<CompletableFuture> queryFutures = new ArrayList<>();

            try(ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(historyIds)) {
                byte[] read = new byte[8];
                while(byteArrayInputStream.read(read, 0, 8) == 8) {
                    byte[] key = new byte[read.length];
                    System.arraycopy(read, 0, key, 0, key.length);

                    queryFutures.add(CompletableFuture.runAsync(() -> {
                        byte[] status;
                        try {
                            status = db.get(key);
                            if(status != null) {
                                StatusSnapshot statusSnapshot = unmarshall(status, StandardStatusSnapshot.class);
                                if(statusSnapshot != null) {
                                    snapshotMap.put(statusSnapshot.getTimestamp().getTime() / 1000L, statusSnapshot);
                                }
                            }
                        } catch(RocksDBException e) {
                            logger.warn("Could not retrieve status snapshot", e);
                        }
                    }, executorService));
                }

                // wait for futures to complete... throws TimeoutException
                CompletableFuture.allOf(queryFutures.toArray(new CompletableFuture[0])).get(30, TimeUnit.SECONDS);

                List<Long> dates = timestamps.asList();
                List<StatusSnapshot> snapshotList = new ArrayList<>(dates.size());

                for(long timestamp: dates) {
                    StatusSnapshot snapshot = snapshotMap.get(timestamp / 1000L);
                    if(snapshot == null) {
                        snapshotList.add(new EmptyStatusSnapshot(new Date(timestamp), defaultMetricDescriptors));
                    } else {
                        snapshotList.add(includeCounters ? snapshot : snapshot.withoutCounters());
                    }
                }

                byte[] detailBytes = db.get(componentDetailsHandle, componentKey);
                ComponentDetails componentDetails = unmarshall(detailBytes, ComponentDetails.class);

                return new StandardStatusHistory(snapshotList, componentDetails.toMap(), new Date());
            }
        } catch (Exception e) {
            logger.error("Could not get status history, returning empty history", e);
            return createEmptyStatusHistory();
        }
    }

    private StatusHistory createEmptyStatusHistory() {
        final Date dateGenerated = new Date();

        return new StatusHistory() {
            @Override
            public Date getDateGenerated() {
                return dateGenerated;
            }

            @Override
            public Map<String, String> getComponentDetails() {
                return Collections.emptyMap();
            }

            @Override
            public List<StatusSnapshot> getStatusSnapshots() {
                return Collections.emptyList();
            }
        };
    }
}
