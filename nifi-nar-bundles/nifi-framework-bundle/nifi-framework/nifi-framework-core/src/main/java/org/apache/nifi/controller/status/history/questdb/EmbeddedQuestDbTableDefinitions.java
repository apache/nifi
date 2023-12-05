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
package org.apache.nifi.controller.status.history.questdb;

import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.NodeStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.controller.status.RemoteProcessGroupStatus;
import org.apache.nifi.controller.status.history.ConnectionStatusDescriptor;
import org.apache.nifi.controller.status.history.GarbageCollectionStatus;
import org.apache.nifi.controller.status.history.MetricDescriptor;
import org.apache.nifi.controller.status.history.NodeStatusDescriptor;
import org.apache.nifi.controller.status.history.ProcessGroupStatusDescriptor;
import org.apache.nifi.controller.status.history.ProcessorStatusDescriptor;
import org.apache.nifi.controller.status.history.RemoteProcessGroupStatusDescriptor;
import org.apache.nifi.controller.status.history.StandardStatusSnapshot;
import org.apache.nifi.questdb.InsertMapping;
import org.apache.nifi.questdb.InsertMappingBuilder;
import org.apache.nifi.questdb.RequestMapping;
import org.apache.nifi.questdb.RequestMappingBuilder;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

final class EmbeddedQuestDbTableDefinitions {
    /**
     * Date format expected by the storage.
     */
    static final String CAPTURE_DATE_FORMAT = "yyyy-MM-dd:HH:mm:ss Z";

    /**
     * Date formatter for the database fields.
     */
    static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern(CAPTURE_DATE_FORMAT).withZone(ZoneId.systemDefault());

    // General component

    static final String COMPONENT_STATUS_QUERY =
        "SELECT * FROM %s " +
        "WHERE componentId = '%s' " +
        "AND capturedAt > to_timestamp('%s', '" + CAPTURE_DATE_FORMAT + "') " +
        "AND capturedAt < to_timestamp('%s', '" + CAPTURE_DATE_FORMAT + "') " +
        "ORDER BY capturedAt ASC";

    // Connection

    static final String TABLE_NAME_CONNECTION_STATUS = "connectionStatus";
    static final String CREATE_CONNECTION_STATUS =
        "CREATE TABLE " + TABLE_NAME_CONNECTION_STATUS + " (" +
        "capturedAt TIMESTAMP," +
        "componentId SYMBOL capacity 2000 nocache index capacity 1500," +
        "inputBytes LONG," +
        "inputCount LONG," +
        "outputBytes LONG," +
        "outputCount LONG," +
        "queuedBytes LONG," +
        "queuedCount LONG," +
        "totalQueuedDuration LONG," +
        "maxQueuedDuration LONG," +
        "averageQueuedDuration LONG" +
        ") TIMESTAMP(capturedAt) PARTITION BY DAY";

    static final Map<Integer, MetricDescriptor<ConnectionStatus>> CONNECTION_METRICS = new HashMap<>() {{
        put(2, ConnectionStatusDescriptor.INPUT_BYTES.getDescriptor());
        put(3, ConnectionStatusDescriptor.INPUT_COUNT.getDescriptor());
        put(4, ConnectionStatusDescriptor.OUTPUT_BYTES.getDescriptor());
        put(5, ConnectionStatusDescriptor.OUTPUT_COUNT.getDescriptor());
        put(6, ConnectionStatusDescriptor.QUEUED_BYTES.getDescriptor());
        put(7, ConnectionStatusDescriptor.QUEUED_COUNT.getDescriptor());
        put(8, ConnectionStatusDescriptor.TOTAL_QUEUED_DURATION.getDescriptor());
        put(9, ConnectionStatusDescriptor.MAX_QUEUED_DURATION.getDescriptor());
        put(10, ConnectionStatusDescriptor.AVERAGE_QUEUED_DURATION.getDescriptor());
    }};

    static final InsertMapping<ConnectionStatus> CONNECTION_STATUS_INSERT_MAPPING  = getInsertMapping(
        ConnectionStatus.class,
        ConnectionStatus::getId,
        s -> Instant.ofEpochMilli(s.getCreatedAtInMs()),
        CONNECTION_METRICS.values()
    );
    static final RequestMapping<StandardStatusSnapshot> CONNECTION_STATUS_REQUEST_MAPPING = getSnapshotRequestMapping(ConnectionStatus.class, CONNECTION_METRICS.values());

    // Processor

    static final String TABLE_NAME_PROCESSOR_STATUS = "processorStatus";
    static final String CREATE_PROCESSOR_STATUS =
        "CREATE TABLE " + TABLE_NAME_PROCESSOR_STATUS + " (" +
        "capturedAt TIMESTAMP," +
        "componentId SYMBOL capacity 2000 nocache index capacity 1500," +
        "bytesRead LONG," +
        "bytesWritten LONG," +
        "bytesTransferred LONG," +
        "inputBytes LONG," +
        "inputCount LONG," +
        "outputBytes LONG," +
        "outputCount LONG," +
        "taskCount LONG," +
        "taskMillis LONG," +
        "taskNanos LONG," +
        "flowFilesRemoved LONG," +
        "averageLineageDuration LONG," +
        "averageTaskNanos LONG" +
        ") TIMESTAMP(capturedAt) PARTITION BY DAY";

    static final Map<Integer, MetricDescriptor<ProcessorStatus>> PROCESSOR_METRICS = new HashMap<>() {{
        put(2, ProcessorStatusDescriptor.BYTES_READ.getDescriptor());
        put(3, ProcessorStatusDescriptor.BYTES_WRITTEN.getDescriptor());
        put(4, ProcessorStatusDescriptor.BYTES_TRANSFERRED.getDescriptor());
        put(5, ProcessorStatusDescriptor.INPUT_BYTES.getDescriptor());
        put(6, ProcessorStatusDescriptor.INPUT_COUNT.getDescriptor());
        put(7, ProcessorStatusDescriptor.OUTPUT_BYTES.getDescriptor());
        put(8, ProcessorStatusDescriptor.OUTPUT_COUNT.getDescriptor());
        put(9, ProcessorStatusDescriptor.TASK_COUNT.getDescriptor());
        put(10, ProcessorStatusDescriptor.TASK_MILLIS.getDescriptor());
        put(11, ProcessorStatusDescriptor.TASK_NANOS.getDescriptor());
        put(12, ProcessorStatusDescriptor.FLOWFILES_REMOVED.getDescriptor());
        put(13, ProcessorStatusDescriptor.AVERAGE_LINEAGE_DURATION.getDescriptor());
        put(14, ProcessorStatusDescriptor.AVERAGE_TASK_NANOS.getDescriptor());
    }};

    static final InsertMapping<ProcessorStatus> PROCESSOR_STATUS_INSERT_MAPPING = getInsertMapping(
        ProcessorStatus.class,
        ProcessorStatus::getId,
        s -> Instant.ofEpochMilli(s.getCreatedAtInMs()),
        PROCESSOR_METRICS.values()
    );
    static final RequestMapping<StandardStatusSnapshot> PROCESSOR_STATUS_REQUEST_MAPPING = getSnapshotRequestMapping(ProcessorStatus.class, PROCESSOR_METRICS.values());

    //  Process group

    static final String TABLE_NAME_PROCESS_GROUP_STATUS = "processGroupStatus";
    static final String CREATE_PROCESS_GROUP_STATUS =
        "CREATE TABLE " + TABLE_NAME_PROCESS_GROUP_STATUS + " (" +
        "capturedAt TIMESTAMP," +
        "componentId SYMBOL capacity 2000 nocache index capacity 1500," +
        "bytesRead LONG," +
        "bytesWritten LONG," +
        "bytesTransferred LONG," +
        "inputBytes LONG," +
        "inputCount LONG," +
        "outputBytes LONG," +
        "outputCount LONG," +
        "queuedBytes LONG," +
        "queuedCount LONG," +
        "taskMillis LONG" +
        ") TIMESTAMP(capturedAt) PARTITION BY DAY";

    static final Map<Integer, MetricDescriptor<ProcessGroupStatus>> PROCESS_GROUP_METRICS = new HashMap<>() {{
        put(2, ProcessGroupStatusDescriptor.BYTES_READ.getDescriptor());
        put(3, ProcessGroupStatusDescriptor.BYTES_WRITTEN.getDescriptor());
        put(4, ProcessGroupStatusDescriptor.BYTES_TRANSFERRED.getDescriptor());
        put(5, ProcessGroupStatusDescriptor.INPUT_BYTES.getDescriptor());
        put(6, ProcessGroupStatusDescriptor.INPUT_COUNT.getDescriptor());
        put(7, ProcessGroupStatusDescriptor.OUTPUT_BYTES.getDescriptor());
        put(8, ProcessGroupStatusDescriptor.OUTPUT_COUNT.getDescriptor());
        put(9, ProcessGroupStatusDescriptor.QUEUED_BYTES.getDescriptor());
        put(10, ProcessGroupStatusDescriptor.QUEUED_COUNT.getDescriptor());
        put(11, ProcessGroupStatusDescriptor.TASK_MILLIS.getDescriptor());
    }};

    static final InsertMapping<ProcessGroupStatus> PROCESS_GROUP_STATUS_INSERT_MAPPING  = getInsertMapping(
        ProcessGroupStatus.class,
        ProcessGroupStatus::getId,
        s -> Instant.ofEpochMilli(s.getCreatedAtInMs()),
        PROCESS_GROUP_METRICS.values()
    );
    static final RequestMapping<StandardStatusSnapshot> PROCESS_GROUP_STATUS_REQUEST_MAPPING = getSnapshotRequestMapping(ProcessGroupStatus.class, PROCESS_GROUP_METRICS.values());

    // Remote process group

    static final String TABLE_NAME_REMOTE_PROCESS_GROUP_STATUS = "remoteProcessGroupStatus";
    static final String CREATE_REMOTE_PROCESS_GROUP_STATUS =
        "CREATE TABLE " + TABLE_NAME_REMOTE_PROCESS_GROUP_STATUS + " (" +
        "capturedAt TIMESTAMP," +
        "componentId SYMBOL capacity 2000 nocache index capacity 1500," +
        "sentBytes LONG," +
        "sentCount LONG," +
        "receivedBytes LONG," +
        "receivedCount LONG," +
        "receivedBytesPerSecond LONG," +
        "sentBytesPerSecond LONG," +
        "totalBytesPerSecond LONG," +
        "averageLineageDuration LONG" +
        ") TIMESTAMP(capturedAt) PARTITION BY DAY";

    static final Map<Integer, MetricDescriptor<RemoteProcessGroupStatus>> REMOTE_PROCESS_GROUP_METRICS = new HashMap<>() {{
        put(2, RemoteProcessGroupStatusDescriptor.SENT_BYTES.getDescriptor());
        put(3, RemoteProcessGroupStatusDescriptor.SENT_COUNT.getDescriptor());
        put(4, RemoteProcessGroupStatusDescriptor.RECEIVED_BYTES.getDescriptor());
        put(5, RemoteProcessGroupStatusDescriptor.RECEIVED_COUNT.getDescriptor());
        put(6, RemoteProcessGroupStatusDescriptor.RECEIVED_BYTES_PER_SECOND.getDescriptor());
        put(7, RemoteProcessGroupStatusDescriptor.SENT_BYTES_PER_SECOND.getDescriptor());
        put(8, RemoteProcessGroupStatusDescriptor.TOTAL_BYTES_PER_SECOND.getDescriptor());
        put(9, RemoteProcessGroupStatusDescriptor.AVERAGE_LINEAGE_DURATION.getDescriptor());
    }};

    static final InsertMapping<RemoteProcessGroupStatus> REMOTE_PROCESS_GROUP_STATUS_INSERT_MAPPING  = getInsertMapping(
        RemoteProcessGroupStatus.class,
        RemoteProcessGroupStatus::getId,
        s -> Instant.ofEpochMilli(s.getCreatedAtInMs()),
        REMOTE_PROCESS_GROUP_METRICS.values()
    );
    static final RequestMapping<StandardStatusSnapshot> REMOTE_PROCESS_GROUP_STATUS_REQUEST_MAPPING = getSnapshotRequestMapping(RemoteProcessGroupStatus.class, REMOTE_PROCESS_GROUP_METRICS.values());

    // Garbage collection status

    static final String TABLE_NAME_GARBAGE_COLLECTION_STATUS = "garbageCollectionStatus";
    static final String CREATE_GARBAGE_COLLECTION_STATUS =
        "CREATE TABLE " + TABLE_NAME_GARBAGE_COLLECTION_STATUS + " (" +
        "capturedAt TIMESTAMP," +
        "memoryManagerName SYMBOL capacity 4 nocache," +
        "collectionCount LONG," +
        "collectionMinis LONG" +
        ") TIMESTAMP(capturedAt) PARTITION BY DAY";
    static final String STATUS_QUERY_GARBAGE_COLLECTION = // TODO COMPONENT_STATUS_QUERY?
        "SELECT * FROM garbageCollectionStatus " +
        "WHERE capturedAt > to_timestamp('%s', '" + CAPTURE_DATE_FORMAT + "') " +
        "AND capturedAt < to_timestamp('%s', '" + CAPTURE_DATE_FORMAT +  "') " +
        "ORDER BY capturedAt ASC";
    static final InsertMapping<GarbageCollectionStatus> GARBAGE_COLLECTION_STATUS_INSERT_MAPPING = InsertMappingBuilder
            .of(GarbageCollectionStatus.class)
            .timestampAt(0)
            .addInstantField(g -> g.getTimestamp().toInstant())
            .addStringField(GarbageCollectionStatus::getMemoryManagerName)
            .addLongField(GarbageCollectionStatus::getCollectionCount)
            .addLongField(GarbageCollectionStatus::getCollectionMillis)
            .build();

    // Component counter

    static final String TABLE_NAME_COMPONENT_COUNTER = "componentCounter";
    static final String CREATE_COMPONENT_COUNTER =
        "CREATE TABLE " + TABLE_NAME_COMPONENT_COUNTER + " (" +
        "capturedAt TIMESTAMP," +
        "componentId SYMBOL capacity 2000 nocache index capacity 1500," +
        "name SYMBOL capacity 256 nocache," +
        "value LONG" +
        ") TIMESTAMP(capturedAt) PARTITION BY DAY";

    // Storage status

    static final String TABLE_NAME_STORAGE_STATUS = "storageStatus";
    static final String CREATE_STORAGE_STATUS =
        "CREATE TABLE " + TABLE_NAME_STORAGE_STATUS + " (" +
        "capturedAt TIMESTAMP," +
        "name SYMBOL capacity 256 nocache," +
        "storageType SHORT," +
        "freeSpace LONG," +
        "usedSpace LONG" +
        ") TIMESTAMP(capturedAt) PARTITION BY DAY";
    static final String STORAGE_STATUS_QUERY =
        "SELECT * FROM storageStatus " +
        "WHERE capturedAt > to_timestamp('%s', '" + CAPTURE_DATE_FORMAT + "') " +
        "AND capturedAt < to_timestamp('%s', '" + CAPTURE_DATE_FORMAT + "') " +
        "ORDER BY capturedAt ASC";

    // Node status

    static final String TABLE_NAME_NODE_STATUS = "nodeStatus";
    static final String CREATE_NODE_STATUS =
        "CREATE TABLE " + TABLE_NAME_NODE_STATUS + " (" +
        "capturedAt TIMESTAMP," +
        "freeHeap LONG," +
        "usedHeap LONG," +
        "heapUtilization LONG," +
        "freeNonHeap LONG," +
        "usedNonHeap LONG," +
        "openFileHandlers LONG," +
        "processorLoadAverage DOUBLE," +
        "totalThreads LONG," +
        "timerDrivenThreads LONG" +
        ") TIMESTAMP(capturedAt) PARTITION BY DAY";
    static final String NODE_STATUS_QUERY =
        "SELECT * FROM nodeStatus " +
        "WHERE capturedAt > to_timestamp('%s', '" + CAPTURE_DATE_FORMAT + "') " +
        "AND capturedAt < to_timestamp('%s', '" + CAPTURE_DATE_FORMAT + "') " +
        "ORDER BY capturedAt ASC";

    static final Map<Integer, MetricDescriptor<NodeStatus>> NODE_STATUS_METRICS = new HashMap<>() {{
       put(1, NodeStatusDescriptor.FREE_HEAP.getDescriptor());
       put(2, NodeStatusDescriptor.USED_HEAP.getDescriptor());
       put(3, NodeStatusDescriptor.HEAP_UTILIZATION.getDescriptor());
       put(4, NodeStatusDescriptor.FREE_NON_HEAP.getDescriptor());
       put(5, NodeStatusDescriptor.USED_NON_HEAP.getDescriptor());
       put(6, NodeStatusDescriptor.OPEN_FILE_HANDLES.getDescriptor());
       put(7, NodeStatusDescriptor.PROCESSOR_LOAD_AVERAGE.getDescriptor());
       put(8, NodeStatusDescriptor.TOTAL_THREADS.getDescriptor());
       put(9, NodeStatusDescriptor.TIME_DRIVEN_THREADS.getDescriptor());
    }};

    static final InsertMapping<NodeStatus> NODE_STATUS_INSERT_MAPPING;

    static {
        final InsertMappingBuilder<NodeStatus> insertMappingBuilder = InsertMappingBuilder
            .of(NodeStatus.class)
            .timestampAt(0)
            .addField(Instant.class, s -> Instant.ofEpochMilli(s.getCreatedAtInMs()));
        NODE_STATUS_METRICS.values().forEach(d -> insertMappingBuilder.addLongField(s -> d.getValueFunction().getValue(s)));
        NODE_STATUS_INSERT_MAPPING = insertMappingBuilder.build();
    }

    private static <T> InsertMapping<T> getInsertMapping(
            final Class<T> type,
            final Function<T, String> idMapping,
            final Function<T, Instant> timestampMapping,
            final Collection<MetricDescriptor<T>> descriptors) {
        final InsertMappingBuilder<T> insertMappingBuilder = InsertMappingBuilder.of(type).timestampAt(0);
        insertMappingBuilder.addField(Instant.class, timestampMapping);
        insertMappingBuilder.addField(String.class, idMapping);

        descriptors.forEach(d -> insertMappingBuilder.addLongField(s -> d.getValueFunction().getValue(s)));
        return insertMappingBuilder.build();
    }

    private static <T> RequestMapping<StandardStatusSnapshot> getSnapshotRequestMapping(Class<T> type, Collection<MetricDescriptor<T>> descriptorSource) {
        final RequestMappingBuilder<StandardStatusSnapshot> requestMappingBuilder = RequestMappingBuilder
                .of(() -> new StandardStatusSnapshot(new HashSet<>(descriptorSource)))
                .addLongField((snapshot, field) -> snapshot.setTimestamp(new Date(TimeUnit.MICROSECONDS.toMillis(field))))
                .addStringField((snapshot, field) -> {}); // Id is not used
        descriptorSource.forEach(descriptor -> requestMappingBuilder.addLongField((snapshot, field) -> snapshot.addStatusMetric(descriptor, field)));
        return requestMappingBuilder.build();
    }

    private EmbeddedQuestDbTableDefinitions() {
        // Not to be instantiated
    }
}
