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
import org.apache.nifi.controller.status.history.GarbageCollectionStatus;
import org.apache.nifi.controller.status.history.StandardMetricDescriptor;
import org.apache.nifi.controller.status.history.StandardStatusSnapshot;
import org.apache.nifi.controller.status.history.StatusHistoryStorage;
import org.apache.nifi.controller.status.history.StatusSnapshot;
import org.apache.nifi.questdb.Client;
import org.apache.nifi.questdb.DatabaseException;
import org.apache.nifi.questdb.InsertMapping;
import org.apache.nifi.questdb.InsertRowDataSource;
import org.apache.nifi.questdb.QueryResultProcessor;
import org.apache.nifi.questdb.RequestMapping;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.controller.status.history.questdb.EmbeddedQuestDbTableDefinitions.COMPONENT_STATUS_QUERY;
import static org.apache.nifi.controller.status.history.questdb.EmbeddedQuestDbTableDefinitions.CONNECTION_STATUS_INSERT_MAPPING;
import static org.apache.nifi.controller.status.history.questdb.EmbeddedQuestDbTableDefinitions.CONNECTION_STATUS_REQUEST_MAPPING;
import static org.apache.nifi.controller.status.history.questdb.EmbeddedQuestDbTableDefinitions.GARBAGE_COLLECTION_STATUS_INSERT_MAPPING;
import static org.apache.nifi.controller.status.history.questdb.EmbeddedQuestDbTableDefinitions.NODE_STATUS_INSERT_MAPPING;
import static org.apache.nifi.controller.status.history.questdb.EmbeddedQuestDbTableDefinitions.NODE_STATUS_QUERY;
import static org.apache.nifi.controller.status.history.questdb.EmbeddedQuestDbTableDefinitions.PROCESSOR_STATUS_INSERT_MAPPING;
import static org.apache.nifi.controller.status.history.questdb.EmbeddedQuestDbTableDefinitions.PROCESSOR_STATUS_REQUEST_MAPPING;
import static org.apache.nifi.controller.status.history.questdb.EmbeddedQuestDbTableDefinitions.PROCESS_GROUP_STATUS_INSERT_MAPPING;
import static org.apache.nifi.controller.status.history.questdb.EmbeddedQuestDbTableDefinitions.PROCESS_GROUP_STATUS_REQUEST_MAPPING;
import static org.apache.nifi.controller.status.history.questdb.EmbeddedQuestDbTableDefinitions.REMOTE_PROCESS_GROUP_STATUS_INSERT_MAPPING;
import static org.apache.nifi.controller.status.history.questdb.EmbeddedQuestDbTableDefinitions.REMOTE_PROCESS_GROUP_STATUS_REQUEST_MAPPING;
import static org.apache.nifi.controller.status.history.questdb.EmbeddedQuestDbTableDefinitions.STATUS_QUERY_GARBAGE_COLLECTION;
import static org.apache.nifi.controller.status.history.questdb.EmbeddedQuestDbTableDefinitions.STORAGE_STATUS_QUERY;
import static org.apache.nifi.controller.status.history.questdb.EmbeddedQuestDbTableDefinitions.TABLE_NAME_COMPONENT_COUNTER;
import static org.apache.nifi.controller.status.history.questdb.EmbeddedQuestDbTableDefinitions.TABLE_NAME_CONNECTION_STATUS;
import static org.apache.nifi.controller.status.history.questdb.EmbeddedQuestDbTableDefinitions.TABLE_NAME_GARBAGE_COLLECTION_STATUS;
import static org.apache.nifi.controller.status.history.questdb.EmbeddedQuestDbTableDefinitions.TABLE_NAME_NODE_STATUS;
import static org.apache.nifi.controller.status.history.questdb.EmbeddedQuestDbTableDefinitions.TABLE_NAME_PROCESSOR_STATUS;
import static org.apache.nifi.controller.status.history.questdb.EmbeddedQuestDbTableDefinitions.TABLE_NAME_PROCESS_GROUP_STATUS;
import static org.apache.nifi.controller.status.history.questdb.EmbeddedQuestDbTableDefinitions.TABLE_NAME_REMOTE_PROCESS_GROUP_STATUS;
import static org.apache.nifi.controller.status.history.questdb.EmbeddedQuestDbTableDefinitions.TABLE_NAME_STORAGE_STATUS;

final class QuestDbStatusHistoryStorage implements StatusHistoryStorage {
    private static final Logger LOGGER = LoggerFactory.getLogger(QuestDbStatusHistoryStorage.class);

    private final Client client;

    public QuestDbStatusHistoryStorage(final Client client) {
        this.client = client;
    }

    @Override
    public List<StatusSnapshot> getConnectionSnapshots(final String componentId, final Date start, final Date end) {
        return getComponentSnapshots(TABLE_NAME_CONNECTION_STATUS, componentId, CONNECTION_STATUS_REQUEST_MAPPING, start, end);
    }

    @Override
    public List<StatusSnapshot> getProcessGroupSnapshots(final String componentId, final Date start, final Date end) {
        return getComponentSnapshots(TABLE_NAME_PROCESS_GROUP_STATUS, componentId, PROCESS_GROUP_STATUS_REQUEST_MAPPING, start, end);
    }

    @Override
    public List<StatusSnapshot> getRemoteProcessGroupSnapshots(final String componentId, final Date start, final Date end) {
        return getComponentSnapshots(TABLE_NAME_REMOTE_PROCESS_GROUP_STATUS, componentId, REMOTE_PROCESS_GROUP_STATUS_REQUEST_MAPPING, start, end);
    }

    @Override
    public List<StatusSnapshot> getProcessorSnapshots(final String componentId, final Date start, final Date end) {
        return getComponentSnapshots(TABLE_NAME_PROCESSOR_STATUS, componentId, PROCESSOR_STATUS_REQUEST_MAPPING, start, end);
    }

    @Override
    public List<StatusSnapshot> getProcessorSnapshotsWithCounters(final String componentId, final Date start, final Date end) {
        final List<StatusSnapshot> componentSnapshots = getComponentSnapshots(TABLE_NAME_PROCESSOR_STATUS, componentId, PROCESSOR_STATUS_REQUEST_MAPPING, start, end);
        final String query = String.format(COMPONENT_STATUS_QUERY, TABLE_NAME_COMPONENT_COUNTER, componentId, getStartTime(start), getEndTime(end));
        return getResult(query, new CounterStatisticsResultProcessor(componentSnapshots), Collections.emptyList());
    }

    @Override
    public List<GarbageCollectionStatus> getGarbageCollectionSnapshots(final Date start, final Date end) {
        final String query = String.format(STATUS_QUERY_GARBAGE_COLLECTION, getStartTime(start), getEndTime(end));
        return getResult(query, new GarbageCollectionQueryResultProcessor(), Collections.emptyList());
    }

    @Override
    public List<StatusSnapshot> getNodeStatusSnapshots(final Date start, final Date end) {
        final String storageStatusQuery = String.format(STORAGE_STATUS_QUERY, getStartTime(start), getEndTime(end));
        final Map<Long, Map<StandardMetricDescriptor<NodeStatus>, Long>> statusMetricsByTime = getResult(storageStatusQuery, new StorageStatusResultProcessor(), new HashMap<>());
        final String nodeStatusQuery = String.format(NODE_STATUS_QUERY, getStartTime(start), getEndTime(end));
        return getSnapshot(nodeStatusQuery, new NodeStatusResultProcessor(statusMetricsByTime));
    }

    @Override
    public void storeNodeStatuses(final Collection<NodeStatus> statuses) {
        store(TABLE_NAME_NODE_STATUS, NODE_STATUS_INSERT_MAPPING, statuses);
        store(TABLE_NAME_STORAGE_STATUS, StorateStatusInsertRowDataSource.getInstance(statuses));
    }

    @Override
    public void storeGarbageCollectionStatuses(final Collection<GarbageCollectionStatus> statuses) {
        store(TABLE_NAME_GARBAGE_COLLECTION_STATUS, GARBAGE_COLLECTION_STATUS_INSERT_MAPPING, statuses);
    }

    @Override
    public void storeProcessGroupStatuses(final Collection<ProcessGroupStatus> statuses) {
        store(TABLE_NAME_PROCESS_GROUP_STATUS,   PROCESS_GROUP_STATUS_INSERT_MAPPING, statuses);
    }

    @Override
    public void storeConnectionStatuses(final Collection<ConnectionStatus> statuses) {
        store(TABLE_NAME_CONNECTION_STATUS, CONNECTION_STATUS_INSERT_MAPPING, statuses);
    }

    @Override
    public void storeRemoteProcessorGroupStatuses(final Collection<RemoteProcessGroupStatus> statuses) {
        store(TABLE_NAME_REMOTE_PROCESS_GROUP_STATUS, REMOTE_PROCESS_GROUP_STATUS_INSERT_MAPPING, statuses);
    }

    @Override
    public void storeProcessorStatuses(final Collection<ProcessorStatus> statuses) {
        store(TABLE_NAME_PROCESSOR_STATUS, PROCESSOR_STATUS_INSERT_MAPPING, statuses);
        store(TABLE_NAME_COMPONENT_COUNTER, CounterStatisticsInsertRowDataSource.getInstance(statuses));
    }

    private <T> void store(final String tableName, final InsertMapping<T> mapping, final Collection<T> statuses) {
        store(tableName, InsertRowDataSource.forMapping(mapping, statuses));
    }

    private <T> void store(final String tableName, final InsertRowDataSource source) {
        try {
            client.insert(tableName, source);
        } catch (final DatabaseException e) {
            LOGGER.error("Error during storing snapshots to " + tableName, e);
        }
    }

    private List<StatusSnapshot> getComponentSnapshots(final String tableName, final String componentId, final RequestMapping<StandardStatusSnapshot> mapping, final Date start, final Date end) {
        final String query = String.format(COMPONENT_STATUS_QUERY, tableName, componentId, getStartTime(start), getEndTime(end));
        return getSnapshot(query, QueryResultProcessor.forMapping(mapping));
    }

    private List<StatusSnapshot> getSnapshot(final String query, final QueryResultProcessor<List<StandardStatusSnapshot>> rowProcessor) {
        return new ArrayList<>(getResult(query, rowProcessor,  Collections.emptyList()));
    }

    private <T> T getResult(final String query, final QueryResultProcessor<T> rowProcessor, final T errorResult) {
        try {
            return client.query(query, rowProcessor);
        } catch (final DatabaseException e) {
            LOGGER.error("Error during returning results for query " + query, e);
            return errorResult;
        }
    }

    private static String getStartTime(final Date start) {
        final Instant startTime = (start == null) ? Instant.now().minus(1, ChronoUnit.DAYS) : start.toInstant();
        return EmbeddedQuestDbTableDefinitions.DATE_FORMATTER.format(startTime);
    }

    private static String getEndTime(final Date end) {
        final Instant endTime = (end == null) ? Instant.now() : end.toInstant();
        return EmbeddedQuestDbTableDefinitions.DATE_FORMATTER.format(endTime);
    }
}
