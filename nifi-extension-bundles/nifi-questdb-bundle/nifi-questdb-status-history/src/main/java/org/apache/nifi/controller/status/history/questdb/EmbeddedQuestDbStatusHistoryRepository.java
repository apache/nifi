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
import org.apache.nifi.controller.status.history.ComponentDetails;
import org.apache.nifi.controller.status.history.GarbageCollectionHistory;
import org.apache.nifi.controller.status.history.GarbageCollectionStatus;
import org.apache.nifi.controller.status.history.StandardGarbageCollectionHistory;
import org.apache.nifi.controller.status.history.StandardStatusHistory;
import org.apache.nifi.controller.status.history.StatusHistory;
import org.apache.nifi.controller.status.history.StatusHistoryRepository;
import org.apache.nifi.controller.status.history.StatusSnapshot;
import org.apache.nifi.questdb.DatabaseManager;
import org.apache.nifi.questdb.embedded.EmbeddedDatabaseManagerBuilder;
import org.apache.nifi.questdb.rollover.RolloverStrategy;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.apache.nifi.controller.status.history.questdb.EmbeddedQuestDbStatusHistoryRepositoryDefinitions.CREATE_COMPONENT_COUNTER;
import static org.apache.nifi.controller.status.history.questdb.EmbeddedQuestDbStatusHistoryRepositoryDefinitions.CREATE_CONNECTION_STATUS;
import static org.apache.nifi.controller.status.history.questdb.EmbeddedQuestDbStatusHistoryRepositoryDefinitions.CREATE_GARBAGE_COLLECTION_STATUS;
import static org.apache.nifi.controller.status.history.questdb.EmbeddedQuestDbStatusHistoryRepositoryDefinitions.CREATE_NODE_STATUS;
import static org.apache.nifi.controller.status.history.questdb.EmbeddedQuestDbStatusHistoryRepositoryDefinitions.CREATE_PROCESSOR_STATUS;
import static org.apache.nifi.controller.status.history.questdb.EmbeddedQuestDbStatusHistoryRepositoryDefinitions.CREATE_PROCESS_GROUP_STATUS;
import static org.apache.nifi.controller.status.history.questdb.EmbeddedQuestDbStatusHistoryRepositoryDefinitions.CREATE_REMOTE_PROCESS_GROUP_STATUS;
import static org.apache.nifi.controller.status.history.questdb.EmbeddedQuestDbStatusHistoryRepositoryDefinitions.CREATE_STORAGE_STATUS;
import static org.apache.nifi.controller.status.history.questdb.EmbeddedQuestDbStatusHistoryRepositoryDefinitions.TABLE_NAME_COMPONENT_COUNTER;
import static org.apache.nifi.controller.status.history.questdb.EmbeddedQuestDbStatusHistoryRepositoryDefinitions.TABLE_NAME_CONNECTION_STATUS;
import static org.apache.nifi.controller.status.history.questdb.EmbeddedQuestDbStatusHistoryRepositoryDefinitions.TABLE_NAME_GARBAGE_COLLECTION_STATUS;
import static org.apache.nifi.controller.status.history.questdb.EmbeddedQuestDbStatusHistoryRepositoryDefinitions.TABLE_NAME_NODE_STATUS;
import static org.apache.nifi.controller.status.history.questdb.EmbeddedQuestDbStatusHistoryRepositoryDefinitions.TABLE_NAME_PROCESSOR_STATUS;
import static org.apache.nifi.controller.status.history.questdb.EmbeddedQuestDbStatusHistoryRepositoryDefinitions.TABLE_NAME_PROCESS_GROUP_STATUS;
import static org.apache.nifi.controller.status.history.questdb.EmbeddedQuestDbStatusHistoryRepositoryDefinitions.TABLE_NAME_REMOTE_PROCESS_GROUP_STATUS;
import static org.apache.nifi.controller.status.history.questdb.EmbeddedQuestDbStatusHistoryRepositoryDefinitions.TABLE_NAME_STORAGE_STATUS;

public class EmbeddedQuestDbStatusHistoryRepository implements StatusHistoryRepository {
    private static final Logger LOGGER = LoggerFactory.getLogger(EmbeddedQuestDbStatusHistoryRepository.class);

    private final InMemoryComponentDetailsStorage componentDetailsProvider = new InMemoryComponentDetailsStorage();
    private final NiFiProperties niFiProperties;
    private DatabaseManager databaseManager;
    private StatusHistoryStorage storage;

    public EmbeddedQuestDbStatusHistoryRepository(final NiFiProperties niFiProperties) {
        this.niFiProperties = niFiProperties;
    }

    @Override
    public void start() {
        LOGGER.debug("Repository start initiated");
        final RolloverStrategy nodeStatusRolloverStrategy = RolloverStrategy.deleteOld(getDaysToKeepNodeData(niFiProperties));
        final RolloverStrategy componentStatusRolloverStrategy = RolloverStrategy.deleteOld(getDaysToKeepComponentData(niFiProperties));

        databaseManager = EmbeddedDatabaseManagerBuilder
                .builder(niFiProperties.getQuestDbStatusRepositoryPath())
                .backupLocation(niFiProperties.getQuestDbStatusRepositoryBackupPath())
                .numberOfAttemptedRetries(2)
                .lockAttemptTime(50, TimeUnit.MILLISECONDS)
                .rolloverFrequency(10, TimeUnit.MINUTES)
                .addTable(TABLE_NAME_NODE_STATUS, CREATE_NODE_STATUS, nodeStatusRolloverStrategy)
                .addTable(TABLE_NAME_STORAGE_STATUS, CREATE_STORAGE_STATUS, nodeStatusRolloverStrategy)
                .addTable(TABLE_NAME_GARBAGE_COLLECTION_STATUS, CREATE_GARBAGE_COLLECTION_STATUS, nodeStatusRolloverStrategy)
                .addTable(TABLE_NAME_PROCESSOR_STATUS, CREATE_PROCESSOR_STATUS, componentStatusRolloverStrategy)
                .addTable(TABLE_NAME_CONNECTION_STATUS, CREATE_CONNECTION_STATUS, componentStatusRolloverStrategy)
                .addTable(TABLE_NAME_PROCESS_GROUP_STATUS, CREATE_PROCESS_GROUP_STATUS, componentStatusRolloverStrategy)
                .addTable(TABLE_NAME_REMOTE_PROCESS_GROUP_STATUS, CREATE_REMOTE_PROCESS_GROUP_STATUS, componentStatusRolloverStrategy)
                .addTable(TABLE_NAME_COMPONENT_COUNTER, CREATE_COMPONENT_COUNTER, componentStatusRolloverStrategy)
                .build();

        storage = new BufferedStatusHistoryStorage(
                new QuestDbStatusHistoryStorage(databaseManager.acquireClient()),
                FormatUtils.getTimeDuration(niFiProperties.getQuestDbStatusRepositoryPersistFrequency(), TimeUnit.MILLISECONDS),
                niFiProperties.getQuestDbStatusRepositoryPersistBatchSize()
        );
        storage.init();
        LOGGER.debug("Repository start completed");
    }

    @Override
    public void shutdown() {
        LOGGER.debug("Repository shutdown started");
        databaseManager.close();
        storage.close();
        LOGGER.debug("Repository shutdown completed");
    }

    @Override
    public void capture(final NodeStatus nodeStatus, final ProcessGroupStatus rootGroupStatus, final List<GarbageCollectionStatus> garbageCollectionStatus, final Date timestamp) {
        final Instant captured = timestamp.toInstant();
        captureNodeStatus(nodeStatus, captured);
        captureGarbageCollectionStatus(garbageCollectionStatus, captured);
        captureComponentStatus(rootGroupStatus, captured);
        updateComponentDetails(rootGroupStatus);
    }

    private void captureNodeStatus(final NodeStatus nodeStatus, final Instant captured) {
        storage.storeNodeStatuses(Collections.singleton(new CapturedStatus<>(nodeStatus, captured)));
    }

    private void captureGarbageCollectionStatus(final List<GarbageCollectionStatus> statuses, final Instant captured) {
        final Set<CapturedStatus<GarbageCollectionStatus>> capturedStatuses = new HashSet<>(statuses.size());
        statuses.forEach(status -> capturedStatuses.add(new CapturedStatus<>(status, captured)));
        storage.storeGarbageCollectionStatuses(capturedStatuses);
    }

    private void captureComponentStatus(final ProcessGroupStatus groupStatus, final Instant captured) {
        storage.storeProcessGroupStatuses(Collections.singleton(new CapturedStatus<>(groupStatus, captured)));
        storage.storeConnectionStatuses(wrapConnectionStatuses(groupStatus, captured));
        storage.storeRemoteProcessorGroupStatuses(wrapRemoteProcessGroupStatuses(groupStatus, captured));
        storage.storeProcessorStatuses(wrapProcessorStatuses(groupStatus, captured));
        groupStatus.getProcessGroupStatus().forEach(child -> captureComponentStatus(child, captured));
    }

    private Collection<CapturedStatus<ConnectionStatus>> wrapConnectionStatuses(final ProcessGroupStatus groupStatus, final Instant captured) {
        final Collection<ConnectionStatus> statuses = groupStatus.getConnectionStatus();
        final Set<CapturedStatus<ConnectionStatus>> result = new HashSet<>(statuses.size());
        statuses.forEach(status ->  result.add(new CapturedStatus<>(status, captured)));
        return result;
    }

    private Collection<CapturedStatus<RemoteProcessGroupStatus>> wrapRemoteProcessGroupStatuses(final ProcessGroupStatus groupStatus, final Instant captured) {
        final Collection<RemoteProcessGroupStatus> statuses = groupStatus.getRemoteProcessGroupStatus();
        final Set<CapturedStatus<RemoteProcessGroupStatus>> result = new HashSet<>(statuses.size());
        statuses.forEach(status ->  result.add(new CapturedStatus<>(status, captured)));
        return result;
    }

    private Collection<CapturedStatus<ProcessorStatus>> wrapProcessorStatuses(final ProcessGroupStatus groupStatus, final Instant captured) {
        final Collection<ProcessorStatus> statuses = groupStatus.getProcessorStatus();
        final Set<CapturedStatus<ProcessorStatus>> result = new HashSet<>(statuses.size());
        statuses.forEach(status ->  result.add(new CapturedStatus<>(status, captured)));
        return result;
    }

    @Override
    public StatusHistory getConnectionStatusHistory(final String connectionId, final Date start, final Date end, final int preferredDataPoints) {
        return generateStatusHistory(connectionId, storage.getConnectionSnapshots(connectionId, start, end), preferredDataPoints);
    }

    @Override
    public StatusHistory getProcessGroupStatusHistory(final String processGroupId, final Date start, final Date end, final int preferredDataPoints) {
        return generateStatusHistory(processGroupId, storage.getProcessGroupSnapshots(processGroupId, start, end), preferredDataPoints);
    }

    @Override
    public StatusHistory getProcessorStatusHistory(final String processorId, final Date start, final Date end, final int preferredDataPoints, final boolean includeCounters) {
        return includeCounters
            ? generateStatusHistory(processorId, storage.getProcessorSnapshotsWithCounters(processorId, start, end), preferredDataPoints)
            : generateStatusHistory(processorId, storage.getProcessorSnapshots(processorId, start, end), preferredDataPoints);
    }

    @Override
    public StatusHistory getRemoteProcessGroupStatusHistory(final String remoteGroupId, final Date start, final Date end, final int preferredDataPoints) {
        return generateStatusHistory(remoteGroupId, storage.getRemoteProcessGroupSnapshots(remoteGroupId, start, end), preferredDataPoints);
    }

    @Override
    public StatusHistory getNodeStatusHistory(final Date start, final Date end) {
        return new StandardStatusHistory(storage.getNodeStatusSnapshots(start, end), new HashMap<>(), new Date());
    }

    @Override
    public GarbageCollectionHistory getGarbageCollectionHistory(final Date start, final Date end) {
        final List<GarbageCollectionStatus> snapshots = storage.getGarbageCollectionSnapshots(start, end);
        final StandardGarbageCollectionHistory result = new StandardGarbageCollectionHistory();
        snapshots.forEach(result::addGarbageCollectionStatus);
        return result;
    }

    private StatusHistory generateStatusHistory(final String componentId, final List<StatusSnapshot> snapshots, final int preferredDataPoints) {
        return new StandardStatusHistory(
            new ArrayList<>(snapshots.subList(Math.max(snapshots.size() - preferredDataPoints, 0), snapshots.size())),
            componentDetailsProvider.getDetails(componentId),
            new Date()
        );
    }

    private Integer getDaysToKeepNodeData(final NiFiProperties niFiProperties) {
        return niFiProperties.getIntegerProperty(
            NiFiProperties.STATUS_REPOSITORY_QUESTDB_PERSIST_NODE_DAYS,
            NiFiProperties.DEFAULT_COMPONENT_STATUS_REPOSITORY_PERSIST_NODE_DAYS);
    }

    private Integer getDaysToKeepComponentData(final NiFiProperties niFiProperties) {
        return niFiProperties.getIntegerProperty(
            NiFiProperties.STATUS_REPOSITORY_QUESTDB_PERSIST_COMPONENT_DAYS,
            NiFiProperties.DEFAULT_COMPONENT_STATUS_REPOSITORY_PERSIST_COMPONENT_DAYS);
    }

    /**
     * Before the first capture, there will be no component detail provided!
     *
     * @param groupStatus Updates component details for components within the group, including the group itself.
     */
    private void updateComponentDetails(final ProcessGroupStatus groupStatus) {
        // Note: details of deleted components will not be maintained (thus they are not reachable), but their status
        // information is stored in the database until rolled out.
        final Map<String, ComponentDetails> accumulator = new HashMap<>();
        updateComponentDetails(groupStatus, accumulator);
        componentDetailsProvider.setComponentDetails(accumulator);
    }

    private void updateComponentDetails(final ProcessGroupStatus groupStatus, final Map<String, ComponentDetails> accumulator) {
        accumulator.put(groupStatus.getId(), ComponentDetails.forProcessGroup(groupStatus));
        groupStatus.getConnectionStatus().forEach(status -> accumulator.put(status.getId(), ComponentDetails.forConnection(status)));
        groupStatus.getRemoteProcessGroupStatus().forEach(status -> accumulator.put(status.getId(), ComponentDetails.forRemoteProcessGroup(status)));
        groupStatus.getProcessorStatus().forEach(status -> accumulator.put(status.getId(), ComponentDetails.forProcessor(status)));
        groupStatus.getProcessGroupStatus().forEach(childGroupStatus -> updateComponentDetails(childGroupStatus, accumulator));
    }
}
