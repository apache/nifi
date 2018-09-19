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

import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.controller.status.RemoteProcessGroupStatus;
import org.apache.nifi.util.ComponentMetrics;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.RingBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class VolatileComponentStatusRepository implements ComponentStatusRepository {
    private static final Logger logger = LoggerFactory.getLogger(VolatileComponentStatusRepository.class);

    private static final Set<MetricDescriptor<?>> DEFAULT_PROCESSOR_METRICS = Arrays.stream(ProcessorStatusDescriptor.values())
        .map(ProcessorStatusDescriptor::getDescriptor)
        .collect(Collectors.toSet());
    private static final Set<MetricDescriptor<?>> DEFAULT_CONNECTION_METRICS = Arrays.stream(ConnectionStatusDescriptor.values())
        .map(ConnectionStatusDescriptor::getDescriptor)
        .collect(Collectors.toSet());
    private static final Set<MetricDescriptor<?>> DEFAULT_GROUP_METRICS = Arrays.stream(ProcessGroupStatusDescriptor.values())
        .map(ProcessGroupStatusDescriptor::getDescriptor)
        .collect(Collectors.toSet());
    private static final Set<MetricDescriptor<?>> DEFAULT_RPG_METRICS = Arrays.stream(RemoteProcessGroupStatusDescriptor.values())
        .map(RemoteProcessGroupStatusDescriptor::getDescriptor)
        .collect(Collectors.toSet());


    public static final String NUM_DATA_POINTS_PROPERTY = "nifi.components.status.repository.buffer.size";
    public static final int DEFAULT_NUM_DATA_POINTS = 288;   // 1 day worth of 5-minute snapshots

    private final Map<String, ComponentStatusHistory> componentStatusHistories = new HashMap<>();

    private final RingBuffer<Date> timestamps;
    private final RingBuffer<List<GarbageCollectionStatus>> gcStatuses;
    private final int numDataPoints;
    private volatile long lastCaptureTime = 0L;

    /**
     * Default no args constructor for service loading only
     */
    public VolatileComponentStatusRepository() {
        numDataPoints = DEFAULT_NUM_DATA_POINTS;
        gcStatuses = null;
        timestamps = null;
    }

    public VolatileComponentStatusRepository(final NiFiProperties nifiProperties) {
        numDataPoints = nifiProperties.getIntegerProperty(NUM_DATA_POINTS_PROPERTY, DEFAULT_NUM_DATA_POINTS);
        gcStatuses = new RingBuffer<>(numDataPoints);
        timestamps = new RingBuffer<>(numDataPoints);
    }

    @Override
    public void capture(final ProcessGroupStatus rootGroupStatus, final List<GarbageCollectionStatus> gcStatus) {
        capture(rootGroupStatus, gcStatus, new Date());
    }

    @Override
    public synchronized void capture(final ProcessGroupStatus rootGroupStatus, final List<GarbageCollectionStatus> gcStatus, final Date timestamp) {
        final Date evicted = timestamps.add(timestamp);
        if (evicted != null) {
            componentStatusHistories.values().forEach(history -> history.expireBefore(evicted));
        }

        capture(rootGroupStatus, timestamp);
        gcStatuses.add(gcStatus);

        logger.debug("Captured metrics for {}", this);
        lastCaptureTime = Math.max(lastCaptureTime, timestamp.getTime());
    }


    private void capture(final ProcessGroupStatus groupStatus, final Date timestamp) {
        // Capture status for the ProcessGroup
        final ComponentDetails groupDetails = ComponentDetails.forProcessGroup(groupStatus);
        final StatusSnapshot groupSnapshot = ComponentMetrics.createSnapshot(groupStatus, timestamp);
        updateStatusHistory(groupSnapshot, groupDetails, timestamp);

        // Capture statuses for the Processors
        for (final ProcessorStatus processorStatus : groupStatus.getProcessorStatus()) {
            final ComponentDetails componentDetails = ComponentDetails.forProcessor(processorStatus);
            final StatusSnapshot snapshot = ComponentMetrics.createSnapshot(processorStatus, timestamp);
            updateStatusHistory(snapshot, componentDetails, timestamp);
        }

        // Capture statuses for the Connections
        for (final ConnectionStatus connectionStatus : groupStatus.getConnectionStatus()) {
            final ComponentDetails componentDetails = ComponentDetails.forConnection(connectionStatus);
            final StatusSnapshot snapshot = ComponentMetrics.createSnapshot(connectionStatus, timestamp);
            updateStatusHistory(snapshot, componentDetails, timestamp);
        }

        // Capture statuses for the RPG's
        for (final RemoteProcessGroupStatus rpgStatus : groupStatus.getRemoteProcessGroupStatus()) {
            final ComponentDetails componentDetails = ComponentDetails.forRemoteProcessGroup(rpgStatus);
            final StatusSnapshot snapshot = ComponentMetrics.createSnapshot(rpgStatus, timestamp);
            updateStatusHistory(snapshot, componentDetails, timestamp);
        }

        // Capture statuses for the child groups
        for (final ProcessGroupStatus childStatus : groupStatus.getProcessGroupStatus()) {
            capture(childStatus, timestamp);
        }
    }


    private void updateStatusHistory(final StatusSnapshot statusSnapshot, final ComponentDetails componentDetails, final Date timestamp) {
        final String componentId = componentDetails.getComponentId();
        final ComponentStatusHistory procHistory = componentStatusHistories.computeIfAbsent(componentId, id -> new ComponentStatusHistory(componentDetails, numDataPoints));
        procHistory.update(statusSnapshot, componentDetails);
    }


    @Override
    public Date getLastCaptureDate() {
        return new Date(lastCaptureTime);
    }

    @Override
    public StatusHistory getProcessorStatusHistory(final String processorId, final Date start, final Date end, final int preferredDataPoints, final boolean includeCounters) {
        return getStatusHistory(processorId, includeCounters, DEFAULT_PROCESSOR_METRICS);
    }

    @Override
    public StatusHistory getConnectionStatusHistory(final String connectionId, final Date start, final Date end, final int preferredDataPoints) {
        return getStatusHistory(connectionId, true, DEFAULT_CONNECTION_METRICS);
    }

    @Override
    public StatusHistory getProcessGroupStatusHistory(final String processGroupId, final Date start, final Date end, final int preferredDataPoints) {
        return getStatusHistory(processGroupId, true, DEFAULT_GROUP_METRICS);
    }

    @Override
    public StatusHistory getRemoteProcessGroupStatusHistory(final String remoteGroupId, final Date start, final Date end, final int preferredDataPoints) {
        return getStatusHistory(remoteGroupId, true, DEFAULT_RPG_METRICS);
    }


    private synchronized StatusHistory getStatusHistory(final String componentId, final boolean includeCounters, final Set<MetricDescriptor<?>> defaultMetricDescriptors) {
        final ComponentStatusHistory history = componentStatusHistories.get(componentId);
        if (history == null) {
            return createEmptyStatusHistory();
        }

        final List<Date> dates = timestamps.asList();
        return history.toStatusHistory(dates, includeCounters, defaultMetricDescriptors);
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


    @Override
    public GarbageCollectionHistory getGarbageCollectionHistory(final Date start, final Date end) {
        final StandardGarbageCollectionHistory history = new StandardGarbageCollectionHistory();

        gcStatuses.forEach(statusSet -> {
            for (final GarbageCollectionStatus gcStatus : statusSet) {
                if (gcStatus.getTimestamp().before(start)) {
                    continue;
                }
                if (gcStatus.getTimestamp().after(end)) {
                    continue;
                }

                history.addGarbageCollectionStatus(gcStatus);
            }

            return true;
        });

        return history;
    }
}
