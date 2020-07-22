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
import org.apache.nifi.controller.status.NodeStatus;
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
import java.util.HashSet;
import java.util.LinkedList;
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
    private static final Set<MetricDescriptor<?>> DEFAULT_NODE_METRICS = Arrays.stream(NodeStatusDescriptor.values())
        .map(NodeStatusDescriptor::getDescriptor)
        .collect(Collectors.toSet());

    private static final String GC_TIME_DESCRIPTION = "The sum time the garbage collection has run since the start of the Java virtual machine";
    private static final String GC_TIME_DIFF_DESCRIPTION = "The sum time the garbage collection has run since the last measurement";
    private static final String GC_COUNT_DESCRIPTION = "The sum amount of occasions the garbage collection has run since the start of the Java virtual machine";
    private static final String GC_COUNT_DIFF_DESCRIPTION = "The sum amount of occasions the garbage collection has run since the last measurement";
    private static final int NUMBER_OF_GC_METRICS = 4;

    public static final String NUM_DATA_POINTS_PROPERTY = "nifi.components.status.repository.buffer.size";
    public static final int DEFAULT_NUM_DATA_POINTS = 288;   // 1 day worth of 5-minute snapshots

    private final Map<String, ComponentStatusHistory> componentStatusHistories = new HashMap<>();

    // Changed to protected to allow unit testing
    protected final RingBuffer<Date> timestamps;
    private final RingBuffer<List<GarbageCollectionStatus>> gcStatuses;
    private final RingBuffer<NodeStatus> nodeStatuses;
    private final int numDataPoints;
    private volatile long lastCaptureTime = 0L;

    /**
     * Default no args constructor for service loading only
     */
    public VolatileComponentStatusRepository() {
        numDataPoints = DEFAULT_NUM_DATA_POINTS;
        gcStatuses = null;
        timestamps = null;
        nodeStatuses = null;
    }

    public VolatileComponentStatusRepository(final NiFiProperties nifiProperties) {
        numDataPoints = nifiProperties.getIntegerProperty(NUM_DATA_POINTS_PROPERTY, DEFAULT_NUM_DATA_POINTS);
        gcStatuses = new RingBuffer<>(numDataPoints);
        timestamps = new RingBuffer<>(numDataPoints);
        nodeStatuses = new RingBuffer<>(numDataPoints);
    }

    @Override
    public void capture(final NodeStatus nodeStatus, final ProcessGroupStatus rootGroupStatus, final List<GarbageCollectionStatus> gcStatus) {
        capture(nodeStatus, rootGroupStatus, gcStatus, new Date());
    }

    @Override
    public synchronized void capture(final NodeStatus nodeStatus, final ProcessGroupStatus rootGroupStatus, final List<GarbageCollectionStatus> gcStatus, final Date timestamp) {
        final Date evicted = timestamps.add(timestamp);
        if (evicted != null) {
            componentStatusHistories.values().forEach(history -> history.expireBefore(evicted));
        }

        capture(rootGroupStatus, timestamp);
        nodeStatuses.add(nodeStatus);
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
        return getStatusHistory(processorId, includeCounters, DEFAULT_PROCESSOR_METRICS, start, end, preferredDataPoints);
    }

    @Override
    public StatusHistory getConnectionStatusHistory(final String connectionId, final Date start, final Date end, final int preferredDataPoints) {
        return getStatusHistory(connectionId, true, DEFAULT_CONNECTION_METRICS, start, end, preferredDataPoints);
    }

    @Override
    public StatusHistory getProcessGroupStatusHistory(final String processGroupId, final Date start, final Date end, final int preferredDataPoints) {
        return getStatusHistory(processGroupId, true, DEFAULT_GROUP_METRICS, start, end, preferredDataPoints);
    }

    @Override
    public StatusHistory getRemoteProcessGroupStatusHistory(final String remoteGroupId, final Date start, final Date end, final int preferredDataPoints) {
        return getStatusHistory(remoteGroupId, true, DEFAULT_RPG_METRICS, start, end, preferredDataPoints);
    }

    @Override
    public StatusHistory getNodeStatusHistory() {
        final List<NodeStatus> nodeStatusList = nodeStatuses.asList();
        final List<List<GarbageCollectionStatus>> gcStatusList = gcStatuses.asList();
        final LinkedList<StatusSnapshot> snapshots = new LinkedList<>();

        final Set<MetricDescriptor<?>> metricDescriptors = new HashSet<>(DEFAULT_NODE_METRICS);
        final List<MetricDescriptor<List<GarbageCollectionStatus>>> gcMetricDescriptors = new LinkedList<>();
        final List<MetricDescriptor<List<GarbageCollectionStatus>>> gcMetricDescriptorsDifferential = new LinkedList<>();

        int ordinal = DEFAULT_NODE_METRICS.size() - 1;

        // Uses the first measurement as reference for GCs
        if (gcStatusList.size() > 0) {
            final List<GarbageCollectionStatus> gcStatuses = gcStatusList.get(0);

            for (int i = 0; i < gcStatuses.size(); i++) {
                final int gcNumber = i;
                final int counter = ordinal + NUMBER_OF_GC_METRICS * gcNumber;
                final String memoryManager = gcStatuses.get(i).getMemoryManagerName();

                gcMetricDescriptors.add(new StandardMetricDescriptor<>(
                        () -> counter + 1,
                        "gc" + gcNumber + "time",
                        memoryManager + " Collection Time (milliseconds)",
                        GC_TIME_DESCRIPTION,
                        MetricDescriptor.Formatter.COUNT,
                        gcs -> gcs.get(gcNumber).getCollectionMillis()));

                gcMetricDescriptorsDifferential.add(new StandardMetricDescriptor<>(
                        () -> counter + 2,
                        "gc" + gcNumber + "time.difference",
                        memoryManager + " Collection Time (5 mins, in milliseconds)",
                        GC_TIME_DIFF_DESCRIPTION,
                        MetricDescriptor.Formatter.COUNT,
                        gcs -> 0L)); // Value function is not in use, filled below as value from previous measurement is needed

                gcMetricDescriptors.add(new StandardMetricDescriptor<>(
                        () -> counter + 3,
                        "gc" + gcNumber + "count",
                        memoryManager + " Collection Count",
                        GC_COUNT_DESCRIPTION,
                        MetricDescriptor.Formatter.COUNT,
                        gcs -> gcs.get(gcNumber).getCollectionCount()));

                gcMetricDescriptorsDifferential.add(new StandardMetricDescriptor<>(
                        () -> counter + 4,
                        "gc" + gcNumber + "count.difference",
                        memoryManager + " Collection Count (5 mins)",
                        GC_COUNT_DIFF_DESCRIPTION,
                        MetricDescriptor.Formatter.COUNT,
                        gcs -> 0L));  // Value function is not in use, filled below as value from previous measurement is needed
            }

            metricDescriptors.addAll(gcMetricDescriptors);
            metricDescriptors.addAll(gcMetricDescriptorsDifferential);
        }

        for (int i = 0; i < nodeStatusList.size(); i++) {
            final StandardStatusSnapshot snapshot  = new StandardStatusSnapshot(metricDescriptors);
            final NodeStatus nodeStatus = nodeStatusList.get(i);
            final List<GarbageCollectionStatus> garbageCollectionStatuses = gcStatusList.get(i);

            snapshot.setTimestamp(new Date(nodeStatus.getCreatedAtInMs()));

            // Adding default metrics
            for (final MetricDescriptor<NodeStatus> descriptor : Arrays.stream(NodeStatusDescriptor.values()).map(d -> d.getDescriptor()).collect(Collectors.toList())) {
                snapshot.addStatusMetric(descriptor, descriptor.getValueFunction().getValue(nodeStatus));
            }

            // Adding simple GC metrics
            for (final MetricDescriptor<List<GarbageCollectionStatus>> descriptor : gcMetricDescriptors) {
                final Long value = descriptor.getValueFunction().getValue(garbageCollectionStatuses);
                snapshot.addStatusMetric(descriptor, value);
            }

            // Adding GC metrics uses previous measurement for generating diff
            if (!snapshots.isEmpty()) {
                final StatusSnapshot previousSnapshot = snapshots.getLast();

                for (int j = 0; j < gcMetricDescriptorsDifferential.size(); j++) {
                    final long diff = snapshot.getStatusMetric(gcMetricDescriptors.get(j)).longValue() - previousSnapshot.getStatusMetric(gcMetricDescriptors.get(j)).longValue();
                    snapshot.addStatusMetric(gcMetricDescriptorsDifferential.get(j), diff);
                }
            } else {
                for (int j = 0; j < gcMetricDescriptorsDifferential.size(); j++) {
                    snapshot.addStatusMetric(gcMetricDescriptorsDifferential.get(j), 0L);
                }
            }

            snapshots.add(snapshot);
        }

        final Map<String, String> componentDetails = new HashMap<>();

        if (nodeStatusList.size() > 0) {
            final NodeStatus nodeStatus = nodeStatusList.get(nodeStatusList.size() - 1);
            componentDetails.put("Uptime", nodeStatus.getUptime() + "ms");
        }

        return new StandardStatusHistory(snapshots, componentDetails, new Date());
    }

    // Updated getStatusHistory to utilize the start/end/preferredDataPoints parameters passed into
    // the calling methods. Although for VolatileComponentStatusRepository the timestamps buffer is
    // rather small it still seemed better that the parameters should be honored rather than
    // silently ignored.
    private synchronized StatusHistory getStatusHistory(final String componentId,
        final boolean includeCounters, final Set<MetricDescriptor<?>> defaultMetricDescriptors,
        final Date start, final Date end, final int preferredDataPoints) {
        final ComponentStatusHistory history = componentStatusHistories.get(componentId);
        if (history == null) {
            return createEmptyStatusHistory();
        }
        final List<Date> dates = filterDates(start, end, preferredDataPoints);
        return history.toStatusHistory(dates, includeCounters, defaultMetricDescriptors);
    }

    // Given a buffer, return a list of Dates based on start/end/preferredDataPoints
    protected List<Date> filterDates(final Date start, final Date end, final int preferredDataPoints) {
        Date startDate = (start == null) ? new Date(0L) : start;
        Date endDate = (end == null) ? new Date() : end;

        // Limit date information to a subset based upon input parameters
        List<Date> filteredDates =
            timestamps.asList()
                .stream()
                .filter(p -> (p.after(startDate) || p.equals(startDate))
                    && (p.before(endDate) || p.equals(endDate))).collect(Collectors.toList());

        // if preferredDataPoints != Integer.MAX_VALUE, Dates returned will be reduced further
        return filteredDates.subList(Math.max(filteredDates.size() - preferredDataPoints, 0), filteredDates.size());
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
