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
package org.apache.nifi.cluster.coordination.http.endpoints;

import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;
import org.apache.nifi.cluster.coordination.http.EndpointResponseMerger;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.controller.status.history.ConnectionStatusDescriptor;
import org.apache.nifi.controller.status.history.MetricDescriptor;
import org.apache.nifi.controller.status.history.ProcessGroupStatusDescriptor;
import org.apache.nifi.controller.status.history.ProcessorStatusDescriptor;
import org.apache.nifi.controller.status.history.RemoteProcessGroupStatusDescriptor;
import org.apache.nifi.controller.status.history.StandardStatusSnapshot;
import org.apache.nifi.controller.status.history.StatusHistoryUtil;
import org.apache.nifi.controller.status.history.StatusSnapshot;
import org.apache.nifi.web.api.dto.status.NodeStatusSnapshotsDTO;
import org.apache.nifi.web.api.dto.status.StatusHistoryDTO;
import org.apache.nifi.web.api.dto.status.StatusSnapshotDTO;
import org.apache.nifi.web.api.entity.StatusHistoryEntity;

public class StatusHistoryEndpointMerger implements EndpointResponseMerger {

    public static final Pattern PROCESSOR_STATUS_HISTORY_URI_PATTERN = Pattern.compile("/nifi-api/flow/processors/[a-f0-9\\-]{36}/status/history");
    public static final Pattern PROCESS_GROUP_STATUS_HISTORY_URI_PATTERN = Pattern.compile("/nifi-api/flow/process-groups/(?:(?:root)|(?:[a-f0-9\\-]{36}))/status/history");
    public static final Pattern REMOTE_PROCESS_GROUP_STATUS_HISTORY_URI_PATTERN = Pattern.compile("/nifi-api/flow/remote-process-groups/[a-f0-9\\-]{36}/status/history");
    public static final Pattern CONNECTION_STATUS_HISTORY_URI_PATTERN = Pattern.compile("/nifi-api/flow/connections/[a-f0-9\\-]{36}/status/history");

    private final long componentStatusSnapshotMillis;

    public StatusHistoryEndpointMerger(final long componentStatusSnapshotMillis) {
        this.componentStatusSnapshotMillis = componentStatusSnapshotMillis;
    }

    private Map<String, MetricDescriptor<?>> getMetricDescriptors(final URI uri) {
        final String path = uri.getPath();

        final Map<String, MetricDescriptor<?>> metricDescriptors = new HashMap<>();

        if (PROCESSOR_STATUS_HISTORY_URI_PATTERN.matcher(path).matches()) {
            for (final ProcessorStatusDescriptor descriptor : ProcessorStatusDescriptor.values()) {
                metricDescriptors.put(descriptor.getField(), descriptor.getDescriptor());
            }
        } else if (PROCESS_GROUP_STATUS_HISTORY_URI_PATTERN.matcher(path).matches()) {
            for (final ProcessGroupStatusDescriptor descriptor : ProcessGroupStatusDescriptor.values()) {
                metricDescriptors.put(descriptor.getField(), descriptor.getDescriptor());
            }
        } else if (REMOTE_PROCESS_GROUP_STATUS_HISTORY_URI_PATTERN.matcher(path).matches()) {
            for (final RemoteProcessGroupStatusDescriptor descriptor : RemoteProcessGroupStatusDescriptor.values()) {
                metricDescriptors.put(descriptor.getField(), descriptor.getDescriptor());
            }
        } else if (CONNECTION_STATUS_HISTORY_URI_PATTERN.matcher(path).matches()) {
            for (final ConnectionStatusDescriptor descriptor : ConnectionStatusDescriptor.values()) {
                metricDescriptors.put(descriptor.getField(), descriptor.getDescriptor());
            }
        }

        return metricDescriptors;
    }

    @Override
    public boolean canHandle(URI uri, String method) {
        if (!"GET".equalsIgnoreCase(method)) {
            return false;
        }

        final Map<String, MetricDescriptor<?>> descriptors = getMetricDescriptors(uri);
        return descriptors != null && !descriptors.isEmpty();
    }

    @Override
    public NodeResponse merge(URI uri, String method, Set<NodeResponse> successfulResponses, Set<NodeResponse> problematicResponses, NodeResponse clientResponse) {
        final Map<String, MetricDescriptor<?>> metricDescriptors = getMetricDescriptors(uri);

        final StatusHistoryEntity responseEntity = clientResponse.getClientResponse().getEntity(StatusHistoryEntity.class);

        StatusHistoryDTO lastStatusHistory = null;
        final List<NodeStatusSnapshotsDTO> nodeStatusSnapshots = new ArrayList<>(successfulResponses.size());
        LinkedHashMap<String, String> noReadPermissionsComponentDetails = null;
        for (final NodeResponse nodeResponse : successfulResponses) {
            final StatusHistoryEntity nodeResponseEntity = nodeResponse == clientResponse ? responseEntity : nodeResponse.getClientResponse().getEntity(StatusHistoryEntity.class);
            final StatusHistoryDTO nodeStatus = nodeResponseEntity.getStatusHistory();
            lastStatusHistory = nodeStatus;
            if (noReadPermissionsComponentDetails == null && !nodeResponseEntity.getCanRead()) {
                // If component details from a history with no read permissions is encountered for the first time, hold on to them to be used in the merged response
                noReadPermissionsComponentDetails = nodeStatus.getComponentDetails();
            }

            final NodeIdentifier nodeId = nodeResponse.getNodeId();
            final NodeStatusSnapshotsDTO nodeStatusSnapshot = new NodeStatusSnapshotsDTO();
            nodeStatusSnapshot.setNodeId(nodeId.getId());
            nodeStatusSnapshot.setAddress(nodeId.getApiAddress());
            nodeStatusSnapshot.setApiPort(nodeId.getApiPort());
            nodeStatusSnapshot.setStatusSnapshots(nodeStatus.getAggregateSnapshots());
            nodeStatusSnapshots.add(nodeStatusSnapshot);
        }

        final StatusHistoryDTO clusterStatusHistory = new StatusHistoryDTO();
        clusterStatusHistory.setAggregateSnapshots(mergeStatusHistories(nodeStatusSnapshots, metricDescriptors));
        clusterStatusHistory.setGenerated(new Date());
        clusterStatusHistory.setNodeSnapshots(nodeStatusSnapshots);
        if (lastStatusHistory != null) {
            clusterStatusHistory.setComponentDetails(noReadPermissionsComponentDetails == null ? lastStatusHistory.getComponentDetails() : noReadPermissionsComponentDetails);
            clusterStatusHistory.setFieldDescriptors(lastStatusHistory.getFieldDescriptors());
        }

        final StatusHistoryEntity clusterEntity = new StatusHistoryEntity();
        clusterEntity.setStatusHistory(clusterStatusHistory);
        clusterEntity.setCanRead(noReadPermissionsComponentDetails == null);

        return new NodeResponse(clientResponse, clusterEntity);
    }

    private List<StatusSnapshotDTO> mergeStatusHistories(final List<NodeStatusSnapshotsDTO> nodeStatusSnapshots, final Map<String, MetricDescriptor<?>> metricDescriptors) {
        // We want a Map<Date, List<StatusSnapshot>>, which is a Map of "normalized Date" (i.e., a time range, essentially)
        // to all Snapshots for that time. The list will contain one snapshot for each node. However, we can have the case
        // where the NCM has a different value for the componentStatusSnapshotMillis than the nodes have. In this case,
        // we end up with multiple entries in the List<StatusSnapshot> for the same node/timestamp, which skews our aggregate
        // results. In order to avoid this, we will use only the latest snapshot for a node that falls into the the time range
        // of interest.
        // To accomplish this, we have an intermediate data structure, which is a Map of "normalized Date" to an inner Map
        // of Node Identifier to StatusSnapshot. We then will flatten this Map and aggregate the results.
        final Map<Date, Map<String, StatusSnapshot>> dateToNodeSnapshots = new TreeMap<>();

        // group status snapshot's for each node by date
        for (final NodeStatusSnapshotsDTO nodeStatusSnapshot : nodeStatusSnapshots) {
            for (final StatusSnapshotDTO snapshotDto : nodeStatusSnapshot.getStatusSnapshots()) {
                final StatusSnapshot snapshot = createSnapshot(snapshotDto, metricDescriptors);
                final Date normalizedDate = normalizeStatusSnapshotDate(snapshot.getTimestamp(), componentStatusSnapshotMillis);

                Map<String, StatusSnapshot> nodeToSnapshotMap = dateToNodeSnapshots.get(normalizedDate);
                if (nodeToSnapshotMap == null) {
                    nodeToSnapshotMap = new HashMap<>();
                    dateToNodeSnapshots.put(normalizedDate, nodeToSnapshotMap);
                }
                nodeToSnapshotMap.put(nodeStatusSnapshot.getNodeId(), snapshot);
            }
        }

        // aggregate the snapshots by (normalized) timestamp
        final Map<Date, List<StatusSnapshot>> snapshotsToAggregate = new TreeMap<>();
        for (final Map.Entry<Date, Map<String, StatusSnapshot>> entry : dateToNodeSnapshots.entrySet()) {
            final Date normalizedDate = entry.getKey();
            final Map<String, StatusSnapshot> nodeToSnapshot = entry.getValue();
            final List<StatusSnapshot> snapshotsForTimestamp = new ArrayList<>(nodeToSnapshot.values());
            snapshotsToAggregate.put(normalizedDate, snapshotsForTimestamp);
        }

        final List<StatusSnapshotDTO> aggregatedSnapshots = aggregate(snapshotsToAggregate);
        return aggregatedSnapshots;
    }

    private StatusSnapshot createSnapshot(final StatusSnapshotDTO snapshotDto, final Map<String, MetricDescriptor<?>> metricDescriptors) {
        final StandardStatusSnapshot snapshot = new StandardStatusSnapshot();
        snapshot.setTimestamp(snapshotDto.getTimestamp());

        final Map<String, Long> metrics = snapshotDto.getStatusMetrics();
        for (final Map.Entry<String, Long> entry : metrics.entrySet()) {
            final String metricId = entry.getKey();
            final Long value = entry.getValue();

            final MetricDescriptor<?> descriptor = metricDescriptors.get(metricId);
            if (descriptor != null) {
                snapshot.addStatusMetric(descriptor, value);
            }
        }

        return snapshot;
    }

    private List<StatusSnapshotDTO> aggregate(Map<Date, List<StatusSnapshot>> snapshotsToAggregate) {
        // Aggregate the snapshots
        final List<StatusSnapshotDTO> aggregatedSnapshotDtos = new ArrayList<>();
        for (final Map.Entry<Date, List<StatusSnapshot>> entry : snapshotsToAggregate.entrySet()) {
            final List<StatusSnapshot> snapshots = entry.getValue();
            final StatusSnapshot reducedSnapshot = snapshots.get(0).getValueReducer().reduce(snapshots);

            final StatusSnapshotDTO dto = new StatusSnapshotDTO();
            dto.setTimestamp(reducedSnapshot.getTimestamp());
            dto.setStatusMetrics(StatusHistoryUtil.createStatusSnapshotDto(reducedSnapshot).getStatusMetrics());

            aggregatedSnapshotDtos.add(dto);
        }

        return aggregatedSnapshotDtos;
    }

    public static Date normalizeStatusSnapshotDate(final Date toNormalize, final long numMillis) {
        final long time = toNormalize.getTime();
        return new Date(time - time % numMillis);
    }
}
