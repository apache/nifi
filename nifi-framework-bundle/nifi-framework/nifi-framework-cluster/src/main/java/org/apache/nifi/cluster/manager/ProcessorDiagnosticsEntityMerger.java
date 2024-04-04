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

package org.apache.nifi.cluster.manager;

import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.dto.diagnostics.ConnectionDiagnosticsDTO;
import org.apache.nifi.web.api.dto.diagnostics.ConnectionDiagnosticsSnapshotDTO;
import org.apache.nifi.web.api.dto.diagnostics.ControllerServiceDiagnosticsDTO;
import org.apache.nifi.web.api.dto.diagnostics.JVMDiagnosticsSnapshotDTO;
import org.apache.nifi.web.api.dto.diagnostics.LocalQueuePartitionDTO;
import org.apache.nifi.web.api.dto.diagnostics.NodeJVMDiagnosticsSnapshotDTO;
import org.apache.nifi.web.api.dto.diagnostics.ProcessorDiagnosticsDTO;
import org.apache.nifi.web.api.dto.diagnostics.RemoteQueuePartitionDTO;
import org.apache.nifi.web.api.dto.diagnostics.ThreadDumpDTO;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ProcessorDiagnosticsEntity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ProcessorDiagnosticsEntityMerger implements ComponentEntityMerger<ProcessorDiagnosticsEntity> {
    private final long componentStatusSnapshotMillis;

    public ProcessorDiagnosticsEntityMerger(final long componentStatusSnapshotMillis) {
        this.componentStatusSnapshotMillis = componentStatusSnapshotMillis;
    }

    @Override
    public void mergeComponents(final ProcessorDiagnosticsEntity clientEntity, final Map<NodeIdentifier, ProcessorDiagnosticsEntity> entityMap) {
        final ProcessorDiagnosticsDTO clientDto = clientEntity.getComponent();

        final List<NodeJVMDiagnosticsSnapshotDTO> nodeJvmDiagnosticsSnapshots = new ArrayList<>(entityMap.size());

        // Merge connection diagnostics
        mergeConnectionDiagnostics(clientEntity, entityMap, entity -> entity.getComponent().getIncomingConnections());
        mergeConnectionDiagnostics(clientEntity, entityMap, entity -> entity.getComponent().getOutgoingConnections());


        // Merge the Processor Statuses and create a separate NodeJVMDiagnosticsSnapshotDTO for each. We do both of these
        // together simply because we are already iterating over the entityMap and we have to create the Node-specific JVM diagnostics
        // before we start merging the values, in the second iteration over the map.
        for (final Map.Entry<NodeIdentifier, ProcessorDiagnosticsEntity> entry : entityMap.entrySet()) {
            final NodeIdentifier nodeId = entry.getKey();
            final ProcessorDiagnosticsEntity diagnosticsEntity = entry.getValue();
            final ProcessorDiagnosticsDTO diagnosticsDto = diagnosticsEntity.getComponent();

            StatusMerger.merge(clientDto.getProcessorStatus(), clientEntity.getPermissions().getCanRead(),
                diagnosticsDto.getProcessorStatus(), diagnosticsEntity.getPermissions().getCanRead(),
                nodeId.getId(), nodeId.getApiAddress(), nodeId.getApiPort());

            final NodeJVMDiagnosticsSnapshotDTO nodeJvmDiagnosticsSnapshot = new NodeJVMDiagnosticsSnapshotDTO();
            nodeJvmDiagnosticsSnapshot.setAddress(nodeId.getApiAddress());
            nodeJvmDiagnosticsSnapshot.setApiPort(nodeId.getApiPort());
            nodeJvmDiagnosticsSnapshot.setNodeId(nodeId.getId());
            nodeJvmDiagnosticsSnapshot.setSnapshot(diagnosticsDto.getJvmDiagnostics().getAggregateSnapshot());
            nodeJvmDiagnosticsSnapshots.add(nodeJvmDiagnosticsSnapshot);
        }
        clientDto.getJvmDiagnostics().setNodeSnapshots(nodeJvmDiagnosticsSnapshots);

        // Merge JVM Diagnostics and thread dumps
        final JVMDiagnosticsSnapshotDTO mergedJvmDiagnosticsSnapshot = clientDto.getJvmDiagnostics().getAggregateSnapshot().clone();
        for (final Map.Entry<NodeIdentifier, ProcessorDiagnosticsEntity> entry : entityMap.entrySet()) {
            final NodeIdentifier nodeId = entry.getKey();
            final ProcessorDiagnosticsEntity diagnosticsEntity = entry.getValue();

            if (diagnosticsEntity == clientEntity) {
                for (final ThreadDumpDTO threadDump : clientDto.getThreadDumps()) {
                    threadDump.setNodeAddress(nodeId.getApiAddress());
                    threadDump.setApiPort(nodeId.getApiPort());
                    threadDump.setNodeId(nodeId.getId());
                }

                continue;
            }

            final ProcessorDiagnosticsDTO diagnosticsDto = diagnosticsEntity.getComponent();
            final JVMDiagnosticsSnapshotDTO snapshot = diagnosticsDto.getJvmDiagnostics().getAggregateSnapshot();
            StatusMerger.merge(mergedJvmDiagnosticsSnapshot, snapshot, componentStatusSnapshotMillis);

            final List<ThreadDumpDTO> threadDumps = diagnosticsEntity.getComponent().getThreadDumps();
            for (final ThreadDumpDTO threadDump : threadDumps) {
                threadDump.setNodeAddress(nodeId.getApiAddress());
                threadDump.setApiPort(nodeId.getApiPort());
                threadDump.setNodeId(nodeId.getId());
                clientDto.getThreadDumps().add(threadDump);
            }
        }
        clientDto.getJvmDiagnostics().setAggregateSnapshot(mergedJvmDiagnosticsSnapshot);

        // Merge permissions on referenced controller services
        final Map<String, ControllerServiceEntity> serviceEntityById = clientDto.getReferencedControllerServices().stream()
            .map(ControllerServiceDiagnosticsDTO::getControllerService)
            .collect(Collectors.toMap(ControllerServiceEntity::getId, Function.identity()));

        for (final Map.Entry<NodeIdentifier, ProcessorDiagnosticsEntity> entry : entityMap.entrySet()) {
            final ProcessorDiagnosticsEntity procDiagnostics = entry.getValue();
            final Set<ControllerServiceDiagnosticsDTO> serviceDtos = procDiagnostics.getComponent().getReferencedControllerServices();

            for (final ControllerServiceDiagnosticsDTO serviceDto : serviceDtos) {
                final ControllerServiceEntity serviceEntity = serviceDto.getControllerService();
                final ControllerServiceEntity targetEntity = serviceEntityById.get(serviceEntity.getId());
                if (targetEntity != null) {
                    PermissionsDtoMerger.mergePermissions(targetEntity.getPermissions(), serviceEntity.getPermissions());
                }
            }
        }
    }

    private void mergeConnectionDiagnostics(final ProcessorDiagnosticsEntity clientEntity, final Map<NodeIdentifier, ProcessorDiagnosticsEntity> entityMap,
                                                                     final Function<ProcessorDiagnosticsEntity, Set<ConnectionDiagnosticsDTO>> extractConnections) {

        final Map<String, List<ConnectionDiagnosticsSnapshotDTO>> snapshotByConnectionId = new HashMap<>();
        final Map<String, ConnectionDiagnosticsDTO> connectionById = new HashMap<>();

        for (final Map.Entry<NodeIdentifier, ProcessorDiagnosticsEntity> entry : entityMap.entrySet()) {
            final NodeIdentifier nodeId = entry.getKey();
            final ProcessorDiagnosticsEntity entity = entry.getValue();

            final Set<ConnectionDiagnosticsDTO> connections = extractConnections.apply(entity);
            for (final ConnectionDiagnosticsDTO connectionDiagnostics : connections) {
                final String connectionId = connectionDiagnostics.getConnection().getId();
                final ConnectionDiagnosticsSnapshotDTO snapshot = connectionDiagnostics.getAggregateSnapshot();

                snapshot.setNodeIdentifier(nodeId.getApiAddress() + ":" + nodeId.getApiPort());

                final List<ConnectionDiagnosticsSnapshotDTO> snapshots = snapshotByConnectionId.computeIfAbsent(connectionId, id -> new ArrayList<>());
                snapshots.add(snapshot);

                if (entity == clientEntity){
                    connectionById.put(connectionId, connectionDiagnostics);
                }
            }
        }

        for (final Map.Entry<String, List<ConnectionDiagnosticsSnapshotDTO>> entry : snapshotByConnectionId.entrySet()) {
            final String connectionId = entry.getKey();
            final List<ConnectionDiagnosticsSnapshotDTO> snapshots = entry.getValue();

            final ConnectionDiagnosticsDTO dto = connectionById.get(connectionId);
            dto.setNodeSnapshots(snapshots);

            dto.setAggregateSnapshot(mergeConnectionSnapshots(snapshots));
        }
    }



    private ConnectionDiagnosticsSnapshotDTO mergeConnectionSnapshots(final List<ConnectionDiagnosticsSnapshotDTO> snapshots) {
        final ConnectionDiagnosticsSnapshotDTO aggregate = new ConnectionDiagnosticsSnapshotDTO();

        final Map<String, List<RemoteQueuePartitionDTO>> remotePartitionsByNodeId = new HashMap<>();

        final LocalQueuePartitionDTO localPartition = new LocalQueuePartitionDTO();
        localPartition.setActiveQueueByteCount(0);
        localPartition.setActiveQueueFlowFileCount(0);
        localPartition.setAllActiveQueueFlowFilesPenalized(true); // set to true because we will update this value by AND'ing it with the snapshot value
        localPartition.setAnyActiveQueueFlowFilesPenalized(false); // set to false because we will update this value by OR'ing it with the snapshot value
        localPartition.setInFlightByteCount(0);
        localPartition.setInFlightFlowFileCount(0);
        localPartition.setSwapByteCount(0);
        localPartition.setSwapFiles(0);
        localPartition.setSwapFlowFileCount(0);
        localPartition.setTotalByteCount(0);
        localPartition.setTotalFlowFileCount(0);

        aggregate.setTotalByteCount(0L);
        aggregate.setTotalFlowFileCount(0);
        aggregate.setLocalQueuePartition(localPartition);

        for (final ConnectionDiagnosticsSnapshotDTO snapshot : snapshots) {
            aggregate.setTotalByteCount(aggregate.getTotalByteCount() + snapshot.getTotalByteCount());
            aggregate.setTotalFlowFileCount(aggregate.getTotalFlowFileCount() + snapshot.getTotalFlowFileCount());

            final LocalQueuePartitionDTO snapshotLocalPartition = snapshot.getLocalQueuePartition();
            localPartition.setActiveQueueByteCount(localPartition.getActiveQueueByteCount() + snapshotLocalPartition.getActiveQueueByteCount());
            localPartition.setActiveQueueFlowFileCount(localPartition.getActiveQueueFlowFileCount() + snapshotLocalPartition.getActiveQueueFlowFileCount());
            localPartition.setAllActiveQueueFlowFilesPenalized(localPartition.getAllActiveQueueFlowFilesPenalized() && snapshotLocalPartition.getAllActiveQueueFlowFilesPenalized());
            localPartition.setAnyActiveQueueFlowFilesPenalized(localPartition.getAnyActiveQueueFlowFilesPenalized() || snapshotLocalPartition.getAnyActiveQueueFlowFilesPenalized());
            localPartition.setInFlightByteCount(localPartition.getInFlightByteCount() + snapshotLocalPartition.getInFlightByteCount());
            localPartition.setInFlightFlowFileCount(localPartition.getInFlightFlowFileCount() + snapshotLocalPartition.getInFlightFlowFileCount());
            localPartition.setSwapByteCount(localPartition.getSwapByteCount() + snapshotLocalPartition.getSwapByteCount());
            localPartition.setSwapFiles(localPartition.getSwapFiles() + snapshotLocalPartition.getSwapFiles());
            localPartition.setSwapFlowFileCount(localPartition.getSwapFlowFileCount() + snapshotLocalPartition.getSwapFlowFileCount());
            localPartition.setTotalByteCount(localPartition.getTotalByteCount() + snapshotLocalPartition.getTotalByteCount());
            localPartition.setTotalFlowFileCount(localPartition.getTotalFlowFileCount() + snapshotLocalPartition.getTotalFlowFileCount());

            for (final RemoteQueuePartitionDTO remoteQueuePartition : snapshot.getRemoteQueuePartitions()) {
                final String nodeId = remoteQueuePartition.getNodeIdentifier();
                final List<RemoteQueuePartitionDTO> partitionsForNodeId = remotePartitionsByNodeId.computeIfAbsent(nodeId, key -> new ArrayList<>());
                partitionsForNodeId.add(remoteQueuePartition);
            }
        }

        final List<RemoteQueuePartitionDTO> mergedRemoteQueuePartitions = new ArrayList<>();
        for (final List<RemoteQueuePartitionDTO> partitions : remotePartitionsByNodeId.values()) {
            final RemoteQueuePartitionDTO merged = mergeRemoteQueuePartitions(partitions);
            mergedRemoteQueuePartitions.add(merged);
        }

        aggregate.setRemoteQueuePartitions(mergedRemoteQueuePartitions);

        return aggregate;
    }

    private RemoteQueuePartitionDTO mergeRemoteQueuePartitions(final List<RemoteQueuePartitionDTO> partitions) {
        final RemoteQueuePartitionDTO merged = new RemoteQueuePartitionDTO();
        merged.setActiveQueueByteCount(0);
        merged.setActiveQueueFlowFileCount(0);
        merged.setInFlightByteCount(0);
        merged.setInFlightFlowFileCount(0);
        merged.setSwapByteCount(0);
        merged.setSwapFiles(0);
        merged.setSwapFlowFileCount(0);
        merged.setTotalByteCount(0);
        merged.setTotalFlowFileCount(0);

        for (final RemoteQueuePartitionDTO partition : partitions) {
            merged.setActiveQueueByteCount(merged.getActiveQueueByteCount() + partition.getActiveQueueByteCount());
            merged.setActiveQueueFlowFileCount(merged.getActiveQueueFlowFileCount() + partition.getActiveQueueFlowFileCount());
            merged.setInFlightByteCount(merged.getInFlightByteCount() + partition.getInFlightByteCount());
            merged.setInFlightFlowFileCount(merged.getInFlightFlowFileCount() + partition.getInFlightFlowFileCount());
            merged.setSwapByteCount(merged.getSwapByteCount() + partition.getSwapByteCount());
            merged.setSwapFiles(merged.getSwapFiles() + partition.getSwapFiles());
            merged.setSwapFlowFileCount(merged.getSwapFlowFileCount() + partition.getSwapFlowFileCount());
            merged.setTotalByteCount(merged.getTotalByteCount() + partition.getTotalByteCount());
            merged.setTotalFlowFileCount(merged.getTotalFlowFileCount() + partition.getTotalFlowFileCount());
            merged.setNodeIdentifier(partition.getNodeIdentifier());
        }

        return merged;
    }
}
