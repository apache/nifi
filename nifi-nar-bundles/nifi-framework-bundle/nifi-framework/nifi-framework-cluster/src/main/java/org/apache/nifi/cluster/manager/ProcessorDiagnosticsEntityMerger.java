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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.dto.diagnostics.ControllerServiceDiagnosticsDTO;
import org.apache.nifi.web.api.dto.diagnostics.JVMDiagnosticsSnapshotDTO;
import org.apache.nifi.web.api.dto.diagnostics.NodeJVMDiagnosticsSnapshotDTO;
import org.apache.nifi.web.api.dto.diagnostics.ProcessorDiagnosticsDTO;
import org.apache.nifi.web.api.dto.diagnostics.ThreadDumpDTO;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;
import org.apache.nifi.web.api.entity.ProcessorDiagnosticsEntity;

public class ProcessorDiagnosticsEntityMerger implements ComponentEntityMerger<ProcessorDiagnosticsEntity> {
    private final long componentStatusSnapshotMillis;

    public ProcessorDiagnosticsEntityMerger(final long componentStatusSnapshotMillis) {
        this.componentStatusSnapshotMillis = componentStatusSnapshotMillis;
    }

    @Override
    public void mergeComponents(final ProcessorDiagnosticsEntity clientEntity, final Map<NodeIdentifier, ProcessorDiagnosticsEntity> entityMap) {
        final ProcessorDiagnosticsDTO clientDto = clientEntity.getComponent();

        final List<NodeJVMDiagnosticsSnapshotDTO> nodeJvmDiagnosticsSnapshots = new ArrayList<>(entityMap.size());

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
            .map(diagnosticsDto -> diagnosticsDto.getControllerService())
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
}
