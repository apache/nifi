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
import org.apache.nifi.web.api.dto.PermissionsDTO;
import org.apache.nifi.web.api.dto.diagnostics.AutoSchedulingDiagnosticsDTO;
import org.apache.nifi.web.api.dto.diagnostics.JVMDiagnosticsDTO;
import org.apache.nifi.web.api.dto.diagnostics.JVMDiagnosticsSnapshotDTO;
import org.apache.nifi.web.api.dto.diagnostics.NodeAutoSchedulingDiagnosticsDTO;
import org.apache.nifi.web.api.dto.diagnostics.ProcessorDiagnosticsDTO;
import org.apache.nifi.web.api.dto.status.ProcessorStatusDTO;
import org.apache.nifi.web.api.entity.ProcessorDiagnosticsEntity;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

class ProcessorDiagnosticsEntityMergerTest {

    @Test
    void testPreservesNodeSpecificAutomaticSchedulingDiagnostics() {
        final AutoSchedulingDiagnosticsDTO firstSnapshot = new AutoSchedulingDiagnosticsDTO();
        firstSnapshot.setCurrentConcurrentTasks(2);
        final AutoSchedulingDiagnosticsDTO secondSnapshot = new AutoSchedulingDiagnosticsDTO();
        secondSnapshot.setCurrentConcurrentTasks(5);
        final ProcessorDiagnosticsEntity firstEntity = createEntity(firstSnapshot);
        final ProcessorDiagnosticsEntity secondEntity = createEntity(secondSnapshot);
        final NodeIdentifier firstNode = createNode("node-1", "first.example", 8443);
        final NodeIdentifier secondNode = createNode("node-2", "second.example", 9443);
        final Map<NodeIdentifier, ProcessorDiagnosticsEntity> entities = new LinkedHashMap<>();
        entities.put(firstNode, firstEntity);
        entities.put(secondNode, secondEntity);

        new ProcessorDiagnosticsEntityMerger(1_000L).mergeComponents(firstEntity, entities);

        final ProcessorDiagnosticsDTO merged = firstEntity.getComponent();
        assertNull(merged.getAutoSchedulingDiagnostics());
        final List<NodeAutoSchedulingDiagnosticsDTO> nodeDiagnostics = merged.getNodeAutoSchedulingDiagnostics();
        assertEquals(2, nodeDiagnostics.size());
        assertEquals("node-1", nodeDiagnostics.get(0).getNodeId());
        assertSame(firstSnapshot, nodeDiagnostics.get(0).getSnapshot());
        assertEquals("node-2", nodeDiagnostics.get(1).getNodeId());
        assertSame(secondSnapshot, nodeDiagnostics.get(1).getSnapshot());
    }

    private ProcessorDiagnosticsEntity createEntity(final AutoSchedulingDiagnosticsDTO autoSchedulingDiagnostics) {
        final ProcessorDiagnosticsDTO diagnostics = new ProcessorDiagnosticsDTO();
        diagnostics.setAutoSchedulingDiagnostics(autoSchedulingDiagnostics);
        diagnostics.setIncomingConnections(Set.of());
        diagnostics.setOutgoingConnections(Set.of());
        diagnostics.setReferencedControllerServices(Set.of());
        diagnostics.setThreadDumps(new ArrayList<>());
        diagnostics.setProcessorStatus(new ProcessorStatusDTO());
        final JVMDiagnosticsDTO jvmDiagnostics = new JVMDiagnosticsDTO();
        jvmDiagnostics.setAggregateSnapshot(new JVMDiagnosticsSnapshotDTO());
        diagnostics.setJvmDiagnostics(jvmDiagnostics);

        final PermissionsDTO permissions = new PermissionsDTO();
        permissions.setCanRead(true);
        final ProcessorDiagnosticsEntity entity = new ProcessorDiagnosticsEntity();
        entity.setPermissions(permissions);
        entity.setComponent(diagnostics);
        return entity;
    }

    private NodeIdentifier createNode(final String identifier, final String address, final int apiPort) {
        return new NodeIdentifier(identifier, address, 10_000, address, 10_001, address, apiPort,
                address, 10_002, 10_003, true, Set.of());
    }
}
