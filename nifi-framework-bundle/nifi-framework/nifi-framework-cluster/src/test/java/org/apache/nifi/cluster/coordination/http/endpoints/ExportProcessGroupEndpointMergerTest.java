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

import jakarta.ws.rs.core.Response;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.flow.VersionedComponentState;
import org.apache.nifi.flow.VersionedNodeState;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExportProcessGroupEndpointMergerTest {

    private static final String CONNECTOR_ID = "12345678-1234-1234-1234-123456789012";
    private static final String PROCESS_GROUP_ID = "abcdef01-2345-6789-abcd-ef0123456789";
    private static final String GLOBAL_URI = "/nifi-api/process-groups/" + PROCESS_GROUP_ID + "/download";
    private static final String CONNECTOR_URI = "/nifi-api/connectors/" + CONNECTOR_ID + "/process-groups/" + PROCESS_GROUP_ID + "/download";

    @Test
    public void testCanHandle() {
        final ExportProcessGroupEndpointMerger merger = new ExportProcessGroupEndpointMerger();

        assertTrue(merger.canHandle(URI.create(GLOBAL_URI), "GET"));
        assertTrue(merger.canHandle(URI.create("/nifi-api/process-groups/root/download"), "GET"));
        assertTrue(merger.canHandle(URI.create(CONNECTOR_URI), "GET"));
        assertTrue(merger.canHandle(URI.create(CONNECTOR_URI + "?includeComponentState=true"), "get"));

        assertFalse(merger.canHandle(URI.create(CONNECTOR_URI), "POST"));
        assertFalse(merger.canHandle(URI.create("/nifi-api/connectors/not-a-uuid/process-groups/" + PROCESS_GROUP_ID + "/download"), "GET"));
        assertFalse(merger.canHandle(URI.create("/nifi-api/connectors/" + CONNECTOR_ID + "/process-groups/not-a-uuid/download"), "GET"));
        assertFalse(merger.canHandle(URI.create(CONNECTOR_URI + "/status"), "GET"));
    }

    @Test
    public void testMergeLocalNodeStates() {
        final ExportProcessGroupEndpointMerger merger = new ExportProcessGroupEndpointMerger();
        final NodeResponse firstNodeResponse = createNodeResponse(1, createSnapshot(0, "first"));
        final NodeResponse secondNodeResponse = createNodeResponse(2, createSnapshot(1, "second"));

        final NodeResponse mergedResponse = merger.merge(
                URI.create(CONNECTOR_URI), "GET", Set.of(firstNodeResponse, secondNodeResponse), Collections.emptySet(), firstNodeResponse);

        final ExportedFlowSnapshotEntity mergedEntity = (ExportedFlowSnapshotEntity) mergedResponse.getUpdatedEntity();
        final List<VersionedNodeState> localNodeStates = mergedEntity.getSnapshot().getFlowContents()
                .getProcessors().iterator().next().getComponentState().getLocalNodeStates();
        assertEquals(2, localNodeStates.size());
        assertEquals("first", localNodeStates.get(0).getState().get("value"));
        assertEquals("second", localNodeStates.get(1).getState().get("value"));
    }

    private NodeResponse createNodeResponse(final int nodeNumber, final RegisteredFlowSnapshot flowSnapshot) {
        final Response response = mock(Response.class);
        when(response.getStatus()).thenReturn(200);
        when(response.readEntity(RegisteredFlowSnapshot.class)).thenReturn(flowSnapshot);
        final NodeIdentifier nodeIdentifier = new NodeIdentifier(
                "node-" + nodeNumber, "localhost", 8080 + nodeNumber, "localhost", 9080 + nodeNumber, null, null, null, false);
        return new NodeResponse(nodeIdentifier, "GET", URI.create(CONNECTOR_URI), response, 0L, "request");
    }

    private RegisteredFlowSnapshot createSnapshot(final int nodeOrdinal, final String value) {
        final List<VersionedNodeState> localNodeStates = new ArrayList<>();
        for (int i = 0; i < nodeOrdinal; i++) {
            localNodeStates.add(null);
        }
        localNodeStates.add(new VersionedNodeState(Map.of("value", value)));

        final VersionedComponentState componentState = new VersionedComponentState();
        componentState.setLocalNodeStates(localNodeStates);
        final VersionedProcessor processor = new VersionedProcessor();
        processor.setIdentifier("processor");
        processor.setComponentState(componentState);
        final VersionedProcessGroup processGroup = new VersionedProcessGroup();
        processGroup.setIdentifier(PROCESS_GROUP_ID);
        processGroup.setProcessors(Set.of(processor));
        final RegisteredFlowSnapshot flowSnapshot = new RegisteredFlowSnapshot();
        flowSnapshot.setFlowContents(processGroup);
        return flowSnapshot;
    }
}
