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

import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.web.api.dto.ComponentStateDTO;
import org.apache.nifi.web.api.dto.StateEntryDTO;
import org.apache.nifi.web.api.dto.StateMapDTO;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestComponentStateEndpointMerger {

    private static final Logger logger = LoggerFactory.getLogger(TestComponentStateEndpointMerger.class);

    private static final NodeIdentifier NODE_1 = new NodeIdentifier("1", "localhost", 9101, "localhost", 9102, "localhost", 9103, 9104, false);
    private static final NodeIdentifier NODE_2 = new NodeIdentifier("2", "localhost", 9201, "localhost", 9202, "localhost", 9203, 9204, false);
    private static final NodeIdentifier NODE_3 = new NodeIdentifier("3", "localhost", 9301, "localhost", 9302, "localhost", 9303, 9304, false);

    private static class StateMapDTOBuilder {
        private final StateMapDTO stateMap;
        private final List<StateEntryDTO> stateEntries;

        public StateMapDTOBuilder(Scope scope) {
            stateEntries = new ArrayList<>();
            stateMap = new StateMapDTO();
            stateMap.setScope(scope.toString());
            stateMap.setState(stateEntries);
        }

        public StateMapDTOBuilder addState(String key, String value) {
            final StateEntryDTO stateEntryDTO = new StateEntryDTO();
            stateEntryDTO.setKey(key);
            stateEntryDTO.setValue(value);
            stateEntries.add(stateEntryDTO);
            stateMap.setTotalEntryCount(stateEntries.size());
            return this;
        }

    }

    private static class ComponentStateDTOBuilder {
        private final ComponentStateDTO state;

        public ComponentStateDTOBuilder() {
            this.state = new ComponentStateDTO();
        }

        public StateMapDTOBuilder buildState(Scope scope) {
            final StateMapDTOBuilder builder = new StateMapDTOBuilder(scope);
            switch (scope) {
                case LOCAL:
                    state.setLocalState(builder.stateMap);
                    break;
                case CLUSTER:
                    state.setClusterState(builder.stateMap);
                    break;
                case EXTERNAL:
                    state.setExternalState(builder.stateMap);
                    break;
            }
            return builder;
        }

    }

    private void printDTO(ComponentStateDTO clientDto) throws IOException {
        try (final StringWriter writer = new StringWriter();) {
            new ObjectMapper().writeValue(writer, clientDto);
            logger.info("clientDto={}", writer);
        }
    }

    private ComponentStateEndpointMerger merger = new ComponentStateEndpointMerger();

    private ComponentStateDTOBuilder builder1;
    private ComponentStateDTOBuilder builder2;
    private ComponentStateDTOBuilder builder3;
    private Map<NodeIdentifier, ComponentStateDTO> dtoMap;
    private ComponentStateDTO clientDto;

    @Before
    public void before() {
        builder1 = new ComponentStateDTOBuilder();
        builder2 = new ComponentStateDTOBuilder();
        builder3 = new ComponentStateDTOBuilder();
        dtoMap = new HashMap<>();
        clientDto = builder1.state;
        dtoMap.put(NODE_1, builder1.state);
        dtoMap.put(NODE_2, builder2.state);
        dtoMap.put(NODE_3, builder3.state);

        // Local scope state is always set.
        builder1.buildState(Scope.LOCAL);
        builder2.buildState(Scope.LOCAL);
        builder3.buildState(Scope.LOCAL);

    }

    @Test
    public void testMergeNothing() throws Exception {

        merger.mergeResponses(clientDto, dtoMap, null, null);

        printDTO(clientDto);

        assertNotNull(clientDto.getLocalState());
        assertEquals(0, clientDto.getLocalState().getTotalEntryCount());
        assertEquals(0, clientDto.getLocalState().getState().size());
        assertNull(clientDto.getClusterState());
        assertNull(clientDto.getExternalState());
    }

    @Test
    public void testMergeLocal() throws Exception {

        builder1.buildState(Scope.LOCAL)
                .addState("k1-1", "v1-1");
        builder2.buildState(Scope.LOCAL)
                .addState("k2-1", "v2-1")
                .addState("k2-2", "v2-2");
        builder3.buildState(Scope.LOCAL)
                .addState("k3-1", "v3-1")
                .addState("k3-2", "v3-2")
                .addState("k3-3", "v3-3");

        merger.mergeResponses(clientDto, dtoMap, null, null);

        printDTO(clientDto);

        // Local
        final StateMapDTO localStateMap = clientDto.getLocalState();
        assertNotNull(localStateMap);
        assertEquals(6, localStateMap.getTotalEntryCount());
        final List<StateEntryDTO> localState = localStateMap.getState();
        assertEquals(6, localState.size());
        assertEquals("k1-1", localState.get(0).getKey());
        assertEquals("v1-1", localState.get(0).getValue());
        assertEquals("k2-1", localState.get(1).getKey());
        assertEquals("v2-1", localState.get(1).getValue());
        assertEquals("k2-2", localState.get(2).getKey());
        assertEquals("v2-2", localState.get(2).getValue());
        assertEquals("k3-1", localState.get(3).getKey());
        assertEquals("v3-1", localState.get(3).getValue());
        assertEquals("k3-2", localState.get(4).getKey());
        assertEquals("v3-2", localState.get(4).getValue());
        assertEquals("k3-3", localState.get(5).getKey());
        assertEquals("v3-3", localState.get(5).getValue());

        assertEquals("1", localState.get(0).getClusterNodeId());
        assertEquals("2", localState.get(1).getClusterNodeId());
        assertEquals("2", localState.get(2).getClusterNodeId());
        assertEquals("3", localState.get(3).getClusterNodeId());
        assertEquals("3", localState.get(4).getClusterNodeId());
        assertEquals("3", localState.get(5).getClusterNodeId());

        assertEquals("localhost:9101", localState.get(0).getClusterNodeAddress());
        assertEquals("localhost:9201", localState.get(1).getClusterNodeAddress());
        assertEquals("localhost:9201", localState.get(2).getClusterNodeAddress());
        assertEquals("localhost:9301", localState.get(3).getClusterNodeAddress());
        assertEquals("localhost:9301", localState.get(4).getClusterNodeAddress());
        assertEquals("localhost:9301", localState.get(5).getClusterNodeAddress());

        // Cluster
        assertNull(clientDto.getClusterState());

        // External
        assertNull(clientDto.getExternalState());
    }

    @Test
    public void testMergeCluster() throws Exception {

        // Node2 is the primary node.
        builder2.buildState(Scope.CLUSTER)
                .addState("c-k1", "c-v1")
                .addState("c-k2", "c-v2");

        merger.mergeResponses(clientDto, dtoMap, null, null);

        printDTO(clientDto);

        // Local
        assertNotNull(clientDto.getLocalState());
        assertEquals(0, clientDto.getLocalState().getTotalEntryCount());
        assertEquals(0, clientDto.getLocalState().getState().size());

        // Cluster
        final StateMapDTO clusterStateMap = clientDto.getClusterState();
        assertNotNull(clusterStateMap);
        assertEquals(2, clusterStateMap.getTotalEntryCount());
        final List<StateEntryDTO> clusterState = clusterStateMap.getState();
        assertEquals(2, clusterState.size());

        assertEquals("c-k1", clusterState.get(0).getKey());
        assertEquals("c-v1", clusterState.get(0).getValue());
        assertEquals("c-k2", clusterState.get(1).getKey());
        assertEquals("c-v2", clusterState.get(1).getValue());

        assertNull(clusterState.get(0).getClusterNodeId());
        assertNull(clusterState.get(1).getClusterNodeId());
        assertNull(clusterState.get(0).getClusterNodeAddress());
        assertNull(clusterState.get(1).getClusterNodeAddress());

        // External
        assertNull(clientDto.getExternalState());

    }

    @Test
    public void testMergeExternal() throws Exception {

        // Node2 is the primary node.
        builder2.buildState(Scope.EXTERNAL)
                .addState("e-k1", "e-v1")
                .addState("e-k2", "e-v2");

        merger.mergeResponses(clientDto, dtoMap, null, null);

        printDTO(clientDto);

        // Local
        assertNotNull(clientDto.getLocalState());
        assertEquals(0, clientDto.getLocalState().getTotalEntryCount());
        assertEquals(0, clientDto.getLocalState().getState().size());

        // Cluster
        assertNull(clientDto.getClusterState());

        // External
        final StateMapDTO externalStateMap = clientDto.getExternalState();
        assertNotNull(externalStateMap);
        assertEquals(2, externalStateMap.getTotalEntryCount());
        final List<StateEntryDTO> externalState = externalStateMap.getState();
        assertEquals(2, externalState.size());

        assertEquals("e-k1", externalState.get(0).getKey());
        assertEquals("e-v1", externalState.get(0).getValue());
        assertEquals("e-k2", externalState.get(1).getKey());
        assertEquals("e-v2", externalState.get(1).getValue());

        assertNull(externalState.get(0).getClusterNodeId());
        assertNull(externalState.get(1).getClusterNodeId());
        assertNull(externalState.get(0).getClusterNodeAddress());
        assertNull(externalState.get(1).getClusterNodeAddress());

    }

    @Test
    public void testMergeExternalPerNode() throws Exception {

        builder1.buildState(Scope.EXTERNAL)
                .addState("e-k11", "e-v11")
                .addState("e-k12", "e-v12");

        builder2.buildState(Scope.EXTERNAL)
                .addState("e-k21", "e-v21")
                .addState("e-k22", "e-v22");

        builder3.buildState(Scope.EXTERNAL)
                .addState("e-k31", "e-v31")
                .addState("e-k32", "e-v32");


        merger.mergeResponses(clientDto, dtoMap, null, null);

        printDTO(clientDto);

        // Local
        assertNotNull(clientDto.getLocalState());
        assertEquals(0, clientDto.getLocalState().getTotalEntryCount());
        assertEquals(0, clientDto.getLocalState().getState().size());

        // Cluster
        assertNull(clientDto.getClusterState());

        // External
        final StateMapDTO externalStateMap = clientDto.getExternalState();
        assertNotNull(externalStateMap);
        assertEquals(6, externalStateMap.getTotalEntryCount());
        final List<StateEntryDTO> externalState = externalStateMap.getState();
        assertEquals(6, externalState.size());

        assertEquals("e-k11", externalState.get(0).getKey());
        assertEquals("e-v11", externalState.get(0).getValue());
        assertEquals("e-k12", externalState.get(1).getKey());
        assertEquals("e-v12", externalState.get(1).getValue());

        assertEquals("e-k21", externalState.get(2).getKey());
        assertEquals("e-v21", externalState.get(2).getValue());
        assertEquals("e-k22", externalState.get(3).getKey());
        assertEquals("e-v22", externalState.get(3).getValue());

        assertEquals("e-k31", externalState.get(4).getKey());
        assertEquals("e-v31", externalState.get(4).getValue());
        assertEquals("e-k32", externalState.get(5).getKey());
        assertEquals("e-v32", externalState.get(5).getValue());

        assertEquals("1", externalState.get(0).getClusterNodeId());
        assertEquals("1", externalState.get(1).getClusterNodeId());
        assertEquals("2", externalState.get(2).getClusterNodeId());
        assertEquals("2", externalState.get(3).getClusterNodeId());
        assertEquals("3", externalState.get(4).getClusterNodeId());
        assertEquals("3", externalState.get(5).getClusterNodeId());
        assertEquals("localhost:9101", externalState.get(0).getClusterNodeAddress());
        assertEquals("localhost:9101", externalState.get(1).getClusterNodeAddress());
        assertEquals("localhost:9201", externalState.get(2).getClusterNodeAddress());
        assertEquals("localhost:9201", externalState.get(3).getClusterNodeAddress());
        assertEquals("localhost:9301", externalState.get(4).getClusterNodeAddress());
        assertEquals("localhost:9301", externalState.get(5).getClusterNodeAddress());

    }

    @Test
    public void testMergeAll() throws Exception {

        builder1.buildState(Scope.LOCAL)
                .addState("k1-1", "v1-1");
        builder2.buildState(Scope.LOCAL)
                .addState("k2-1", "v2-1")
                .addState("k2-2", "v2-2");
        builder3.buildState(Scope.LOCAL)
                .addState("k3-1", "v3-1")
                .addState("k3-2", "v3-2")
                .addState("k3-3", "v3-3");

        // Node2 is the primary node.
        builder2.buildState(Scope.CLUSTER)
                .addState("c-k1", "c-v1")
                .addState("c-k2", "c-v2");
        builder2.buildState(Scope.EXTERNAL)
                .addState("e-k1", "e-v1")
                .addState("e-k2", "e-v2");

        merger.mergeResponses(clientDto, dtoMap, null, null);

        printDTO(clientDto);

        // Local
        final StateMapDTO localStateMap = clientDto.getLocalState();
        assertNotNull(localStateMap);
        assertEquals(6, localStateMap.getTotalEntryCount());
        final List<StateEntryDTO> localState = localStateMap.getState();
        assertEquals(6, localState.size());
        assertEquals("k1-1", localState.get(0).getKey());
        assertEquals("v1-1", localState.get(0).getValue());
        assertEquals("k2-1", localState.get(1).getKey());
        assertEquals("v2-1", localState.get(1).getValue());
        assertEquals("k2-2", localState.get(2).getKey());
        assertEquals("v2-2", localState.get(2).getValue());
        assertEquals("k3-1", localState.get(3).getKey());
        assertEquals("v3-1", localState.get(3).getValue());
        assertEquals("k3-2", localState.get(4).getKey());
        assertEquals("v3-2", localState.get(4).getValue());
        assertEquals("k3-3", localState.get(5).getKey());
        assertEquals("v3-3", localState.get(5).getValue());

        assertEquals("1", localState.get(0).getClusterNodeId());
        assertEquals("2", localState.get(1).getClusterNodeId());
        assertEquals("2", localState.get(2).getClusterNodeId());
        assertEquals("3", localState.get(3).getClusterNodeId());
        assertEquals("3", localState.get(4).getClusterNodeId());
        assertEquals("3", localState.get(5).getClusterNodeId());

        assertEquals("localhost:9101", localState.get(0).getClusterNodeAddress());
        assertEquals("localhost:9201", localState.get(1).getClusterNodeAddress());
        assertEquals("localhost:9201", localState.get(2).getClusterNodeAddress());
        assertEquals("localhost:9301", localState.get(3).getClusterNodeAddress());
        assertEquals("localhost:9301", localState.get(4).getClusterNodeAddress());
        assertEquals("localhost:9301", localState.get(5).getClusterNodeAddress());

        // Cluster
        final StateMapDTO clusterStateMap = clientDto.getClusterState();
        assertNotNull(clusterStateMap);
        assertEquals(2, clusterStateMap.getTotalEntryCount());
        final List<StateEntryDTO> clusterState = clusterStateMap.getState();
        assertEquals(2, clusterState.size());

        assertEquals("c-k1", clusterState.get(0).getKey());
        assertEquals("c-v1", clusterState.get(0).getValue());
        assertEquals("c-k2", clusterState.get(1).getKey());
        assertEquals("c-v2", clusterState.get(1).getValue());

        assertNull(clusterState.get(0).getClusterNodeId());
        assertNull(clusterState.get(1).getClusterNodeId());
        assertNull(clusterState.get(0).getClusterNodeAddress());
        assertNull(clusterState.get(1).getClusterNodeAddress());

        // External
        final StateMapDTO externalStateMap = clientDto.getExternalState();
        assertNotNull(externalStateMap);
        assertEquals(2, externalStateMap.getTotalEntryCount());
        final List<StateEntryDTO> externalState = externalStateMap.getState();
        assertEquals(2, externalState.size());

        assertEquals("e-k1", externalState.get(0).getKey());
        assertEquals("e-v1", externalState.get(0).getValue());
        assertEquals("e-k2", externalState.get(1).getKey());
        assertEquals("e-v2", externalState.get(1).getValue());

        assertNull(externalState.get(0).getClusterNodeId());
        assertNull(externalState.get(1).getClusterNodeId());
        assertNull(externalState.get(0).getClusterNodeAddress());
        assertNull(externalState.get(1).getClusterNodeAddress());

    }
}
