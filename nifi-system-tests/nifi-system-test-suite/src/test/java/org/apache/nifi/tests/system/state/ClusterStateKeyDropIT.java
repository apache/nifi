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
package org.apache.nifi.tests.system.state;

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.tests.system.NiFiInstanceFactory;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.entity.ComponentStateEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ClusterStateKeyDropIT extends AbstractStateKeyDropIT {

    @Override
    public NiFiInstanceFactory getInstanceFactory() {
        return createTwoNodeInstanceFactory();
    }

    @Test
    public void testCannotDropStateKeyWithLocalAndClusterState() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile");
        getClientUtil().updateProcessorProperties(generate, Collections.singletonMap("State Scope", "LOCAL"));
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");
        getClientUtil().createConnection(generate, terminate, "success");

        runProcessorOnce(generate);

        getClientUtil().updateProcessorProperties(generate, Collections.singletonMap("State Scope", "CLUSTER"));
        runProcessorOnce(generate);

        // GenerateFlowFile has both local and cluster state, so dropping state should
        // fail
        assertThrows(NiFiClientException.class, () -> {
            dropProcessorState(generate.getId(), Collections.emptyMap());
        });
    }

    @Test
    public void testCannotDropStateKeyIfFlagNotTrue() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity processor = getClientUtil().createProcessor("MultiKeyStateNotDroppable");
        final String processorId = processor.getId();

        assertFalse(getNifiClient().getProcessorClient().getProcessorState(processorId).getComponentState().isDropStateKeySupported());

        runProcessorOnce(processor);

        final Map<String, String> currentState = getProcessorState(processorId, Scope.CLUSTER);
        assertEquals(Map.of("a", "1", "b", "1", "c", "1"), currentState);

        // trying to remove key a
        final Map<String, String> newState = Map.of("b", "1", "c", "1");

        // MultiKeyStateNotDroppable processor has state but has dropStateKeySupported =
        // false so it should also fail
        assertThrows(NiFiClientException.class, () -> {
            dropProcessorState(processorId, newState);
        });
    }

    @Test
    public void testCannotDropStateKeyWithMismatchedState() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity processor = getClientUtil().createProcessor("MultiKeyState");
        final String processorId = processor.getId();

        assertTrue(getNifiClient().getProcessorClient().getProcessorState(processorId).getComponentState().isDropStateKeySupported());

        runProcessorOnce(processor);

        final Map<String, String> currentState = getProcessorState(processorId, Scope.CLUSTER);
        assertEquals(Map.of("a", "1", "b", "1", "c", "1"), currentState);

        // trying to remove key "a" but with wrong value for "b"
        assertThrows(NiFiClientException.class, () -> {
            dropProcessorState(processorId, Map.of("b", "2", "c", "1"));
        });
    }

    @Test
    public void testCannotDropMultipleStateKeys() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity processor = getClientUtil().createProcessor("MultiKeyState");
        final String processorId = processor.getId();
        runProcessorOnce(processor);

        final Map<String, String> currentState = getProcessorState(processorId, Scope.CLUSTER);
        assertEquals(Map.of("a", "1", "b", "1", "c", "1"), currentState);

        // trying to remove two keys
        assertThrows(NiFiClientException.class, () -> {
            dropProcessorState(processorId, Map.of("c", "1"));
        });
    }

    @Test
    public void testCanDropSpecificStateKey() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity processor = getClientUtil().createProcessor("MultiKeyState");
        final String processorId = processor.getId();
        runProcessorOnce(processor);

        final Map<String, String> currentState = getProcessorState(processorId, Scope.CLUSTER);
        assertEquals(Map.of("a", "1", "b", "1", "c", "1"), currentState);

        // trying to remove key a
        final Map<String, String> newState = Map.of("b", "1", "c", "1");
        final ComponentStateEntity response = dropProcessorState(processorId, newState);

        final Map<String, String> updatedState = new HashMap<>();
        response.getComponentState().getClusterState().getState().forEach(entry -> updatedState.put(entry.getKey(), entry.getValue()));
        assertEquals(newState, updatedState);

        final Map<String, String> state = getProcessorState(processorId, Scope.CLUSTER);
        assertEquals(newState, state);
    }

    @Test
    public void testClearAllStateWithNullPayload() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity processor = getClientUtil().createProcessor("MultiKeyState");
        final String processorId = processor.getId();
        runProcessorOnce(processor);

        final Map<String, String> currentState = getProcessorState(processorId, Scope.CLUSTER);
        assertEquals(Map.of("a", "1", "b", "1", "c", "1"), currentState);

        final ComponentStateEntity response = dropProcessorState(processorId, null);
        assertTrue(response.getComponentState().getClusterState().getState().isEmpty());

        final Map<String, String> state = getProcessorState(processorId, Scope.CLUSTER);
        assertTrue(state.isEmpty());
    }

    @Test
    public void testClearAllStateWithEmptyPayload() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity processor = getClientUtil().createProcessor("MultiKeyState");
        final String processorId = processor.getId();
        runProcessorOnce(processor);

        final Map<String, String> currentState = getProcessorState(processorId, Scope.CLUSTER);
        assertEquals(Map.of("a", "1", "b", "1", "c", "1"), currentState);

        final ComponentStateEntity response = dropProcessorState(processorId, Map.of());
        assertTrue(response.getComponentState().getClusterState().getState().isEmpty());

        final Map<String, String> state = getProcessorState(processorId, Scope.CLUSTER);
        assertTrue(state.isEmpty());
    }
}
