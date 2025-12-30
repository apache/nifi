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
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.entity.ComponentStateEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StandaloneStateKeyDropIT extends AbstractStateKeyDropIT {

    @Test
    public void testDeleteStateKeyOnStandalone() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile");
        getClientUtil().updateProcessorProperties(generate, Collections.singletonMap("State Scope", "CLUSTER"));
        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");
        getClientUtil().createConnection(generate, terminate, "success");

        runProcessorOnce(generate);

        // even if the processor is configured to use CLUSTER scope, it will still have
        // a local state because this is a standalone instance
        final Map<String, String> state = getProcessorState(generate.getId(), Scope.CLUSTER);
        assertNull(state.get("count"));

        final Map<String, String> localState = getProcessorState(generate.getId(), Scope.LOCAL);
        assertEquals("1", localState.get("count"));

        // drop specific keys with LOCAL state
        ComponentStateEntity newState = dropProcessorState(generate.getId(), Collections.emptyMap());
        assertNull(newState.getComponentState().getClusterState());
        assertTrue(newState.getComponentState().getLocalState().getState().isEmpty());

        final Map<String, String> after = getProcessorState(generate.getId(), Scope.LOCAL);
        assertTrue(after.isEmpty());

        runProcessorOnce(generate);

        // can drop full state
        dropProcessorState(generate.getId(), null);
        assertTrue(getProcessorState(generate.getId(), Scope.LOCAL).isEmpty());
    }

    @Test
    public void testCannotDropStateKeyIfFlagNotTrue() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity multi = getClientUtil().createProcessor("MultiKeyStateNotDroppable");

        assertFalse(getNifiClient().getProcessorClient().getProcessorState(multi.getId()).getComponentState().isDropStateKeySupported());

        runProcessorOnce(multi);

        final Map<String, String> currentState = getProcessorState(multi.getId(), Scope.LOCAL);
        assertEquals(Map.of("a", "1", "b", "1", "c", "1"), currentState);

        // trying to remove key a
        final Map<String, String> newState = Map.of("b", "1", "c", "1");

        // MultiKeyStateNotDroppable processor has state but has dropStateKeySupported =
        // false so it should also fail
        assertThrows(NiFiClientException.class, () -> {
            dropProcessorState(multi.getId(), newState);
        });
    }

    @Test
    public void testCannotDropStateKeyWithMismatchedState() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity multi = getClientUtil().createProcessor("MultiKeyState");
        runProcessorOnce(multi);

        final Map<String, String> currentState = getProcessorState(multi.getId(), Scope.LOCAL);
        assertEquals(Map.of("a", "1", "b", "1", "c", "1"), currentState);

        // trying to remove key "a" but with wrong value for "b"
        assertThrows(NiFiClientException.class, () -> {
            dropProcessorState(multi.getId(), Map.of("b", "2", "c", "1"));
        });
    }

    @Test
    public void testCannotDropMultipleStateKeys() throws NiFiClientException, IOException, InterruptedException {
        final ProcessorEntity multi = getClientUtil().createProcessor("MultiKeyState");
        runProcessorOnce(multi);

        final Map<String, String> currentState = getProcessorState(multi.getId(), Scope.LOCAL);
        assertEquals(Map.of("a", "1", "b", "1", "c", "1"), currentState);

        // trying to remove two keys
        assertThrows(NiFiClientException.class, () -> {
            dropProcessorState(multi.getId(), Map.of("c", "1"));
        });
    }
}
