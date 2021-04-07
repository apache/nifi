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

package org.apache.nifi.stateless.basics;

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.registry.flow.VersionedConnection;
import org.apache.nifi.registry.flow.VersionedPort;
import org.apache.nifi.registry.flow.VersionedProcessor;
import org.apache.nifi.stateless.StatelessSystemIT;
import org.apache.nifi.stateless.VersionedFlowBuilder;
import org.apache.nifi.stateless.config.StatelessConfigurationException;
import org.apache.nifi.stateless.flow.DataflowTrigger;
import org.apache.nifi.stateless.flow.StatelessDataflow;
import org.apache.nifi.stateless.flow.TriggerResult;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class StateStorageIT extends StatelessSystemIT {
    @Test
    public void testStateAvailableOnSuccess() throws InterruptedException, IOException, StatelessConfigurationException {
        // Build the flow
        final VersionedFlowBuilder flowBuilder = new VersionedFlowBuilder();

        final VersionedProcessor generate = flowBuilder.createSimpleProcessor("GenerateFlowFile");
        final VersionedPort outPort = flowBuilder.createOutputPort("Out");
        final VersionedConnection connection = flowBuilder.createConnection(generate, outPort, "success");

        // Startup the dataflow
        final StatelessDataflow dataflow = loadDataflow(flowBuilder.getFlowSnapshot(), Collections.emptyList());

        // Enqueue data and trigger
        final DataflowTrigger trigger = dataflow.trigger();
        final TriggerResult result = trigger.getResult();
        assertTrue(result.isSuccessful());
        result.acknowledge();

        final Map<String, String> states = dataflow.getComponentStates(Scope.LOCAL);
        assertEquals(1, states.size());

        final String state = states.values().iterator().next();
        assertNotNull(state);

        assertTrue(state.contains("\"count\""));
    }

    @Test
    public void testStateRolledBackOnCancel() throws InterruptedException, IOException, StatelessConfigurationException {
        // Build the flow
        final VersionedFlowBuilder flowBuilder = new VersionedFlowBuilder();

        final VersionedProcessor generate = flowBuilder.createSimpleProcessor("GenerateFlowFile");
        final VersionedPort outPort = flowBuilder.createOutputPort("Out");
        final VersionedConnection connection = flowBuilder.createConnection(generate, outPort, "success");

        // Startup the dataflow
        final StatelessDataflow dataflow = loadDataflow(flowBuilder.getFlowSnapshot(), Collections.emptyList());

        // Enqueue data and trigger. Then cancel.
        DataflowTrigger trigger = dataflow.trigger();
        TriggerResult result = trigger.getResult();
        assertTrue(result.isSuccessful());
        trigger.cancel();

        // State should not be stored.
        Map<String, String> states = dataflow.getComponentStates(Scope.LOCAL);
        assertTrue(states.isEmpty());

        // Trigger a few times, acknowledging it
        for (int i=0; i < 3; i++) {
            trigger = dataflow.trigger();
            result = trigger.getResult();
            assertTrue(result.isSuccessful());
            result.acknowledge();

            states = dataflow.getComponentStates(Scope.LOCAL);
            assertEquals(1, states.size());

            final String state = states.values().iterator().next();
            assertTrue(state.contains("\"count\":\"" + (i+1) + "\""));
        }

        // Trigger again and cancel. State should still be available but should be the previously stored state.
        trigger = dataflow.trigger();
        result = trigger.getResult();
        assertTrue(result.isSuccessful());
        trigger.cancel();

        // State should not be stored.
        states = dataflow.getComponentStates(Scope.LOCAL);
        assertEquals(1, states.size());

        final String state = states.values().iterator().next();
        assertTrue(state.contains("\"count\":\"3\""));
    }

    @Test
    public void testStateNotStoredOnException() throws InterruptedException, IOException, StatelessConfigurationException {
        // Build the flow
        final VersionedFlowBuilder flowBuilder = new VersionedFlowBuilder();

        final VersionedProcessor generate = flowBuilder.createSimpleProcessor("GenerateFlowFile");
        final VersionedProcessor failure = flowBuilder.createSimpleProcessor("ThrowProcessException");
        final VersionedConnection connection = flowBuilder.createConnection(generate, failure, "success");

        // Startup the dataflow
        final StatelessDataflow dataflow = loadDataflow(flowBuilder.getFlowSnapshot(), Collections.emptyList());

        // Trigger dataflow. It should result in failure because of the ThrowProcessException processor
        final DataflowTrigger trigger = dataflow.trigger();
        final TriggerResult result = trigger.getResult();
        assertFalse(result.isSuccessful());
        result.acknowledge();

        final Map<String, String> states = dataflow.getComponentStates(Scope.LOCAL);
        assertTrue(states.isEmpty());
    }

}
