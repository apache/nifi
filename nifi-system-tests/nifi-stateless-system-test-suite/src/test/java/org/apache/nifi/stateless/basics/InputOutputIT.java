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

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
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
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class InputOutputIT extends StatelessSystemIT {

    @Test
    public void testFlowFileInputProcessedAndOutputProvided() throws IOException, StatelessConfigurationException, InterruptedException {
        // Build flow
        final VersionedFlowSnapshot versionedFlowSnapshot = createFlow();

        // Startup the dataflow
        final StatelessDataflow dataflow = loadDataflow(versionedFlowSnapshot, Collections.emptyList());

        // Enqueue data and trigger
        dataflow.enqueue(new byte[0], Collections.singletonMap("abc", "123"), "In");
        final DataflowTrigger trigger = dataflow.trigger();
        final TriggerResult result = trigger.getResult();
        assertTrue(result.isSuccessful());

        // Validate results
        final List<FlowFile> outputFlowFiles = result.getOutputFlowFiles("Out");
        assertEquals(1, outputFlowFiles.size());

        final FlowFile output = outputFlowFiles.get(0);
        assertEquals("123", output.getAttribute("abc"));
        assertEquals("bar", output.getAttribute("foo"));
    }

    @Test
    public void testMultipleFlowFilesIn() throws IOException, StatelessConfigurationException, InterruptedException {
        // Build flow
        final VersionedFlowSnapshot versionedFlowSnapshot = createFlow();

        // Startup the dataflow
        final StatelessDataflow dataflow = loadDataflow(versionedFlowSnapshot, Collections.emptyList());

        // Enqueue data and trigger
        dataflow.enqueue(new byte[0], Collections.singletonMap("abc", "123"), "In");
        dataflow.enqueue(new byte[0], Collections.singletonMap("abc", "321"), "In");

        DataflowTrigger trigger = dataflow.trigger();
        TriggerResult result = trigger.getResult();
        assertTrue(result.isSuccessful());

        // Triggering once will only process 1 of the FlowFiles and leave the other input FlowFile queued.
        result.acknowledge();

        assertTrue(dataflow.isFlowFileQueued());

        // Validate results of first run
        List<FlowFile> outputFlowFiles = result.getOutputFlowFiles("Out");
        final FlowFile output1 = outputFlowFiles.get(0);
        assertEquals("123", output1.getAttribute("abc"));
        assertEquals("bar", output1.getAttribute("foo"));

        trigger = dataflow.trigger();
        result = trigger.getResult();
        assertTrue(result.isSuccessful());

        result.acknowledge();

        // Validate results of second run
        outputFlowFiles = result.getOutputFlowFiles("Out");
        assertEquals(1, outputFlowFiles.size());

        final FlowFile output2 = outputFlowFiles.get(0);
        assertEquals("321", output2.getAttribute("abc"));
        assertEquals("bar", output2.getAttribute("foo"));
    }


    private VersionedFlowSnapshot createFlow() {
        final VersionedFlowBuilder flowBuilder = new VersionedFlowBuilder();
        final VersionedPort inPort = flowBuilder.createInputPort("In");
        final VersionedPort outPort = flowBuilder.createOutputPort("Out");
        final VersionedProcessor setAttribute = flowBuilder.createProcessor(SYSTEM_TEST_EXTENSIONS_BUNDLE, "org.apache.nifi.processors.tests.system.SetAttribute");
        setAttribute.setProperties(Collections.singletonMap("foo", "bar"));

        flowBuilder.createConnection(inPort, setAttribute, Relationship.ANONYMOUS.getName());
        flowBuilder.createConnection(setAttribute, outPort, "success");

        return flowBuilder.getFlowSnapshot();
    }
}
