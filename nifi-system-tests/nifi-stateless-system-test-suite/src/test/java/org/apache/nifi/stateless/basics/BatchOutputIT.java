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

import org.apache.nifi.flow.VersionedPort;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.stateless.StatelessSystemIT;
import org.apache.nifi.stateless.VersionedFlowBuilder;
import org.apache.nifi.stateless.config.StatelessConfigurationException;
import org.apache.nifi.stateless.flow.DataflowTrigger;
import org.apache.nifi.stateless.flow.StatelessDataflow;
import org.apache.nifi.stateless.flow.TriggerResult;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BatchOutputIT extends StatelessSystemIT {

    @Test
    public void testOutputOfSingleFlowFileRuns() throws IOException, StatelessConfigurationException, InterruptedException {
        // Build the flow
        final VersionedFlowBuilder flowBuilder = new VersionedFlowBuilder();
        final VersionedProcessGroup rootGroup = flowBuilder.getRootGroup();

        final VersionedProcessGroup innerGroup = flowBuilder.createProcessGroup("Inner", rootGroup);
        innerGroup.setFlowFileOutboundPolicy("BATCH_OUTPUT");

        final VersionedProcessor generate = flowBuilder.createSimpleProcessor("GenerateFlowFile", innerGroup);
        generate.setProperties(Collections.singletonMap("Text", "Hello World"));
        final VersionedPort outputPort = flowBuilder.createOutputPort("Out", innerGroup);
        flowBuilder.createConnection(generate, outputPort, "success", innerGroup);

        final VersionedPort fin = flowBuilder.createOutputPort("Fin");
        flowBuilder.createConnection(outputPort, fin, "", rootGroup);

        // Startup the dataflow allowing a Transaction Threshold large enough to accommodate
        final StatelessDataflow dataflow = loadDataflow(flowBuilder.getFlowSnapshot());

        // Enqueue data and trigger
        final DataflowTrigger trigger = dataflow.trigger();
        final TriggerResult result = trigger.getResult();
        assertTrue(result.isSuccessful());

        final List<FlowFile> outputFlowFiles = result.getOutputFlowFiles("Fin");
        assertEquals(1, outputFlowFiles.size());

        final String outputText = new String(result.readContentAsByteArray(outputFlowFiles.get(0)), StandardCharsets.UTF_8);
        assertEquals("Hello World", outputText);
    }

    @Test
    public void testWaitsForAvailableFlowFiles() throws IOException, StatelessConfigurationException, InterruptedException {
        // Build the flow
        final VersionedFlowBuilder flowBuilder = new VersionedFlowBuilder();
        final VersionedProcessGroup rootGroup = flowBuilder.getRootGroup();

        final VersionedProcessGroup innerGroup = flowBuilder.createProcessGroup("Inner", rootGroup);
        innerGroup.setFlowFileOutboundPolicy("BATCH_OUTPUT");

        final VersionedProcessor generate = flowBuilder.createSimpleProcessor("GenerateFlowFile", innerGroup);
        generate.setProperties(Collections.singletonMap("Batch Size", "100"));

        final VersionedProcessor route = flowBuilder.createSimpleProcessor("RoundRobinFlowFiles", innerGroup);
        route.setProperties(Collections.singletonMap("Number of Relationships", "2"));

        final VersionedProcessor sleep1 = flowBuilder.createSimpleProcessor("Sleep", innerGroup);
        sleep1.setProperties(Collections.singletonMap("onTrigger Sleep Time", "5 ms"));

        final VersionedProcessor sleep2 = flowBuilder.createSimpleProcessor("Sleep", innerGroup);
        sleep2.setProperties(Collections.singletonMap("onTrigger Sleep Time", "5 ms"));

        final VersionedProcessor sleep3 = flowBuilder.createSimpleProcessor("Sleep", innerGroup);
        sleep3.setProperties(Collections.singletonMap("onTrigger Sleep Time", "5 ms"));

        final VersionedPort outputPort = flowBuilder.createOutputPort("Out", innerGroup);
        flowBuilder.createConnection(generate, route, "success", innerGroup);
        flowBuilder.createConnection(route, outputPort, "1", innerGroup);
        flowBuilder.createConnection(route, sleep1, "2", innerGroup);
        flowBuilder.createConnection(sleep1, sleep2, "success", innerGroup);
        flowBuilder.createConnection(sleep2, sleep3, "success", innerGroup);
        flowBuilder.createConnection(sleep3, outputPort, "success", innerGroup);

        final VersionedPort fin = flowBuilder.createOutputPort("Fin");
        flowBuilder.createConnection(outputPort, fin, "", rootGroup);

        // Startup the dataflow allowing a Transaction Threshold large enough to accommodate
        final StatelessDataflow dataflow = loadDataflow(flowBuilder.getFlowSnapshot());

        // Enqueue data and trigger
        final DataflowTrigger trigger = dataflow.trigger();
        final TriggerResult result = trigger.getResult();
        assertTrue(result.isSuccessful());

        final List<FlowFile> outputFlowFiles = result.getOutputFlowFiles("Fin");
        assertEquals(100, outputFlowFiles.size());
    }
}
