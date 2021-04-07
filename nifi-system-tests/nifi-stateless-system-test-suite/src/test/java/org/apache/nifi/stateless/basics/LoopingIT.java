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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LoopingIT extends StatelessSystemIT {

    @Test
    public void testLooping() throws IOException, StatelessConfigurationException, InterruptedException {
        // Build the flow
        final VersionedFlowBuilder flowBuilder = new VersionedFlowBuilder();
        final VersionedPort outPort = flowBuilder.createOutputPort("Out");

        final VersionedProcessor generate = flowBuilder.createSimpleProcessor("GenerateFlowFile");
        final Map<String, String> generateProperties = new HashMap<>();
        generateProperties.put("Text", "abc\n123\nxyz\n321");
        generateProperties.put("Batch Size", "3");
        generate.setProperties(generateProperties);

        // Loop the FlowFile 10,000 times to be absolutely sure that we do not encounter any StackOverflowError's
        final String loopIterations = String.valueOf(10_000);
        final VersionedProcessor loop = flowBuilder.createSimpleProcessor("LoopFlowFile");
        loop.setProperties(Collections.singletonMap("Count", loopIterations));

        flowBuilder.createConnection(generate, loop, "success");
        flowBuilder.createConnection(loop, outPort, "finished");
        flowBuilder.createConnection(loop, loop, "loop");

        // Startup the dataflow
        final StatelessDataflow dataflow = loadDataflow(flowBuilder.getFlowSnapshot(), Collections.emptyList());

        // Enqueue data and trigger
        final DataflowTrigger trigger = dataflow.trigger();
        final TriggerResult result = trigger.getResult();
        assertTrue(result.isSuccessful());
        result.acknowledge();

        final List<FlowFile> flowFiles = result.getOutputFlowFiles("Out");
        assertEquals(3, flowFiles.size());

        for (final FlowFile flowFile : flowFiles) {
            assertEquals(loopIterations, flowFile.getAttribute("loop.count"));
        }
    }
}
