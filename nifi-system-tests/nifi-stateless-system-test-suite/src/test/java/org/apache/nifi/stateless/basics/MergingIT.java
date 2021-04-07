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

public class MergingIT extends StatelessSystemIT {

    @Test
    public void testSplitMerge() throws IOException, StatelessConfigurationException, InterruptedException {
        // Build the flow
        final VersionedFlowBuilder flowBuilder = new VersionedFlowBuilder();
        final VersionedPort outPort = flowBuilder.createOutputPort("Out");

        final VersionedProcessor generate = flowBuilder.createSimpleProcessor("GenerateFlowFile");
        final Map<String, String> generateProperties = new HashMap<>();
        generateProperties.put("Text", "abc\n123\nxyz\n321");
        generateProperties.put("Batch Size", "3");
        generate.setProperties(generateProperties);

        final VersionedProcessor split = flowBuilder.createSimpleProcessor("SplitByLine");
        final VersionedProcessor merge = flowBuilder.createSimpleProcessor("ConcatenateFlowFiles");
        merge.setProperties(Collections.singletonMap("FlowFile Count", "12"));
        merge.setAutoTerminatedRelationships(Collections.singleton("original"));

        flowBuilder.createConnection(generate, split, "success");
        flowBuilder.createConnection(split, merge, "success");
        flowBuilder.createConnection(merge, outPort, "merged");

        // Startup the dataflow
        final StatelessDataflow dataflow = loadDataflow(flowBuilder.getFlowSnapshot(), Collections.emptyList());

        // Enqueue data and trigger
        final DataflowTrigger trigger = dataflow.trigger();
        final TriggerResult result = trigger.getResult();
        assertTrue(result.isSuccessful());
        result.acknowledge();

        final List<FlowFile> flowFiles = result.getOutputFlowFiles("Out");
        assertEquals(1, flowFiles.size());

        final FlowFile first = flowFiles.get(0);
        final String outputContent = new String(result.readContent(first));
        assertEquals("abc123xyz321abc123xyz321abc123xyz321", outputContent);
    }
}
