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
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.stateless.StatelessSystemIT;
import org.apache.nifi.stateless.VersionedFlowBuilder;
import org.apache.nifi.stateless.config.StatelessConfigurationException;
import org.apache.nifi.stateless.flow.DataflowTrigger;
import org.apache.nifi.stateless.flow.StatelessDataflow;
import org.apache.nifi.stateless.flow.TriggerResult;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PartitioningContentIT extends StatelessSystemIT {

    @Override
    protected Optional<File> getContentRepoDirectory() {
        return Optional.of(new File("target/content-repo"));
    }

    @Test
    public void testPartitionThenUpdate() throws IOException, StatelessConfigurationException, InterruptedException {
        final VersionedFlowBuilder flowBuilder = new VersionedFlowBuilder();
        final VersionedPort outPort = flowBuilder.createOutputPort("Out");

        final VersionedProcessor generate = flowBuilder.createSimpleProcessor("GenerateFlowFile");
        final Map<String, String> generateProperties = new HashMap<>();
        generateProperties.put("Text", "abc\n123\nxyz\n321");
        generateProperties.put("Batch Size", "1");
        generate.setProperties(generateProperties);

        final VersionedProcessor partition = flowBuilder.createSimpleProcessor("PartitionText");

        flowBuilder.createConnection(generate, partition, "success");
        flowBuilder.createConnection(partition, outPort, "success");

        // Startup the dataflow
        final StatelessDataflow dataflow = loadDataflow(flowBuilder.getFlowSnapshot(), Collections.emptyList());

        // Enqueue data and trigger
        final DataflowTrigger trigger = dataflow.trigger();
        final TriggerResult result = trigger.getResult();
        assertTrue(result.isSuccessful());

        final List<FlowFile> flowFiles = result.getOutputFlowFiles("Out");
        assertEquals(2, flowFiles.size());

        final String[] partitionedContent = new String[] {"abc\nxyz\n", "123\n321\n"};
        final String[] expectedContent = partitionedContent;
        for (int i=0; i < expectedContent.length; i++) {
            final String expected = expectedContent[i];

            final FlowFile flowFile = flowFiles.get(i);
            final String outputContent = new String(result.readContentAsByteArray(flowFile));
            assertEquals(expected, outputContent);
        }

        result.acknowledge();
    }
}
