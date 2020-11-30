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

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CreatesFlowFileIT extends StatelessSystemIT {

    @Test
    public void testFlowFileCreated() throws IOException, StatelessConfigurationException, InterruptedException {
        final StatelessDataflow dataflow = loadDataflow(new File("src/test/resources/flows/GenerateFlowFile.json"), Collections.emptyList());
        final DataflowTrigger trigger = dataflow.trigger();
        final TriggerResult result = trigger.getResult();
        assertTrue(result.isSuccessful());

        assertEquals(Collections.singleton("Out"), dataflow.getOutputPortNames());

        final List<FlowFile> flowFiles = result.getOutputFlowFiles("Out");
        assertEquals(1, flowFiles.size());

        final FlowFile flowFile = flowFiles.get(0);
        assertEquals("hello", flowFile.getAttribute("greeting"));

        final byte[] bytes = result.readContent(flowFile);
        assertEquals("Hello", new String(bytes, StandardCharsets.UTF_8));
    }

    @Test
    public void testMultipleFlowFilesCreated() throws IOException, StatelessConfigurationException, InterruptedException {
        final VersionedFlowBuilder builder = new VersionedFlowBuilder();
        final VersionedProcessor generate = builder.createSimpleProcessor("GenerateFlowFile");
        generate.setProperties(Collections.singletonMap("Batch Size", "500"));

        final VersionedProcessor setAttribute = builder.createSimpleProcessor("SetAttribute");
        builder.createConnection(generate, setAttribute, "success");

        final VersionedPort outPort = builder.createOutputPort("Out");
        builder.createConnection(setAttribute, outPort, "success");

        final StatelessDataflow dataflow = loadDataflow(builder.getFlowSnapshot());
        for (int i=0; i < 10; i++) {
            final DataflowTrigger trigger = dataflow.trigger();
            final TriggerResult result = trigger.getResult();

            final List<FlowFile> output = result.getOutputFlowFiles("Out");
            assertEquals(500, output.size());
            result.acknowledge();

            // Wait for the number of FlowFiles queued to be equal to 0. It may take a few milliseconds.
            while (dataflow.isFlowFileQueued()) {
                Thread.sleep(5);
            }
        }
    }
}
