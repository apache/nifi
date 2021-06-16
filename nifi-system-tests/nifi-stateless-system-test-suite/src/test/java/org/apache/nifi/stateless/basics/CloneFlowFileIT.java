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
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CloneFlowFileIT extends StatelessSystemIT {

    @Test
    public void testClone() throws IOException, StatelessConfigurationException, InterruptedException {
        // Build the flow
        final VersionedFlowBuilder flowBuilder = new VersionedFlowBuilder();
        final VersionedPort inPort = flowBuilder.createInputPort("In");
        final VersionedPort outPort = flowBuilder.createOutputPort("Out");

        final VersionedProcessor setAttribute = flowBuilder.createSimpleProcessor("SetAttribute");
        setAttribute.setProperties(Collections.singletonMap("foo", "bar"));

        final VersionedProcessor reverse = flowBuilder.createSimpleProcessor("ReverseContents");

        // Clone flowfile from setAttribute to both reverse and output port. Route reverse to output port.
        flowBuilder.createConnection(inPort, setAttribute, Relationship.ANONYMOUS.getName());
        flowBuilder.createConnection(setAttribute, outPort, "success");
        flowBuilder.createConnection(setAttribute, reverse, "success");
        flowBuilder.createConnection(reverse, outPort, "success");

        // Startup the dataflow
        final StatelessDataflow dataflow = loadDataflow(flowBuilder.getFlowSnapshot(), Collections.emptyList());

        // Enqueue data and trigger
        dataflow.enqueue("Hello".getBytes(StandardCharsets.UTF_8), Collections.singletonMap("abc", "123"), "In");
        final DataflowTrigger trigger = dataflow.trigger();
        final TriggerResult result = trigger.getResult();
        assertTrue(result.isSuccessful());
        result.acknowledge();

        final List<FlowFile> flowFiles = result.getOutputFlowFiles("Out");
        assertEquals(2, flowFiles.size());

        final FlowFile first = flowFiles.get(0);
        assertEquals("123", first.getAttribute("abc"));

        final FlowFile second = flowFiles.get(1);
        assertEquals("123", second.getAttribute("abc"));

        final long countNormal = flowFiles.stream()
            .filter(flowFile -> new String(result.readContent(flowFile), StandardCharsets.UTF_8).equals("Hello"))
            .count();

        final long countReversed = flowFiles.stream()
            .filter(flowFile -> new String(result.readContent(flowFile), StandardCharsets.UTF_8).equals("olleH"))
            .count();

        assertEquals(1L, countNormal);
        assertEquals(1L, countReversed);
    }

}
