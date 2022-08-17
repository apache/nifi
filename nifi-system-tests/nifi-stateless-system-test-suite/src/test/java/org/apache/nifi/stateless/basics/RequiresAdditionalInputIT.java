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
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.stateless.StatelessSystemIT;
import org.apache.nifi.stateless.VersionedFlowBuilder;
import org.apache.nifi.stateless.config.StatelessConfigurationException;
import org.apache.nifi.stateless.flow.DataflowTrigger;
import org.apache.nifi.stateless.flow.StatelessDataflow;
import org.apache.nifi.stateless.flow.TransactionThresholds;
import org.apache.nifi.stateless.flow.TriggerResult;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RequiresAdditionalInputIT extends StatelessSystemIT {

    @Test
    public void testMergeAsFirstProcessor() throws IOException, StatelessConfigurationException, InterruptedException {
        final VersionedFlowBuilder flowBuilder = new VersionedFlowBuilder();
        final VersionedPort inPort = flowBuilder.createInputPort("In");
        final VersionedPort outPort = flowBuilder.createOutputPort("Out");

        final VersionedProcessor merge = flowBuilder.createSimpleProcessor("ConcatenateRangeOfFlowFiles");
        merge.setAutoTerminatedRelationships(new HashSet<>(Arrays.asList("original", "failure")));

        flowBuilder.createConnection(inPort, merge, Relationship.ANONYMOUS.getName());
        flowBuilder.createConnection(merge, outPort, "merged");

        // Startup the dataflow
        final StatelessDataflow dataflow = loadDataflow(flowBuilder.getFlowSnapshot(), Collections.emptyList(), Collections.emptySet(), createTransactionThresholds(1000));

        // Enqueue data and trigger
        for (int i=1; i <= 3; i++) {
            dataflow.enqueue(String.valueOf(i).getBytes(StandardCharsets.UTF_8), Collections.emptyMap(), "In");
        }

        final DataflowTrigger trigger = dataflow.trigger();
        final TriggerResult result = trigger.getResult();
        assertTrue(result.isSuccessful());

        final List<FlowFile> flowFiles = result.getOutputFlowFiles("Out");
        assertEquals(1, flowFiles.size());

        final FlowFile first = flowFiles.get(0);
        final String outputContent = new String(result.readContentAsByteArray(first));
        assertEquals("123", outputContent);

        result.acknowledge();
    }


    @Test
    public void testMergeAsFirstProcessorWithoutEnoughData() throws IOException, StatelessConfigurationException, InterruptedException {
        final VersionedFlowBuilder flowBuilder = new VersionedFlowBuilder();
        final VersionedPort inPort = flowBuilder.createInputPort("In");
        final VersionedPort outPort = flowBuilder.createOutputPort("Out");

        final VersionedProcessor merge = flowBuilder.createSimpleProcessor("ConcatenateRangeOfFlowFiles");
        merge.setProperties(Collections.singletonMap("Minimum Number of Entries", "100"));
        merge.setAutoTerminatedRelationships(new HashSet<>(Arrays.asList("original", "failure")));

        flowBuilder.createConnection(inPort, merge, Relationship.ANONYMOUS.getName());
        flowBuilder.createConnection(merge, outPort, "merged");

        // Startup the dataflow
        final StatelessDataflow dataflow = loadDataflow(flowBuilder.getFlowSnapshot(), Collections.emptyList(), Collections.emptySet(), createTransactionThresholds(1000));

        // Enqueue data and trigger
        for (int i=1; i <= 3; i++) {
            dataflow.enqueue(String.valueOf(i).getBytes(StandardCharsets.UTF_8), Collections.emptyMap(), "In");
        }

        final DataflowTrigger trigger = dataflow.trigger();
        final Optional<TriggerResult> resultOption = trigger.getResult(2, TimeUnit.SECONDS);

        // We expect this to timeout
        assertFalse(resultOption.isPresent());

        trigger.cancel();
    }


    @Test
    public void testMergeDownstream() throws IOException, StatelessConfigurationException, InterruptedException {
        final VersionedFlowBuilder flowBuilder = new VersionedFlowBuilder();
        final VersionedPort inPort = flowBuilder.createInputPort("In");
        final VersionedPort outPort = flowBuilder.createOutputPort("Out");

        final VersionedProcessor firstUpdate = flowBuilder.createSimpleProcessor("UpdateContent");
        final Map<String, String> firstUpdateProperties = new HashMap<>();
        firstUpdateProperties.put("Content", "\n1");
        firstUpdateProperties.put("Update Strategy", "Append");
        firstUpdate.setProperties(firstUpdateProperties);

        final VersionedProcessor secondUpdate = flowBuilder.createSimpleProcessor("UpdateContent");
        final Map<String, String> secondUpdateProperties = new HashMap<>();
        secondUpdateProperties.put("Content", "\n2");
        secondUpdateProperties.put("Update Strategy", "Append");
        secondUpdate.setProperties(secondUpdateProperties);

        final VersionedProcessor thirdUpdate = flowBuilder.createSimpleProcessor("UpdateContent");
        final Map<String, String> thirdUpdateProperties = new HashMap<>();
        thirdUpdateProperties.put("Content", "\n3");
        thirdUpdateProperties.put("Update Strategy", "Append");
        thirdUpdate.setProperties(thirdUpdateProperties);

        final VersionedProcessor merge = flowBuilder.createSimpleProcessor("ConcatenateRangeOfFlowFiles");
        merge.setAutoTerminatedRelationships(new HashSet<>(Arrays.asList("original", "failure")));

        flowBuilder.createConnection(inPort, firstUpdate, Relationship.ANONYMOUS.getName());
        flowBuilder.createConnection(firstUpdate, secondUpdate, "success");
        flowBuilder.createConnection(secondUpdate, thirdUpdate, "success");
        flowBuilder.createConnection(thirdUpdate, merge, "success");
        flowBuilder.createConnection(merge, outPort, "merged");

        // Startup the dataflow
        final StatelessDataflow dataflow = loadDataflow(flowBuilder.getFlowSnapshot(), Collections.emptyList(), Collections.emptySet(), createTransactionThresholds(1000));

        // Enqueue data and trigger
        for (int i=1; i <= 3; i++) {
            dataflow.enqueue(("hello " + i).getBytes(StandardCharsets.UTF_8), Collections.emptyMap(), "In");
        }

        final DataflowTrigger trigger = dataflow.trigger();
        final TriggerResult result = trigger.getResult();
        assertTrue(result.isSuccessful());

        final List<FlowFile> out = result.getOutputFlowFiles("Out");
        assertEquals(1, out.size());
        final byte[] outputContent = result.readContentAsByteArray(out.get(0));
        final String outputText = new String(outputContent, StandardCharsets.UTF_8);

        final StringBuilder expectedContentBuilder = new StringBuilder();
        for (int i=1; i <= 3; i++) {
            expectedContentBuilder.append("hello ").append(i).append("\n1\n2\n3");
        }
        final String expectedContent = expectedContentBuilder.toString();
        assertEquals(expectedContent, outputText);
    }

    @Test
    public void testSourceProcessorsTriggeredAsOftenAsRequired() throws IOException, StatelessConfigurationException, InterruptedException {
        // Build the flow
        final int flowFileCount = 12;
        final VersionedFlowBuilder flowBuilder = new VersionedFlowBuilder();
        final VersionedPort mergedPort = flowBuilder.createOutputPort("merged");
        final VersionedPort originalPort = flowBuilder.createOutputPort("original");

        final VersionedProcessor generate = flowBuilder.createSimpleProcessor("GenerateFlowFile");
        final Map<String, String> generateProperties = new HashMap<>();
        generateProperties.put("Text", "abc");
        generate.setProperties(generateProperties);

        final VersionedProcessor merge = flowBuilder.createSimpleProcessor("ConcatenateFlowFiles");
        merge.setProperties(Collections.singletonMap("FlowFile Count", String.valueOf(flowFileCount)));

        flowBuilder.createConnection(generate, merge, "success");
        flowBuilder.createConnection(merge, mergedPort, "merged");
        flowBuilder.createConnection(merge, originalPort, "original");

        // If allowing only a single FlowFile, we expect the dataflow to timeout.
        final StatelessDataflow timeoutDataflow = loadDataflow(flowBuilder.getFlowSnapshot(), Collections.emptyList(),
            Collections.emptySet(), TransactionThresholds.SINGLE_FLOWFILE);

        final DataflowTrigger timeoutTrigger = timeoutDataflow.trigger();
        final Optional<TriggerResult> optionalResult = timeoutTrigger.getResult(1, TimeUnit.SECONDS);
        assertFalse(optionalResult.isPresent());

        // Startup the dataflow allowing a Transaction Threshold large enough to accommodate
        final StatelessDataflow dataflow = loadDataflow(flowBuilder.getFlowSnapshot(), Collections.emptyList(), Collections.emptySet(), createTransactionThresholds(flowFileCount));

        // Enqueue data and trigger
        final DataflowTrigger trigger = dataflow.trigger();
        final TriggerResult result = trigger.getResult();
        assertTrue(result.isSuccessful());

        final List<FlowFile> mergedFlowFiles = result.getOutputFlowFiles().get("merged");
        assertEquals(1, mergedFlowFiles.size());

        final List<FlowFile> originalFlowFiles = result.getOutputFlowFiles().get("original");
        assertEquals(flowFileCount, originalFlowFiles.size());

        final String outputText = new String(result.readContentAsByteArray(mergedFlowFiles.get(0)));
        final StringBuilder expectedTextBuilder = new StringBuilder();
        for (int i=0; i < flowFileCount; i++) {
            expectedTextBuilder.append("abc");
        }

        final String expectedText = expectedTextBuilder.toString();
        assertEquals(expectedText, outputText);

        result.acknowledge();
    }

    @Test
    public void testMultipleLayersOfAdditionalFlowFilesRequired() throws IOException, StatelessConfigurationException, InterruptedException {
        // Build the flow
        final VersionedFlowBuilder flowBuilder = new VersionedFlowBuilder();
        final VersionedPort outPort = flowBuilder.createOutputPort("out");

        final VersionedProcessor generate = flowBuilder.createSimpleProcessor("GenerateFlowFile");
        final Map<String, String> generateProperties = new HashMap<>();
        generateProperties.put("Text", "abc");
        generate.setProperties(generateProperties);

        final VersionedProcessor levelOne = flowBuilder.createSimpleProcessor("ConcatenateFlowFiles");
        levelOne.setProperties(Collections.singletonMap("FlowFile Count", "3"));
        levelOne.setAutoTerminatedRelationships(Collections.singleton("original"));
        levelOne.setName("Level 1");

        final VersionedProcessor levelTwo = flowBuilder.createSimpleProcessor("ConcatenateFlowFiles");
        levelTwo.setProperties(Collections.singletonMap("FlowFile Count", "2"));
        levelTwo.setAutoTerminatedRelationships(Collections.singleton("original"));
        levelTwo.setName("Level 2");

        final VersionedProcessor levelThree = flowBuilder.createSimpleProcessor("ConcatenateFlowFiles");
        levelThree.setProperties(Collections.singletonMap("FlowFile Count", "6"));
        levelThree.setAutoTerminatedRelationships(Collections.singleton("original"));
        levelThree.setName("Level 3");

        flowBuilder.createConnection(generate, levelOne, "success");
        flowBuilder.createConnection(levelOne, levelTwo, "merged");
        flowBuilder.createConnection(levelTwo, levelThree, "merged");
        flowBuilder.createConnection(levelThree, outPort, "merged");

        // If allowing only a single FlowFile, we expect the dataflow to timeout.
        final StatelessDataflow timeoutDataflow = loadDataflow(flowBuilder.getFlowSnapshot(), Collections.emptyList(),
            Collections.emptySet(), TransactionThresholds.SINGLE_FLOWFILE);

        final DataflowTrigger timeoutTrigger = timeoutDataflow.trigger();
        final Optional<TriggerResult> optionalResult = timeoutTrigger.getResult(1, TimeUnit.SECONDS);
        assertFalse(optionalResult.isPresent());

        // Startup the dataflow allowing a Transaction Threshold large enough to accommodate
        final StatelessDataflow dataflow = loadDataflow(flowBuilder.getFlowSnapshot(), Collections.emptyList(), Collections.emptySet(), createTransactionThresholds(36));

        // Enqueue data and trigger
        final DataflowTrigger trigger = dataflow.trigger();
        final TriggerResult result = trigger.getResult();
        assertTrue(result.isSuccessful());

        final List<FlowFile> mergedFlowFiles = result.getOutputFlowFiles().get("out");
        assertEquals(1, mergedFlowFiles.size());

        final String outputText = new String(result.readContentAsByteArray(mergedFlowFiles.get(0)));
        final StringBuilder expectedTextBuilder = new StringBuilder();
        for (int i=0; i < 36; i++) {
            expectedTextBuilder.append("abc");
        }

        final String expectedText = expectedTextBuilder.toString();
        assertEquals(expectedText, outputText);

        result.acknowledge();
    }

    private TransactionThresholds createTransactionThresholds(final int maxFlowFiles) {
        return new TransactionThresholds() {
            @Override
            public OptionalLong getMaxFlowFiles() {
                return OptionalLong.of(maxFlowFiles);
            }

            @Override
            public OptionalLong getMaxContentSize(final DataUnit dataUnit) {
                return OptionalLong.empty();
            }

            @Override
            public OptionalLong getMaxTime(final TimeUnit timeUnit) {
                return OptionalLong.empty();
            }
        };
    }
}
