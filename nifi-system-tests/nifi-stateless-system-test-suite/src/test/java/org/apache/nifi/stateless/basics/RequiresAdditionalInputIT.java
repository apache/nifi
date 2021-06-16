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
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.registry.flow.VersionedPort;
import org.apache.nifi.registry.flow.VersionedProcessor;
import org.apache.nifi.stateless.StatelessSystemIT;
import org.apache.nifi.stateless.VersionedFlowBuilder;
import org.apache.nifi.stateless.config.StatelessConfigurationException;
import org.apache.nifi.stateless.flow.DataflowTrigger;
import org.apache.nifi.stateless.flow.StatelessDataflow;
import org.apache.nifi.stateless.flow.TransactionThresholds;
import org.apache.nifi.stateless.flow.TriggerResult;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RequiresAdditionalInputIT extends StatelessSystemIT {

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
        result.acknowledge();

        final List<FlowFile> mergedFlowFiles = result.getOutputFlowFiles().get("merged");
        assertEquals(1, mergedFlowFiles.size());

        final List<FlowFile> originalFlowFiles = result.getOutputFlowFiles().get("original");
        assertEquals(flowFileCount, originalFlowFiles.size());

        final String outputText = new String(result.readContent(mergedFlowFiles.get(0)));
        final StringBuilder expectedTextBuilder = new StringBuilder();
        for (int i=0; i < flowFileCount; i++) {
            expectedTextBuilder.append("abc");
        }

        final String expectedText = expectedTextBuilder.toString();
        assertEquals(expectedText, outputText);
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
        result.acknowledge();

        final List<FlowFile> mergedFlowFiles = result.getOutputFlowFiles().get("out");
        assertEquals(1, mergedFlowFiles.size());

        final String outputText = new String(result.readContent(mergedFlowFiles.get(0)));
        final StringBuilder expectedTextBuilder = new StringBuilder();
        for (int i=0; i < 36; i++) {
            expectedTextBuilder.append("abc");
        }

        final String expectedText = expectedTextBuilder.toString();
        assertEquals(expectedText, outputText);
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
