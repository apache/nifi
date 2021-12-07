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
import org.apache.nifi.flow.VersionedPort;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.stateless.StatelessSystemIT;
import org.apache.nifi.stateless.VersionedFlowBuilder;
import org.apache.nifi.stateless.config.StatelessConfigurationException;
import org.apache.nifi.stateless.flow.DataflowTrigger;
import org.apache.nifi.stateless.flow.StatelessDataflow;
import org.apache.nifi.stateless.flow.TransactionIngestStrategy;
import org.apache.nifi.stateless.flow.TransactionThresholds;
import org.apache.nifi.stateless.flow.TriggerResult;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.stateless.TransactionThresholdsFactory.createTransactionThresholds;
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
        final StatelessDataflow dataflow = loadDataflow(flowBuilder.getFlowSnapshot());

        // Enqueue data and trigger
        final DataflowTrigger trigger = dataflow.trigger();
        final TriggerResult result = trigger.getResult();
        assertTrue(result.isSuccessful());

        final List<FlowFile> flowFiles = result.getOutputFlowFiles("Out");
        assertEquals(1, flowFiles.size());

        final FlowFile first = flowFiles.get(0);
        final String outputContent = new String(result.readContentAsByteArray(first));
        assertEquals("abc123xyz321abc123xyz321abc123xyz321", outputContent);

        result.acknowledge();
    }

    // InputPort -> PassThrough -> ConcatenateFlowFiles -> OutputPort
    //   ConcatenateFlowFiles simulates MergeContent/Record with Minimum Entries/Records = 1 (no wait for more FlowFiles, merge the current ones)
    //   PassThrough was added before ConcatenateFlowFiles in order to prevent ConcatenateFlowFiles to pull multiple FlowFiles directly from InputPort

    @Test
    public void testMergeUsingLazyIngestStrategy() throws Exception {
        List<String> flowFileContents = executeMergeTest(5, 5, createTransactionThresholds(null, TransactionIngestStrategy.LAZY));

        assertEquals(5, flowFileContents.size());
        for (int i = 0; i < 5; i++) {
            assertEquals(Integer.toString(i), flowFileContents.get(i));
        }
    }

    @Test
    public void testMergeUsingEagerIngestStrategyWithoutThresholdAndMergeCountIsLessThanInputCount() throws Exception {
        List<String> flowFileContents = executeMergeTest(8, 5, createTransactionThresholds(null, TransactionIngestStrategy.EAGER));

        assertEquals(2, flowFileContents.size());
        assertEquals("01234", flowFileContents.get(0));
        assertEquals("567", flowFileContents.get(1));
    }

    @Test
    public void testMergeUsingEagerIngestStrategyWithoutThresholdAndMergeCountIsGreaterThanInputCount() throws Exception {
        List<String> flowFileContents = executeMergeTest(8, 10, createTransactionThresholds(null, TransactionIngestStrategy.EAGER));

        assertEquals(1, flowFileContents.size());
        assertEquals("01234567", flowFileContents.get(0));
    }

    @Test
    public void testMergeUsingEagerIngestStrategyWithThreshold() throws Exception {
        List<String> flowFileContents = executeMergeTest(8, 10, createTransactionThresholds(3, TransactionIngestStrategy.EAGER));

        assertEquals(3, flowFileContents.size());
        assertEquals("012", flowFileContents.get(0));
        assertEquals("345", flowFileContents.get(1));
        assertEquals("67", flowFileContents.get(2));
    }

    private List<String> executeMergeTest(int inputCount, int mergeCount, TransactionThresholds transactionThresholds) throws Exception {
        // Build the flow
        final VersionedFlowBuilder flowBuilder = new VersionedFlowBuilder();
        final VersionedPort inPort = flowBuilder.createInputPort("In");
        final VersionedPort outPort = flowBuilder.createOutputPort("Out");

        final VersionedProcessor passThrough = flowBuilder.createSimpleProcessor("PassThrough");

        final VersionedProcessor merge = flowBuilder.createSimpleProcessor("ConcatenateFlowFiles");
        final Map<String, String> mergeProperties = new HashMap<>();
        mergeProperties.put("FlowFile Count", Integer.toString(mergeCount));
        mergeProperties.put("Strict Size", "false");
        merge.setProperties(mergeProperties);
        merge.setAutoTerminatedRelationships(Collections.singleton("original"));

        flowBuilder.createConnection(inPort, passThrough, Relationship.ANONYMOUS.getName());
        flowBuilder.createConnection(passThrough, merge, "success");
        flowBuilder.createConnection(merge, outPort, "merged");

        // Startup the dataflow
        final StatelessDataflow dataflow = loadDataflow(flowBuilder.getFlowSnapshot(), transactionThresholds);

        // Enqueue data and trigger
        for (int i = 0; i < inputCount; i++) {
            dataflow.enqueue(Integer.toString(i).getBytes(), Collections.emptyMap(), "In");
        }

        List<String> flowFileContents = new ArrayList<>();

        while (dataflow.isFlowFileQueued()) {
            final DataflowTrigger trigger = dataflow.trigger();
            final TriggerResult result = trigger.getResult();
            assertTrue(result.isSuccessful());

            final List<FlowFile> flowFiles = result.getOutputFlowFiles("Out");
            for (FlowFile flowFile : flowFiles) {
                flowFileContents.add(new String(result.readContentAsByteArray(flowFile)));
            }

            result.acknowledge();
        }

        return flowFileContents;
    }

}
