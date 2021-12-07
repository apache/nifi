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
import org.apache.nifi.stateless.flow.DataflowTrigger;
import org.apache.nifi.stateless.flow.StatelessDataflow;
import org.apache.nifi.stateless.flow.TransactionIngestStrategy;
import org.apache.nifi.stateless.flow.TransactionThresholds;
import org.apache.nifi.stateless.flow.TriggerResult;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.stateless.TransactionThresholdsFactory.createTransactionThresholds;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TransactionIngestStrategyIT extends StatelessSystemIT {

    @Test
    public void testLazyStrategy() throws Exception {
        testTransactionIngestStrategy(createTransactionThresholds(10, null, TransactionIngestStrategy.LAZY), 1,1);
    }

    @Test
    public void testEagerStrategyUsingMaxFlowFiles() throws Exception {
        testTransactionIngestStrategy(createTransactionThresholds(10, null, TransactionIngestStrategy.EAGER), 1,10);
    }

    @Test
    public void testEagerStrategyUsingMaxBytes() throws Exception {
        testTransactionIngestStrategy(createTransactionThresholds(10, 10, TransactionIngestStrategy.EAGER), 1,4);
    }
    @Test
    public void testLazyStrategyWithBatchedProcessor() throws Exception {
        testTransactionIngestStrategy(createTransactionThresholds(10, null, TransactionIngestStrategy.LAZY), 3,3);
    }

    @Test
    public void testEagerStrategyUsingMaxFlowFilesWithBatchedProcessor() throws Exception {
        testTransactionIngestStrategy(createTransactionThresholds(10, null, TransactionIngestStrategy.EAGER), 3,12);
    }

    @Test
    public void testEagerStrategyUsingMaxBytesWithBatchedProcessor() throws Exception {
        testTransactionIngestStrategy(createTransactionThresholds(10, 10, TransactionIngestStrategy.EAGER), 3,6);
    }

    private void testTransactionIngestStrategy(TransactionThresholds transactionThresholds, int batchSize, int expectedCount) throws Exception {
        // Build the flow
        final VersionedFlowBuilder flowBuilder = new VersionedFlowBuilder();
        final VersionedPort outPort = flowBuilder.createOutputPort("Out");

        final VersionedProcessor generate = flowBuilder.createSimpleProcessor("GenerateFlowFile");
        final Map<String, String> generateProperties = new HashMap<>();
        generateProperties.put("Text", "abc");
        generateProperties.put("Batch Size", Integer.toString(batchSize));
        generate.setProperties(generateProperties);

        flowBuilder.createConnection(generate, outPort, "success");

        // Startup the dataflow
        final StatelessDataflow dataflow = loadDataflow(flowBuilder.getFlowSnapshot(), transactionThresholds);

        // Enqueue data and trigger
        final DataflowTrigger trigger = dataflow.trigger();
        final TriggerResult result = trigger.getResult();
        assertTrue(result.isSuccessful());

        final List<FlowFile> flowFiles = result.getOutputFlowFiles("Out");
        assertEquals(expectedCount, flowFiles.size());

        result.acknowledge();
    }

}
