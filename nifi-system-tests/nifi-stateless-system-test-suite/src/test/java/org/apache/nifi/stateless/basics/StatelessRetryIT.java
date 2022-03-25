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

import org.apache.nifi.flow.VersionedControllerService;
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

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StatelessRetryIT extends StatelessSystemIT {
    private static final int RETRY_COUNT = 2;
    private static final String EXPECTED_COUNTER = String.valueOf(RETRY_COUNT + 1);

    @Test
    public void testRetryHappensTwiceThenFinishes() throws StatelessConfigurationException, IOException, InterruptedException {
        final VersionedFlowBuilder flowBuilder = new VersionedFlowBuilder();

        //Create a GenerateFlowFile processor
        final VersionedProcessor generateFlowFile = flowBuilder.createSimpleProcessor("GenerateFlowFile");

        //Create a CountFlowFiles processor and configure 2 retries
        final VersionedProcessor countFlowFiles = flowBuilder.createSimpleProcessor("CountFlowFiles");
        countFlowFiles.setMaxBackoffPeriod("1 ms");
        countFlowFiles.setRetryCount(RETRY_COUNT);
        countFlowFiles.setBackoffMechanism("PENALIZE_FLOWFILE");
        countFlowFiles.setRetriedRelationships(Collections.singleton("success"));
        countFlowFiles.setPenaltyDuration("1 ms");

        //Create a CountService and add it to the CountFlowFiles processor
        final VersionedControllerService countService = flowBuilder.createSimpleControllerService("StandardCountService", "CountService");
        flowBuilder.addControllerServiceReference(countFlowFiles, countService, "Count Service");

        //Create an Output port
        final VersionedPort outPort = flowBuilder.createOutputPort("Out");

        //Connect GenerateFlowFile and CountFlowFiles processors
        flowBuilder.createConnection(generateFlowFile, countFlowFiles, "success");

        //Connect Output port and CountFlowFiles processor
        flowBuilder.createConnection(countFlowFiles, outPort, "success");

        // Startup the dataflow
        final StatelessDataflow dataflow = loadDataflow(flowBuilder.getFlowSnapshot(), Collections.emptyList());

        // Enqueue data and trigger
        final DataflowTrigger trigger = dataflow.trigger();
        final TriggerResult result = trigger.getResult();
        assertTrue(result.isSuccessful());

        //Assert that CountService has been called 3 times (2 retries and 1 final run)
        final List<FlowFile> flowFiles = result.getOutputFlowFiles("Out");
        assertEquals(1, flowFiles.size());
        assertEquals(EXPECTED_COUNTER, flowFiles.get(0).getAttribute("count"));

        result.acknowledge();
    }
}
