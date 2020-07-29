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

package org.apache.nifi.tests.system.pg;

import org.apache.nifi.groups.FlowFileConcurrency;
import org.apache.nifi.groups.FlowFileOutboundPolicy;
import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.cli.impl.client.nifi.NiFiClientException;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

public class BatchFlowBetweenGroupsIT extends NiFiSystemIT {

    @Test
    public void testSingleConcurrencyAndBatchOutputToBatchInputOutput() throws NiFiClientException, IOException, InterruptedException {
        // Create Flow such as:
        // Generate -> Group A -> Group B -> CountEvents
        // where Group A has FlowFile Concurrency of one at a time, Output Policy = Batch
        // Group B has FlowFile Concurrency of Batch at a time, Output Policy = Batch
        //
        // Group A consists of:
        // Input Port -> Duplicate (output of 5) -> Sleep (delay 10 ms) -> Output Port
        // Group B consists of:
        // Input Port -> Output Port
        //
        // Scenario being tested:
        // Start Group A.
        // Start Generate to Queue up 5 FlowFiles before Group A.
        // Wait for 5 FlowFiles to exist between Group A and Group B
        // Start input port for Group B
        // Wait for all 5 FlowFiles to be ingested into Group B
        // Wait for 5 additional FlowFiles to be queued between A and B
        // Ensure that queue between A and B has 2 FlowFiles
        // Start Output Port of Group B
        // Wait for count from CountEvents to equal 25

        //
        // Build Process Group A
        //
        final ProcessGroupEntity processGroupA = getClientUtil().createProcessGroup("Group A", "root");
        final PortEntity inputPortA = getClientUtil().createInputPort("In", processGroupA.getId());
        final PortEntity outputPortA = getClientUtil().createOutputPort("Out", processGroupA.getId());

        final ProcessorEntity duplicate = getClientUtil().createProcessor("Duplicate", processGroupA.getId());
        getClientUtil().updateProcessorProperties(duplicate, Collections.singletonMap("Output Count", "5"));

        final ProcessorEntity sleep = getClientUtil().createProcessor("Sleep", processGroupA.getId());
        getClientUtil().updateProcessorProperties(sleep, Collections.singletonMap("onTrigger Sleep Time", "10 ms"));

        getClientUtil().createConnection(inputPortA, duplicate);
        getClientUtil().createConnection(duplicate, sleep, "success");
        getClientUtil().createConnection(sleep, outputPortA, "success");

        processGroupA.getComponent().setFlowfileConcurrency(FlowFileConcurrency.SINGLE_FLOWFILE_PER_NODE.name());
        processGroupA.getComponent().setFlowfileOutboundPolicy(FlowFileOutboundPolicy.BATCH_OUTPUT.name());
        getNifiClient().getProcessGroupClient().updateProcessGroup(processGroupA);


        //
        // Build Process Group B
        //
        final ProcessGroupEntity processGroupB = getClientUtil().createProcessGroup("Group B", "root");

        final PortEntity inputPortB = getClientUtil().createInputPort("In", processGroupB.getId());
        final PortEntity outputPortB = getClientUtil().createOutputPort("Out", processGroupB.getId());

        final ConnectionEntity inputPortBToOutputPortB = getClientUtil().createConnection(inputPortB, outputPortB);

        processGroupB.getComponent().setFlowfileConcurrency(FlowFileConcurrency.SINGLE_BATCH_PER_NODE.name());
        processGroupB.getComponent().setFlowfileOutboundPolicy(FlowFileOutboundPolicy.BATCH_OUTPUT.name());
        getNifiClient().getProcessGroupClient().updateProcessGroup(processGroupB);

        //
        // Create external processors
        //
        final ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile");
        getClientUtil().updateProcessorProperties(generate, Collections.singletonMap("Batch Size", "5"));
        final ConnectionEntity generateToInputPortA = getClientUtil().createConnection(generate, inputPortA, "success");

        final ProcessorEntity countEvents = getClientUtil().createProcessor("CountEvents");
        final ConnectionEntity outputPortToCountEvents = getClientUtil().createConnection(outputPortB, countEvents);

        // Connect  Group A to Group B
        final ConnectionEntity interGroupConnection = getClientUtil().createConnection(outputPortA, inputPortB, "root");

        // Start Group A
        getClientUtil().startProcessGroupComponents(processGroupA.getId());

        // Start generate processor and wait for data to queue up. Then stop.
        getNifiClient().getProcessorClient().startProcessor(generate);
        waitForQueueNotEmpty(generateToInputPortA.getId());
        getNifiClient().getProcessorClient().stopProcessor(generate);

        waitForQueueCount(interGroupConnection.getId(), 5);

        // Start input port for Group B
        getNifiClient().getInputPortClient().startInputPort(inputPortB);

        // Wait for all 5 FlowFiles to be ingested into Group B
        waitForQueueCount(inputPortBToOutputPortB.getId(), 5);

        // Wait for 5 additional FlowFiles to be queued between A and B
        waitForQueueCount(interGroupConnection.getId(), 5);

        // Ensure that queue between generate and A has 2 FlowFiles (1 batch in Group B, 1 batch between groups, 1 batch in Group A)
        waitForQueueCount(generateToInputPortA.getId(), 2);
        waitForQueueCount(inputPortBToOutputPortB.getId(), 5);
        waitForQueueCount(interGroupConnection.getId(), 5);

        // Start Output Port of Group B
        getNifiClient().getOutputPortClient().startOutputPort(outputPortB);

        // Wait for count from CountEvents to equal 25
        waitForQueueCount(outputPortToCountEvents.getId(), 25);
    }
}
