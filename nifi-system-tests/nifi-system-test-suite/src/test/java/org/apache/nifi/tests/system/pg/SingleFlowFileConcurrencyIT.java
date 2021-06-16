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
import org.apache.nifi.web.api.entity.FlowFileEntity;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SingleFlowFileConcurrencyIT extends NiFiSystemIT {


    @Test
    public void testSingleConcurrency() throws NiFiClientException, IOException, InterruptedException {
        final ProcessGroupEntity processGroupEntity = getClientUtil().createProcessGroup("My Group", "root");
        final PortEntity inputPort = getClientUtil().createInputPort("In", processGroupEntity.getId());
        final PortEntity outputPort = getClientUtil().createOutputPort("Out", processGroupEntity.getId());

        final ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile");
        getClientUtil().updateProcessorProperties(generate, Collections.singletonMap("Batch Size", "3"));
        getClientUtil().updateProcessorSchedulingPeriod(generate, "10 mins");

        final ProcessorEntity sleep = getClientUtil().createProcessor("Sleep", processGroupEntity.getId());
        getClientUtil().updateProcessorProperties(sleep, Collections.singletonMap("onTrigger Sleep Time", "5 sec"));

        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");

        // Connect Generate -> Input Port -> Sleep -> Output Port -> Terminate
        // Since we will use Single FlowFile at a time concurrency, we should see that the connection between Input Port and Sleep
        // never has more than 1 FlowFile even though the Sleep processor takes a long time.
        final ConnectionEntity generateToInput = getClientUtil().createConnection(generate, inputPort, "success");
        final ConnectionEntity inputToSleep = getClientUtil().createConnection(inputPort, sleep);
        getClientUtil().createConnection(sleep, outputPort, "success");
        final ConnectionEntity outputToTerminate = getClientUtil().createConnection(outputPort, terminate);

        processGroupEntity.getComponent().setFlowfileConcurrency(FlowFileConcurrency.SINGLE_FLOWFILE_PER_NODE.name());
        getNifiClient().getProcessGroupClient().updateProcessGroup(processGroupEntity);

        // Start all components except for Terminate. We want the data to queue up before terminate so we can ensure that the
        // correct number of FlowFiles are queued up.
        getClientUtil().startProcessGroupComponents("root");
        getNifiClient().getProcessorClient().stopProcessor(terminate);

        // Wait for 1 FlowFile to queue up for the Sleep Processor. This should leave 2 FlowFiles queued up for the input port.
        waitForQueueCount(inputToSleep.getId(), 1);
        assertEquals(2, getConnectionQueueSize(generateToInput.getId()));

        // Wait until only 1 FlowFile is queued up for the input port. Because Sleep should take 5 seconds to complete its job,
        // It should take 5 seconds for this to happen. But it won't be exact. So we'll ensure that it takes at least 3 seconds. We could
        // put an upper bound such as 6 or 7 seconds as well, but it's a good idea to avoid that because the tests may run in some environments
        // with constrained resources that may take a lot longer to run.
        final long startTime = System.currentTimeMillis();
        waitForQueueCount(generateToInput.getId(), 1);
        final long endTime = System.currentTimeMillis();
        final long delay = endTime - startTime;
        assertTrue(delay > 3000L);

        assertEquals(1, getConnectionQueueSize(inputToSleep.getId()));

        waitForQueueCount(outputToTerminate.getId(), 2);

        // Wait until all FlowFiles have been ingested.
        waitForQueueCount(generateToInput.getId(), 0);
        assertEquals(1, getConnectionQueueSize(inputToSleep.getId()));

        // Ensure that 3 FlowFiles are queued up for Terminate
        waitForQueueCount(outputToTerminate.getId(), 3);
    }


    @Test
    public void testSingleConcurrencyAndBatchOutput() throws NiFiClientException, IOException, InterruptedException {
        final ProcessGroupEntity processGroupEntity = getClientUtil().createProcessGroup("My Group", "root");
        final PortEntity inputPort = getClientUtil().createInputPort("In", processGroupEntity.getId());
        final PortEntity outputPort = getClientUtil().createOutputPort("Out", processGroupEntity.getId());
        final PortEntity secondOut = getClientUtil().createOutputPort("Out2", processGroupEntity.getId());

        final ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile");
        getClientUtil().updateProcessorSchedulingPeriod(generate, "10 mins");

        final ProcessorEntity sleep = getClientUtil().createProcessor("Sleep", processGroupEntity.getId()); // sleep with default configuration is just a simple pass-through

        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");

        // Connect Generate -> Input Port -> Count -> Output Port -> Terminate
        // Also connect InputPort -> Out2 -> Terminate
        final ConnectionEntity generateToInput = getClientUtil().createConnection(generate, inputPort, "success");
        final ConnectionEntity inputToSleep = getClientUtil().createConnection(inputPort, sleep);
        final ConnectionEntity sleepToOutput = getClientUtil().createConnection(sleep, outputPort, "success");
        final ConnectionEntity inputToSecondOut = getClientUtil().createConnection(inputPort, secondOut);
        final ConnectionEntity outputToTerminate = getClientUtil().createConnection(outputPort, terminate);
        final ConnectionEntity secondOutToTerminate = getClientUtil().createConnection(secondOut, terminate);

        processGroupEntity.getComponent().setFlowfileConcurrency(FlowFileConcurrency.SINGLE_FLOWFILE_PER_NODE.name());
        processGroupEntity.getComponent().setFlowfileOutboundPolicy(FlowFileOutboundPolicy.BATCH_OUTPUT.name());
        getNifiClient().getProcessGroupClient().updateProcessGroup(processGroupEntity);

        // Start generate so that data is created. Start Input Port so that the data is ingested.
        // Start Output Ports but not the Sleep processor. This will keep data queued up for the Sleep processor,
        // and that should prevent data from being transferred by Output Port "Out2" also.
        getNifiClient().getProcessorClient().startProcessor(generate);
        getNifiClient().getInputPortClient().startInputPort(inputPort);
        getNifiClient().getOutputPortClient().startOutputPort(outputPort);
        getNifiClient().getOutputPortClient().startOutputPort(secondOut);

        waitForQueueCount(inputToSleep.getId(), 1);
        assertEquals(1, getConnectionQueueSize(inputToSecondOut.getId()));

        // Wait 3 seconds to ensure that data is never transferred
        for (int i=0; i < 3; i++) {
            Thread.sleep(1000L);
            assertEquals(1, getConnectionQueueSize(inputToSleep.getId()));
            assertEquals(1, getConnectionQueueSize(inputToSecondOut.getId()));
        }

        // Start Sleep
        getNifiClient().getProcessorClient().startProcessor(sleep);

        // Data should now flow from both output ports.
        waitForQueueCount(inputToSleep.getId(), 0);
        waitForQueueCount(inputToSecondOut.getId(), 0);

        assertEquals(1, getConnectionQueueSize(outputToTerminate.getId()));
        assertEquals(1, getConnectionQueueSize(secondOutToTerminate.getId()));

        final FlowFileEntity outputToTerminateFlowFile = getClientUtil().getQueueFlowFile(outputToTerminate.getId(), 0);
        assertNotNull(outputToTerminateFlowFile);
        final Map<String, String> attributes = outputToTerminateFlowFile.getFlowFile().getAttributes();
        assertEquals("1", attributes.get("batch.output.Out"));
        assertEquals("1", attributes.get("batch.output.Out2"));

        final FlowFileEntity secondOutFlowFile = getClientUtil().getQueueFlowFile(outputToTerminate.getId(), 0);
        assertNotNull(secondOutFlowFile);
        final Map<String, String> secondOutAttributes = secondOutFlowFile.getFlowFile().getAttributes();
        assertEquals("1", secondOutAttributes.get("batch.output.Out"));
        assertEquals("1", secondOutAttributes.get("batch.output.Out2"));
    }


    @Test
    public void testBatchOutputHasCorrectNumbersOnRestart() throws NiFiClientException, IOException, InterruptedException {
        final ProcessGroupEntity processGroupEntity = getClientUtil().createProcessGroup("My Group", "root");
        final PortEntity inputPort = getClientUtil().createInputPort("In", processGroupEntity.getId());
        final PortEntity outputPort = getClientUtil().createOutputPort("Out", processGroupEntity.getId());
        PortEntity secondOut = getClientUtil().createOutputPort("Out2", processGroupEntity.getId());

        final ProcessorEntity generate = getClientUtil().createProcessor("GenerateFlowFile");
        getClientUtil().updateProcessorSchedulingPeriod(generate, "10 mins");

        final ProcessorEntity terminate = getClientUtil().createProcessor("TerminateFlowFile");

        // Connect Generate -> Input Port -> Count -> Output Port -> Terminate
        // Also connect InputPort -> Out2 -> Terminate
        final ConnectionEntity generateToInput = getClientUtil().createConnection(generate, inputPort, "success");
        final ConnectionEntity inputToOutput = getClientUtil().createConnection(inputPort, outputPort);
        final ConnectionEntity inputToSecondOut = getClientUtil().createConnection(inputPort, secondOut);
        final ConnectionEntity outputToTerminate = getClientUtil().createConnection(outputPort, terminate);
        final ConnectionEntity secondOutToTerminate = getClientUtil().createConnection(secondOut, terminate);

        processGroupEntity.getComponent().setFlowfileConcurrency(FlowFileConcurrency.SINGLE_FLOWFILE_PER_NODE.name());
        processGroupEntity.getComponent().setFlowfileOutboundPolicy(FlowFileOutboundPolicy.BATCH_OUTPUT.name());
        getNifiClient().getProcessGroupClient().updateProcessGroup(processGroupEntity);

        // Start generate so that data is created. Start Input Port so that the data is ingested.
        // Start "Out" Output Ports but "Out2.". This will keep data queued up for the Out2 output port.
        getNifiClient().getProcessorClient().startProcessor(generate);
        getNifiClient().getInputPortClient().startInputPort(inputPort);
        getNifiClient().getOutputPortClient().startOutputPort(outputPort);

        waitForQueueCount(inputToSecondOut.getId(), 1);
        assertEquals(1, getConnectionQueueSize(inputToSecondOut.getId()));

        // Stop processor so that it won't generate data upon restart
        getNifiClient().getProcessorClient().stopProcessor(generate);

        // Everything is queued up at an Output Port so the first Output Port should run and its queue should become empty.
        waitForQueueCount(inputToOutput.getId(), 0);

        // Wait a bit before shutting down so that nifi has a chance to save the changes to the flow
        Thread.sleep(2000L);

        // Restart nifi.
        getNiFiInstance().stop();
        getNiFiInstance().start(true);

        // After nifi, we should see that one FlowFile is still queued up for Out2.
        assertEquals(1, getConnectionQueueSize(inputToSecondOut.getId()));

        // Get a new copy of the Out2 port because NiFi has restarted so that Revision will be different.
        secondOut = getNifiClient().getOutputPortClient().getOutputPort(secondOut.getId());
        getNifiClient().getOutputPortClient().startOutputPort(secondOut);

        // Data should now flow from both output ports.
        waitForQueueCount(inputToOutput.getId(), 0);
        waitForQueueCount(inputToSecondOut.getId(), 0);
        waitForQueueCount(outputToTerminate.getId(), 1);
        waitForQueueCount(secondOutToTerminate.getId(), 1);

        // Each FlowFile should now have attributes batch.output.Out = 1, batch.output.Out2 = 1
        // Even though upon restart, the "Out" Port had no FlowFiles because there still was 1 FlowFile that went to Out as part of the same batch.
        final FlowFileEntity outputToTerminateFlowFile = getClientUtil().getQueueFlowFile(outputToTerminate.getId(), 0);
        assertNotNull(outputToTerminateFlowFile);
        final Map<String, String> attributes = outputToTerminateFlowFile.getFlowFile().getAttributes();
        assertEquals("1", attributes.get("batch.output.Out"));
        assertEquals("1", attributes.get("batch.output.Out2"));

        final FlowFileEntity secondOutFlowFile = getClientUtil().getQueueFlowFile(outputToTerminate.getId(), 0);
        assertNotNull(secondOutFlowFile);
        final Map<String, String> secondOutAttributes = secondOutFlowFile.getFlowFile().getAttributes();
        assertEquals("1", secondOutAttributes.get("batch.output.Out"));
        assertEquals("1", secondOutAttributes.get("batch.output.Out2"));
    }

}
