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
package org.apache.nifi.processors.azure.eventhub;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.PartitionReceiver;
import com.microsoft.azure.servicebus.ServiceBusException;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedList;
import java.util.concurrent.ExecutionException;


public class GetAzureEventHubTest {

    private static final String namespaceName = "nifi-azure-hub";
    private static final String eventHubName = "get-test";
    private static final String sasKeyName = "bogus-policy";
    private static final String sasKey = "9rHmHqxoOVWOb8wS09dvqXYxnNiLqxNMCbmt6qMaQyU!";


    private TestRunner testRunner;
    private MockGetAzureEventHub processor;

    @Before
    public void setUp() throws Exception {
        processor = new MockGetAzureEventHub();
        testRunner = TestRunners.newTestRunner(processor);
    }

    @Test
    public void testProcessorConfigValidity() {
        testRunner.setProperty(GetAzureEventHub.EVENT_HUB_NAME,eventHubName);
        testRunner.assertNotValid();
        testRunner.setProperty(GetAzureEventHub.NAMESPACE,namespaceName);
        testRunner.assertNotValid();
        testRunner.setProperty(GetAzureEventHub.ACCESS_POLICY,sasKeyName);
        testRunner.assertNotValid();
        testRunner.setProperty(GetAzureEventHub.POLICY_PRIMARY_KEY,sasKey);
        testRunner.assertNotValid();
        testRunner.setProperty(GetAzureEventHub.NUM_PARTITIONS,"4");
        testRunner.assertValid();
    }
    @Test
    public void verifyRelationships(){

        assert(1 == processor.getRelationships().size());

    }
    @Test
    public void testNoPartitions(){
        MockGetAzureEventHubNoPartitions mockProcessor = new MockGetAzureEventHubNoPartitions();
        testRunner = TestRunners.newTestRunner(mockProcessor);
        setUpStandardTestConfig();
        testRunner.run(1, true);
        testRunner.assertAllFlowFilesTransferred(GetAzureEventHub.REL_SUCCESS, 0);
        testRunner.clearTransferState();
    }
    @Test
    public void testNullRecieve(){
        setUpStandardTestConfig();
        processor.nullReceive = true;
        testRunner.run(1, true);
        testRunner.assertAllFlowFilesTransferred(GetAzureEventHub.REL_SUCCESS, 0);
        testRunner.clearTransferState();
    }
    @Test(expected = AssertionError.class)
    public void testThrowGetReceiver(){
        setUpStandardTestConfig();
        processor.getReceiverThrow = true;
        testRunner.run(1, true);
        testRunner.assertAllFlowFilesTransferred(GetAzureEventHub.REL_SUCCESS, 0);
        testRunner.clearTransferState();
    }
    @Test
    public void testNormalFlow() throws Exception {

        setUpStandardTestConfig();
        testRunner.run(1, true);
        testRunner.assertAllFlowFilesTransferred(GetAzureEventHub.REL_SUCCESS, 10);
        testRunner.clearTransferState();
    }

    /**
     * Provides a stubbed processor instance for testing
     */
    public static class MockGetAzureEventHub extends GetAzureEventHub{

        boolean nullReceive = false;
        boolean getReceiverThrow = false;

        @Override
        protected void setupReceiver(final String connectionString) throws ProcessException{
            //do nothing
        }
        @Override
        protected PartitionReceiver getReceiver(final ProcessContext context, final String partitionId) throws IOException, ServiceBusException, ExecutionException, InterruptedException {
            if(getReceiverThrow){
                throw new IOException("Could not create receiver");
            }
            return null;
        }

        @Override
        protected Iterable<EventData> receiveEvents(final ProcessContext context, final String partitionId) throws ProcessException{
            if(nullReceive){
                return null;
            }
            if(getReceiverThrow){
                throw new ProcessException("Could not create receiver");
            }
            final LinkedList<EventData> receivedEvents = new LinkedList<>();
            for(int i = 0; i < 10; i++){
                final EventData eventData = new EventData(String.format("test event number: %d",i).getBytes());
                Whitebox.setInternalState(eventData,"isReceivedEvent",true);
                Whitebox.setInternalState(eventData, "partitionKey","0");
                Whitebox.setInternalState(eventData, "offset", "100");
                Whitebox.setInternalState(eventData, "sequenceNumber",13L);
                Whitebox.setInternalState(eventData, "enqueuedTime",Instant.now().minus(100L, ChronoUnit.SECONDS));
                receivedEvents.add(eventData);
            }

            return receivedEvents;

        }
    }

    public static class MockGetAzureEventHubNoPartitions extends GetAzureEventHub{


        @Override
        protected void setupReceiver(final String connectionString) throws ProcessException{
            //do nothing
        }

        @Override
        public void onScheduled(final ProcessContext context) throws ProcessException {

        }
    }



    private void setUpStandardTestConfig() {
        testRunner.setProperty(GetAzureEventHub.EVENT_HUB_NAME,eventHubName);
        testRunner.setProperty(GetAzureEventHub.NAMESPACE,namespaceName);
        testRunner.setProperty(GetAzureEventHub.ACCESS_POLICY,sasKeyName);
        testRunner.setProperty(GetAzureEventHub.POLICY_PRIMARY_KEY,sasKey);
        testRunner.setProperty(GetAzureEventHub.NUM_PARTITIONS,"4");
        testRunner.assertValid();
    }

}