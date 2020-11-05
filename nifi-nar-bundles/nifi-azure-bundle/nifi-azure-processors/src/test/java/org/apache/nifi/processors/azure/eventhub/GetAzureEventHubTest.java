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
import com.microsoft.azure.eventhubs.EventData.SystemProperties;
import com.microsoft.azure.eventhubs.PartitionReceiver;
import com.microsoft.azure.eventhubs.EventHubException;
import com.microsoft.azure.eventhubs.impl.AmqpConstants;

import java.io.IOException;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

public class GetAzureEventHubTest {
    private static final String namespaceName = "nifi-azure-hub";
    private static final String eventHubName = "get-test";
    private static final String sasKeyName = "bogus-policy";
    private static final String sasKey = "9rHmHqxoOVWOb8wS09dvqXYxnNiLqxNMCbmt6qMaQyU!";
    private static final Date ENQUEUED_TIME_VALUE = Date.from(Clock.fixed(Instant.now(), ZoneId.systemDefault()).instant());
    public static final long SEQUENCE_NUMBER_VALUE = 13L;
    public static final String OFFSET_VALUE = "100";
    public static final String PARTITION_KEY_VALUE = "0";

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
        testRunner.setProperty(GetAzureEventHub.ENQUEUE_TIME,"2015-12-22T21:55:10.000Z");
        testRunner.assertValid();
        testRunner.setProperty(GetAzureEventHub.RECEIVER_FETCH_SIZE, "5");
        testRunner.assertValid();
        testRunner.setProperty(GetAzureEventHub.RECEIVER_FETCH_TIMEOUT,"10000");
        testRunner.assertValid();
    }
    @Test
    public void testProcessorConfigValidityWithManagedIdentityFlag() {
        testRunner.setProperty(PutAzureEventHub.EVENT_HUB_NAME,eventHubName);
        testRunner.assertNotValid();
        testRunner.setProperty(PutAzureEventHub.NAMESPACE,namespaceName);
        testRunner.assertNotValid();
        testRunner.setProperty(PutAzureEventHub.USE_MANAGED_IDENTITY,"true");
        testRunner.assertNotValid();
        testRunner.setProperty(GetAzureEventHub.NUM_PARTITIONS,"4");
        testRunner.assertValid();
        testRunner.setProperty(GetAzureEventHub.ENQUEUE_TIME,"2015-12-22T21:55:10.000Z");
        testRunner.assertValid();
        testRunner.setProperty(GetAzureEventHub.RECEIVER_FETCH_SIZE, "5");
        testRunner.assertValid();
        testRunner.setProperty(GetAzureEventHub.RECEIVER_FETCH_TIMEOUT,"10000");
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

        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(GetAzureEventHub.REL_SUCCESS).get(0);
        flowFile.assertContentEquals("test event number: 0");
        flowFile.assertAttributeEquals("eventhub.enqueued.timestamp", ENQUEUED_TIME_VALUE.toInstant().toString());
        flowFile.assertAttributeEquals("eventhub.offset", OFFSET_VALUE);
        flowFile.assertAttributeEquals("eventhub.sequence", String.valueOf(SEQUENCE_NUMBER_VALUE));
        flowFile.assertAttributeEquals("eventhub.name", eventHubName);

        testRunner.clearTransferState();
    }

    @Test
    public void testNormalFlowWithApplicationProperties() throws Exception {
        setUpStandardTestConfig();
        testRunner.run(1, true);
        testRunner.assertAllFlowFilesTransferred(GetAzureEventHub.REL_SUCCESS, 10);

        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(GetAzureEventHub.REL_SUCCESS).get(0);
        flowFile.assertAttributeEquals("eventhub.property.event-sender", "Apache NiFi");
        flowFile.assertAttributeEquals("eventhub.property.application", "TestApp");

        testRunner.clearTransferState();
    }

    @Test
    public void testNormalNotReceivedEventsFlow() throws Exception {
        setUpStandardTestConfig();
        processor.received = false;
        testRunner.run(1, true);
        testRunner.assertAllFlowFilesTransferred(GetAzureEventHub.REL_SUCCESS, 10);

        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(GetAzureEventHub.REL_SUCCESS).get(0);
        flowFile.assertContentEquals("test event number: 0");
        flowFile.assertAttributeNotExists("eventhub.enqueued.timestamp");
        flowFile.assertAttributeNotExists("eventhub.offset");
        flowFile.assertAttributeNotExists("eventhub.sequence");
        flowFile.assertAttributeEquals("eventhub.name", eventHubName);

        testRunner.clearTransferState();
    }

    /**
     * Provides a stubbed processor instance for testing
     */
    public static class MockGetAzureEventHub extends GetAzureEventHub{

        boolean nullReceive = false;
        boolean getReceiverThrow = false;
        boolean received = true;

        @Override
        protected void setupReceiver(final String connectionString, final ScheduledExecutorService executor) throws ProcessException{
            //do nothing
        }
        @Override
        protected PartitionReceiver getReceiver(final ProcessContext context, final String partitionId) throws IOException, EventHubException, ExecutionException, InterruptedException {
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
                EventData eventData = EventData.create(String.format("test event number: %d", i).getBytes());
                eventData.getProperties().put("event-sender", "Apache NiFi");
                eventData.getProperties().put("application", "TestApp");
                if (received) {
                    HashMap<String, Object> properties = new HashMap<>();
                    properties.put(AmqpConstants.PARTITION_KEY_ANNOTATION_NAME, PARTITION_KEY_VALUE);
                    properties.put(AmqpConstants.OFFSET_ANNOTATION_NAME, OFFSET_VALUE);
                    properties.put(AmqpConstants.SEQUENCE_NUMBER_ANNOTATION_NAME, SEQUENCE_NUMBER_VALUE);
                    properties.put(AmqpConstants.ENQUEUED_TIME_UTC_ANNOTATION_NAME, ENQUEUED_TIME_VALUE);

                    SystemProperties systemProperties = new SystemProperties(properties);
                    eventData.setSystemProperties(systemProperties);
                }
                receivedEvents.add(eventData);
            }

            return receivedEvents;
        }
    }

    public static class MockGetAzureEventHubNoPartitions extends GetAzureEventHub{
        @Override
        protected void setupReceiver(final String connectionString, final ScheduledExecutorService executor) throws ProcessException{
            //do nothing
        }

        @Override
        public void onScheduled(final ProcessContext context) throws ProcessException {

        }
        @Override
        public void tearDown() throws ProcessException {
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
