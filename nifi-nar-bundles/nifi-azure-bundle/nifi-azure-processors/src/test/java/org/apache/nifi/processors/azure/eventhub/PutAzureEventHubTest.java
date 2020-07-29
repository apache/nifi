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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.azure.storage.utils.FlowFileResultCarrier;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import com.google.common.collect.ImmutableMap;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;

public class PutAzureEventHubTest {
    private static final String namespaceName = "nifi-azure-hub";
    private static final String eventHubName = "get-test";
    private static final String sasKeyName = "bogus-policy";
    private static final String sasKey = "9rHmHqxoOVWOb8wS09dvqXYxnNiLqxNMCbmt6qMaQyU!";
    private static final String TEST_PARTITIONING_KEY_ATTRIBUTE_NAME = "x-opt-partition-key";
    private static final String TEST_PARTITIONING_KEY = "some-partitioning-key";


    private TestRunner testRunner;
    private PutAzureEventHubTest.MockPutAzureEventHub processor;

    @Before
    public void setUp() throws Exception {
        processor = new PutAzureEventHubTest.MockPutAzureEventHub();
        testRunner = TestRunners.newTestRunner(processor);
    }
    @Test
    public void testProcessorConfigValidity() {
        testRunner.setProperty(PutAzureEventHub.EVENT_HUB_NAME,eventHubName);
        testRunner.assertNotValid();
        testRunner.setProperty(PutAzureEventHub.NAMESPACE,namespaceName);
        testRunner.assertNotValid();
        testRunner.setProperty(PutAzureEventHub.ACCESS_POLICY,sasKeyName);
        testRunner.assertNotValid();
        testRunner.setProperty(PutAzureEventHub.POLICY_PRIMARY_KEY,sasKey);
        testRunner.assertValid();
    }
    @Test
    public void testProcessorConfigValidityWithManagedIdentityFlag() {
        testRunner.setProperty(PutAzureEventHub.EVENT_HUB_NAME,eventHubName);
        testRunner.assertNotValid();
        testRunner.setProperty(PutAzureEventHub.NAMESPACE,namespaceName);
        testRunner.assertNotValid();
        testRunner.setProperty(PutAzureEventHub.USE_MANAGED_IDENTITY,"true");
        testRunner.assertValid();
    }
    @Test
    public void verifyRelationships(){
        assert(2 == processor.getRelationships().size());
    }
    @Test
    public void testNoFlow() {
        setUpStandardTestConfig();
        testRunner.run(1, true);
        testRunner.assertAllFlowFilesTransferred(PutAzureEventHub.REL_SUCCESS, 0);
        testRunner.clearTransferState();
    }
    @Test
    public void testNormalFlow(){
        setUpStandardTestConfig();
        String flowFileContents = "TEST MESSAGE";
        testRunner.enqueue(flowFileContents);
        testRunner.run(1, true);
        assert(flowFileContents.contentEquals(new String(processor.getReceivedBuffer())));
        testRunner.assertAllFlowFilesTransferred(PutAzureEventHub.REL_SUCCESS, 1);
        testRunner.clearTransferState();
    }
    @Test
    public void testSendMessageThrows() {
        PutAzureEventHubTest.OnSendThrowingMockPutAzureEventHub throwingProcessor = new PutAzureEventHubTest.OnSendThrowingMockPutAzureEventHub();
        testRunner = TestRunners.newTestRunner(throwingProcessor);
        setUpStandardTestConfig();
        String flowFileContents = "TEST MESSAGE";
        testRunner.enqueue(flowFileContents);
        testRunner.run(1, true);
        testRunner.assertAllFlowFilesTransferred(PutAzureEventHub.REL_FAILURE);
        testRunner.clearTransferState();
    }

    @Test(expected = AssertionError.class)
    public void testBadConnectionString() {
        PutAzureEventHubTest.BogusConnectionStringMockPutAzureEventHub badConnectionStringProcessor = new PutAzureEventHubTest.BogusConnectionStringMockPutAzureEventHub();
        testRunner = TestRunners.newTestRunner(badConnectionStringProcessor);
        setUpStandardTestConfig();
        testRunner.run(1, true);
    }

    @Test
    public void testMessageIsSentWithPartitioningKeyIfSpecifiedAndPopulated() {
        MockedEventhubClientMockPutAzureEventHub processor = new PutAzureEventHubTest.MockedEventhubClientMockPutAzureEventHub();
        MockitoAnnotations.initMocks(processor);

        EventHubClient eventHubClient = processor.getEventHubClient();
        when(eventHubClient.send(any(EventData.class), anyString()))
        .thenReturn(CompletableFuture.completedFuture(null));

        when(eventHubClient.send(any(EventData.class)))
        .thenThrow(new RuntimeException("Partition-key-less method called despite key is defined and required."));

        testRunner = TestRunners.newTestRunner(processor);
        setUpStandardTestConfig();
        testRunner.setProperty(PutAzureEventHub.PARTITIONING_KEY_ATTRIBUTE_NAME, TEST_PARTITIONING_KEY_ATTRIBUTE_NAME);

        MockFlowFile flowFile = new MockFlowFile(1234);
        flowFile.putAttributes(ImmutableMap.of(TEST_PARTITIONING_KEY_ATTRIBUTE_NAME, TEST_PARTITIONING_KEY));
        testRunner.enqueue(flowFile);
        testRunner.run(1, true);

        Mockito.verify(eventHubClient).send(any(EventData.class), eq(TEST_PARTITIONING_KEY));
    }

    @Test
    public void testMessageIsSentWithoutPartitioningKeyIfNotSpecifiedOrNotPopulated() {
        MockedEventhubClientMockPutAzureEventHub processor = new PutAzureEventHubTest.MockedEventhubClientMockPutAzureEventHub();
        MockitoAnnotations.initMocks(processor);

        EventHubClient eventHubClient = processor.getEventHubClient();
        when(eventHubClient.send(any(EventData.class), anyString()))
        .thenThrow(new RuntimeException("Partition-key-full method called despite key is Not required or not populated."));

        when(eventHubClient.send(any(EventData.class)))
        .thenReturn(CompletableFuture.completedFuture(null));

        testRunner = TestRunners.newTestRunner(processor);
        setUpStandardTestConfig();

        MockFlowFile flowFile = new MockFlowFile(1234);
        flowFile.putAttributes(ImmutableMap.of(TEST_PARTITIONING_KEY_ATTRIBUTE_NAME, TEST_PARTITIONING_KEY));

        // Key not specified
        testRunner.enqueue(flowFile);
        testRunner.run(1, true);

        Mockito.verify(eventHubClient, never()).send(any(EventData.class), eq(TEST_PARTITIONING_KEY));
        Mockito.verify(eventHubClient).send(any(EventData.class));

        // Key wanted but not available
        testRunner.setProperty(PutAzureEventHub.PARTITIONING_KEY_ATTRIBUTE_NAME, "Non-existing-attribute");

        testRunner.enqueue(flowFile);
        testRunner.run(1, true);

        Mockito.verify(eventHubClient, never()).send(any(EventData.class), eq(TEST_PARTITIONING_KEY));
        Mockito.verify(eventHubClient, times(2)).send(any(EventData.class));
    }

    @Test
    public void testAllAttributesAreLiftedToProperties() {
        MockedEventhubClientMockPutAzureEventHub processor = new PutAzureEventHubTest.MockedEventhubClientMockPutAzureEventHub();
        MockitoAnnotations.initMocks(processor);

        EventHubClient eventHubClient = processor.getEventHubClient();
        when(eventHubClient.send(any(EventData.class)))
        .thenReturn(CompletableFuture.completedFuture(null));

        testRunner = TestRunners.newTestRunner(processor);
        setUpStandardTestConfig();

        MockFlowFile flowFile = new MockFlowFile(1234);
        ImmutableMap<String, String> demoAttributes = ImmutableMap.of("A", "a", "B", "b", "D", "d", "C", "c");
        flowFile.putAttributes(demoAttributes);

        testRunner.enqueue(flowFile);
        testRunner.run(1, true);
        ArgumentCaptor<EventData> eventDataCaptor = ArgumentCaptor.forClass(EventData.class);

        Mockito.verify(eventHubClient).send(eventDataCaptor.capture());

        EventData event = eventDataCaptor.getValue();
        assertTrue(event.getProperties().entrySet().containsAll(demoAttributes.entrySet()));
    }

    @Test
    public void testBatchProcessesUptoMaximum() {
        MockedEventhubClientMockPutAzureEventHub processor = new PutAzureEventHubTest.MockedEventhubClientMockPutAzureEventHub();
        MockitoAnnotations.initMocks(processor);

        EventHubClient eventHubClient = processor.getEventHubClient();

        CompletableFuture<Void> failedFuture = new CompletableFuture<Void>();
        failedFuture.completeExceptionally(new IllegalArgumentException());

        when(eventHubClient.send(any(EventData.class)))
        .thenReturn(failedFuture)
        .thenReturn(CompletableFuture.completedFuture(null));

        testRunner = TestRunners.newTestRunner(processor);
        setUpStandardTestConfig();

        List<MockFlowFile> flowFiles = Arrays.asList(new MockFlowFile(1), new MockFlowFile(2), new MockFlowFile(3),
                new MockFlowFile(4), new MockFlowFile(5), new MockFlowFile(6));

        flowFiles.stream().forEachOrdered(ff -> testRunner.enqueue(ff));

        testRunner.setProperty(PutAzureEventHub.MAX_BATCH_SIZE, "4");
        testRunner.run(1, true);

        Mockito.verify(eventHubClient, times(4)).send(any(EventData.class));
        testRunner.assertTransferCount(PutAzureEventHub.REL_SUCCESS, 3);
        testRunner.assertTransferCount(PutAzureEventHub.REL_FAILURE, 1);
    }

    @Test
    public void testFailedBatchProcessesRollsBackTransactions() throws InterruptedException, ExecutionException {
        MockedEventhubClientMockPutAzureEventHub processor = new PutAzureEventHubTest.MockedEventhubClientMockPutAzureEventHub();
        MockitoAnnotations.initMocks(processor);

        final BlockingQueue<CompletableFuture<FlowFileResultCarrier<Relationship>>> futureQueue = new LinkedBlockingQueue<CompletableFuture<FlowFileResultCarrier<Relationship>>>();

        @SuppressWarnings("unchecked")
        CompletableFuture<FlowFileResultCarrier<Relationship>> throwingFuture = (CompletableFuture<FlowFileResultCarrier<Relationship>>)mock(CompletableFuture.class);

        when(throwingFuture.get()).thenThrow(new ExecutionException(new IllegalArgumentException()));

        MockFlowFile flowFile1 = new MockFlowFile(1);
        MockFlowFile flowFile2 = new MockFlowFile(2);

        futureQueue.offer(CompletableFuture.completedFuture(null));
        futureQueue.offer(CompletableFuture.completedFuture(new FlowFileResultCarrier<Relationship>(flowFile1, PutAzureEventHub.REL_SUCCESS)));
        futureQueue.offer(CompletableFuture.completedFuture(new FlowFileResultCarrier<Relationship>(flowFile2, PutAzureEventHub.REL_FAILURE, new IllegalArgumentException())));
        futureQueue.offer(throwingFuture);

        testRunner = TestRunners.newTestRunner(processor);
        setUpStandardTestConfig();
        testRunner.enqueue(flowFile1);
        testRunner.enqueue(flowFile2);


        final ProcessContext context = testRunner.getProcessContext();
        final ProcessSession session = spy(testRunner.getProcessSessionFactory().createSession());
        doNothing().when(session).transfer(any(FlowFile.class), any());
        doReturn(flowFile2).when(session).penalize(any());

        try {
            processor.waitForAllFutures(context, session,  new StopWatch(true), futureQueue);
            assertFalse(true);
        }catch(ProcessException pe) {
            assertTrue(true);
            assertFalse(Thread.currentThread().isInterrupted());
        }

        verify(session).transfer(flowFile1, PutAzureEventHub.REL_SUCCESS);
        verify(session).transfer(flowFile2, PutAzureEventHub.REL_FAILURE);
        verify(session).rollback();

        //Second run to test interrupted exception
        Mockito.reset(throwingFuture, session);
        when(throwingFuture.get()).thenThrow(new InterruptedException());
        doNothing().when(session).transfer(any(FlowFile.class), any());
        doReturn(flowFile2).when(session).penalize(any());

        try {
            processor.waitForAllFutures(context, session,  new StopWatch(true), futureQueue);
            assertFalse(true);
        }catch(ProcessException pe) {
            assertTrue(true);
            assertTrue(Thread.currentThread().isInterrupted());
        }

    }

    private static class MockPutAzureEventHub extends PutAzureEventHub{
        byte[] receivedBuffer = null;

        byte[] getReceivedBuffer(){
            return receivedBuffer;
        }


        @Override
        protected EventHubClient createEventHubClient(
                final String namespace,
                final String eventHubName,
                final String policyName,
                final String policyKey,
                final ScheduledExecutorService executor) throws ProcessException {
            return null;
        }

        @Override
        protected CompletableFuture<Void> sendMessage(final byte[] buffer, String partitioningKey, Map<String, Object> userProperties) throws ProcessException {
            receivedBuffer = buffer;

            return CompletableFuture.completedFuture(null);
        }
    }
    private static class OnSendThrowingMockPutAzureEventHub extends PutAzureEventHub{
        @Override
        protected EventHubClient createEventHubClient(
                final String namespace,
                final String eventHubName,
                final String policyName,
                final String policyKey,
                final ScheduledExecutorService executor) throws ProcessException {
            return null;
        }
    }
    private static class BogusConnectionStringMockPutAzureEventHub extends PutAzureEventHub{
        @Override
        protected String getConnectionString(final String namespace, final String eventHubName, final String policyName, final String policyKey){
            return "Bogus Connection String";
        }
    }
    private static class MockedEventhubClientMockPutAzureEventHub extends PutAzureEventHub{

        @Mock
        private EventHubClient client;

        public EventHubClient getEventHubClient() {
            return client;
        }

        @Override
        protected EventHubClient createEventHubClient(
                final String namespace,
                final String eventHubName,
                final String policyName,
                final String policyKey,
                final ScheduledExecutorService executor) throws ProcessException {
            return client;
        }
    }
    private void setUpStandardTestConfig() {
        testRunner.setProperty(PutAzureEventHub.EVENT_HUB_NAME,eventHubName);
        testRunner.setProperty(PutAzureEventHub.NAMESPACE,namespaceName);
        testRunner.setProperty(PutAzureEventHub.ACCESS_POLICY,sasKeyName);
        testRunner.setProperty(PutAzureEventHub.POLICY_PRIMARY_KEY,sasKey);
        testRunner.assertValid();
    }
}