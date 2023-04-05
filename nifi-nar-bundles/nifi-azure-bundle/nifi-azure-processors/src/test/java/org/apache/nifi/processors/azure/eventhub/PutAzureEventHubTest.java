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

import com.azure.messaging.eventhubs.EventHubProducerClient;
import com.azure.messaging.eventhubs.models.SendOptions;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.shared.azure.eventhubs.AzureEventHubTransportType;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyIterable;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
public class PutAzureEventHubTest {
    private static final String EVENT_HUB_NAMESPACE = "NAMESPACE";
    private static final String EVENT_HUB_NAME = "NAME";
    private static final String POLICY_NAME = "POLICY";
    private static final String POLICY_KEY = "POLICY-KEY";
    private static final String PARTITION_KEY_ATTRIBUTE_NAME = "eventPartitionKey";
    private static final String PARTITION_KEY = "partition";
    private static final String CONTENT = String.class.getSimpleName();

    @Mock
    EventHubProducerClient eventHubProducerClient;

    @Captor
    ArgumentCaptor<SendOptions> sendOptionsArgumentCaptor;

    TestRunner testRunner;

    @BeforeEach
    public void setUp() throws Exception {
        testRunner = TestRunners.newTestRunner(new MockPutAzureEventHub());
    }

    @Test
    public void testProperties() {
        testRunner.setProperty(PutAzureEventHub.EVENT_HUB_NAME, EVENT_HUB_NAME);
        testRunner.assertNotValid();
        testRunner.setProperty(PutAzureEventHub.NAMESPACE, EVENT_HUB_NAMESPACE);
        testRunner.assertNotValid();
        testRunner.setProperty(PutAzureEventHub.ACCESS_POLICY, POLICY_NAME);
        testRunner.assertNotValid();
        testRunner.setProperty(PutAzureEventHub.POLICY_PRIMARY_KEY, POLICY_KEY);
        testRunner.assertValid();
        testRunner.setProperty(PutAzureEventHub.TRANSPORT_TYPE, AzureEventHubTransportType.AMQP_WEB_SOCKETS.getValue());
        testRunner.assertValid();
    }

    @Test
    public void testPropertiesManagedIdentityEnabled() {
        testRunner.setProperty(PutAzureEventHub.EVENT_HUB_NAME, EVENT_HUB_NAME);
        testRunner.assertNotValid();
        testRunner.setProperty(PutAzureEventHub.NAMESPACE, EVENT_HUB_NAMESPACE);
        testRunner.assertNotValid();
        testRunner.setProperty(PutAzureEventHub.USE_MANAGED_IDENTITY, Boolean.TRUE.toString());
        testRunner.assertValid();
    }

    @Test
    public void testRunNoFlowFiles() {
        setProperties();

        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutAzureEventHub.REL_SUCCESS, 0);
    }

    @Test
    public void testRunSuccess(){
        setProperties();

        testRunner.enqueue(CONTENT);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutAzureEventHub.REL_SUCCESS, 1);
    }

    @Test
    public void testRunFailure() {
        setProperties();

        doThrow(new RuntimeException()).when(eventHubProducerClient).send(anyIterable(), any(SendOptions.class));

        testRunner.enqueue(CONTENT);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutAzureEventHub.REL_FAILURE, 1);
    }

    @Test
    public void testRunBatchSuccess(){
        setProperties();

        final int batchSize = 2;

        testRunner.setProperty(PutAzureEventHub.MAX_BATCH_SIZE, Integer.toString(batchSize));

        testRunner.enqueue(CONTENT);
        testRunner.enqueue(CONTENT);
        testRunner.enqueue(CONTENT);
        testRunner.enqueue(CONTENT);

        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutAzureEventHub.REL_SUCCESS, batchSize);
        testRunner.clearTransferState();

        testRunner.run();
        testRunner.assertAllFlowFilesTransferred(PutAzureEventHub.REL_SUCCESS, batchSize);
    }

    @Test
    public void testRunSuccessPartitionKey(){
        setProperties();

        final Map<String, String> attributes = Collections.singletonMap(PARTITION_KEY_ATTRIBUTE_NAME, PARTITION_KEY);
        testRunner.setProperty(PutAzureEventHub.PARTITIONING_KEY_ATTRIBUTE_NAME, PARTITION_KEY_ATTRIBUTE_NAME);

        testRunner.enqueue(CONTENT, attributes);
        testRunner.run();

        testRunner.assertAllFlowFilesTransferred(PutAzureEventHub.REL_SUCCESS, 1);

        verify(eventHubProducerClient).send(anyIterable(), sendOptionsArgumentCaptor.capture());

        final SendOptions sendOptions = sendOptionsArgumentCaptor.getValue();
        assertEquals(PARTITION_KEY, sendOptions.getPartitionKey());
    }

    private class MockPutAzureEventHub extends PutAzureEventHub {

        @Override
        protected EventHubProducerClient createEventHubProducerClient(final ProcessContext context) {
            return eventHubProducerClient;
        }
    }

    private void setProperties() {
        testRunner.setProperty(PutAzureEventHub.EVENT_HUB_NAME, EVENT_HUB_NAME);
        testRunner.setProperty(PutAzureEventHub.NAMESPACE, EVENT_HUB_NAMESPACE);
        testRunner.setProperty(PutAzureEventHub.ACCESS_POLICY, POLICY_NAME);
        testRunner.setProperty(PutAzureEventHub.POLICY_PRIMARY_KEY, POLICY_KEY);
        testRunner.assertValid();
    }
}