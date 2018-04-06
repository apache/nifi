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
package org.apache.nifi.processors.pulsar.pubsub;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public class TestConsumePulsar_1_X extends AbstractPulsarProcessorTest {

    @Mock
    protected Consumer mockConsumer;

    @Mock
    protected Message mockMessage;

    @Rule public MockitoRule mockitoRule = MockitoJUnit.rule();

    @Before
    public void init() throws InitializationException {
        runner = TestRunners.newTestRunner(ConsumePulsar_1_X.class);
        mockClient = mock(PulsarClient.class);
        mockConsumer = mock(Consumer.class);
        mockMessage = mock(Message.class);

        try {
            when(mockClient.subscribe(anyString(), anyString())).thenReturn(mockConsumer);
            when(mockConsumer.receive()).thenReturn(mockMessage);

            CompletableFuture<Message> future = CompletableFuture.supplyAsync(() -> {
                return mockMessage;
            });

            when(mockConsumer.receiveAsync()).thenReturn(future);

        } catch (PulsarClientException e) {
            e.printStackTrace();
        }

        addPulsarClientService();
    }

    @Test
    public void singleSyncMessageTest() throws PulsarClientException {
            this.sendMessages("Mocked Message", "foo", "bar", false, 1);
    }

    @Test
    public void multipleSyncMessagesTest() throws PulsarClientException {
            this.sendMessages("Mocked Message", "foo", "bar", false, 40);
    }

    @Test
    public void singleAsyncMessageTest() throws PulsarClientException {
            this.sendMessages("Mocked Message", "foo", "bar", true, 1);
    }

    @Test
    public void multipleAsyncMessagesTest() throws PulsarClientException {
            this.sendMessages("Mocked Message", "foo", "bar", true, 40);
    }

    /*
     * Verify that the consumer gets closed.
     */
    @Test
    public void onStoppedTest() throws NoSuchMethodException, SecurityException, PulsarClientException {
        when(mockMessage.getData()).thenReturn("Mocked Message".getBytes());

        runner.setProperty(ConsumePulsar_1_X.TOPIC, "foo");
        runner.setProperty(ConsumePulsar_1_X.SUBSCRIPTION, "bar");
        runner.run(10, true);
        runner.assertAllFlowFilesTransferred(ConsumePulsar_1_X.REL_SUCCESS);

        runner.assertQueueEmpty();

        // Verify that the receive method on the consumer was called 10 times
        verify(mockConsumer, times(10)).receive();

        // Verify that each message was acknowledged
        verify(mockConsumer, times(10)).acknowledge(mockMessage);

        // Verify that the consumer was closed
        verify(mockConsumer, times(1)).close();

    }

    protected void sendMessages(String msg, String topic, String sub, boolean async, int itertions) throws PulsarClientException {

        when(mockMessage.getData()).thenReturn(msg.getBytes());

        runner.setProperty(ConsumePulsar_1_X.ASYNC_ENABLED, Boolean.toString(async));
        runner.setProperty(ConsumePulsar_1_X.TOPIC, topic);
        runner.setProperty(ConsumePulsar_1_X.SUBSCRIPTION, sub);
        runner.run(itertions, true);

        runner.assertAllFlowFilesTransferred(ConsumePulsar_1_X.REL_SUCCESS);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumePulsar_1_X.REL_SUCCESS);
        assertEquals(itertions, flowFiles.size());

        for (MockFlowFile ff : flowFiles) {
                ff.assertContentEquals(msg);
        }

        if (async) {
            verify(mockConsumer, times(itertions)).receiveAsync();
        } else {
            verify(mockConsumer, times(itertions)).receive();
        }

        // Verify that every message was acknowledged
        if (async) {
            verify(mockConsumer, times(itertions)).acknowledgeAsync(mockMessage);
        } else {
            verify(mockConsumer, times(itertions)).acknowledge(mockMessage);
        }

    }
}
