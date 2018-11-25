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

import org.apache.nifi.processors.pulsar.AbstractPulsarProcessorTest;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import static org.junit.Assert.assertEquals;

import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class TestConsumePulsar extends AbstractPulsarProcessorTest<byte[]> {

    @Mock
    protected Message<byte[]> mockMessage;

    @Rule public MockitoRule mockitoRule = MockitoJUnit.rule();

    @Before
    public void init() throws InitializationException {
        runner = TestRunners.newTestRunner(ConsumePulsar.class);
        mockMessage = mock(Message.class);
        addPulsarClientService();
    }

    @Test
    public void singleSyncMessageTest() throws PulsarClientException {
        this.sendMessages("Mocked Message", "foo", "bar", false, 1);
    }

    @Test
    public void multipleSyncMessagesTest() throws PulsarClientException {
        this.batchMessages("Mocked Message", "foo", "bar", false, 40);
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
        when(mockMessage.getValue()).thenReturn("Mocked Message".getBytes());
        mockClientService.setMockMessage(mockMessage);

        runner.setProperty(ConsumePulsar.TOPICS, "foo");
        runner.setProperty(ConsumePulsar.SUBSCRIPTION_NAME, "bar");
        runner.run(10, true);
        runner.assertAllFlowFilesTransferred(ConsumePulsar.REL_SUCCESS);

        runner.assertQueueEmpty();

        // Verify that the receive method on the consumer was called 10 times
        int batchSize = Integer.parseInt(ConsumePulsar.CONSUMER_BATCH_SIZE.getDefaultValue());
        verify(mockClientService.getMockConsumer(), atLeast(10 * batchSize)).receive(0, TimeUnit.SECONDS);

        // Verify that each message was acknowledged
        verify(mockClientService.getMockConsumer(), times(10)).acknowledgeCumulative(mockMessage);

        // Verify that the consumer was closed
        verify(mockClientService.getMockConsumer(), times(1)).close();

    }

    protected void batchMessages(String msg, String topic, String sub, boolean async, int batchSize) throws PulsarClientException {
        when(mockMessage.getValue()).thenReturn(msg.getBytes());
        mockClientService.setMockMessage(mockMessage);

        runner.setProperty(ConsumePulsar.ASYNC_ENABLED, Boolean.toString(async));
        runner.setProperty(ConsumePulsar.TOPICS, topic);
        runner.setProperty(ConsumePulsar.SUBSCRIPTION_NAME, sub);
        runner.setProperty(ConsumePulsar.CONSUMER_BATCH_SIZE, batchSize + "");
        runner.setProperty(ConsumePulsar.MESSAGE_DEMARCATOR, "\n");
        runner.run(1, true);

        runner.assertAllFlowFilesTransferred(ConsumePulsar.REL_SUCCESS);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumePulsar.REL_SUCCESS);
        assertEquals(1, flowFiles.size());

        flowFiles.get(0).assertAttributeEquals(ConsumePulsar.MSG_COUNT, batchSize + "");

        StringBuffer sb = new StringBuffer();
        for (int idx = 0; idx < batchSize; idx++) {
            sb.append(msg);
            sb.append("\n");
        }

        flowFiles.get(0).assertContentEquals(sb.toString());

        // Verify that every message was acknowledged
        if (async) {
            verify(mockClientService.getMockConsumer(), times(batchSize)).receive();
            verify(mockClientService.getMockConsumer(), times(batchSize)).acknowledgeAsync(mockMessage);
        } else {
            verify(mockClientService.getMockConsumer(), times(batchSize + 1)).receive(0, TimeUnit.SECONDS);
            verify(mockClientService.getMockConsumer(), times(1)).acknowledgeCumulative(mockMessage);
        }
    }

    protected void sendMessages(String msg, String topic, String sub, boolean async, int iterations) throws PulsarClientException {

        when(mockMessage.getValue()).thenReturn(msg.getBytes());
        mockClientService.setMockMessage(mockMessage);

        runner.setProperty(ConsumePulsar.ASYNC_ENABLED, Boolean.toString(async));
        runner.setProperty(ConsumePulsar.TOPICS, topic);
        runner.setProperty(ConsumePulsar.SUBSCRIPTION_NAME, sub);
        runner.setProperty(ConsumePulsar.CONSUMER_BATCH_SIZE, 1 + "");
        runner.run(iterations, true);

        runner.assertAllFlowFilesTransferred(ConsumePulsar.REL_SUCCESS);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumePulsar.REL_SUCCESS);
        assertEquals(iterations, flowFiles.size());

        for (MockFlowFile ff : flowFiles) {
            ff.assertContentEquals(msg + ConsumePulsar.MESSAGE_DEMARCATOR.getDefaultValue());
        }

        verify(mockClientService.getMockConsumer(), times(iterations * 2)).receive(0, TimeUnit.SECONDS);

        // Verify that every message was acknowledged
        if (async) {
            verify(mockClientService.getMockConsumer(), times(iterations)).acknowledgeCumulativeAsync(mockMessage);
        } else {
            verify(mockClientService.getMockConsumer(), times(iterations)).acknowledgeCumulative(mockMessage);
        }
    }
}
