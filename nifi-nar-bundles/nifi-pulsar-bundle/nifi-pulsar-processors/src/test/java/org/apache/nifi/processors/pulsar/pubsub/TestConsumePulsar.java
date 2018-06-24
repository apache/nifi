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

import org.apache.nifi.processors.pulsar.AbstractPulsarConsumerProcessorTest;
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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

public class TestConsumePulsar extends AbstractPulsarConsumerProcessorTest<byte[]> {

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
        mockClientService.setMockMessage(mockMessage);

        runner.setProperty(ConsumePulsar.TOPICS, "foo");
        runner.setProperty(ConsumePulsar.SUBSCRIPTION_NAME, "bar");
        runner.run(10, true);
        runner.assertAllFlowFilesTransferred(ConsumePulsar.REL_SUCCESS);

        runner.assertQueueEmpty();

        // Verify that the receive method on the consumer was called 10 times
        verify(mockClientService.getMockConsumer(), times(10)).receive();

        // Verify that each message was acknowledged
        verify(mockClientService.getMockConsumer(), times(10)).acknowledge(mockMessage);

        // Verify that the consumer was closed
        verify(mockClientService.getMockConsumer(), times(1)).close();

    }

    protected void sendMessages(String msg, String topic, String sub, boolean async, int itertions) throws PulsarClientException {

        when(mockMessage.getData()).thenReturn(msg.getBytes());
        mockClientService.setMockMessage(mockMessage);

        runner.setProperty(ConsumePulsar.ASYNC_ENABLED, Boolean.toString(async));
        runner.setProperty(ConsumePulsar.TOPICS, topic);
        runner.setProperty(ConsumePulsar.SUBSCRIPTION_NAME, sub);
        runner.run(itertions, true);

        runner.assertAllFlowFilesTransferred(ConsumePulsar.REL_SUCCESS);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumePulsar.REL_SUCCESS);
        assertEquals(itertions, flowFiles.size());

        for (MockFlowFile ff : flowFiles) {
            ff.assertContentEquals(msg);
        }

        verify(mockClientService.getMockConsumer(), times(itertions)).receive();

        // Verify that every message was acknowledged
        if (async) {
            verify(mockClientService.getMockConsumer(), times(itertions)).acknowledgeAsync(mockMessage);
        } else {
            verify(mockClientService.getMockConsumer(), times(itertions)).acknowledge(mockMessage);
        }
    }
}
