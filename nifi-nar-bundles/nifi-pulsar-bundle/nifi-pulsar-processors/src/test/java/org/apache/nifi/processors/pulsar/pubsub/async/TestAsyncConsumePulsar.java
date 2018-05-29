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
package org.apache.nifi.processors.pulsar.pubsub.async;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.apache.nifi.processors.pulsar.pubsub.ConsumePulsar;
import org.apache.nifi.processors.pulsar.pubsub.TestConsumePulsar;
import org.apache.nifi.util.MockFlowFile;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.Test;

public class TestAsyncConsumePulsar extends TestConsumePulsar {

   @Test
    public void nullMessageTest() throws PulsarClientException {
        when(mockMessage.getData()).thenReturn(null);
        mockClientService.setMockMessage(mockMessage);

        runner.setProperty(ConsumePulsar.TOPICS, "foo");
        runner.setProperty(ConsumePulsar.SUBSCRIPTION_NAME, "bar");
        runner.setProperty(ConsumePulsar.ASYNC_ENABLED, Boolean.toString(true));
        runner.run();
        runner.assertAllFlowFilesTransferred(ConsumePulsar.REL_SUCCESS);

        // Make sure no Flowfiles were generated
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumePulsar.REL_SUCCESS);
        assertEquals(0, flowFiles.size());

        verify(mockClientService.getMockConsumer(), times(0)).acknowledge(mockMessage);
    }

    @Test
    public void pulsarClientExceptionTest() throws PulsarClientException {
        when(mockClientService.getMockConsumer().receive()).thenThrow(PulsarClientException.class);

        runner.setProperty(ConsumePulsar.TOPICS, "foo");
        runner.setProperty(ConsumePulsar.SUBSCRIPTION_NAME, "bar");
        runner.setProperty(ConsumePulsar.ASYNC_ENABLED, Boolean.toString(true));
        runner.run();
        runner.assertAllFlowFilesTransferred(ConsumePulsar.REL_SUCCESS);

        // Make sure no Flowfiles were generated
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumePulsar.REL_SUCCESS);
        assertEquals(0, flowFiles.size());

        verify(mockClientService.getMockConsumer(), times(0)).acknowledge(mockMessage);
    }

    @Test
    public void emptyMessageTest() {
        when(mockMessage.getData()).thenReturn("".getBytes());
        mockClientService.setMockMessage(mockMessage);

        runner.setProperty(ConsumePulsar.TOPICS, "foo");
        runner.setProperty(ConsumePulsar.SUBSCRIPTION_NAME, "bar");
        runner.setProperty(ConsumePulsar.ASYNC_ENABLED, Boolean.toString(true));
        runner.run();
        runner.assertAllFlowFilesTransferred(ConsumePulsar.REL_SUCCESS);

        // Make sure no Flowfiles were generated
        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumePulsar.REL_SUCCESS);
        assertEquals(0, flowFiles.size());
    }

    @Test
    public void singleMessageTest() throws PulsarClientException {
        this.sendMessages("Mocked Message", "foo", "bar", true, 1);
    }

    @Test
    public void multipleMessagesTest() throws PulsarClientException {
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
}
