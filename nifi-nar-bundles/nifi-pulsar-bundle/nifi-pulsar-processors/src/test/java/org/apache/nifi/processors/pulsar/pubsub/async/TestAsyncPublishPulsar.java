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
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.processors.pulsar.pubsub.PublishPulsar;
import org.apache.nifi.processors.pulsar.pubsub.PublishPulsarRecord;
import org.apache.nifi.processors.pulsar.pubsub.TestPublishPulsar;
import org.apache.nifi.util.MockFlowFile;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.Test;

public class TestAsyncPublishPulsar extends TestPublishPulsar {

    @Test
    public void singleFlowFileTest() throws UnsupportedEncodingException, PulsarClientException, InterruptedException {
        when(mockClientService.getMockProducer().getTopic()).thenReturn("my-topic");

        runner.setProperty(PublishPulsar.TOPIC, "my-topic");
        runner.setProperty(PublishPulsar.ASYNC_ENABLED, Boolean.TRUE.toString());

        final String content = "some content";
        runner.enqueue(content.getBytes("UTF-8"));
        runner.run();
        runner.assertAllFlowFilesTransferred(PublishPulsar.REL_SUCCESS);

        // Verify that we sent the data to my-topic.
        verify(mockClientService.getMockProducerBuilder(), times(1)).topic("my-topic");

        // Verify that the send method on the producer was called with the expected content
        verify(mockClientService.getMockProducer(), times(1)).sendAsync(content.getBytes());
    }

    @Test
    public void demarcatedFlowFileTest() throws UnsupportedEncodingException, PulsarClientException {
        final String content = "some content";
        final String demarcator = "\n";
        when(mockClientService.getMockProducer().getTopic()).thenReturn("my-topic");

        runner.setProperty(PublishPulsar.TOPIC, "my-topic");
        runner.setProperty(PublishPulsar.MESSAGE_DEMARCATOR, demarcator);
        runner.setProperty(PublishPulsar.ASYNC_ENABLED, Boolean.TRUE.toString());

        final StringBuffer sb = new StringBuffer();

        for (int idx = 0; idx < 20; idx++) {
           sb.append(content).append(demarcator);
        }

        runner.enqueue(sb.toString().getBytes("UTF-8"));
        runner.run(10, true, true);
        runner.assertAllFlowFilesTransferred(PublishPulsar.REL_SUCCESS);
        verify(mockClientService.getMockProducer(), times(20)).sendAsync(content.getBytes());
    }

    @Test
    public void pulsarClientExceptionTest() throws UnsupportedEncodingException {

        when(mockClientService.getMockProducer().sendAsync(any(byte[].class))).thenThrow(PulsarClientException.class);
        when(mockClientService.getMockProducer().getTopic()).thenReturn("my-topic");

        runner.setProperty(PublishPulsar.TOPIC, "my-topic");
        runner.setProperty(PublishPulsar.ASYNC_ENABLED, Boolean.TRUE.toString());
        runner.addConnection(PublishPulsarRecord.REL_FAILURE);

        final String content = "some content";
        runner.enqueue(content.getBytes("UTF-8"));
        runner.run(5000, false, true);
        List<MockFlowFile> success = runner.getFlowFilesForRelationship("success");
        List<MockFlowFile> failures = runner.getFlowFilesForRelationship("failure");

        assertNotNull(success);
        assertEquals(1, success.size());

        assertNotNull(failures);
        assertEquals(1, failures.size());
    }

    @Test
    public void multipleFlowFilesTest() throws UnsupportedEncodingException, PulsarClientException {
        when(mockClientService.getMockProducer().getTopic()).thenReturn("my-async-topic");

        runner.setProperty(PublishPulsar.TOPIC, "my-async-topic");
        runner.setProperty(PublishPulsar.ASYNC_ENABLED, Boolean.TRUE.toString());

        final String content = "some content";

        for (int idx = 0; idx < 20; idx++) {
            runner.enqueue(content.getBytes("UTF-8"));
        }

        runner.run(20, true, true);
        runner.assertAllFlowFilesTransferred(PublishPulsar.REL_SUCCESS);

        // Verify that the send method on the producer was called with the expected content
        verify(mockClientService.getMockProducer(), times(20)).sendAsync(content.getBytes());
    }

    @Test
    public final void multipleTopicsTest() throws UnsupportedEncodingException {
        when(mockProducer.getTopic()).thenReturn("topic-a").thenReturn("topic-b");
        mockClientService.setMockProducer(mockProducer);

        runner.setProperty(PublishPulsar.TOPIC, "${topic}");

        final String contentA = "topic A content";
        Map<String, String> attributesA = new HashMap<String, String>();
        attributesA.put("topic", "topic-a");
        runner.enqueue(contentA.getBytes("UTF-8"), attributesA);

        final String contentB = "topic B content";
        Map<String, String> attributesB = new HashMap<String, String>();
        attributesB.put("topic", "topic-b");
        runner.enqueue(contentB.getBytes("UTF-8"), attributesB);

        runner.run(2, true, true);
        runner.assertAllFlowFilesTransferred(PublishPulsar.REL_SUCCESS);

        // Verify that we sent the data to topic-b.
        verify(mockClientService.getMockProducerBuilder(), times(1)).topic("topic-a");
        verify(mockClientService.getMockProducerBuilder(), times(1)).topic("topic-b");
    }
}
