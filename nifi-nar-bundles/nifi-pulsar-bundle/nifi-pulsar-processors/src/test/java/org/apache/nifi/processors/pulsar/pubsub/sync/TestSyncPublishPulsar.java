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
package org.apache.nifi.processors.pulsar.pubsub.sync;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.processors.pulsar.pubsub.PublishPulsar;
import org.apache.nifi.processors.pulsar.pubsub.TestPublishPulsar;
import org.apache.nifi.util.MockFlowFile;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Matchers;

public class TestSyncPublishPulsar extends TestPublishPulsar {

    @Test
    public void pulsarClientExceptionTest() throws PulsarClientException, UnsupportedEncodingException {
       when(mockClientService.getMockProducer().send(Matchers.argThat(new ArgumentMatcher<byte[]>() {
          @Override
          public boolean matches(Object argument) {
              return true;
          }
       }))).thenThrow(PulsarClientException.class);

       mockClientService.setMockProducer(mockProducer);

       runner.setProperty(PublishPulsar.TOPIC, "my-topic");

       final String content = "some content";
       runner.enqueue(content.getBytes("UTF-8"));
       runner.run();
       runner.assertAllFlowFilesTransferred(PublishPulsar.REL_FAILURE);
    }

    @Test
    public void invalidTopicTest() throws UnsupportedEncodingException, PulsarClientException {
        runner.setProperty(PublishPulsar.TOPIC, "${topic}");

        final String content = "some content";
        Map<String, String> attributes = new HashMap<String, String>();
        attributes.put(PublishPulsar.TOPIC.getName(), "");

        runner.enqueue(content.getBytes("UTF-8"), attributes );
        runner.run();
        runner.assertAllFlowFilesTransferred(PublishPulsar.REL_FAILURE);

        // Confirm that no Producer as created
        verify(mockClientService.getMockProducerBuilder(), times(0)).topic(anyString());
    }

    @Test
    public void dynamicTopicTest() throws UnsupportedEncodingException, PulsarClientException {
        when(mockProducer.getTopic()).thenReturn("topic-b");
        mockClientService.setMockProducer(mockProducer);

        runner.setProperty(PublishPulsar.TOPIC, "${topic}");

        final String content = "some content";
        Map<String, String> attributes = new HashMap<String, String>();
        attributes.put(PublishPulsar.TOPIC.getName(), "topic-b");

        runner.enqueue(content.getBytes("UTF-8"), attributes );
        runner.run();
        runner.assertAllFlowFilesTransferred(PublishPulsar.REL_SUCCESS);

        // Verify that we sent the data to topic-b.
        verify(mockClientService.getMockProducerBuilder(), times(1)).topic("topic-b");
    }

    @Test
    public void singleFlowFileTest() throws UnsupportedEncodingException, PulsarClientException {
       when(mockClientService.getMockProducer().getTopic()).thenReturn("my-topic");

       runner.setProperty(PublishPulsar.TOPIC, "my-topic");

       final String content = "some content";
       runner.enqueue(content.getBytes("UTF-8"));
       runner.run();
       runner.assertAllFlowFilesTransferred(PublishPulsar.REL_SUCCESS);

       final MockFlowFile outFile = runner.getFlowFilesForRelationship(PublishPulsar.REL_SUCCESS).get(0);
       outFile.assertContentEquals(content);

       // Verify that we sent the data to my-topic.
       verify(mockClientService.getMockProducerBuilder(), times(1)).topic("my-topic");

       // Verify that the send method on the producer was called with the expected content
       verify(mockClientService.getMockProducer(), times(1)).send(content.getBytes());
    }

    @Test
    public void multipleFlowFilesTest() throws UnsupportedEncodingException, PulsarClientException {
        when(mockClientService.getMockProducer().getTopic()).thenReturn("my-topic");

        runner.setProperty(PublishPulsar.TOPIC, "my-topic");
        final String content = "some content";
        for (int idx = 0; idx < 20; idx++) {
            runner.enqueue(content.getBytes("UTF-8"));
            runner.run();
            runner.assertAllFlowFilesTransferred(PublishPulsar.REL_SUCCESS);
        }
        // Verify that the send method on the producer was called with the expected content
        verify(mockClientService.getMockProducer(), times(20)).send(content.getBytes());
    }

    @Test
    public void demarcatedFlowFileTest() throws UnsupportedEncodingException, PulsarClientException {
        final String content = "some content";
        final String demarcator = "\n";
        when(mockClientService.getMockProducer().getTopic()).thenReturn("my-topic");

        runner.setProperty(PublishPulsar.TOPIC, "my-topic");
        runner.setProperty(PublishPulsar.MESSAGE_DEMARCATOR, demarcator);

        final StringBuffer sb = new StringBuffer();

        for (int idx = 0; idx < 20; idx++) {
           sb.append(content).append(demarcator);
        }

        runner.enqueue(sb.toString().getBytes("UTF-8"));
        runner.run();
        runner.assertAllFlowFilesTransferred(PublishPulsar.REL_SUCCESS);
        verify(mockClientService.getMockProducer(), times(20)).send(content.getBytes());
    }
}
