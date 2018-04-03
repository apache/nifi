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

import org.apache.nifi.processors.pulsar.pubsub.PublishPulsar_1_X;
import org.apache.nifi.processors.pulsar.pubsub.TestPublishPulsar_1_X;
import org.apache.nifi.util.MockFlowFile;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Matchers;

public class TestSyncPublishPulsar_1_X extends TestPublishPulsar_1_X {

    @SuppressWarnings("unchecked")
    @Test
    public void pulsarClientExceptionTest() throws PulsarClientException, UnsupportedEncodingException {

       when(mockProducer.send(Matchers.argThat(new ArgumentMatcher<byte[]>() {
          @Override
          public boolean matches(Object argument) {
              return true;
          }
       }))).thenThrow(PulsarClientException.class);

       runner.setProperty(PublishPulsar_1_X.TOPIC, "my-topic");

       final String content = "some content";
       runner.enqueue(content.getBytes("UTF-8"));
       runner.run();
       runner.assertAllFlowFilesTransferred(PublishPulsar_1_X.REL_FAILURE);
    }

    @Test
    public void invalidTopicTest() throws UnsupportedEncodingException, PulsarClientException {

        runner.setProperty(PublishPulsar_1_X.TOPIC, "${topic}");

        final String content = "some content";
        Map<String, String> attributes = new HashMap<String, String>();
        attributes.put(PublishPulsar_1_X.TOPIC.getName(), "");

        runner.enqueue(content.getBytes("UTF-8"), attributes );
        runner.run();
        runner.assertAllFlowFilesTransferred(PublishPulsar_1_X.REL_FAILURE);

        // Confirm that no Producer as created
        verify(mockClient, times(0)).createProducer(anyString());
    }

    @Test
    public void dynamicTopicTest() throws UnsupportedEncodingException, PulsarClientException {
        when(mockProducer.getTopic()).thenReturn("topic-b");
        runner.setProperty(PublishPulsar_1_X.TOPIC, "${topic}");

        final String content = "some content";
        Map<String, String> attributes = new HashMap<String, String>();
        attributes.put(PublishPulsar_1_X.TOPIC.getName(), "topic-b");

        runner.enqueue(content.getBytes("UTF-8"), attributes );
        runner.run();
        runner.assertAllFlowFilesTransferred(PublishPulsar_1_X.REL_SUCCESS);

        // Verify that we sent the data to topic-b.
        verify(mockClient, times(1)).createProducer("topic-b");
    }

    @Test
    public void singleFlowFileTest() throws UnsupportedEncodingException, PulsarClientException {
       when(mockProducer.getTopic()).thenReturn("my-topic");
       runner.setProperty(PublishPulsar_1_X.TOPIC, "my-topic");

       final String content = "some content";
       runner.enqueue(content.getBytes("UTF-8"));
       runner.run();
       runner.assertAllFlowFilesTransferred(PublishPulsar_1_X.REL_SUCCESS);

       final MockFlowFile outFile = runner.getFlowFilesForRelationship(PublishPulsar_1_X.REL_SUCCESS).get(0);
       outFile.assertContentEquals(content);

       // Verify that we sent the data to my-topic.
       verify(mockClient, times(1)).createProducer("my-topic");

       // Verify that the send method on the producer was called with the expected content
       verify(mockProducer, times(1)).send(content.getBytes());
    }

    @Test
    public void multipleFlowFilesTest() throws UnsupportedEncodingException, PulsarClientException {
        when(mockProducer.getTopic()).thenReturn("my-topic");
        runner.setProperty(PublishPulsar_1_X.TOPIC, "my-topic");
        final String content = "some content";

        // Hack, since runner.run(20, false); doesn't work as advertised
        for (int idx = 0; idx < 20; idx++) {
            runner.enqueue(content.getBytes("UTF-8"));
            runner.run();
            runner.assertAllFlowFilesTransferred(PublishPulsar_1_X.REL_SUCCESS);

        }

        // Verify that the send method on the producer was called with the expected content
        verify(mockProducer, times(20)).send(content.getBytes());
    }

}
