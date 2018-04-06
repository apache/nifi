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

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.UnsupportedEncodingException;

import org.apache.nifi.processors.pulsar.pubsub.PublishPulsar_1_X;
import org.apache.nifi.processors.pulsar.pubsub.TestPublishPulsar_1_X;
import org.apache.nifi.util.MockFlowFile;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.Test;

public class TestAsyncPublishPulsar_1_X extends TestPublishPulsar_1_X {

    @Test
    public void singleFlowFileTest() throws UnsupportedEncodingException, PulsarClientException, InterruptedException {
        when(mockProducer.getTopic()).thenReturn("my-topic");
        runner.setProperty(PublishPulsar_1_X.TOPIC, "my-topic");
        runner.setProperty(PublishPulsar_1_X.ASYNC_ENABLED, Boolean.TRUE.toString());

        final String content = "some content";
        runner.enqueue(content.getBytes("UTF-8"));
        runner.run();
        runner.assertAllFlowFilesTransferred(PublishPulsar_1_X.REL_SUCCESS);

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(PublishPulsar_1_X.REL_SUCCESS).get(0);
        outFile.assertContentEquals(content);
        outFile.assertAttributeEquals(PublishPulsar_1_X.MSG_COUNT, "1");

        // Verify that we sent the data to my-topic.
        verify(mockClient, times(1)).createProducer("my-topic");

        // Verify that the send method on the producer was called with the expected content
        verify(mockProducer, times(1)).sendAsync(content.getBytes());
    }

    @Test
    public void multipleFlowFilesTest() throws UnsupportedEncodingException, PulsarClientException {
        when(mockProducer.getTopic()).thenReturn("my-async-topic");
        runner.setProperty(PublishPulsar_1_X.TOPIC, "my-async-topic");
        runner.setProperty(PublishPulsar_1_X.ASYNC_ENABLED, Boolean.TRUE.toString());

        final String content = "some content";

        // Hack, since runner.run(20, false); doesn't work as advertised
        for (int idx = 0; idx < 20; idx++) {
            runner.enqueue(content.getBytes("UTF-8"));
            runner.run();
            runner.assertAllFlowFilesTransferred(PublishPulsar_1_X.REL_SUCCESS);
        }

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(PublishPulsar_1_X.REL_SUCCESS).get(0);
        outFile.assertContentEquals(content);
        outFile.assertAttributeEquals(PublishPulsar_1_X.MSG_COUNT, "1");

        // Verify that the send method on the producer was called with the expected content
        verify(mockProducer, times(20)).sendAsync(content.getBytes());
    }

    @Test
    public void stressTest() throws UnsupportedEncodingException {
        when(mockProducer.getTopic()).thenReturn("my-async-topic");
        runner.setProperty(PublishPulsar_1_X.TOPIC, "my-async-topic");
        runner.setProperty(PublishPulsar_1_X.ASYNC_ENABLED, Boolean.TRUE.toString());

        final String content = "some content";

        for (int idx = 0; idx < 99; idx++) {
            runner.enqueue(content.getBytes("UTF-8"));
            runner.run();
            runner.assertAllFlowFilesTransferred(PublishPulsar_1_X.REL_SUCCESS);
        }

        final MockFlowFile outFile = runner.getFlowFilesForRelationship(PublishPulsar_1_X.REL_SUCCESS).get(0);
        outFile.assertContentEquals(content);
        outFile.assertAttributeEquals(PublishPulsar_1_X.MSG_COUNT, "1");

        verify(mockProducer, times(99)).sendAsync(content.getBytes());

    }
}
