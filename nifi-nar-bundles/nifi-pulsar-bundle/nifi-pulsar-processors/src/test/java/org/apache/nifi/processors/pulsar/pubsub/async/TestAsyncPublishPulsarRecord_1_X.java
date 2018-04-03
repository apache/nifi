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

import org.apache.nifi.processors.pulsar.AbstractPulsarProducerProcessor;
import org.apache.nifi.processors.pulsar.pubsub.PublishPulsarRecord_1_X;
import org.apache.nifi.processors.pulsar.pubsub.TestPublishPulsarRecord_1_X;
import org.apache.nifi.util.MockFlowFile;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Matchers;

public class TestAsyncPublishPulsarRecord_1_X extends TestPublishPulsarRecord_1_X {

    @SuppressWarnings("unchecked")
    @Test
    public void pulsarClientExceptionTest() throws PulsarClientException {
       when(mockProducer.sendAsync(Matchers.argThat(new ArgumentMatcher<byte[]>() {
            @Override
            public boolean matches(Object argument) {
                return true;
            }
        }))).thenThrow(PulsarClientException.class);

        final String content = "Mary Jane, 32";

        runner.enqueue(content);
        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, TOPIC_NAME);
        runner.setProperty(AbstractPulsarProducerProcessor.ASYNC_ENABLED, Boolean.TRUE.toString());
        runner.run();

        verify(mockProducer, times(1)).sendAsync("\"Mary Jane\",\"32\"\n".getBytes());
        runner.assertAllFlowFilesTransferred(PublishPulsarRecord_1_X.REL_FAILURE);

        List<MockFlowFile> results = runner.getFlowFilesForRelationship(PublishPulsarRecord_1_X.REL_FAILURE);
        assertEquals(1, results.size());

        String flowFileContents = new String(runner.getContentAsByteArray(results.get(0)));
        assertEquals("\"Mary Jane\",\"32\"", flowFileContents);

    }

    // Malformed content test, using "some content"
    @Test
    public void malformedContentTest() throws PulsarClientException {
        final String content = "invalid content";

        runner.enqueue(content);
        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, TOPIC_NAME);
        runner.setProperty(AbstractPulsarProducerProcessor.ASYNC_ENABLED, Boolean.TRUE.toString());
        runner.run();
        runner.assertAllFlowFilesTransferred(PublishPulsarRecord_1_X.REL_FAILURE);

        verify(mockProducer, times(0)).send(content.getBytes());
    }

    @Test
    public void testSingleRecordSuccess() throws PulsarClientException {

        final String content = "Mary Jane, 32";

        runner.enqueue(content);
        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, TOPIC_NAME);
        runner.setProperty(AbstractPulsarProducerProcessor.ASYNC_ENABLED, Boolean.TRUE.toString());
        runner.run();
        runner.assertAllFlowFilesTransferred(PublishPulsarRecord_1_X.REL_SUCCESS);

        List<MockFlowFile> results = runner.getFlowFilesForRelationship(PublishPulsarRecord_1_X.REL_SUCCESS);
        MockFlowFile result = results.get(0);

        result.assertAttributeEquals(PublishPulsarRecord_1_X.MSG_COUNT, "1");
        result.assertAttributeEquals(PublishPulsarRecord_1_X.TOPIC_NAME, TOPIC_NAME);

        verify(mockProducer, times(1)).sendAsync("\"Mary Jane\",\"32\"\n".getBytes());

    }

    @Test
    public void testMultipleRecordSuccess() throws PulsarClientException {

        StringBuilder sb = new StringBuilder().append("Mary Jane, 32").append("\n")
                                              .append("John Doe, 35").append("\n")
                                              .append("Busta Move, 26").append("\n");

        runner.enqueue(sb.toString());
        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, TOPIC_NAME);
        runner.setProperty(AbstractPulsarProducerProcessor.ASYNC_ENABLED, Boolean.TRUE.toString());
        runner.run();
        runner.assertAllFlowFilesTransferred(PublishPulsarRecord_1_X.REL_SUCCESS);

        List<MockFlowFile> results = runner.getFlowFilesForRelationship(PublishPulsarRecord_1_X.REL_SUCCESS);
        assertEquals(1, results.size());

        MockFlowFile result = results.get(0);

        result.assertContentEquals(sb.toString());
        result.assertAttributeEquals(PublishPulsarRecord_1_X.MSG_COUNT, "3");
        result.assertAttributeEquals(PublishPulsarRecord_1_X.TOPIC_NAME, TOPIC_NAME);

        verify(mockProducer, times(1)).sendAsync("\"Mary Jane\",\"32\"\n".getBytes());
        verify(mockProducer, times(1)).sendAsync("\"John Doe\",\"35\"\n".getBytes());
        verify(mockProducer, times(1)).sendAsync("\"Busta Move\",\"26\"\n".getBytes());

    }

    @Test
    public void testBulkRecordSuccess() throws PulsarClientException {

        StringBuilder sb = new StringBuilder();

        for (int idx = 0; idx < 98634; idx++) {
            sb.append("Mary Jane, 32").append("\n");
        }

        runner.enqueue(sb.toString());
        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, TOPIC_NAME);
        runner.setProperty(AbstractPulsarProducerProcessor.ASYNC_ENABLED, Boolean.TRUE.toString());
        runner.run();
        runner.assertAllFlowFilesTransferred(PublishPulsarRecord_1_X.REL_SUCCESS);

        List<MockFlowFile> results = runner.getFlowFilesForRelationship(PublishPulsarRecord_1_X.REL_SUCCESS);
        assertEquals(1, results.size());

        MockFlowFile result = results.get(0);
        result.assertContentEquals(sb.toString());
        result.assertAttributeEquals(PublishPulsarRecord_1_X.MSG_COUNT, "98634");
        result.assertAttributeEquals(PublishPulsarRecord_1_X.TOPIC_NAME, TOPIC_NAME);

        verify(mockProducer, times(98634)).sendAsync("\"Mary Jane\",\"32\"\n".getBytes());

    }
}
