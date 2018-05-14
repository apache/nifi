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

import static org.apache.nifi.processors.pulsar.pubsub.RecordBasedConst.RECORD_READER;
import static org.apache.nifi.processors.pulsar.pubsub.RecordBasedConst.RECORD_WRITER;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.nifi.processors.pulsar.AbstractPulsarProcessor;
import org.apache.nifi.processors.pulsar.AbstractPulsarProducerProcessor;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockPulsarClientService;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockRecordParser;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockRecordWriter;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Matchers;
import org.mockito.Mock;

public class TestPublishPulsarRecord_1_X {

    protected static final String TOPIC_NAME = "unit-test";

    protected TestRunner runner;

    @Mock
    protected PulsarClient mockClient;

    @Mock
    protected Producer mockProducer;

    @Before
    public void setup() throws InitializationException {

        mockClient = mock(PulsarClient.class);
        mockProducer = mock(Producer.class);

        try {
            // Use the mockProducer for all Producer interactions
            when(mockClient.createProducer(anyString())).thenReturn(mockProducer);

            when(mockProducer.send(Matchers.argThat(new ArgumentMatcher<byte[]>() {
                    @Override
                    public boolean matches(Object argument) {
                        return true;
                    }
            }))).thenReturn(mock(MessageId.class));

            CompletableFuture<MessageId> future = CompletableFuture.supplyAsync(() -> {
                return mock(MessageId.class);
            });

            when(mockProducer.sendAsync(Matchers.argThat(new ArgumentMatcher<byte[]>() {
                @Override
                public boolean matches(Object argument) {
                    return true;
                }
            }))).thenReturn(future);


        } catch (PulsarClientException e) {
           e.printStackTrace();
        }

        runner = TestRunners.newTestRunner(PublishPulsarRecord_1_X.class);

        final String readerId = "record-reader";
        final MockRecordParser readerService = new MockRecordParser();
        readerService.addSchemaField("name", RecordFieldType.STRING);
        readerService.addSchemaField("age", RecordFieldType.INT);
        runner.addControllerService(readerId, readerService);
        runner.enableControllerService(readerService);

        final String writerId = "record-writer";
        final RecordSetWriterFactory writerService = new MockRecordWriter("name, age");
        runner.addControllerService(writerId, writerService);
        runner.enableControllerService(writerService);

        final MockPulsarClientService pulsarClient = new MockPulsarClientService(mockClient);
        runner.addControllerService("pulsarClient", pulsarClient);
        runner.enableControllerService(pulsarClient);

        runner.setProperty(RECORD_READER, readerId);
        runner.setProperty(RECORD_WRITER, writerId);
        runner.setProperty(AbstractPulsarProcessor.PULSAR_CLIENT_SERVICE, "pulsarClient");
    }

    @Test
    public void propertyValidationTest() throws Exception {

        // Initially the processor won't be properly configured
        runner.assertNotValid();

        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, TOPIC_NAME);
        runner.assertValid();
    }

    @Test
    public void invalidTopicTest() throws UnsupportedEncodingException, PulsarClientException {

        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, "${topic}");

        final String content = "some content";
        Map<String, String> attributes = new HashMap<String, String>();
        attributes.put(AbstractPulsarProducerProcessor.TOPIC.getName(), "");

        runner.enqueue(content.getBytes("UTF-8"), attributes );
        runner.run();
        runner.assertAllFlowFilesTransferred(PublishPulsarRecord_1_X.REL_FAILURE);

        // Confirm that no Producer as created
        verify(mockClient, times(0)).createProducer(anyString());
    }

    @Test
    public void dynamicTopicTest() throws UnsupportedEncodingException, PulsarClientException {

        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, "${topic}");

        final String content = "Mary Jane, 32";
        Map<String, String> attributes = new HashMap<String, String>();
        attributes.put(AbstractPulsarProducerProcessor.TOPIC.getName(), TOPIC_NAME);

        runner.enqueue(content.getBytes("UTF-8"), attributes );
        runner.run();
        runner.assertAllFlowFilesTransferred(PublishPulsarRecord_1_X.REL_SUCCESS);

        // Verify that we sent the data to topic-b.
        verify(mockClient, times(1)).createProducer(TOPIC_NAME);
    }


}
