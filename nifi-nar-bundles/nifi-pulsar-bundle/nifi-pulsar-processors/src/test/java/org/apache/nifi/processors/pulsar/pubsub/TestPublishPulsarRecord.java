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

import static org.apache.nifi.processors.pulsar.pubsub.PublishPulsarRecord.RECORD_READER;
import static org.apache.nifi.processors.pulsar.pubsub.PublishPulsarRecord.RECORD_WRITER;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import org.apache.nifi.processors.pulsar.AbstractPulsarConsumerProcessorTest;
import org.apache.nifi.processors.pulsar.AbstractPulsarProducerProcessor;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockRecordParser;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockRecordWriter;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.Before;
import org.junit.Test;

public class TestPublishPulsarRecord extends AbstractPulsarConsumerProcessorTest<byte[]> {

    protected static final String TOPIC_NAME = "unit-test";

    @Before
    public void setup() throws InitializationException {

        runner = TestRunners.newTestRunner(PublishPulsarRecord.class);

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

        runner.setProperty(RECORD_READER, readerId);
        runner.setProperty(RECORD_WRITER, writerId);

        addPulsarClientService();
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
        runner.assertAllFlowFilesTransferred(PublishPulsarRecord.REL_FAILURE);

        // Confirm that no Producer as created
        verify(mockClientService.getMockProducerBuilder(), times(0)).topic(anyString());
    }

    @Test
    public void dynamicTopicTest() throws UnsupportedEncodingException, PulsarClientException {
        runner.setProperty(AbstractPulsarProducerProcessor.TOPIC, "${topic}");

        final String content = "Mary Jane, 32";
        Map<String, String> attributes = new HashMap<String, String>();
        attributes.put(AbstractPulsarProducerProcessor.TOPIC.getName(), TOPIC_NAME);

        runner.enqueue(content.getBytes("UTF-8"), attributes );
        runner.run();
        runner.assertAllFlowFilesTransferred(PublishPulsarRecord.REL_SUCCESS);

        // Verify that we sent the data to topic-b.
        verify(mockClientService.getMockProducerBuilder(), times(1)).topic(TOPIC_NAME);
    }
}
