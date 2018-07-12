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

import static org.apache.nifi.processors.pulsar.pubsub.ConsumePulsarRecord.RECORD_READER;
import static org.apache.nifi.processors.pulsar.pubsub.ConsumePulsarRecord.RECORD_WRITER;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;

import org.apache.nifi.processors.pulsar.AbstractPulsarConsumerProcessor;
import org.apache.nifi.processors.pulsar.AbstractPulsarConsumerProcessorTest;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockRecordParser;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockRecordWriter;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class TestConsumePulsarRecord extends AbstractPulsarConsumerProcessorTest<byte[]> {

    protected static String BAD_MSG = "Malformed message";
    protected static String MOCKED_MSG = "Mocked Message, 1";
    protected static String DEFAULT_TOPIC = "foo";
    protected static String DEFAULT_SUB = "bar";

    @Mock
    protected Message<byte[]> mockMessage;

    @Before
    public void setup() throws InitializationException {

        mockMessage = mock(Message.class);

        runner = TestRunners.newTestRunner(ConsumePulsarRecord.class);

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
    public void validatePropertiesValidation() throws Exception {
        // Initially the processor won't be properly configured
        runner.assertNotValid();

        runner.setProperty(AbstractPulsarConsumerProcessor.TOPICS, "my-topic");
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION_NAME, "my-sub");
        runner.assertValid();
    }

    protected List<MockFlowFile> sendMessages(String msg, boolean async, int iterations) throws PulsarClientException {
        return sendMessages(msg, DEFAULT_TOPIC, DEFAULT_SUB, async, iterations, 1);
    }

    protected List<MockFlowFile> sendMessages(String msg, boolean async, int iterations, int batchSize) throws PulsarClientException {
        return sendMessages(msg, DEFAULT_TOPIC, DEFAULT_SUB, async, iterations, batchSize);
    }

    protected List<MockFlowFile> sendMessages(String msg, String topic, String sub, boolean async, int iterations) throws PulsarClientException {
        return sendMessages(msg, topic, sub, async, iterations, 1);
    }

    protected List<MockFlowFile> sendMessages(String msg, String topic, String sub, boolean async, int iterations, int batchSize) throws PulsarClientException {
        when(mockMessage.getData()).thenReturn(msg.getBytes());
        mockClientService.setMockMessage(mockMessage);

        runner.setProperty(ConsumePulsarRecord.ASYNC_ENABLED, Boolean.toString(async));
        runner.setProperty(ConsumePulsarRecord.TOPICS, topic);
        runner.setProperty(ConsumePulsarRecord.SUBSCRIPTION_NAME, sub);
        runner.setProperty(ConsumePulsarRecord.BATCH_SIZE, batchSize + "");

        if (async) {
          runner.setProperty(ConsumePulsarRecord.MAX_WAIT_TIME, "5 sec");
        } else {
          runner.setProperty(ConsumePulsarRecord.MAX_WAIT_TIME, "0 sec");
        }

        runner.run(iterations, true);
        runner.assertAllFlowFilesTransferred(ConsumePulsarRecord.REL_SUCCESS);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumePulsarRecord.REL_SUCCESS);
        assertEquals(iterations, flowFiles.size());

        verify(mockClientService.getMockConsumer(), times(batchSize * iterations)).receive();

        // Verify that every message was acknowledged
        if (async) {
            verify(mockClientService.getMockConsumer(), times(batchSize * iterations)).acknowledgeAsync(mockMessage);
        } else {
            verify(mockClientService.getMockConsumer(), times(batchSize * iterations)).acknowledge(mockMessage);
        }
        return flowFiles;
    }
}
