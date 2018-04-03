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
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.nifi.processors.pulsar.AbstractPulsarConsumerProcessor;
import org.apache.nifi.processors.pulsar.AbstractPulsarProcessor;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockPulsarClientService;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockRecordParser;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockRecordWriter;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

public class TestConsumePulsarRecord_1_x {

    protected static String BAD_MSG = "Malformed message";
    protected static String MOCKED_MSG = "Mocked Message, 1";
    protected static String DEFAULT_TOPIC = "foo";
    protected static String DEFAULT_SUB = "bar";

    protected TestRunner runner;

    @Mock
    protected PulsarClient mockClient;

    @Mock
    protected Consumer mockConsumer;

    @Mock
    protected Message mockMessage;

    @Before
    public void setup() throws InitializationException {

        mockClient = mock(PulsarClient.class);
        mockConsumer = mock(Consumer.class);
        mockMessage = mock(Message.class);

        try {
            when(mockClient.subscribe(anyString(), anyString())).thenReturn(mockConsumer);
            when(mockConsumer.receive()).thenReturn(mockMessage);

            CompletableFuture<Message> future = CompletableFuture.supplyAsync(() -> {
                return mockMessage;
            });

            when(mockConsumer.receiveAsync()).thenReturn(future);

        } catch (PulsarClientException e) {
            e.printStackTrace();
        }

        runner = TestRunners.newTestRunner(ConsumePulsarRecord_1_X.class);

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
    public void validatePropertiesValidation() throws Exception {

        // Initially the processor won't be properly configured
        runner.assertNotValid();

        runner.setProperty(AbstractPulsarConsumerProcessor.TOPIC, "my-topic");
        runner.setProperty(AbstractPulsarConsumerProcessor.SUBSCRIPTION, "my-sub");
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

        runner.setProperty(ConsumePulsarRecord_1_X.ASYNC_ENABLED, Boolean.toString(async));
        runner.setProperty(ConsumePulsarRecord_1_X.TOPIC, topic);
        runner.setProperty(ConsumePulsarRecord_1_X.SUBSCRIPTION, sub);
        runner.setProperty(ConsumePulsarRecord_1_X.BATCH_SIZE, batchSize + "");
        runner.run(iterations, true);

        runner.assertAllFlowFilesTransferred(ConsumePulsarRecord_1_X.REL_SUCCESS);

        List<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ConsumePulsarRecord_1_X.REL_SUCCESS);
        assertEquals(iterations, flowFiles.size());

        if (async) {
            verify(mockConsumer, times(batchSize * iterations)).receiveAsync();
        } else {
            verify(mockConsumer, times(batchSize * iterations)).receive();
        }

        // Verify that every message was acknowledged
        if (async) {
            verify(mockConsumer, times(batchSize * iterations)).acknowledgeAsync(mockMessage);
        } else {
            verify(mockConsumer, times(batchSize * iterations)).acknowledge(mockMessage);
        }

        return flowFiles;
    }

}
