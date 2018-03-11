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

import org.apache.nifi.processors.pulsar.pubsub.mocks.MockPulsarClientService;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockRecordParser;
import org.apache.nifi.processors.pulsar.pubsub.mocks.MockRecordWriter;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.PulsarClient;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import static org.apache.nifi.processors.pulsar.RecordBasedConst.RECORD_READER;
import static org.apache.nifi.processors.pulsar.RecordBasedConst.RECORD_WRITER;

import org.apache.nifi.processors.pulsar.AbstractPulsarConsumerProcessor;
import org.apache.nifi.processors.pulsar.AbstractPulsarProcessor;

public class TestConsumePulsarRecord_1_0 {

    private TestRunner runner;

    @Mock
    protected PulsarClient mockClient;

    @Before
    public void setup() throws InitializationException {
        runner = TestRunners.newTestRunner(ConsumePulsarRecord_1_0.class);

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

    @Test
    public void validateGetAllMessages() throws Exception {
        // TODO
    }
}
