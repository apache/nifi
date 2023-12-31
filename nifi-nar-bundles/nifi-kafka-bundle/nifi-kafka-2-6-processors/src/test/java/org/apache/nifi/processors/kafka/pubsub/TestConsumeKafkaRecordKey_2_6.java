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
package org.apache.nifi.processors.kafka.pubsub;

import org.apache.nifi.kafka.shared.property.KeyFormat;
import org.apache.nifi.kafka.shared.property.OutputStrategy;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.kafka.pubsub.util.MockRecordParser;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.nifi.kafka.shared.component.KafkaClientComponent.BOOTSTRAP_SERVERS;
import static org.mockito.Mockito.mock;

public class TestConsumeKafkaRecordKey_2_6 {

    private ConsumerPool mockConsumerPool = null;
    private TestRunner runner;

    @BeforeEach
    public void setup() throws InitializationException {
        mockConsumerPool = mock(ConsumerPool.class);

        ConsumeKafkaRecord_2_6 proc = new ConsumeKafkaRecord_2_6() {
            @Override
            protected ConsumerPool createConsumerPool(final ProcessContext context, final ComponentLog log) {
                return mockConsumerPool;
            }
        };

        runner = TestRunners.newTestRunner(proc);
        runner.setProperty(BOOTSTRAP_SERVERS, "okeydokey:1234");

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

        runner.setProperty(ConsumeKafkaRecord_2_6.RECORD_READER, readerId);
        runner.setProperty(ConsumeKafkaRecord_2_6.RECORD_WRITER, writerId);
    }

    @Test
    public void testConfigValidation() {
        runner.assertNotValid();
        runner.setProperty(ConsumeKafkaRecord_2_6.TOPICS, "foo");
        runner.setProperty(ConsumeKafkaRecord_2_6.GROUP_ID, "foo");
        runner.assertValid();
        runner.setProperty(ConsumeKafkaRecord_2_6.OUTPUT_STRATEGY, "foo");
        runner.assertNotValid();
        runner.setProperty(ConsumeKafkaRecord_2_6.OUTPUT_STRATEGY, OutputStrategy.USE_VALUE);
        runner.assertValid();
        runner.setProperty(ConsumeKafkaRecord_2_6.OUTPUT_STRATEGY, OutputStrategy.USE_WRAPPER);
        runner.assertValid();
        runner.setProperty(ConsumeKafkaRecord_2_6.KEY_FORMAT, "foo");
        runner.assertNotValid();
        runner.setProperty(ConsumeKafkaRecord_2_6.KEY_FORMAT, KeyFormat.RECORD);
        runner.assertValid();
        runner.setProperty(ConsumeKafkaRecord_2_6.KEY_RECORD_READER, "no-record-reader");
        runner.assertNotValid();
        runner.setProperty(ConsumeKafkaRecord_2_6.KEY_RECORD_READER, "record-reader");
        runner.assertValid();
    }

    @Test
    public void testConsumeOneCallNoData() {
        runner.setProperty(ConsumeKafkaRecord_2_6.TOPICS, "foo");
        runner.setProperty(ConsumeKafkaRecord_2_6.GROUP_ID, "foo");
        runner.run(1, true);
        runner.assertTransferCount(ConsumeKafkaRecord_2_6.REL_SUCCESS, 0);
        runner.assertTransferCount(ConsumeKafkaRecord_2_6.REL_PARSE_FAILURE, 0);
    }
}
