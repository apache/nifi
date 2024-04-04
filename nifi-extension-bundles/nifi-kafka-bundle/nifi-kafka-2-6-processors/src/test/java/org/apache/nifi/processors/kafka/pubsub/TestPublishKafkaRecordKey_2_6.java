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

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.kafka.shared.property.PublishStrategy;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processors.kafka.pubsub.util.MockRecordParser;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.function.Function;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestPublishKafkaRecordKey_2_6 {

    private static final String TOPIC_NAME = "unit-test";

    private PublisherPool mockPool;
    private TestRunner runner;

    @BeforeEach
    public void setup() throws InitializationException, IOException {
        mockPool = mock(PublisherPool.class);
        PublisherLease mockLease = mock(PublisherLease.class);
        Mockito.doCallRealMethod().when(mockLease).publish(any(FlowFile.class), any(RecordSet.class), any(RecordSetWriterFactory.class),
            any(RecordSchema.class), any(String.class), any(String.class), nullable(Function.class), any(PublishMetadataStrategy.class));

        when(mockPool.obtainPublisher()).thenReturn(mockLease);

        runner = TestRunners.newTestRunner(new PublishKafkaRecord_2_6() {
            @Override
            protected PublisherPool createPublisherPool(final ProcessContext context) {
                return mockPool;
            }
        });

        runner.setProperty(PublishKafkaRecord_2_6.TOPIC, TOPIC_NAME);

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

        runner.setProperty(PublishKafkaRecord_2_6.RECORD_READER, readerId);
        runner.setProperty(PublishKafkaRecord_2_6.RECORD_WRITER, writerId);
        runner.setProperty(PublishKafka_2_6.DELIVERY_GUARANTEE, PublishKafka_2_6.DELIVERY_REPLICATED);
    }

    @Test
    public void testConfigValidation() {
        runner.assertValid();
        runner.setProperty(PublishKafkaRecord_2_6.PUBLISH_STRATEGY, "foo");
        runner.assertNotValid();
        runner.setProperty(PublishKafkaRecord_2_6.PUBLISH_STRATEGY, PublishStrategy.USE_VALUE);
        runner.assertValid();
        runner.setProperty(PublishKafkaRecord_2_6.PUBLISH_STRATEGY, PublishStrategy.USE_WRAPPER);
        runner.assertValid();
        runner.setProperty(PublishKafkaRecord_2_6.RECORD_KEY_WRITER, "no-record-writer");
        runner.assertNotValid();
        runner.setProperty(PublishKafkaRecord_2_6.RECORD_KEY_WRITER, "record-writer");
        runner.assertValid();
    }
}
