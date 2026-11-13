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
package org.apache.nifi.kafka.processors.consumer.convert;

import org.apache.nifi.kafka.processors.ConsumeKafka;
import org.apache.nifi.kafka.processors.consumer.OffsetTracker;
import org.apache.nifi.kafka.service.api.record.ByteRecord;
import org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute;
import org.apache.nifi.kafka.shared.property.KeyEncoding;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.MockProcessSession;
import org.apache.nifi.util.SharedSessionState;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;

class FlowFileStreamKafkaMessageConverterTest {

    private static final String TOPIC = "topic";
    private static final String BROKER_URI = "brokerUri";
    private static final byte[] NON_EMPTY_VALUE = "payload".getBytes(StandardCharsets.UTF_8);

    private MockProcessSession session;

    @BeforeEach
    void setUp() {
        final Processor processor = TestRunners.newTestRunner(ConsumeKafka.class).getProcessor();
        session = new MockProcessSession(new SharedSessionState(processor, new AtomicLong(0)), processor);
    }

    @Test
    void testTombstoneAttributeSetOnlyForTombstoneRecords() throws IOException {
        final FlowFileStreamKafkaMessageConverter converter = newConverter();

        final ByteRecord tombstone = new ByteRecord(TOPIC, 0, 0L, 1000L, List.of(), null, new byte[0], 1L, true);
        final ByteRecord emptyValue = new ByteRecord(TOPIC, 0, 1L, 1000L, List.of(), null, new byte[0], 1L, false);
        final ByteRecord value = new ByteRecord(TOPIC, 0, 2L, 1000L, List.of(), null, NON_EMPTY_VALUE, 1L, false);
        converter.toFlowFiles(session, List.of(tombstone, emptyValue, value).iterator());

        final List<MockFlowFile> flowFiles = session.getFlowFilesForRelationship(ConsumeKafka.SUCCESS);
        assertEquals(3, flowFiles.size());

        final MockFlowFile tombstoneFlowFile = flowFiles.get(0);
        tombstoneFlowFile.assertContentEquals("");
        tombstoneFlowFile.assertAttributeEquals(KafkaFlowFileAttribute.KAFKA_TOMBSTONE, Boolean.TRUE.toString());

        final MockFlowFile emptyFlowFile = flowFiles.get(1);
        emptyFlowFile.assertContentEquals("");
        emptyFlowFile.assertAttributeNotExists(KafkaFlowFileAttribute.KAFKA_TOMBSTONE);

        final MockFlowFile valueFlowFile = flowFiles.get(2);
        valueFlowFile.assertContentEquals(NON_EMPTY_VALUE);
        valueFlowFile.assertAttributeNotExists(KafkaFlowFileAttribute.KAFKA_TOMBSTONE);
    }

    private static FlowFileStreamKafkaMessageConverter newConverter() {
        return new FlowFileStreamKafkaMessageConverter(
                value -> new String(value, StandardCharsets.UTF_8),
                null,
                null,
                KeyEncoding.UTF8,
                true,
                new OffsetTracker(),
                BROKER_URI
        );
    }
}
