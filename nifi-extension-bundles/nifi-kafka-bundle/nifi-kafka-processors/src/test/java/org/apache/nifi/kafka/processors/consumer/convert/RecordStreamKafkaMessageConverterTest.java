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

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.kafka.processors.consumer.KafkaMetricName;
import org.apache.nifi.kafka.processors.consumer.OffsetTracker;
import org.apache.nifi.kafka.service.api.record.ByteRecord;
import org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute;
import org.apache.nifi.kafka.shared.property.KeyEncoding;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.metrics.CommitTiming;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockFlowFile;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

class RecordStreamKafkaMessageConverterTest {

    private static final String FIRST_TOPIC = "topic1";
    private static final String SECOND_TOPIC = "topic2";
    private static final int FIRST_PARTITION = 0;
    private static final String BROKER_URI = "brokerUri";

    private final RecordSetWriterFactory writerFactory = mock(RecordSetWriterFactory.class, withSettings().defaultAnswer(Answers.RETURNS_DEEP_STUBS));
    private final ComponentLog logger = mock(ComponentLog.class);
    private final ProcessSession session = mock(ProcessSession.class, withSettings().defaultAnswer(Answers.RETURNS_DEEP_STUBS));

    private final OffsetTracker offsetTracker = new OffsetTracker();

    @Test
    void testGroupingOfMessagesByTopicAndPartition() {
        // Initialize MockRecordParser
        final MockRecordParser readerFactory = new MockRecordParser();
        readerFactory.addSchemaField("field1", RecordFieldType.STRING);

        // Add records to MockRecordParser
        readerFactory.addRecord("value1");
        readerFactory.addRecord("value2");
        readerFactory.addRecord("value3");
        readerFactory.addRecord("value4");

        // Initialize the converter
        final RecordStreamKafkaMessageConverter converter = new RecordStreamKafkaMessageConverter(
                readerFactory,
                writerFactory,
                value -> new String(value, StandardCharsets.UTF_8),
                Pattern.compile(".*"),
                KeyEncoding.UTF8,
                true,
                offsetTracker,
                logger,
                BROKER_URI,
                new CreateNewFlowFileGrouping(writerFactory, logger, BROKER_URI, true)
        );

        // Create ByteRecords
        final ByteRecord group1Record1 = new ByteRecord(FIRST_TOPIC, 0, 0, 1000L, List.of(), null, "value1".getBytes(), 0L);
        final ByteRecord group1Record2 = new ByteRecord(FIRST_TOPIC, 0, 3, 500L, List.of(), null, "value4".getBytes(), 0L);

        final ByteRecord group2 = new ByteRecord(FIRST_TOPIC, 1, 1, 2000L, List.of(), null, "value2".getBytes(), 0L);

        final ByteRecord group3 = new ByteRecord(SECOND_TOPIC, 0, 2, 3000L, List.of(), null, "value3".getBytes(), 0L);

        final Iterator<ByteRecord> consumerRecords = List.of(group1Record1, group2, group3, group1Record2).iterator();
        // Mock the session.create() and session.write() methods
        final FlowFile flowFile1 = new MockFlowFile(1);
        final FlowFile flowFile2 = new MockFlowFile(2);
        final FlowFile flowFile3 = new MockFlowFile(3);
        final FlowFile flowFile4 = new MockFlowFile(4);
        when(session.create()).thenReturn(flowFile1, flowFile2, flowFile3, flowFile4);
        when(session.write(any(FlowFile.class))).thenReturn(mock(OutputStream.class));

        // Call the method under test
        converter.toFlowFiles(session, consumerRecords);

        // Verify that the messages are grouped correctly
        final ArgumentCaptor<Map<String, String>> attributesCaptor = ArgumentCaptor.forClass(Map.class);
        verify(session, atLeastOnce()).putAllAttributes(any(FlowFile.class), attributesCaptor.capture());

        final List<Map<String, String>> capturedAttributes = attributesCaptor.getAllValues();

        // check group1 records
        assertEquals(FIRST_TOPIC, capturedAttributes.get(0).get(KafkaFlowFileAttribute.KAFKA_TOPIC));
        assertEquals("0", capturedAttributes.get(0).get(KafkaFlowFileAttribute.KAFKA_PARTITION));

        assertEquals(FIRST_TOPIC, capturedAttributes.get(3).get(KafkaFlowFileAttribute.KAFKA_TOPIC));
        assertEquals("0", capturedAttributes.get(3).get(KafkaFlowFileAttribute.KAFKA_PARTITION));

        //check group2 records
        assertEquals(FIRST_TOPIC, capturedAttributes.get(1).get(KafkaFlowFileAttribute.KAFKA_TOPIC));
        assertEquals("1", capturedAttributes.get(1).get(KafkaFlowFileAttribute.KAFKA_PARTITION));

        //check group3 records
        assertEquals(SECOND_TOPIC, capturedAttributes.get(2).get(KafkaFlowFileAttribute.KAFKA_TOPIC));
        assertEquals("0", capturedAttributes.get(2).get(KafkaFlowFileAttribute.KAFKA_PARTITION));

        final List<String> timestamps = capturedAttributes.stream()
                .map(attrs -> attrs.get(KafkaFlowFileAttribute.KAFKA_TIMESTAMP))
                .filter(Objects::nonNull)
                .toList();
        assertTrue(timestamps.contains("500"), "Expected timestamp from group1Record2");
        assertTrue(timestamps.contains("2000"), "Expected timestamp from group2");
        assertTrue(timestamps.contains("3000"), "Expected timestamp from group3");
    }

    @Test
    void testParseFailureIncrementsParseErrorCounterImmediately() throws Exception {
        final RecordReaderFactory readerFactory = mock(RecordReaderFactory.class);
        when(readerFactory.createRecordReader(any(), any(), anyLong(), any()))
                .thenThrow(new MalformedRecordException("invalid record"));

        final FlowFile flowFile = new MockFlowFile(1);
        when(session.create()).thenReturn(flowFile);
        when(session.putAllAttributes(any(FlowFile.class), any())).thenReturn(flowFile);
        when(session.write(any(FlowFile.class))).thenReturn(mock(OutputStream.class));

        final RecordStreamKafkaMessageConverter converter = new RecordStreamKafkaMessageConverter(
                readerFactory,
                writerFactory,
                value -> new String(value, StandardCharsets.UTF_8),
                Pattern.compile(".*"),
                KeyEncoding.UTF8,
                true,
                offsetTracker,
                logger,
                BROKER_URI,
                new CreateNewFlowFileGrouping(writerFactory, logger, BROKER_URI, true)
        );

        final ByteRecord consumerRecord = new ByteRecord(FIRST_TOPIC, FIRST_PARTITION, 0, 1000L, List.of(), null, "invalid".getBytes(StandardCharsets.UTF_8), 1L);
        converter.toFlowFiles(session, List.of(consumerRecord).iterator());

        verify(session).adjustCounter(eq(KafkaMetricName.RECORDS_PARSED_ERRORS.getMetricName()), eq(1L),
                eq(Map.of(
                        KafkaFlowFileAttribute.KAFKA_TOPIC, FIRST_TOPIC,
                        KafkaFlowFileAttribute.KAFKA_PARTITION, Integer.toString(FIRST_PARTITION))),
                eq(CommitTiming.NOW));
    }
}
