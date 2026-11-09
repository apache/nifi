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
import org.apache.nifi.kafka.processors.ConsumeKafka;
import org.apache.nifi.kafka.processors.consumer.OffsetTracker;
import org.apache.nifi.kafka.service.api.record.ByteRecord;
import org.apache.nifi.kafka.shared.attribute.KafkaFlowFileAttribute;
import org.apache.nifi.kafka.shared.property.KeyEncoding;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.util.MockFlowFile;
import org.junit.jupiter.api.Test;
import org.mockito.Answers;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

class FlowFileStreamKafkaMessageConverterTest {

    private static final String TOPIC = "topic";
    private static final String BROKER_URI = "brokerUri";
    private static final byte[] NON_EMPTY_VALUE = "payload".getBytes(StandardCharsets.UTF_8);

    @Test
    void testTombstoneAttributeSetOnlyForTombstoneRecords() {
        final ProcessSession session = mock(ProcessSession.class, withSettings().defaultAnswer(Answers.RETURNS_DEEP_STUBS));
        final FlowFile tombstoneFlowFile = new MockFlowFile(1);
        final FlowFile emptyFlowFile = new MockFlowFile(2);
        final FlowFile valueFlowFile = new MockFlowFile(3);
        when(session.create()).thenReturn(tombstoneFlowFile, emptyFlowFile, valueFlowFile);
        when(session.putAttribute(any(FlowFile.class), any(), any())).thenAnswer(invocation -> invocation.getArgument(0));
        when(session.putAllAttributes(any(FlowFile.class), any())).thenAnswer(invocation -> invocation.getArgument(0));
        when(session.write(any(FlowFile.class), any(OutputStreamCallback.class))).thenAnswer(invocation -> invocation.getArgument(0));

        final FlowFileStreamKafkaMessageConverter converter = newConverter();

        final ByteRecord tombstone = new ByteRecord(TOPIC, 0, 0L, 1000L, List.of(), null, new byte[0], 1L, true);
        final ByteRecord emptyValue = new ByteRecord(TOPIC, 0, 1L, 1000L, List.of(), null, new byte[0], 1L, false);
        final ByteRecord value = new ByteRecord(TOPIC, 0, 2L, 1000L, List.of(), null, NON_EMPTY_VALUE, 1L, false);
        converter.toFlowFiles(session, List.of(tombstone, emptyValue, value).iterator());

        verify(session).putAttribute(eq(tombstoneFlowFile), eq(KafkaFlowFileAttribute.KAFKA_TOMBSTONE), eq(Boolean.TRUE.toString()));
        verify(session, never()).putAttribute(eq(emptyFlowFile), eq(KafkaFlowFileAttribute.KAFKA_TOMBSTONE), any());
        verify(session, never()).putAttribute(eq(valueFlowFile), eq(KafkaFlowFileAttribute.KAFKA_TOMBSTONE), any());
        verify(session, never()).write(eq(tombstoneFlowFile), any(OutputStreamCallback.class));
        verify(session, never()).write(eq(emptyFlowFile), any(OutputStreamCallback.class));
        verify(session).write(eq(valueFlowFile), any(OutputStreamCallback.class));
        verify(session).transfer(tombstoneFlowFile, ConsumeKafka.SUCCESS);
        verify(session).transfer(emptyFlowFile, ConsumeKafka.SUCCESS);
        verify(session).transfer(valueFlowFile, ConsumeKafka.SUCCESS);
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
