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

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processors.kafka.pubsub.PublishResult;
import org.apache.nifi.processors.kafka.pubsub.PublisherLease;
import org.apache.nifi.processors.kafka.pubsub.util.MockRecordParser;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;
import org.apache.nifi.util.MockFlowFile;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TestPublisherLease {
    private ComponentLog logger;
    private Producer<byte[], byte[]> producer;

    @Before
    @SuppressWarnings("unchecked")
    public void setup() {
        logger = Mockito.mock(ComponentLog.class);
        producer = Mockito.mock(Producer.class);
    }

    @Test
    public void testPoisonOnException() throws IOException {
        final AtomicInteger poisonCount = new AtomicInteger(0);

        final PublisherLease lease = new PublisherLease(producer, 1024 * 1024, 1000L, logger, true, null, StandardCharsets.UTF_8) {
            @Override
            public void poison() {
                poisonCount.incrementAndGet();
                super.poison();
            }
        };

        final FlowFile flowFile = Mockito.spy(new MockFlowFile(1L));
        // Need a size grater than zero to make the lease reads the InputStream.
        Mockito.when(flowFile.getSize()).thenReturn(1L);
        final String topic = "unit-test";
        final byte[] messageKey = null;
        final byte[] demarcatorBytes = null;

        final InputStream failureInputStream = new InputStream() {
            @Override
            public int read() throws IOException {
                throw new IOException("Intentional Unit Test Exception");
            }
        };

        try {
            lease.publish(flowFile, failureInputStream, messageKey, demarcatorBytes, topic);
            Assert.fail("Expected IOException");
        } catch (final IOException ioe) {
            // expected
        }

        assertEquals(1, poisonCount.get());

        final PublishResult result = lease.complete();
        assertTrue(result.isFailure());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testPoisonOnFailure() throws IOException {
        final AtomicInteger poisonCount = new AtomicInteger(0);

        final PublisherLease lease = new PublisherLease(producer, 1024 * 1024, 1000L, logger, true, null, StandardCharsets.UTF_8) {
            @Override
            public void poison() {
                poisonCount.incrementAndGet();
                super.poison();
            }
        };

        final FlowFile flowFile = new MockFlowFile(1L);
        final String topic = "unit-test";
        final byte[] messageKey = null;
        final byte[] demarcatorBytes = null;

        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(final InvocationOnMock invocation) throws Throwable {
                final Callback callback = invocation.getArgumentAt(1, Callback.class);
                callback.onCompletion(null, new RuntimeException("Unit Test Intentional Exception"));
                return null;
            }
        }).when(producer).send(any(ProducerRecord.class), any(Callback.class));

        lease.publish(flowFile, new ByteArrayInputStream(new byte[1]), messageKey, demarcatorBytes, topic);

        assertEquals(1, poisonCount.get());

        final PublishResult result = lease.complete();
        assertTrue(result.isFailure());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testAllDelimitedMessagesSent() throws IOException {
        final AtomicInteger poisonCount = new AtomicInteger(0);

        final PublisherLease lease = new PublisherLease(producer, 1024 * 1024, 10L, logger, true, null, StandardCharsets.UTF_8) {
            @Override
            protected void poison() {
                poisonCount.incrementAndGet();
                super.poison();
            }
        };

        final AtomicInteger correctMessages = new AtomicInteger(0);
        final AtomicInteger incorrectMessages = new AtomicInteger(0);
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                final ProducerRecord<byte[], byte[]> record = invocation.getArgumentAt(0, ProducerRecord.class);
                final byte[] value = record.value();
                final String valueString = new String(value, StandardCharsets.UTF_8);
                if ("1234567890".equals(valueString)) {
                    correctMessages.incrementAndGet();
                } else {
                    incorrectMessages.incrementAndGet();
                }

                return null;
            }
        }).when(producer).send(any(ProducerRecord.class), any(Callback.class));

        final FlowFile flowFile = new MockFlowFile(1L);
        final String topic = "unit-test";
        final byte[] messageKey = null;
        final byte[] demarcatorBytes = "\n".getBytes(StandardCharsets.UTF_8);

        final byte[] flowFileContent = "1234567890\n1234567890\n1234567890\n\n\n\n1234567890\n\n\n1234567890\n\n\n\n".getBytes(StandardCharsets.UTF_8);
        lease.publish(flowFile, new ByteArrayInputStream(flowFileContent), messageKey, demarcatorBytes, topic);

        final byte[] flowFileContent2 = new byte[0];
        lease.publish(new MockFlowFile(2L), new ByteArrayInputStream(flowFileContent2), messageKey, demarcatorBytes, topic);

        final byte[] flowFileContent3 = "1234567890\n1234567890".getBytes(StandardCharsets.UTF_8); // no trailing new line
        lease.publish(new MockFlowFile(3L), new ByteArrayInputStream(flowFileContent3), messageKey, demarcatorBytes, topic);

        final byte[] flowFileContent4 = "\n\n\n".getBytes(StandardCharsets.UTF_8);
        lease.publish(new MockFlowFile(4L), new ByteArrayInputStream(flowFileContent4), messageKey, demarcatorBytes, topic);

        assertEquals(0, poisonCount.get());

        verify(producer, times(0)).flush();

        final PublishResult result = lease.complete();
        assertTrue(result.isFailure());

        assertEquals(7, correctMessages.get());
        assertEquals(0, incorrectMessages.get());

        verify(producer, times(1)).flush();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testZeroByteMessageSent() throws IOException {
        final AtomicInteger poisonCount = new AtomicInteger(0);

        final PublisherLease lease = new PublisherLease(producer, 1024 * 1024, 10L, logger, true, null, StandardCharsets.UTF_8) {
            @Override
            protected void poison() {
                poisonCount.incrementAndGet();
                super.poison();
            }
        };

        final AtomicInteger correctMessages = new AtomicInteger(0);
        final AtomicInteger incorrectMessages = new AtomicInteger(0);
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                final ProducerRecord<byte[], byte[]> record = invocation.getArgumentAt(0, ProducerRecord.class);
                final byte[] value = record.value();
                final String valueString = new String(value, StandardCharsets.UTF_8);
                if ("".equals(valueString)) {
                    correctMessages.incrementAndGet();
                } else {
                    incorrectMessages.incrementAndGet();
                }

                return null;
            }
        }).when(producer).send(any(ProducerRecord.class), any(Callback.class));

        final FlowFile flowFile = new MockFlowFile(1L);
        final String topic = "unit-test";
        final byte[] messageKey = null;
        final byte[] demarcatorBytes = null;

        final byte[] flowFileContent = new byte[0];
        lease.publish(flowFile, new ByteArrayInputStream(flowFileContent), messageKey, demarcatorBytes, topic);

        assertEquals(0, poisonCount.get());

        verify(producer, times(0)).flush();

        final PublishResult result = lease.complete();

        assertEquals(1, correctMessages.get());
        assertEquals(0, incorrectMessages.get());

        verify(producer, times(1)).flush();
    }

    @Test
    public void testRecordsSentToRecordWriterAndThenToProducer() throws IOException, SchemaNotFoundException, MalformedRecordException {
        final PublisherLease lease = new PublisherLease(producer, 1024 * 1024, 10L, logger, true, null, StandardCharsets.UTF_8);

        final FlowFile flowFile = new MockFlowFile(1L);
        final byte[] exampleInput = "101, John Doe, 48\n102, Jane Doe, 47".getBytes(StandardCharsets.UTF_8);

        final MockRecordParser readerService = new MockRecordParser();
        readerService.addSchemaField("person_id", RecordFieldType.LONG);
        readerService.addSchemaField("name", RecordFieldType.STRING);
        readerService.addSchemaField("age", RecordFieldType.INT);

        final RecordReader reader = readerService.createRecordReader(Collections.emptyMap(), new ByteArrayInputStream(exampleInput), logger);
        final RecordSet recordSet = reader.createRecordSet();
        final RecordSchema schema = reader.getSchema();

        final String topic = "unit-test";
        final String keyField = "person_id";

        final RecordSetWriterFactory writerFactory = Mockito.mock(RecordSetWriterFactory.class);
        final RecordSetWriter writer = Mockito.mock(RecordSetWriter.class);
        Mockito.when(writer.write(Mockito.any(Record.class))).thenReturn(WriteResult.of(1, Collections.emptyMap()));

        Mockito.when(writerFactory.createWriter(eq(logger), eq(schema), any())).thenReturn(writer);

        lease.publish(flowFile, recordSet, writerFactory, schema, keyField, topic);

        verify(writerFactory, times(2)).createWriter(eq(logger), eq(schema), any());
        verify(writer, times(2)).write(any(Record.class));
        verify(producer, times(2)).send(any(), any());
    }
}
