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
package org.apache.nifi.kafka.processors.producer.convert;

import org.apache.nifi.kafka.processors.producer.common.ProducerUtils;
import org.apache.nifi.kafka.processors.producer.header.HeadersFactory;
import org.apache.nifi.kafka.processors.producer.key.KeyFactory;
import org.apache.nifi.kafka.processors.producer.value.RecordValueFactory;
import org.apache.nifi.kafka.processors.producer.value.ValueFactory;
import org.apache.nifi.kafka.service.api.header.RecordHeader;
import org.apache.nifi.kafka.service.api.record.KafkaRecord;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.PushBackRecordSet;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.RecordSet;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * {@link KafkaRecordConverter} implementation for transforming NiFi
 * {@link org.apache.nifi.serialization.record.Record} objects to {@link KafkaRecord} for publish to
 * Kafka.
 */
public class RecordStreamKafkaRecordConverter implements KafkaRecordConverter {
    private final RecordReaderFactory readerFactory;
    private final RecordSetWriterFactory writerFactory;
    private final HeadersFactory headersFactory;
    private final KeyFactory keyFactory;
    private final int maxMessageSize;
    private final ComponentLog logger;

    public RecordStreamKafkaRecordConverter(
            final RecordReaderFactory readerFactory,
            final RecordSetWriterFactory writerFactory,
            final HeadersFactory headersFactory,
            final KeyFactory keyFactory,
            final int maxMessageSize,
            final ComponentLog logger) {
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;
        this.headersFactory = headersFactory;
        this.keyFactory = keyFactory;
        this.maxMessageSize = maxMessageSize;
        this.logger = logger;
    }

    @Override
    public Iterator<KafkaRecord> convert(
            final Map<String, String> attributes, final InputStream in, final long inputLength)
            throws IOException {
        try {
            final RecordReader reader = readerFactory.createRecordReader(attributes, in, inputLength, logger);
            final RecordSet recordSet = reader.createRecordSet();
            final RecordSchema schema = writerFactory.getSchema(attributes, recordSet.getSchema());

            final ByteArrayOutputStream os = new ByteArrayOutputStream();
            final RecordSetWriter writer = writerFactory.createWriter(logger, schema, os, attributes);
            final PushBackRecordSet pushBackRecordSet = new PushBackRecordSet(recordSet);
            return toKafkaRecordIterator(attributes, os, writer, pushBackRecordSet);
        } catch (MalformedRecordException | SchemaNotFoundException e) {
            throw new IOException(e);
        }
    }

    private Iterator<KafkaRecord> toKafkaRecordIterator(
            final Map<String, String> attributes,
            final ByteArrayOutputStream os,
            final RecordSetWriter writer,
            final PushBackRecordSet pushBackRecordSet) throws IOException {
        final ValueFactory valueFactory = new RecordValueFactory(os, writer);
        final List<RecordHeader> headers = headersFactory.getHeaders(attributes);

        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                try {
                    return pushBackRecordSet.isAnotherRecord();
                } catch (final IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            @Override
            public KafkaRecord next() {
                try {
                    final Record record = pushBackRecordSet.next();
                    final byte[] key = keyFactory.getKey(attributes, record);
                    final byte[] value = valueFactory.getValue(record);
                    ProducerUtils.checkMessageSize(maxMessageSize, value.length);
                    return new KafkaRecord(null, null, null, key, value, headers);
                } catch (final IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        };
    }
}
