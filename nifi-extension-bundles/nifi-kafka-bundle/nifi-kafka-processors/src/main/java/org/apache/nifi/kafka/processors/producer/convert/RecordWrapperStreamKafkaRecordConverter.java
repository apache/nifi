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

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.kafka.processors.producer.common.ProducerUtils;
import org.apache.nifi.kafka.processors.producer.wrapper.RecordFieldConverter;
import org.apache.nifi.kafka.processors.producer.wrapper.RecordMetadataStrategy;
import org.apache.nifi.kafka.processors.producer.wrapper.WrapperRecord;
import org.apache.nifi.kafka.service.api.header.RecordHeader;
import org.apache.nifi.kafka.service.api.record.KafkaRecord;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.PushBackRecordSet;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSet;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * {@link KafkaRecordConverter} implementation for transforming NiFi
 * {@link org.apache.nifi.serialization.record.Record} objects to {@link KafkaRecord} for publish to
 * Kafka.
 */
public class RecordWrapperStreamKafkaRecordConverter implements KafkaRecordConverter {
    private final FlowFile flowFile;
    private final RecordMetadataStrategy metadataStrategy;
    private final RecordReaderFactory readerFactory;
    private final RecordSetWriterFactory writerFactory;
    private final RecordSetWriterFactory keyWriterFactory;
    private final int maxMessageSize;
    private final ComponentLog logger;

    public RecordWrapperStreamKafkaRecordConverter(
            final FlowFile flowFile,
            final RecordMetadataStrategy metadataStrategy,
            final RecordReaderFactory readerFactory,
            final RecordSetWriterFactory writerFactory,
            final RecordSetWriterFactory keyWriterFactory,
            final int maxMessageSize,
            final ComponentLog logger) {
        this.flowFile = flowFile;
        this.metadataStrategy = metadataStrategy;
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;
        this.keyWriterFactory = keyWriterFactory;
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
            final PushBackRecordSet pushBackRecordSet = new PushBackRecordSet(recordSet);
            return toKafkaRecordIterator(pushBackRecordSet);
        } catch (MalformedRecordException | SchemaNotFoundException e) {
            throw new IOException("Stream to Record conversion failed", e);
        }
    }

    private Iterator<KafkaRecord> toKafkaRecordIterator(
            final PushBackRecordSet pushBackRecordSet) {
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
                    final RecordFieldConverter converter = new RecordFieldConverter(record, flowFile, logger);
                    final byte[] key = converter.toBytes(WrapperRecord.KEY, keyWriterFactory);
                    final byte[] value = converter.toBytes(WrapperRecord.VALUE, writerFactory);

                    if (value != null) {
                        ProducerUtils.checkMessageSize(maxMessageSize, value.length);
                    }

                    final List<RecordHeader> headers = getKafkaHeaders(record);

                    // wrapper record may specify custom topic / partition
                    String topic = null;
                    Integer partition = null;
                    if (metadataStrategy == RecordMetadataStrategy.FROM_RECORD) {
                        final MapRecord myMetadataRecord = (MapRecord) record.getValue(WrapperRecord.METADATA);
                        topic = myMetadataRecord.getAsString(WrapperRecord.TOPIC);
                        partition = myMetadataRecord.getAsInt(WrapperRecord.PARTITION);
                    }

                    return new KafkaRecord(topic, partition, null, key, value, headers);
                } catch (final IOException e) {
                    throw new UncheckedIOException("Record conversion failed", e);
                }
            }

            private List<RecordHeader> getKafkaHeaders(final Record record) {
                final List<RecordHeader> headers = new ArrayList<>();
                final MapRecord headersRecord = (MapRecord) record.getValue(WrapperRecord.HEADERS);
                if (headersRecord != null) {
                    headersRecord.toMap().forEach((key, value) -> headers.add(
                            new RecordHeader(key, value.toString().getBytes(StandardCharsets.UTF_8))));
                }
                return headers;
            }
        };
    }
}
