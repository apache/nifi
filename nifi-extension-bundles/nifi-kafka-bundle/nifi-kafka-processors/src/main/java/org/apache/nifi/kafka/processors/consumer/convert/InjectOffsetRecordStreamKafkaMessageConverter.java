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

import org.apache.nifi.kafka.processors.consumer.OffsetTracker;
import org.apache.nifi.kafka.service.api.record.ByteRecord;
import org.apache.nifi.kafka.shared.property.KeyEncoding;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.RecordSetWriterFactory;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Kafka Message Converter supporting injection of Kafka record offset in the kafkaOffset field of the output record
 */
public class InjectOffsetRecordStreamKafkaMessageConverter extends AbstractRecordStreamKafkaMessageConverter {

    private static final String KAFKA_OFFSET = "kafkaOffset";

    private static final RecordField KAFKA_OFFSET_FIELD = new RecordField(KAFKA_OFFSET, RecordFieldType.LONG.getDataType());

    public InjectOffsetRecordStreamKafkaMessageConverter(
            final RecordReaderFactory readerFactory,
            final RecordSetWriterFactory writerFactory,
            final Charset headerEncoding,
            final Pattern headerNamePattern,
            final KeyEncoding keyEncoding,
            final boolean commitOffsets,
            final OffsetTracker offsetTracker,
            final ComponentLog logger,
            final String brokerUri
    ) {
        super(
                readerFactory,
                writerFactory,
                headerEncoding,
                headerNamePattern,
                keyEncoding,
                commitOffsets,
                offsetTracker,
                logger,
                brokerUri
        );
    }

    @Override
    protected RecordSchema getWriteSchema(final RecordSchema inputSchema, final ByteRecord consumerRecord, final Map<String, String> attributes) {
        return getConvertedRecordSchema(inputSchema);
    }

    @Override
    protected Record convertRecord(final ByteRecord consumerRecord, final Record record, final Map<String, String> attributes) {
        final Map<String, Object> values = new HashMap<>(record.toMap());

        final long offset = consumerRecord.getOffset();
        values.put(KAFKA_OFFSET, offset);

        final RecordSchema convertedRecordSchema = getConvertedRecordSchema(record.getSchema());
        return new MapRecord(convertedRecordSchema, values);
    }

    private RecordSchema getConvertedRecordSchema(final RecordSchema inputRecordSchema) {
        final List<RecordField> schemaFields = new ArrayList<>(inputRecordSchema.getFields());
        schemaFields.add(KAFKA_OFFSET_FIELD);
        return new SimpleRecordSchema(schemaFields);
    }
}
