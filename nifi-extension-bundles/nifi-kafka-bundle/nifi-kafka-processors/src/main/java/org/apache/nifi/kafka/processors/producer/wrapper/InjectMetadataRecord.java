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
package org.apache.nifi.kafka.processors.producer.wrapper;

import org.apache.nifi.kafka.service.api.header.RecordHeader;
import org.apache.nifi.kafka.service.api.record.ByteRecord;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.Tuple;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Record implementation for use with {@link org.apache.nifi.kafka.processors.PublishKafka} publish strategy
 * {@link org.apache.nifi.kafka.shared.property.PublishStrategy#USE_WRAPPER}.
 */
public class InjectMetadataRecord extends MapRecord {

    public static final String TOPIC = "topic";
    public static final String PARTITION = "partition";
    public static final String OFFSET = "offset";
    public static final String TIMESTAMP = "timestamp";

    public static final String METADATA = "kafkaMetadata";
    public static final String HEADERS = "headers";
    public static final String KEY = "key";

    private static final RecordField FIELD_TOPIC = new RecordField(TOPIC, RecordFieldType.STRING.getDataType());
    private static final RecordField FIELD_PARTITION = new RecordField(PARTITION, RecordFieldType.INT.getDataType());
    private static final RecordField FIELD_OFFSET = new RecordField(OFFSET, RecordFieldType.LONG.getDataType());
    private static final RecordField FIELD_TIMESTAMP = new RecordField(TIMESTAMP, RecordFieldType.TIMESTAMP.getDataType());
    private static final RecordField FIELD_HEADERS = new RecordField(HEADERS, RecordFieldType.MAP.getMapDataType(RecordFieldType.STRING.getDataType()));

    private static RecordSchema toRecordSchema(final Record record, final String messageKeyField) {
        final Record recordKey = (Record) record.getValue(messageKeyField);
        final RecordSchema recordSchemaKey = recordKey == null ? null : recordKey.getSchema();
        final RecordField fieldKey = new RecordField(KEY, RecordFieldType.RECORD.getRecordDataType(recordSchemaKey));

        final RecordSchema metadataRecordSchema = new SimpleRecordSchema(List.of(FIELD_TOPIC, FIELD_PARTITION, FIELD_OFFSET, FIELD_TIMESTAMP, fieldKey, FIELD_HEADERS));
        final RecordField metadataField = new RecordField(METADATA, RecordFieldType.RECORD.getRecordDataType(metadataRecordSchema));

        final RecordSchema valueSchema = record.getSchema();
        final List<RecordField> valueFields = valueSchema.getFields();
        valueFields.add(metadataField);

        return new SimpleRecordSchema(valueFields);
    }

    private static Map<String, Object> toValues(
            final Record record, final List<RecordHeader> headers, final Charset headerCharset,
            final String messageKeyField, final String topic, final int partition, final long offset, final long timestamp) {
        final Map<String, Object> valuesMetadata = new HashMap<>();
        valuesMetadata.put(TOPIC, topic);
        valuesMetadata.put(PARTITION, partition);
        valuesMetadata.put(OFFSET, offset);
        valuesMetadata.put(TIMESTAMP, timestamp);
        valuesMetadata.put(KEY, record.getValue(messageKeyField));

        final Map<String, Object> valuesHeaders = new HashMap<>();
        for (RecordHeader header : headers) {
            valuesHeaders.put(header.key(), new String(header.value(), headerCharset));
        }
        valuesMetadata.put(HEADERS, valuesHeaders);

        Map<String, Object> valueMap = record.toMap();
        valueMap.put(METADATA, valuesMetadata);

        return valueMap;
    }

    public InjectMetadataRecord(final Record record, final String messageKeyField,
                         final List<RecordHeader> headers, final Charset headerCharset,
                         final String topic, final int partition, final long offset, final long timestamp) {
        super(toRecordSchema(record, messageKeyField),
                toValues(record, headers, headerCharset, messageKeyField, topic, partition, offset, timestamp));
    }

    public static RecordSchema toWrapperSchema(final RecordField fieldKey, final RecordSchema recordSchema) {
        final RecordSchema metadataRecordSchema = fieldKey == null
                ? new SimpleRecordSchema(List.of(FIELD_TOPIC, FIELD_PARTITION, FIELD_OFFSET, FIELD_TIMESTAMP, FIELD_HEADERS))
                : new SimpleRecordSchema(List.of(FIELD_TOPIC, FIELD_PARTITION, FIELD_OFFSET, FIELD_TIMESTAMP, fieldKey, FIELD_HEADERS));
        final RecordField metadataField = new RecordField(METADATA, RecordFieldType.RECORD.getRecordDataType(metadataRecordSchema));
        final List<RecordField> valueFields = recordSchema.getFields();
        final List<RecordField> valueFieldsWithInjectedMetadata = new ArrayList<>(valueFields);
        valueFieldsWithInjectedMetadata.add(metadataField);
        return new SimpleRecordSchema(valueFieldsWithInjectedMetadata);
    }

    public static MapRecord toWrapperRecord(final Charset headerCharacterSet, final ByteRecord consumerRecord, final Record record, final Tuple<RecordField, Object> tupleKey) {
        final RecordSchema schema = record.getSchema();
        RecordSchema finalSchema = InjectMetadataRecord.toWrapperSchema(tupleKey.getKey(), schema);

        final Map<String, Object> valuesMetadata = new HashMap<>();
        valuesMetadata.put(InjectMetadataRecord.TOPIC, consumerRecord.getTopic());
        valuesMetadata.put(InjectMetadataRecord.PARTITION, consumerRecord.getPartition());
        valuesMetadata.put(InjectMetadataRecord.OFFSET, consumerRecord.getOffset());
        valuesMetadata.put(InjectMetadataRecord.TIMESTAMP, consumerRecord.getTimestamp());
        if (tupleKey.getKey() != null) {
            valuesMetadata.put(InjectMetadataRecord.KEY, tupleKey.getValue());
        }

        final Map<String, Object> valuesHeaders = new HashMap<>();
        for (RecordHeader header : consumerRecord.getHeaders()) {
            valuesHeaders.put(header.key(), new String(header.value(), headerCharacterSet));
        }
        valuesMetadata.put(InjectMetadataRecord.HEADERS, valuesHeaders);

        final Map<String, Object> valueMap = record.toMap();
        final Map<String, Object> valueMapWithInjectedMetadata = new HashMap<>(valueMap);
        valueMapWithInjectedMetadata.put(InjectMetadataRecord.METADATA, valuesMetadata);

        return new MapRecord(finalSchema, valueMapWithInjectedMetadata);
    }
}
