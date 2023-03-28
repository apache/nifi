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
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Record implementation for use with {@link org.apache.nifi.kafka.processors.PublishKafka} publish strategy
 * {@link org.apache.nifi.kafka.shared.property.PublishStrategy#USE_WRAPPER}.
 */
public class WrapperRecord extends MapRecord {

    public static final String TOPIC = "topic";
    public static final String PARTITION = "partition";
    public static final String OFFSET = "offset";
    public static final String TIMESTAMP = "timestamp";

    public static final String METADATA = "metadata";
    public static final String HEADERS = "headers";
    public static final String KEY = "key";
    public static final String VALUE = "value";

    private static final RecordField FIELD_TOPIC = new RecordField(TOPIC, RecordFieldType.STRING.getDataType());
    private static final RecordField FIELD_PARTITION = new RecordField(PARTITION, RecordFieldType.INT.getDataType());
    private static final RecordField FIELD_OFFSET = new RecordField(OFFSET, RecordFieldType.LONG.getDataType());
    private static final RecordField FIELD_TIMESTAMP = new RecordField(TIMESTAMP, RecordFieldType.TIMESTAMP.getDataType());
    public static final RecordSchema SCHEMA_METADATA = new SimpleRecordSchema(Arrays.asList(FIELD_TOPIC, FIELD_PARTITION, FIELD_OFFSET, FIELD_TIMESTAMP));

    public static final RecordField FIELD_METADATA = new RecordField(METADATA, RecordFieldType.RECORD.getRecordDataType(SCHEMA_METADATA));
    public static final RecordField FIELD_HEADERS = new RecordField(HEADERS, RecordFieldType.MAP.getMapDataType(RecordFieldType.STRING.getDataType()));

    private static RecordSchema toRecordSchema(final Record record, final String messageKeyField) {
        final Record recordKey = (Record) record.getValue(messageKeyField);
        final RecordSchema recordSchemaKey = ((recordKey == null) ? null : recordKey.getSchema());
        final RecordField fieldKey = new RecordField(KEY, RecordFieldType.RECORD.getRecordDataType(recordSchemaKey));
        final RecordField fieldValue = new RecordField(VALUE, RecordFieldType.RECORD.getRecordDataType(record.getSchema()));
        return new SimpleRecordSchema(Arrays.asList(FIELD_METADATA, FIELD_HEADERS, fieldKey, fieldValue));
    }

    private static Map<String, Object> toValues(
            final Record record, final List<RecordHeader> headers, final Charset headerCharset,
            final String messageKeyField, final String topic, final int partition, final long offset, final long timestamp) {
        final Map<String, Object> valuesMetadata = new HashMap<>();
        valuesMetadata.put(TOPIC, topic);
        valuesMetadata.put(PARTITION, partition);
        valuesMetadata.put(OFFSET, offset);
        valuesMetadata.put(TIMESTAMP, timestamp);
        final Record recordMetadata = new MapRecord(SCHEMA_METADATA, valuesMetadata);

        final Map<String, Object> valuesHeaders = new HashMap<>();
        for (RecordHeader header : headers) {
            valuesHeaders.put(header.key(), new String(header.value(), headerCharset));
        }

        final Map<String, Object> valuesWrapper = new HashMap<>();
        valuesWrapper.put(METADATA, recordMetadata);
        valuesWrapper.put(HEADERS, valuesHeaders);
        valuesWrapper.put(KEY, record.getValue(messageKeyField));
        valuesWrapper.put(VALUE, record);
        return valuesWrapper;
    }

    public WrapperRecord(final Record record, final String messageKeyField,
                         final List<RecordHeader> headers, final Charset headerCharset,
                         final String topic, final int partition, final long offset, final long timestamp) {
        super(toRecordSchema(record, messageKeyField),
                toValues(record, headers, headerCharset, messageKeyField, topic, partition, offset, timestamp));
    }

    public static RecordSchema toWrapperSchema(final RecordField fieldKey, final RecordSchema recordSchema) {
        final RecordField fieldValue = new RecordField(VALUE, RecordFieldType.RECORD.getRecordDataType(recordSchema));
        return new SimpleRecordSchema(Arrays.asList(FIELD_METADATA, FIELD_HEADERS, fieldKey, fieldValue));
    }
}
