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

package org.apache.nifi.avro;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

public abstract class AvroRecordReader implements RecordReader {


    protected abstract GenericRecord nextAvroRecord() throws IOException;


    @Override
    public Record nextRecord() throws IOException, MalformedRecordException {
        GenericRecord record = nextAvroRecord();
        if (record == null) {
            return null;
        }

        final RecordSchema schema = getSchema();
        final Map<String, Object> values = convertAvroRecordToMap(record, schema);
        return new MapRecord(schema, values);
    }


    private Map<String, Object> convertAvroRecordToMap(final GenericRecord avroRecord, final RecordSchema recordSchema) {
        final Map<String, Object> values = new HashMap<>(recordSchema.getFieldCount());

        for (final RecordField recordField : recordSchema.getFields()) {
            Object value = avroRecord.get(recordField.getFieldName());
            if (value == null) {
                for (final String alias : recordField.getAliases()) {
                    value = avroRecord.get(alias);
                    if (value != null) {
                        break;
                    }
                }
            }

            final String fieldName = recordField.getFieldName();
            final Field avroField = avroRecord.getSchema().getField(fieldName);
            if (avroField == null) {
                values.put(fieldName, null);
                continue;
            }

            final Schema fieldSchema = avroField.schema();
            final Object rawValue = normalizeValue(value, fieldSchema);

            final DataType desiredType = recordField.getDataType();
            final Object coercedValue = DataTypeUtils.convertType(rawValue, desiredType, fieldName);

            values.put(fieldName, coercedValue);
        }

        return values;
    }

    private Object normalizeValue(final Object value, final Schema avroSchema) {
        if (value == null) {
            return null;
        }

        switch (avroSchema.getType()) {
            case INT: {
                final LogicalType logicalType = avroSchema.getLogicalType();
                if (logicalType == null) {
                    return value;
                }

                final String logicalName = logicalType.getName();
                if (LogicalTypes.date().getName().equals(logicalName)) {
                    // date logical name means that the value is number of days since Jan 1, 1970
                    return new java.sql.Date(TimeUnit.DAYS.toMillis((int) value));
                } else if (LogicalTypes.timeMillis().equals(logicalName)) {
                    // time-millis logical name means that the value is number of milliseconds since midnight.
                    return new java.sql.Time((int) value);
                }

                break;
            }
            case LONG: {
                final LogicalType logicalType = avroSchema.getLogicalType();
                if (logicalType == null) {
                    return value;
                }

                final String logicalName = logicalType.getName();
                if (LogicalTypes.timeMicros().getName().equals(logicalName)) {
                    return new java.sql.Time(TimeUnit.MICROSECONDS.toMillis((long) value));
                } else if (LogicalTypes.timestampMillis().getName().equals(logicalName)) {
                    return new java.sql.Timestamp((long) value);
                } else if (LogicalTypes.timestampMicros().getName().equals(logicalName)) {
                    return new java.sql.Timestamp(TimeUnit.MICROSECONDS.toMillis((long) value));
                }
                break;
            }
            case UNION:
                if (value instanceof GenericData.Record) {
                    final GenericData.Record avroRecord = (GenericData.Record) value;
                    return normalizeValue(value, avroRecord.getSchema());
                }
                break;
            case RECORD:
                final GenericData.Record record = (GenericData.Record) value;
                final Schema recordSchema = record.getSchema();
                final List<Field> recordFields = recordSchema.getFields();
                final Map<String, Object> values = new HashMap<>(recordFields.size());
                for (final Field field : recordFields) {
                    final Object avroFieldValue = record.get(field.name());
                    final Object fieldValue = normalizeValue(avroFieldValue, field.schema());
                    values.put(field.name(), fieldValue);
                }
                final RecordSchema childSchema = AvroTypeUtil.createSchema(recordSchema);
                return new MapRecord(childSchema, values);
            case BYTES:
                final ByteBuffer bb = (ByteBuffer) value;
                return AvroTypeUtil.convertByteArray(bb.array());
            case FIXED:
                final GenericFixed fixed = (GenericFixed) value;
                return AvroTypeUtil.convertByteArray(fixed.bytes());
            case ENUM:
                return value.toString();
            case NULL:
                return null;
            case STRING:
                return value.toString();
            case ARRAY:
                final Array<?> array = (Array<?>) value;
                final Object[] valueArray = new Object[array.size()];
                for (int i = 0; i < array.size(); i++) {
                    final Schema elementSchema = avroSchema.getElementType();
                    valueArray[i] = normalizeValue(array.get(i), elementSchema);
                }
                return valueArray;
            case MAP:
                final Map<?, ?> avroMap = (Map<?, ?>) value;
                final Map<String, Object> map = new HashMap<>(avroMap.size());
                for (final Map.Entry<?, ?> entry : avroMap.entrySet()) {
                    Object obj = entry.getValue();
                    if (obj instanceof Utf8 || obj instanceof CharSequence) {
                        obj = obj.toString();
                    }

                    final String key = entry.getKey().toString();
                    obj = normalizeValue(obj, avroSchema.getValueType());

                    map.put(key, obj);
                }

                final DataType elementType = AvroTypeUtil.determineDataType(avroSchema.getValueType());
                final List<RecordField> mapFields = new ArrayList<>();
                for (final String key : map.keySet()) {
                    mapFields.add(new RecordField(key, elementType));
                }
                final RecordSchema mapSchema = new SimpleRecordSchema(mapFields);
                return new MapRecord(mapSchema, map);
        }

        return value;
    }



}
