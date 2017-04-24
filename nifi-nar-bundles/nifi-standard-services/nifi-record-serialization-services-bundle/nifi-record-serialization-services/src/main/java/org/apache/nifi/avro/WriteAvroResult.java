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
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.EnumSymbol;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.nifi.serialization.RecordSetWriter;
import org.apache.nifi.serialization.WriteResult;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.serialization.record.util.IllegalTypeConversionException;

public abstract class WriteAvroResult implements RecordSetWriter {
    private final Schema schema;

    public WriteAvroResult(final Schema schema) {
        this.schema = schema;
    }

    protected Schema getSchema() {
        return schema;
    }

    protected GenericRecord createAvroRecord(final Record record, final Schema avroSchema) throws IOException {
        final GenericRecord rec = new GenericData.Record(avroSchema);
        final RecordSchema recordSchema = record.getSchema();

        for (final RecordField recordField : recordSchema.getFields()) {
            final Object rawValue = record.getValue(recordField);
            final String fieldName = recordField.getFieldName();

            final Field field = avroSchema.getField(fieldName);
            if (field == null) {
                continue;
            }

            final Object converted = convertToAvroObject(rawValue, field.schema(), fieldName);
            rec.put(fieldName, converted);
        }

        return rec;
    }

    protected Object convertToAvroObject(final Object rawValue, final Schema fieldSchema, final String fieldName) throws IOException {
        if (rawValue == null) {
            return null;
        }

        switch (fieldSchema.getType()) {
            case INT: {
                final LogicalType logicalType = fieldSchema.getLogicalType();
                if (logicalType == null) {
                    return DataTypeUtils.toInteger(rawValue, fieldName);
                }

                if (LogicalTypes.date().getName().equals(logicalType.getName())) {
                    final long longValue = DataTypeUtils.toLong(rawValue, fieldName);
                    final Date date = new Date(longValue);
                    final Duration duration = Duration.between(new Date(0L).toInstant(), date.toInstant());
                    final long days = duration.toDays();
                    return (int) days;
                } else if (LogicalTypes.timeMillis().getName().equals(logicalType.getName())) {
                    final long longValue = DataTypeUtils.toLong(rawValue, fieldName);
                    final Date date = new Date(longValue);
                    final Duration duration = Duration.between(date.toInstant().truncatedTo(ChronoUnit.DAYS), date.toInstant());
                    final long millisSinceMidnight = duration.toMillis();
                    return (int) millisSinceMidnight;
                }

                return DataTypeUtils.toInteger(rawValue, fieldName);
            }
            case LONG: {
                final LogicalType logicalType = fieldSchema.getLogicalType();
                if (logicalType == null) {
                    return DataTypeUtils.toLong(rawValue, fieldName);
                }

                if (LogicalTypes.timeMicros().getName().equals(logicalType.getName())) {
                    final long longValue = DataTypeUtils.toLong(rawValue, fieldName);
                    final Date date = new Date(longValue);
                    final Duration duration = Duration.between(date.toInstant().truncatedTo(ChronoUnit.DAYS), date.toInstant());
                    return duration.toMillis() * 1000L;
                } else if (LogicalTypes.timestampMillis().getName().equals(logicalType.getName())) {
                    return DataTypeUtils.toLong(rawValue, fieldName);
                } else if (LogicalTypes.timestampMicros().getName().equals(logicalType.getName())) {
                    return DataTypeUtils.toLong(rawValue, fieldName) * 1000L;
                }

                return DataTypeUtils.toLong(rawValue, fieldName);
            }
            case BYTES:
            case FIXED:
                if (rawValue instanceof byte[]) {
                    return ByteBuffer.wrap((byte[]) rawValue);
                }
                if (rawValue instanceof Object[]) {
                    return AvroTypeUtil.convertByteArray((Object[]) rawValue);
                } else {
                    throw new IllegalTypeConversionException("Cannot convert value " + rawValue + " of type " + rawValue.getClass() + " to a ByteBuffer");
                }
            case MAP:
                if (rawValue instanceof Record) {
                    final Record recordValue = (Record) rawValue;
                    final Map<String, Object> map = new HashMap<>();
                    for (final RecordField recordField : recordValue.getSchema().getFields()) {
                        final Object v = recordValue.getValue(recordField);
                        if (v != null) {
                            map.put(recordField.getFieldName(), v);
                        }
                    }

                    return map;
                } else {
                    throw new IllegalTypeConversionException("Cannot convert value " + rawValue + " of type " + rawValue.getClass() + " to a Map");
                }
            case RECORD:
                final GenericData.Record avroRecord = new GenericData.Record(fieldSchema);

                final Record record = (Record) rawValue;
                for (final RecordField recordField : record.getSchema().getFields()) {
                    final Object recordFieldValue = record.getValue(recordField);
                    final String recordFieldName = recordField.getFieldName();

                    final Field field = fieldSchema.getField(recordFieldName);
                    if (field == null) {
                        continue;
                    }

                    final Object converted = convertToAvroObject(recordFieldValue, field.schema(), fieldName);
                    avroRecord.put(recordFieldName, converted);
                }
                return avroRecord;
            case ARRAY:
                final Object[] objectArray = (Object[]) rawValue;
                final List<Object> list = new ArrayList<>(objectArray.length);
                for (final Object o : objectArray) {
                    final Object converted = convertToAvroObject(o, fieldSchema.getElementType(), fieldName);
                    list.add(converted);
                }
                return list;
            case BOOLEAN:
                return DataTypeUtils.toBoolean(rawValue, fieldName);
            case DOUBLE:
                return DataTypeUtils.toDouble(rawValue, fieldName);
            case FLOAT:
                return DataTypeUtils.toFloat(rawValue, fieldName);
            case NULL:
                return null;
            case ENUM:
                return new EnumSymbol(fieldSchema, rawValue);
            case STRING:
                return DataTypeUtils.toString(rawValue, RecordFieldType.DATE.getDefaultFormat(), RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat());
        }

        return rawValue;
    }

    @Override
    public WriteResult write(final Record record, final OutputStream out) throws IOException {
        final GenericRecord rec = createAvroRecord(record, schema);

        final DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try (final DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter)) {
            dataFileWriter.create(schema, out);
            dataFileWriter.append(rec);
        }

        return WriteResult.of(1, Collections.emptyMap());
    }


    @Override
    public String getMimeType() {
        return "application/avro-binary";
    }

    public static String normalizeNameForAvro(String inputName) {
        String normalizedName = inputName.replaceAll("[^A-Za-z0-9_]", "_");
        if (Character.isDigit(normalizedName.charAt(0))) {
            normalizedName = "_" + normalizedName;
        }
        return normalizedName;
    }
}
