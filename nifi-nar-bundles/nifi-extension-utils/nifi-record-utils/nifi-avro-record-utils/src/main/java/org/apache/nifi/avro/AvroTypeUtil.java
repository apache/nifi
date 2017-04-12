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

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.serialization.record.util.IllegalTypeConversionException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class AvroTypeUtil {
    public static final String AVRO_SCHEMA_FORMAT = "avro";

    public static Schema extractAvroSchema(final RecordSchema recordSchema) throws SchemaNotFoundException {
        if (recordSchema == null) {
            throw new IllegalArgumentException("RecordSchema cannot be null");
        }

        final Optional<String> schemaFormatOption = recordSchema.getSchemaFormat();
        if (!schemaFormatOption.isPresent()) {
            throw new SchemaNotFoundException("No Schema Format was present in the RecordSchema");
        }

        final String schemaFormat = schemaFormatOption.get();
        if (!schemaFormat.equals(AVRO_SCHEMA_FORMAT)) {
            throw new SchemaNotFoundException("Schema provided is not in Avro format");
        }

        final Optional<String> textOption = recordSchema.getSchemaText();
        if (!textOption.isPresent()) {
            throw new SchemaNotFoundException("No Schema text was present in the RecordSchema");
        }

        final String text = textOption.get();
        return new Schema.Parser().parse(text);
    }

    public static DataType determineDataType(final Schema avroSchema) {
        final Type avroType = avroSchema.getType();

        switch (avroType) {
            case BYTES:
            case FIXED:
                return RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType());
            case ARRAY:
                final DataType elementType = determineDataType(avroSchema.getElementType());
                return RecordFieldType.ARRAY.getArrayDataType(elementType);
            case BOOLEAN:
                return RecordFieldType.BOOLEAN.getDataType();
            case DOUBLE:
                return RecordFieldType.DOUBLE.getDataType();
            case ENUM:
            case STRING:
                return RecordFieldType.STRING.getDataType();
            case FLOAT:
                return RecordFieldType.FLOAT.getDataType();
            case INT: {
                final LogicalType logicalType = avroSchema.getLogicalType();
                if (logicalType == null) {
                    return RecordFieldType.INT.getDataType();
                }

                if (LogicalTypes.date().getName().equals(logicalType.getName())) {
                    return RecordFieldType.DATE.getDataType();
                } else if (LogicalTypes.timeMillis().getName().equals(logicalType.getName())) {
                    return RecordFieldType.TIME.getDataType();
                }

                return RecordFieldType.INT.getDataType();
            }
            case LONG: {
                final LogicalType logicalType = avroSchema.getLogicalType();
                if (logicalType == null) {
                    return RecordFieldType.LONG.getDataType();
                }

                if (LogicalTypes.timestampMillis().getName().equals(logicalType.getName())) {
                    return RecordFieldType.TIMESTAMP.getDataType();
                } else if (LogicalTypes.timestampMicros().getName().equals(logicalType.getName())) {
                    return RecordFieldType.TIMESTAMP.getDataType();
                } else if (LogicalTypes.timeMicros().getName().equals(logicalType.getName())) {
                    return RecordFieldType.TIME.getDataType();
                }

                return RecordFieldType.LONG.getDataType();
            }
            case RECORD: {
                final List<Field> avroFields = avroSchema.getFields();
                final List<RecordField> recordFields = new ArrayList<>(avroFields.size());

                for (final Field field : avroFields) {
                    final String fieldName = field.name();
                    final Schema fieldSchema = field.schema();
                    final DataType fieldType = determineDataType(fieldSchema);
                    recordFields.add(new RecordField(fieldName, fieldType, field.defaultVal(), field.aliases()));
                }

                final RecordSchema recordSchema = new SimpleRecordSchema(recordFields, avroSchema.toString(), AVRO_SCHEMA_FORMAT, SchemaIdentifier.EMPTY);
                return RecordFieldType.RECORD.getRecordDataType(recordSchema);
            }
            case NULL:
                return RecordFieldType.STRING.getDataType();
            case MAP:
                final Schema valueSchema = avroSchema.getValueType();
                final DataType valueType = determineDataType(valueSchema);
                return RecordFieldType.MAP.getMapDataType(valueType);
            case UNION: {
                final List<Schema> nonNullSubSchemas = avroSchema.getTypes().stream()
                    .filter(s -> s.getType() != Type.NULL)
                    .collect(Collectors.toList());

                if (nonNullSubSchemas.size() == 1) {
                    return determineDataType(nonNullSubSchemas.get(0));
                }

                final List<DataType> possibleChildTypes = new ArrayList<>(nonNullSubSchemas.size());
                for (final Schema subSchema : nonNullSubSchemas) {
                    final DataType childDataType = determineDataType(subSchema);
                    possibleChildTypes.add(childDataType);
                }

                return RecordFieldType.CHOICE.getChoiceDataType(possibleChildTypes);
            }
        }

        return null;
    }

    public static RecordSchema createSchema(final Schema avroSchema) {
        if (avroSchema == null) {
            throw new IllegalArgumentException("Avro Schema cannot be null");
        }

        final List<RecordField> recordFields = new ArrayList<>(avroSchema.getFields().size());
        for (final Field field : avroSchema.getFields()) {
            final String fieldName = field.name();
            final DataType dataType = AvroTypeUtil.determineDataType(field.schema());

            recordFields.add(new RecordField(fieldName, dataType, field.defaultVal(), field.aliases()));
        }

        final RecordSchema recordSchema = new SimpleRecordSchema(recordFields, avroSchema.toString(), AVRO_SCHEMA_FORMAT, SchemaIdentifier.EMPTY);
        return recordSchema;
    }

    public static Object[] convertByteArray(final byte[] bytes) {
        final Object[] array = new Object[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            array[i] = Byte.valueOf(bytes[i]);
        }
        return array;
    }

    public static ByteBuffer convertByteArray(final Object[] bytes) {
        final ByteBuffer bb = ByteBuffer.allocate(bytes.length);
        for (final Object o : bytes) {
            if (o instanceof Byte) {
                bb.put(((Byte) o).byteValue());
            } else {
                throw new IllegalTypeConversionException("Cannot convert value " + bytes + " of type " + bytes.getClass() + " to ByteBuffer");
            }
        }
        bb.flip();
        return bb;
    }

    public static GenericRecord createAvroRecord(final Record record, final Schema avroSchema) throws IOException {
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

    private static Object convertToAvroObject(final Object rawValue, final Schema fieldSchema, final String fieldName) throws IOException {
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
            case UNION:
                List<Schema> unionFieldSchemas = fieldSchema.getTypes();
                if (unionFieldSchemas != null) {
                    // Ignore null types in union
                    final List<Schema> nonNullFieldSchemas = unionFieldSchemas.stream()
                            .filter(s -> s.getType() != Type.NULL)
                            .collect(Collectors.toList());

                    // If at least one non-null type exists, find the first compatible type
                    if (nonNullFieldSchemas.size() >= 1) {
                        for (final Schema nonNullFieldSchema : nonNullFieldSchemas) {
                            final Object avroObject = convertToAvroObject(rawValue, nonNullFieldSchema, fieldName);
                            final DataType desiredDataType = AvroTypeUtil.determineDataType(nonNullFieldSchema);
                            if (DataTypeUtils.isCompatibleDataType(avroObject, desiredDataType)) {
                                return avroObject;
                            }
                        }

                        throw new IllegalTypeConversionException("Cannot convert value " + rawValue + " of type " + rawValue.getClass()
                                + " because no compatible types exist in the UNION");
                    }
                }
                return null;
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
                return new GenericData.EnumSymbol(fieldSchema, rawValue);
            case STRING:
                return DataTypeUtils.toString(rawValue, RecordFieldType.DATE.getDefaultFormat(), RecordFieldType.TIME.getDefaultFormat(), RecordFieldType.TIMESTAMP.getDefaultFormat());
        }

        return rawValue;
    }

    public static Map<String, Object> convertAvroRecordToMap(final GenericRecord avroRecord, final RecordSchema recordSchema) {
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

    private static Object normalizeValue(final Object value, final Schema avroSchema) {
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
                final GenericData.Array<?> array = (GenericData.Array<?>) value;
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
