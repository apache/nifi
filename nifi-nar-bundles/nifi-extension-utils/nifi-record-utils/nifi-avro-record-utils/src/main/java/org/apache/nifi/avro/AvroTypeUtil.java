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
import java.math.BigDecimal;
import java.math.MathContext;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.avro.Conversions;
import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.util.Utf8;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.serialization.record.util.IllegalTypeConversionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroTypeUtil {
    private static final Logger logger = LoggerFactory.getLogger(AvroTypeUtil.class);
    public static final String AVRO_SCHEMA_FORMAT = "avro";

    private static final String LOGICAL_TYPE_DATE = "date";
    private static final String LOGICAL_TYPE_TIME_MILLIS = "time-millis";
    private static final String LOGICAL_TYPE_TIME_MICROS = "time-micros";
    private static final String LOGICAL_TYPE_TIMESTAMP_MILLIS = "timestamp-millis";
    private static final String LOGICAL_TYPE_TIMESTAMP_MICROS = "timestamp-micros";
    private static final String LOGICAL_TYPE_DECIMAL = "decimal";


    public static Schema extractAvroSchema(final RecordSchema recordSchema) {
        if (recordSchema == null) {
            throw new IllegalArgumentException("RecordSchema cannot be null");
        }

        final Optional<String> schemaFormatOption = recordSchema.getSchemaFormat();
        if (!schemaFormatOption.isPresent()) {
            return buildAvroSchema(recordSchema);
        }

        final String schemaFormat = schemaFormatOption.get();
        if (!schemaFormat.equals(AVRO_SCHEMA_FORMAT)) {
            return buildAvroSchema(recordSchema);
        }

        final Optional<String> textOption = recordSchema.getSchemaText();
        if (!textOption.isPresent()) {
            return buildAvroSchema(recordSchema);
        }

        final String text = textOption.get();
        return new Schema.Parser().parse(text);
    }

    private static Schema buildAvroSchema(final RecordSchema recordSchema) {
        final List<Field> avroFields = new ArrayList<>(recordSchema.getFieldCount());
        for (final RecordField recordField : recordSchema.getFields()) {
            avroFields.add(buildAvroField(recordField));
        }

        final Schema avroSchema = Schema.createRecord("nifiRecord", null, "org.apache.nifi", false, avroFields);
        return avroSchema;
    }

    private static Field buildAvroField(final RecordField recordField) {
        final Schema schema = buildAvroSchema(recordField.getDataType(), recordField.getFieldName(), recordField.isNullable());
        final Field field = new Field(recordField.getFieldName(), schema, null, recordField.getDefaultValue());
        for (final String alias : recordField.getAliases()) {
            field.addAlias(alias);
        }

        return field;
    }

    private static Schema buildAvroSchema(final DataType dataType, final String fieldName, final boolean nullable) {
        final Schema schema;

        switch (dataType.getFieldType()) {
            case ARRAY:
                final ArrayDataType arrayDataType = (ArrayDataType) dataType;
                final DataType elementDataType = arrayDataType.getElementType();
                if (RecordFieldType.BYTE.equals(elementDataType.getFieldType())) {
                    schema = Schema.create(Type.BYTES);
                } else {
                    final Schema elementType = buildAvroSchema(elementDataType, fieldName, false);
                    schema = Schema.createArray(elementType);
                }
                break;
            case BIGINT:
                schema = Schema.create(Type.STRING);
                break;
            case BOOLEAN:
                schema = Schema.create(Type.BOOLEAN);
                break;
            case BYTE:
                schema = Schema.create(Type.INT);
                break;
            case CHAR:
                schema = Schema.create(Type.STRING);
                break;
            case CHOICE:
                final ChoiceDataType choiceDataType = (ChoiceDataType) dataType;
                final List<DataType> options = choiceDataType.getPossibleSubTypes();

                final List<Schema> unionTypes = new ArrayList<>(options.size());
                for (final DataType option : options) {
                    unionTypes.add(buildAvroSchema(option, fieldName, false));
                }

                schema = Schema.createUnion(unionTypes);
                break;
            case DATE:
                schema = Schema.create(Type.INT);
                LogicalTypes.date().addToSchema(schema);
                break;
            case DOUBLE:
                schema = Schema.create(Type.DOUBLE);
                break;
            case FLOAT:
                schema = Schema.create(Type.FLOAT);
                break;
            case INT:
                schema = Schema.create(Type.INT);
                break;
            case LONG:
                schema = Schema.create(Type.LONG);
                break;
            case MAP:
                schema = Schema.createMap(buildAvroSchema(((MapDataType) dataType).getValueType(), fieldName, false));
                break;
            case RECORD:
                final RecordDataType recordDataType = (RecordDataType) dataType;
                final RecordSchema childSchema = recordDataType.getChildSchema();

                final List<Field> childFields = new ArrayList<>(childSchema.getFieldCount());
                for (final RecordField field : childSchema.getFields()) {
                    childFields.add(buildAvroField(field));
                }

                schema = Schema.createRecord(fieldName + "Type", null, "org.apache.nifi", false, childFields);
                break;
            case SHORT:
                schema = Schema.create(Type.INT);
                break;
            case STRING:
                schema = Schema.create(Type.STRING);
                break;
            case TIME:
                schema = Schema.create(Type.INT);
                LogicalTypes.timeMillis().addToSchema(schema);
                break;
            case TIMESTAMP:
                schema = Schema.create(Type.LONG);
                LogicalTypes.timestampMillis().addToSchema(schema);
                break;
            default:
                return null;
        }

        if (nullable) {
            return nullable(schema);
        } else {
            return schema;
        }
    }

    private static Schema nullable(final Schema schema) {
        return Schema.createUnion(Schema.create(Type.NULL), schema);
    }

    /**
     * Returns a DataType for the given Avro Schema
     *
     * @param avroSchema the Avro Schema to convert
     * @return a Data Type that corresponds to the given Avro Schema
     */
    public static DataType determineDataType(final Schema avroSchema) {
        return determineDataType(avroSchema, new HashMap<>());
    }

    public static DataType determineDataType(final Schema avroSchema, Map<String, DataType> knownRecordTypes) {

        if (knownRecordTypes == null) {
            throw new IllegalArgumentException("'knownRecordTypes' cannot be null.");
        }

        final Type avroType = avroSchema.getType();

        final LogicalType logicalType = avroSchema.getLogicalType();
        if (logicalType != null) {
            final String logicalTypeName = logicalType.getName();
            switch (logicalTypeName) {
                case LOGICAL_TYPE_DATE:
                    return RecordFieldType.DATE.getDataType();
                case LOGICAL_TYPE_TIME_MILLIS:
                case LOGICAL_TYPE_TIME_MICROS:
                    return RecordFieldType.TIME.getDataType();
                case LOGICAL_TYPE_TIMESTAMP_MILLIS:
                case LOGICAL_TYPE_TIMESTAMP_MICROS:
                    return RecordFieldType.TIMESTAMP.getDataType();
                case LOGICAL_TYPE_DECIMAL:
                    // We convert Decimal to Double.
                    // Alternatively we could convert it to String, but numeric type is generally more preferable by users.
                    return RecordFieldType.DOUBLE.getDataType();
            }
        }

        switch (avroType) {
            case ARRAY:
                return RecordFieldType.ARRAY.getArrayDataType(determineDataType(avroSchema.getElementType(), knownRecordTypes));
            case BYTES:
            case FIXED:
                return RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType());
            case BOOLEAN:
                return RecordFieldType.BOOLEAN.getDataType();
            case DOUBLE:
                return RecordFieldType.DOUBLE.getDataType();
            case ENUM:
            case STRING:
                return RecordFieldType.STRING.getDataType();
            case FLOAT:
                return RecordFieldType.FLOAT.getDataType();
            case INT:
                return RecordFieldType.INT.getDataType();
            case LONG:
                return RecordFieldType.LONG.getDataType();
            case RECORD: {
                String schemaFullName = avroSchema.getNamespace() + "." + avroSchema.getName();

                if (knownRecordTypes.containsKey(schemaFullName)) {
                    return knownRecordTypes.get(schemaFullName);
                } else {
                    SimpleRecordSchema recordSchema = new SimpleRecordSchema(avroSchema.toString(), AVRO_SCHEMA_FORMAT, SchemaIdentifier.EMPTY);
                    DataType recordSchemaType = RecordFieldType.RECORD.getRecordDataType(recordSchema);
                    knownRecordTypes.put(schemaFullName, recordSchemaType);

                    final List<Field> avroFields = avroSchema.getFields();
                    final List<RecordField> recordFields = new ArrayList<>(avroFields.size());

                    for (final Field field : avroFields) {
                        final String fieldName = field.name();
                        final Schema fieldSchema = field.schema();
                        final DataType fieldType = determineDataType(fieldSchema, knownRecordTypes);

                        if (field.defaultVal() == JsonProperties.NULL_VALUE) {
                            recordFields.add(new RecordField(fieldName, fieldType, field.aliases()));
                        } else {
                            recordFields.add(new RecordField(fieldName, fieldType, field.defaultVal(), field.aliases()));
                        }
                    }

                    recordSchema.setFields(recordFields);
                    return recordSchemaType;
                }
            }
            case NULL:
                return RecordFieldType.STRING.getDataType();
            case MAP:
                final Schema valueSchema = avroSchema.getValueType();
                final DataType valueType = determineDataType(valueSchema, knownRecordTypes);
                return RecordFieldType.MAP.getMapDataType(valueType);
            case UNION: {
                final List<Schema> nonNullSubSchemas = getNonNullSubSchemas(avroSchema);

                if (nonNullSubSchemas.size() == 1) {
                    return determineDataType(nonNullSubSchemas.get(0), knownRecordTypes);
                }

                final List<DataType> possibleChildTypes = new ArrayList<>(nonNullSubSchemas.size());
                for (final Schema subSchema : nonNullSubSchemas) {
                    final DataType childDataType = determineDataType(subSchema, knownRecordTypes);
                    possibleChildTypes.add(childDataType);
                }

                return RecordFieldType.CHOICE.getChoiceDataType(possibleChildTypes);
            }
        }

        return null;
    }

    private static List<Schema> getNonNullSubSchemas(Schema avroSchema) {
        List<Schema> unionFieldSchemas = avroSchema.getTypes();
        if (unionFieldSchemas == null) {
            return Collections.emptyList();
        }
        return unionFieldSchemas.stream()
                .filter(s -> s.getType() != Type.NULL)
                .collect(Collectors.toList());
    }

    public static RecordSchema createSchema(final Schema avroSchema) {
        if (avroSchema == null) {
            throw new IllegalArgumentException("Avro Schema cannot be null");
        }

        return createSchema(avroSchema, avroSchema.toString(), SchemaIdentifier.EMPTY);
    }

    /**
     * Converts an Avro Schema to a RecordSchema
     *
     * @param avroSchema the Avro Schema to convert
     * @param schemaText the textual representation of the schema
     * @param schemaId the identifier of the schema
     * @return the Corresponding Record Schema
     */
    public static RecordSchema createSchema(final Schema avroSchema, final String schemaText, final SchemaIdentifier schemaId) {
        if (avroSchema == null) {
            throw new IllegalArgumentException("Avro Schema cannot be null");
        }

        String schemaFullName = avroSchema.getNamespace() + "." + avroSchema.getName();
        SimpleRecordSchema recordSchema = new SimpleRecordSchema(schemaText, AVRO_SCHEMA_FORMAT, schemaId);
        DataType recordSchemaType = RecordFieldType.RECORD.getRecordDataType(recordSchema);
        Map<String, DataType> knownRecords = new HashMap<>();
        knownRecords.put(schemaFullName, recordSchemaType);

        final List<RecordField> recordFields = new ArrayList<>(avroSchema.getFields().size());
        for (final Field field : avroSchema.getFields()) {
            final String fieldName = field.name();
            final Schema fieldSchema = field.schema();
            final DataType dataType = AvroTypeUtil.determineDataType(fieldSchema, knownRecords);
            final boolean nullable = isNullable(fieldSchema);

            if (field.defaultVal() == JsonProperties.NULL_VALUE) {
                recordFields.add(new RecordField(fieldName, dataType, field.aliases(), nullable));
            } else {
                recordFields.add(new RecordField(fieldName, dataType, field.defaultVal(), field.aliases(), nullable));
            }
        }

        recordSchema.setFields(recordFields);
        return recordSchema;
    }

    public static boolean isNullable(final Schema schema) {
        final Type schemaType = schema.getType();
        if (schemaType == Type.UNION) {
            for (final Schema unionSchema : schema.getTypes()) {
                if (isNullable(unionSchema)) {
                    return true;
                }
            }
        }

        return schemaType == Type.NULL;
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

        // see if the Avro schema has any fields that aren't in the RecordSchema, and if those fields have a default
        // value then we want to populate it in the GenericRecord being produced
        for (final Field field : avroSchema.getFields()) {
            final Optional<RecordField> recordField = recordSchema.getField(field.name());
            if (!recordField.isPresent() && rec.get(field.name()) == null && field.defaultVal() != null) {
                rec.put(field.name(), field.defaultVal());
            }
        }

        return rec;
    }

    /**
     * Convert a raw value to an Avro object to serialize in Avro type system.
     * The counter-part method which reads an Avro object back to a raw value is {@link #normalizeValue(Object, Schema)}.
     */
    public static Object convertToAvroObject(final Object rawValue, final Schema fieldSchema) {
        return convertToAvroObject(rawValue, fieldSchema, fieldSchema.getName());
    }

    @SuppressWarnings("unchecked")
    private static Object convertToAvroObject(final Object rawValue, final Schema fieldSchema, final String fieldName) {
        if (rawValue == null) {
            return null;
        }

        switch (fieldSchema.getType()) {
            case INT: {
                final LogicalType logicalType = fieldSchema.getLogicalType();
                if (logicalType == null) {
                    return DataTypeUtils.toInteger(rawValue, fieldName);
                }

                if (LOGICAL_TYPE_DATE.equals(logicalType.getName())) {
                    final long longValue = DataTypeUtils.toLong(rawValue, fieldName);
                    final Date date = new Date(longValue);
                    final Duration duration = Duration.between(new Date(0L).toInstant(), date.toInstant());
                    final long days = duration.toDays();
                    return (int) days;
                } else if (LOGICAL_TYPE_TIME_MILLIS.equals(logicalType.getName())) {
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

                if (LOGICAL_TYPE_TIME_MICROS.equals(logicalType.getName())) {
                    final long longValue = DataTypeUtils.toLong(rawValue, fieldName);
                    final Date date = new Date(longValue);
                    final Duration duration = Duration.between(date.toInstant().truncatedTo(ChronoUnit.DAYS), date.toInstant());
                    return duration.toMillis() * 1000L;
                } else if (LOGICAL_TYPE_TIMESTAMP_MILLIS.equals(logicalType.getName())) {
                    return DataTypeUtils.toLong(rawValue, fieldName);
                } else if (LOGICAL_TYPE_TIMESTAMP_MICROS.equals(logicalType.getName())) {
                    return DataTypeUtils.toLong(rawValue, fieldName) * 1000L;
                }

                return DataTypeUtils.toLong(rawValue, fieldName);
            }
            case BYTES:
            case FIXED:
                final LogicalType logicalType = fieldSchema.getLogicalType();
                if (logicalType != null && LOGICAL_TYPE_DECIMAL.equals(logicalType.getName())) {
                    final LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) logicalType;
                    final BigDecimal decimal;
                    if (rawValue instanceof BigDecimal) {
                        final BigDecimal rawDecimal = (BigDecimal) rawValue;
                        final int desiredScale = decimalType.getScale();
                        // If the desired scale is different than this value's coerce scale.
                        decimal = rawDecimal.scale() == desiredScale ? rawDecimal : rawDecimal.setScale(desiredScale, BigDecimal.ROUND_HALF_UP);
                    } else if (rawValue instanceof Double) {
                        // Scale is adjusted based on precision. If double was 123.456 and precision is 5, then decimal would be 123.46.
                        decimal = new BigDecimal((Double) rawValue, new MathContext(decimalType.getPrecision()));
                    } else {
                        throw new IllegalTypeConversionException("Cannot convert value " + rawValue + " of type " + rawValue.getClass() + " to a logical decimal");
                    }
                    return new Conversions.DecimalConversion().toBytes(decimal, fieldSchema, logicalType);
                }
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
                } else if (rawValue instanceof Map) {
                    final Map<String, Object> objectMap = (Map<String, Object>) rawValue;
                    final Map<String, Object> map = new HashMap<>(objectMap.size());
                    for (final String s : objectMap.keySet()) {
                        final Object converted = convertToAvroObject(objectMap.get(s), fieldSchema.getValueType(), fieldName + "[" + s + "]");
                        map.put(s, converted);
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

                    final Object converted = convertToAvroObject(recordFieldValue, field.schema(), fieldName + "/" + recordFieldName);
                    avroRecord.put(recordFieldName, converted);
                }
                return avroRecord;
            case UNION:
                return convertUnionFieldValue(rawValue, fieldSchema, schema -> convertToAvroObject(rawValue, schema, fieldName), fieldName);
            case ARRAY:
                final Object[] objectArray = (Object[]) rawValue;
                final List<Object> list = new ArrayList<>(objectArray.length);
                int i = 0;
                for (final Object o : objectArray) {
                    final Object converted = convertToAvroObject(o, fieldSchema.getElementType(), fieldName + "[" + i + "]");
                    list.add(converted);
                    i++;
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
                return DataTypeUtils.toString(rawValue, (String) null);
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
            final Object rawValue = normalizeValue(value, fieldSchema, fieldName);

            final DataType desiredType = recordField.getDataType();
            final Object coercedValue = DataTypeUtils.convertType(rawValue, desiredType, fieldName);

            values.put(fieldName, coercedValue);
        }

        return values;
    }

    /**
     * Convert value of a nullable union field.
     * @param originalValue original value
     * @param fieldSchema the union field schema
     * @param conversion the conversion function which takes a non-null field schema within the union field and returns a converted value
     * @return a converted value
     */
    private static Object convertUnionFieldValue(Object originalValue, Schema fieldSchema, Function<Schema, Object> conversion, final String fieldName) {
        // Ignore null types in union
        final List<Schema> nonNullFieldSchemas = getNonNullSubSchemas(fieldSchema);

        // If at least one non-null type exists, find the first compatible type
        if (nonNullFieldSchemas.size() >= 1) {
            for (final Schema nonNullFieldSchema : nonNullFieldSchemas) {
                final DataType desiredDataType = AvroTypeUtil.determineDataType(nonNullFieldSchema);
                try {
                    final Object convertedValue = conversion.apply(nonNullFieldSchema);

                    if (isCompatibleDataType(convertedValue, desiredDataType)) {
                        return convertedValue;
                    }

                    // For logical types those store with different type (e.g. BigDecimal as ByteBuffer), check compatibility using the original rawValue
                    if (nonNullFieldSchema.getLogicalType() != null && DataTypeUtils.isCompatibleDataType(originalValue, desiredDataType)) {
                        return convertedValue;
                    }
                } catch (Exception e) {
                    // If failed with one of possible types, continue with the next available option.
                    if (logger.isDebugEnabled()) {
                        logger.debug("Cannot convert value {} to type {}", originalValue, desiredDataType, e);
                    }
                }
            }

            throw new IllegalTypeConversionException("Cannot convert value " + originalValue + " of type " + originalValue.getClass()
                + " because no compatible types exist in the UNION for field " + fieldName);
        }
        return null;
    }

    private static boolean isCompatibleDataType(final Object value, final DataType dataType) {
        if (value == null) {
            return false;
        }

        switch (dataType.getFieldType()) {
            case RECORD:
                if (value instanceof GenericRecord || value instanceof SpecificRecord) {
                    return true;
                }
                break;
            case STRING:
                if (value instanceof Utf8) {
                    return true;
                }
                break;
            case ARRAY:
                if (value instanceof Array || value instanceof List) {
                    return true;
                }
                break;
        }

        return DataTypeUtils.isCompatibleDataType(value, dataType);
    }


    /**
     * Convert an Avro object to a normal Java objects for further processing.
     * The counter-part method which convert a raw value to an Avro object is {@link #convertToAvroObject(Object, Schema, String)}
     */
    private static Object normalizeValue(final Object value, final Schema avroSchema, final String fieldName) {
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
                if (LOGICAL_TYPE_DATE.equals(logicalName)) {
                    // date logical name means that the value is number of days since Jan 1, 1970
                    return new java.sql.Date(TimeUnit.DAYS.toMillis((int) value));
                } else if (LOGICAL_TYPE_TIME_MILLIS.equals(logicalName)) {
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
                if (LOGICAL_TYPE_TIME_MICROS.equals(logicalName)) {
                    return new java.sql.Time(TimeUnit.MICROSECONDS.toMillis((long) value));
                } else if (LOGICAL_TYPE_TIMESTAMP_MILLIS.equals(logicalName)) {
                    return new java.sql.Timestamp((long) value);
                } else if (LOGICAL_TYPE_TIMESTAMP_MICROS.equals(logicalName)) {
                    return new java.sql.Timestamp(TimeUnit.MICROSECONDS.toMillis((long) value));
                }
                break;
            }
            case UNION:
                if (value instanceof GenericData.Record) {
                    final GenericData.Record avroRecord = (GenericData.Record) value;
                    return normalizeValue(value, avroRecord.getSchema(), fieldName);
                }
                return convertUnionFieldValue(value, avroSchema, schema -> normalizeValue(value, schema, fieldName), fieldName);
            case RECORD:
                final GenericData.Record record = (GenericData.Record) value;
                final Schema recordSchema = record.getSchema();
                final List<Field> recordFields = recordSchema.getFields();
                final Map<String, Object> values = new HashMap<>(recordFields.size());
                for (final Field field : recordFields) {
                    final Object avroFieldValue = record.get(field.name());
                    final Object fieldValue = normalizeValue(avroFieldValue, field.schema(), fieldName + "/" + field.name());
                    values.put(field.name(), fieldValue);
                }
                final RecordSchema childSchema = AvroTypeUtil.createSchema(recordSchema);
                return new MapRecord(childSchema, values);
            case BYTES:
                final ByteBuffer bb = (ByteBuffer) value;
                final LogicalType logicalType = avroSchema.getLogicalType();
                if (logicalType != null && LOGICAL_TYPE_DECIMAL.equals(logicalType.getName())) {
                    return new Conversions.DecimalConversion().fromBytes(bb, avroSchema, logicalType);
                }
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
                    valueArray[i] = normalizeValue(array.get(i), elementSchema, fieldName + "[" + i + "]");
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
                    obj = normalizeValue(obj, avroSchema.getValueType(), fieldName + "[" + key + "]");

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
