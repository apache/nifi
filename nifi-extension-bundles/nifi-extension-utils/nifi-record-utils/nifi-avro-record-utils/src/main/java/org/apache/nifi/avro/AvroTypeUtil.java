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

import java.io.InputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.avro.Conversions;
import org.apache.avro.JsonProperties;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.serialization.record.StandardSchemaIdentifier;
import org.apache.nifi.serialization.record.field.FieldConverter;
import org.apache.nifi.serialization.record.field.StandardFieldConverterRegistry;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.DecimalDataType;
import org.apache.nifi.serialization.record.type.EnumDataType;
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
    private static final String LOGICAL_TYPE_UUID = "uuid";

    private static final long ONE_THOUSAND_MILLISECONDS = 1000;

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
            avroFields.add(buildAvroField(recordField, ""));
        }

        final Schema avroSchema = Schema.createRecord("nifiRecord", null, "org.apache.nifi", false, avroFields);
        return avroSchema;
    }

    private static Field buildAvroField(final RecordField recordField, final String fieldNamePrefix) {
        final Schema schema = buildAvroSchema(recordField.getDataType(), recordField.getFieldName(), fieldNamePrefix, recordField.isNullable());

        final Field field;
        final String recordFieldName = recordField.getFieldName();
        if (isValidAvroFieldName(recordFieldName)) {
            final Object avroDefaultValue = convertToAvroObject(recordField.getDefaultValue(), schema);
            field = new Field(recordField.getFieldName(), schema, null, avroDefaultValue);
        } else {
            final String validName = createValidAvroFieldName(recordField.getFieldName());
            final Object avroDefaultValue = convertToAvroObject(recordField.getDefaultValue(), schema);
            field = new Field(validName, schema, null, avroDefaultValue);
            field.addAlias(recordField.getFieldName());
        }

        for (final String alias : recordField.getAliases()) {
            field.addAlias(alias);
        }

        return field;
    }

    private static boolean isValidAvroFieldName(final String fieldName) {
        // Avro field names must match the following criteria:
        // 1. Must be non-empty
        // 2. Must begin with a letter or an underscore
        // 3. Must consist only of letters, underscores, and numbers.
        if (fieldName.isEmpty()) {
            return false;
        }

        final char firstChar = fieldName.charAt(0);
        if (firstChar != '_' && !Character.isLetter(firstChar)) {
            return false;
        }

        for (int i = 1; i < fieldName.length(); i++) {
            final char c = fieldName.charAt(i);
            if (c != '_' && !Character.isLetterOrDigit(c)) {
                return false;
            }
        }

        return true;
    }

    private static String createValidAvroFieldName(final String fieldName) {
        if (fieldName.isEmpty()) {
            return "UNNAMED_FIELD";
        }

        final StringBuilder sb = new StringBuilder();

        final char firstChar = fieldName.charAt(0);
        if (firstChar == '_' || Character.isLetter(firstChar)) {
            sb.append(firstChar);
        } else {
            sb.append("_");
        }

        for (int i = 1; i < fieldName.length(); i++) {
            final char c = fieldName.charAt(i);
            if (c == '_' || Character.isLetterOrDigit(c)) {
                sb.append(c);
            } else {
                sb.append("_");
            }
        }

        return sb.toString();
    }

    private static Schema buildAvroSchema(final DataType dataType, final String fieldName, String fieldNamePrefix, final boolean nullable) {
       Schema schema = null;

        switch (dataType.getFieldType()) {
            case ARRAY:
                final ArrayDataType arrayDataType = (ArrayDataType) dataType;
                final DataType elementDataType = arrayDataType.getElementType();
                if (RecordFieldType.BYTE.equals(elementDataType.getFieldType())) {
                    schema = Schema.create(Type.BYTES);
                } else {
                    final Schema elementType = buildAvroSchema(elementDataType, fieldName, fieldNamePrefix, false);
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

                // We need to keep track of which types have been added to the union, because if we have
                // two elements in the UNION with the same type, it will fail - even if the logical type is
                // different. So if we have an int and a logical type date (which also has a 'concrete type' of int)
                // then an Exception will be thrown when we try to create the union. To avoid this, we just keep track
                // of the Types and avoid adding it in such a case.
                final List<Schema> unionTypes = new ArrayList<>(options.size());
                final Set<Type> typesAdded = new HashSet<>();

                int optionCounter = 1;
                for (final DataType option : options) {
                    final Schema optionSchema = buildAvroSchema(option, fieldName, fieldNamePrefix, false);
                    if (!typesAdded.contains(optionSchema.getType())) {
                        unionTypes.add(optionSchema);
                        typesAdded.add(optionSchema.getType());
                    } else if (Type.RECORD.equals(optionSchema.getType()) && !unionTypes.contains(optionSchema)) {
                        final Schema indexedOptionSchema = buildAvroSchema(option, fieldName + ++optionCounter, fieldNamePrefix, false);
                        unionTypes.add(indexedOptionSchema);
                    }
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
            case DECIMAL:
                final DecimalDataType decimalDataType = (DecimalDataType) dataType;
                schema = Schema.create(Type.BYTES);
                LogicalTypes.decimal(decimalDataType.getPrecision(), decimalDataType.getScale()).addToSchema(schema);
                break;
            case MAP:
                schema = Schema.createMap(buildAvroSchema(((MapDataType) dataType).getValueType(), fieldName, fieldNamePrefix, false));
                break;
            case RECORD:
                final RecordDataType recordDataType = (RecordDataType) dataType;
                final RecordSchema childSchema = recordDataType.getChildSchema();

                final List<Field> childFields = new ArrayList<>(childSchema.getFieldCount());
                for (final RecordField field : childSchema.getFields()) {
                    String childFieldNamePrefix = StringUtils.isBlank(fieldNamePrefix) ? fieldName + "_" : fieldNamePrefix + fieldName + "_";
                    childFields.add(buildAvroField(field, childFieldNamePrefix));
                }

                schema = Schema.createRecord(fieldNamePrefix + fieldName + "Type", null, "org.apache.nifi", false, childFields);
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
            case UUID:
                schema = Schema.create(Type.STRING);
                LogicalTypes.uuid().addToSchema(schema);
                break;
            case ENUM:
                final EnumDataType enumType = (EnumDataType) dataType;
                schema = Schema.createEnum(fieldName, "", "org.apache.nifi", enumType.getEnums());
                break;
        }

        if (nullable) {
            return nullable(schema);
        } else {
            return schema;
        }
    }

    private static Schema nullable(final Schema schema) {
        if (schema.getType() == Type.UNION) {
            final List<Schema> unionTypes = new ArrayList<>(schema.getTypes());
            final Schema nullSchema = Schema.create(Type.NULL);
            if (unionTypes.contains(nullSchema)) {
                return schema;
            }

            unionTypes.add(nullSchema);
            return Schema.createUnion(unionTypes);
        }

        return Schema.createUnion(schema, Schema.create(Type.NULL));
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
                    final LogicalTypes.Decimal decimal = (LogicalTypes.Decimal) logicalType;
                    return RecordFieldType.DECIMAL.getDecimalDataType(decimal.getPrecision(), decimal.getScale());
                case LOGICAL_TYPE_UUID:
                    return RecordFieldType.UUID.getDataType();
            }
        }

        switch (avroType) {
            case ARRAY:
                final DataType elementType = determineDataType(avroSchema.getElementType(), knownRecordTypes);
                final boolean elementsNullable = isNullable(avroSchema.getElementType());
                return RecordFieldType.ARRAY.getArrayDataType(elementType, elementsNullable);
            case BYTES:
            case FIXED:
                return RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType());
            case BOOLEAN:
                return RecordFieldType.BOOLEAN.getDataType();
            case DOUBLE:
                return RecordFieldType.DOUBLE.getDataType();
            case ENUM:
                return RecordFieldType.ENUM.getEnumDataType(avroSchema.getEnumSymbols());
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
                    SimpleRecordSchema recordSchema = new SimpleRecordSchema(SchemaIdentifier.EMPTY);
                    recordSchema.setSchemaName(avroSchema.getName());
                    recordSchema.setSchemaNamespace(avroSchema.getNamespace());
                    DataType recordSchemaType = RecordFieldType.RECORD.getRecordDataType(recordSchema);
                    knownRecordTypes.put(schemaFullName, recordSchemaType);

                    final List<Field> avroFields = avroSchema.getFields();
                    final List<RecordField> recordFields = new ArrayList<>(avroFields.size());

                    for (final Field field : avroFields) {
                        final String fieldName = field.name();
                        final Schema fieldSchema = field.schema();
                        final DataType fieldType = determineDataType(fieldSchema, knownRecordTypes);
                        final boolean nullable = isNullable(fieldSchema);
                        addFieldToList(recordFields, field, fieldName, fieldSchema, fieldType, nullable);
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
                final boolean valuesNullable = isNullable(valueSchema);
                return RecordFieldType.MAP.getMapDataType(valueType, valuesNullable);
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

    private static List<Schema> getNonNullSubSchemas(final Schema avroSchema) {
        final List<Schema> unionFieldSchemas = avroSchema.getTypes();
        if (unionFieldSchemas == null) {
            return Collections.emptyList();
        }

        final List<Schema> nonNullTypes = new ArrayList<>(unionFieldSchemas.size());
        for (final Schema fieldSchema : unionFieldSchemas) {
            if (fieldSchema.getType() != Type.NULL) {
                nonNullTypes.add(fieldSchema);
            }
        }

        return nonNullTypes;
    }

    public static RecordSchema createSchema(final Schema avroSchema) {
        return createSchema(avroSchema, true);
    }

    public static RecordSchema createSchema(final Schema avroSchema, final boolean includeText) {
        if (avroSchema == null) {
            throw new IllegalArgumentException("Avro Schema cannot be null");
        }

        SchemaIdentifier identifier = new StandardSchemaIdentifier.Builder().name(avroSchema.getName()).build();
        return createSchema(avroSchema, includeText ? avroSchema.toString() : null, identifier);
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

        final String schemaFullName = avroSchema.getNamespace() + "." + avroSchema.getName();
        final SimpleRecordSchema recordSchema = schemaText == null ? new SimpleRecordSchema(schemaId) : new SimpleRecordSchema(schemaText, AVRO_SCHEMA_FORMAT, schemaId);
        recordSchema.setSchemaName(avroSchema.getName());
        recordSchema.setSchemaNamespace(avroSchema.getNamespace());
        final DataType recordSchemaType = RecordFieldType.RECORD.getRecordDataType(recordSchema);
        final Map<String, DataType> knownRecords = new HashMap<>();
        knownRecords.put(schemaFullName, recordSchemaType);

        final List<RecordField> recordFields = new ArrayList<>(avroSchema.getFields().size());
        for (final Field field : avroSchema.getFields()) {
            final String fieldName = field.name();
            final Schema fieldSchema = field.schema();
            final DataType dataType = determineDataType(fieldSchema, knownRecords);
            final boolean nullable = isNullable(fieldSchema);
            addFieldToList(recordFields, field, fieldName, fieldSchema, dataType, nullable);
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

    /**
     * Method that attempts to map a record field into a provided schema
     * @param avroSchema - Schema to map into
     * @param recordField - The field of the record to be mapped
     * @return Pair with the LHS being the field name and RHS being the mapped field from the schema
     */
    protected static Pair<String, Field> lookupField(final Schema avroSchema, final RecordField recordField) {
        String fieldName = recordField.getFieldName();

        // Attempt to locate the field as is in a true 1:1 mapping with the same name
        Field field = avroSchema.getField(fieldName);
        if (field == null) {
            // No straight mapping was found, so check the aliases to see if it can be mapped
            for (final String alias : recordField.getAliases()) {
                field = avroSchema.getField(alias);
                if (field != null) {
                    fieldName = alias;
                    break;
                }
            }
        }

        if (field == null) {
            for (final Field childField : avroSchema.getFields()) {
                final Set<String> aliases = childField.aliases();
                if (aliases.isEmpty()) {
                    continue;
                }

                if (aliases.contains(fieldName)) {
                    field = childField;
                    break;
                }

                for (final String alias : recordField.getAliases()) {
                    if (aliases.contains(alias)) {
                        field = childField;
                        fieldName = alias;
                        break;
                    }
                }
            }
        }

        return new ImmutablePair<>(fieldName, field);
    }

    public static GenericRecord createAvroRecord(final Record record, final Schema avroSchema) {
        return createAvroRecord(record, avroSchema, StandardCharsets.UTF_8);
    }

    public static GenericRecord createAvroRecord(final Record record, final Schema avroSchema, final Charset charset) {
        final GenericRecord rec = new GenericData.Record(avroSchema);
        final RecordSchema recordSchema = record.getSchema();

        final Map<String, Object> recordValues = record.toMap();
        for (final Map.Entry<String, Object> entry : recordValues.entrySet()) {
            final Object rawValue = entry.getValue();
            if (rawValue == null) {
                continue;
            }

            final String rawFieldName = entry.getKey();
            final Optional<RecordField> optionalRecordField = recordSchema.getField(rawFieldName);
            if (!optionalRecordField.isPresent()) {
                continue;
            }

            final RecordField recordField = optionalRecordField.get();

            final Field field;
            final Field avroField = avroSchema.getField(rawFieldName);
            if (avroField == null) {
                final Pair<String, Field> fieldPair = lookupField(avroSchema, recordField);
                field = fieldPair.getRight();

                if (field == null) {
                    continue;
                }
            } else {
                field = avroField;
            }

            final String fieldName = field.name();
            final Object converted = convertToAvroObject(rawValue, field.schema(), fieldName, charset);
            rec.put(fieldName, converted);
        }

        // see if the Avro schema has any fields that aren't in the RecordSchema, and if those fields have a default
        // value then we want to populate it in the GenericRecord being produced
        for (final Field field : avroSchema.getFields()) {
            final Object defaultValue = field.defaultVal();
            if (defaultValue == null || defaultValue == JsonProperties.NULL_VALUE) {
                continue;
            }

            if (rec.get(field.name()) == null) {
                // The default value may not actually be the proper value for Avro. For example, the schema may indicate that we need a long but provide a default value of 0.
                // To address this, we need to ensure that the value that we set is correct based on the Avro schema, so we need to call convertToAvroObject even on the default value.
                final Object normalized = convertToAvroObject(defaultValue, field.schema());
                rec.put(field.name(), normalized);
            }
        }

        return rec;
    }

    /**
     * Convert a raw value to an Avro object to serialize in Avro type system, using the provided character set when necessary.
     * The counter-part method which reads an Avro object back to a raw value is {@link #normalizeValue(Object, Schema, String)}.
     */
    public static Object convertToAvroObject(final Object rawValue, final Schema fieldSchema) {
        return convertToAvroObject(rawValue, fieldSchema, StandardCharsets.UTF_8);
    }

    /**
     * Convert a raw value to an Avro object to serialize in Avro type system, using the provided character set when necessary.
     * The counter-part method which reads an Avro object back to a raw value is {@link #normalizeValue(Object, Schema, String)}.
     */
    public static Object convertToAvroObject(final Object rawValue, final Schema fieldSchema, final Charset charset) {
        return convertToAvroObject(rawValue, fieldSchema, fieldSchema.getName(), charset);
    }

    /**
     * Adds fields to <tt>recordFields</tt> list.
     * @param recordFields - record fields are added to this list.
     * @param field - the field
     * @param fieldName - field name
     * @param fieldSchema - field schema
     * @param dataType -  data type
     * @param nullable - is nullable?
     */
    private static void addFieldToList(final List<RecordField> recordFields, final Field field, final String fieldName,
            final Schema fieldSchema, final DataType dataType, final boolean nullable) {
        if (field.defaultVal() == JsonProperties.NULL_VALUE) {
            recordFields.add(new RecordField(fieldName, dataType, field.aliases(), nullable));
        } else {
            Object defaultValue = field.defaultVal();
            if (defaultValue != null && fieldSchema.getType() == Schema.Type.ARRAY && !DataTypeUtils.isArrayTypeCompatible(defaultValue, ((ArrayDataType) dataType).getElementType())) {
                defaultValue = defaultValue instanceof List ? ((List<?>) defaultValue).toArray() : new Object[0];
            }
            recordFields.add(new RecordField(fieldName, dataType, defaultValue, field.aliases(), nullable));
        }
    }

    private static Long getLongFromTimestamp(final Object rawValue, final Schema fieldSchema, final String fieldName) {
        final String format = determineDataType(fieldSchema).getFormat();
        final FieldConverter<Object, Timestamp> converter = StandardFieldConverterRegistry.getRegistry().getFieldConverter(Timestamp.class);
        final Timestamp timestamp = converter.convertField(rawValue, Optional.ofNullable(format), fieldName);
        return timestamp.getTime();
    }

    @SuppressWarnings("unchecked")
    private static Object convertToAvroObject(final Object rawValue, final Schema fieldSchema, final String fieldName, final Charset charset) {
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
                    final String format = determineDataType(fieldSchema).getFormat();
                    final FieldConverter<Object, LocalDate> fieldConverter = StandardFieldConverterRegistry.getRegistry().getFieldConverter(LocalDate.class);
                    final LocalDate localDate = fieldConverter.convertField(rawValue, Optional.ofNullable(format), fieldName);
                    return (int) localDate.toEpochDay();
                } else if (LOGICAL_TYPE_TIME_MILLIS.equals(logicalType.getName())) {
                    final String format = determineDataType(fieldSchema).getFormat();
                    return getLogicalTimeMillis(rawValue, format, fieldName);
                }

                return DataTypeUtils.toInteger(rawValue, fieldName);
            }
            case LONG: {
                final LogicalType logicalType = fieldSchema.getLogicalType();
                if (logicalType == null) {
                    return DataTypeUtils.toLong(rawValue, fieldName);
                }

                if (LOGICAL_TYPE_TIME_MICROS.equals(logicalType.getName())) {
                    final long epochMilli = getLongFromTimestamp(rawValue, fieldSchema, fieldName);
                    final ZonedDateTime zonedDateTime = Instant.ofEpochMilli(epochMilli).atZone(ZoneId.systemDefault());
                    final ZonedDateTime midnight = zonedDateTime.truncatedTo(ChronoUnit.DAYS);
                    final Duration duration = Duration.between(midnight, zonedDateTime);
                    return duration.toMillis() * ONE_THOUSAND_MILLISECONDS;
                } else if (LOGICAL_TYPE_TIMESTAMP_MILLIS.equals(logicalType.getName())) {
                    return getLongFromTimestamp(rawValue, fieldSchema, fieldName);
                } else if (LOGICAL_TYPE_TIMESTAMP_MICROS.equals(logicalType.getName())) {
                    return getLongFromTimestamp(rawValue, fieldSchema, fieldName) * ONE_THOUSAND_MILLISECONDS;
                }

                return DataTypeUtils.toLong(rawValue, fieldName);
            }
            case BYTES:
            case FIXED:
                final LogicalType logicalType = fieldSchema.getLogicalType();
                if (logicalType != null && LOGICAL_TYPE_DECIMAL.equals(logicalType.getName())) {
                    final LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) logicalType;
                    final BigDecimal rawDecimal;
                    if (rawValue instanceof BigDecimal) {
                        rawDecimal = (BigDecimal) rawValue;

                    } else if (rawValue instanceof Double) {
                        rawDecimal = BigDecimal.valueOf((Double) rawValue);

                    } else if (rawValue instanceof String) {
                        rawDecimal = new BigDecimal((String) rawValue);

                    } else if (rawValue instanceof Integer) {
                        rawDecimal = new BigDecimal((Integer) rawValue);

                    } else if (rawValue instanceof Long) {
                        rawDecimal = new BigDecimal((Long) rawValue);

                    } else {
                        throw new IllegalTypeConversionException("Cannot convert value " + rawValue + " of type " + rawValue.getClass() + " to a logical decimal");
                    }
                    // If the desired scale is different than this value's coerce scale.
                    final int desiredScale = decimalType.getScale();
                    final BigDecimal decimal = rawDecimal.scale() == desiredScale
                        ? rawDecimal : rawDecimal.setScale(desiredScale, RoundingMode.HALF_UP);
                    return fieldSchema.getType() == Type.BYTES
                        ? new Conversions.DecimalConversion().toBytes(decimal, fieldSchema, logicalType)
                        : new Conversions.DecimalConversion().toFixed(decimal, fieldSchema, logicalType);
                }
                if (rawValue instanceof byte[]) {
                    final byte[] bytes = (byte[]) rawValue;
                    if (fieldSchema.getType() == Type.FIXED) {
                        final int expectedSize = fieldSchema.getFixedSize();
                        if (bytes.length != expectedSize) {
                            throw new IllegalTypeConversionException("Cannot convert byte[] of length " + bytes.length +
                                " to FIXED(" + expectedSize + ") for field '" + fieldName + "'");
                        }
                        return new GenericData.Fixed(fieldSchema, bytes);
                    }
                    return ByteBuffer.wrap(bytes);
                }
                if (rawValue instanceof String) {
                    final byte[] bytes = ((String) rawValue).getBytes(charset);
                    if (fieldSchema.getType() == Type.FIXED) {
                        final int expectedSize = fieldSchema.getFixedSize();
                        if (bytes.length != expectedSize) {
                            throw new IllegalTypeConversionException("Cannot convert String bytes of length " + bytes.length +
                                " to FIXED(" + expectedSize + ") for field '" + fieldName + "'");
                        }
                        return new GenericData.Fixed(fieldSchema, bytes);
                    }
                    return ByteBuffer.wrap(bytes);
                }
                if (rawValue instanceof Object[]) {
                    final Object[] rawObjects = (Object[]) rawValue;
                    final byte[] bytes = new byte[rawObjects.length];
                    for (int elementIndex = 0; elementIndex < rawObjects.length; elementIndex++) {
                        final Object o = rawObjects[elementIndex];
                        if (!(o instanceof Byte)) {
                            throw new IllegalTypeConversionException("Cannot convert non-Byte element in Object[] to binary for field '" + fieldName + "'");
                        }
                        bytes[elementIndex] = (Byte) o;
                    }

                    if (fieldSchema.getType() == Type.FIXED) {
                        final int expectedSize = fieldSchema.getFixedSize();
                        if (bytes.length != expectedSize) {
                            throw new IllegalTypeConversionException("Cannot convert Object[] of length " + bytes.length +
                                " to FIXED(" + expectedSize + ") for field '" + fieldName + "'");
                        }
                        return new GenericData.Fixed(fieldSchema, bytes);
                    }
                    return ByteBuffer.wrap(bytes);
                }
                try {
                    if (rawValue instanceof Blob blob) {
                        final InputStream binaryStream = blob.getBinaryStream();
                        final byte[] bytes = binaryStream.readAllBytes();
                        if (fieldSchema.getType() == Type.FIXED) {
                            final int expectedSize = fieldSchema.getFixedSize();
                            if (bytes.length != expectedSize) {
                                throw new IllegalTypeConversionException("Cannot convert Blob of length " + bytes.length +
                                    " to FIXED(" + expectedSize + ") for field '" + fieldName + "'");
                            }
                            return new GenericData.Fixed(fieldSchema, bytes);
                        }
                        return ByteBuffer.wrap(bytes);
                    } else {
                        throw new IllegalTypeConversionException("Cannot convert value " + rawValue + " of type " + rawValue.getClass() + " to a ByteBuffer");
                    }
                } catch (IllegalTypeConversionException itce) {
                    throw itce;
                } catch (Exception e) {
                    throw new IllegalTypeConversionException("Cannot convert value " + rawValue + " of type " + rawValue.getClass() + " to a ByteBuffer", e);
                }
            case MAP:
                if (rawValue instanceof Record) {
                    final Record recordValue = (Record) rawValue;
                    final Map<String, Object> map = new HashMap<>();
                    for (final RecordField recordField : recordValue.getSchema().getFields()) {
                        final Object v = recordValue.getValue(recordField);
                        if (v != null) {
                            map.put(recordField.getFieldName(), convertToAvroObject(v, fieldSchema.getValueType(), fieldName + "[" + recordField.getFieldName() + "]", charset));
                        }
                    }

                    return map;
                } else if (rawValue instanceof Map) {
                    final Map<String, Object> objectMap = (Map<String, Object>) rawValue;
                    final Map<String, Object> map = new HashMap<>(objectMap.size());
                    for (final String s : objectMap.keySet()) {
                        final Object converted = convertToAvroObject(objectMap.get(s), fieldSchema.getValueType(), fieldName + "[" + s + "]", charset);
                        map.put(s, converted);
                    }
                    return map;
                } else {
                    throw new IllegalTypeConversionException("Cannot convert value " + rawValue + " of type " + rawValue.getClass() + " to a Map");
                }
            case RECORD:
                final GenericData.Record avroRecord = new GenericData.Record(fieldSchema);

                final Set<Map.Entry<String, Object>> entries;
                if (rawValue instanceof Map) {
                    final Map<String, Object> map = (Map<String, Object>) rawValue;
                    entries = map.entrySet();
                } else if (rawValue instanceof Record) {
                    entries = new HashSet<>();
                    final Record record = (Record) rawValue;
                    record.getSchema().getFields().forEach(field -> entries.add(new AbstractMap.SimpleEntry<>(field.getFieldName(), record.getValue(field))));
                } else {
                    throw new IllegalTypeConversionException("Cannot convert value " + rawValue + " of type " + rawValue.getClass() + " to a Record");
                }
                for (final Map.Entry<String, Object> e : entries) {
                    final Object recordFieldValue = e.getValue();
                    final String recordFieldName = e.getKey();

                    final Field field = fieldSchema.getField(recordFieldName);
                    if (field == null) {
                        continue;
                    }

                    final Object converted = convertToAvroObject(recordFieldValue, field.schema(), fieldName + "/" + recordFieldName, charset);
                    avroRecord.put(recordFieldName, converted);
                }
                return avroRecord;
            case UNION:
                return convertUnionFieldValue(rawValue, fieldSchema, schema -> convertToAvroObject(rawValue, schema, fieldName, charset), fieldName);
            case ARRAY:
                final Object[] objectArray;
                if (rawValue instanceof List) {
                    objectArray = ((List) rawValue).toArray();
                } else if (rawValue instanceof Object[]) {
                    objectArray = (Object[]) rawValue;
                } else {
                    throw new IllegalTypeConversionException("Cannot convert value " + rawValue + " of type " + rawValue.getClass() + " to an Array");
                }
                final List<Object> list = new ArrayList<>(objectArray.length);
                int i = 0;
                for (final Object o : objectArray) {
                    final Object converted = convertToAvroObject(o, fieldSchema.getElementType(), fieldName + "[" + i + "]", charset);
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
                List<String> enums = fieldSchema.getEnumSymbols();
                if (enums != null && enums.contains(rawValue)) {
                    return new GenericData.EnumSymbol(fieldSchema, rawValue);
                } else {
                    throw new IllegalTypeConversionException(rawValue + " is not a possible value of the ENUM" + enums + ".");
                }
            case STRING:
                if (rawValue instanceof String) {
                    return rawValue;
                }

                return DataTypeUtils.toString(rawValue, (String) null, charset);
        }

        return rawValue;
    }

    public static Map<String, Object> convertAvroRecordToMap(final GenericRecord avroRecord, final RecordSchema recordSchema) {
        return convertAvroRecordToMap(avroRecord, recordSchema, StandardCharsets.UTF_8);
    }

    private static String getMatchingFieldName(final GenericRecord record, final RecordField field) {
        final Schema schema = record.getSchema();
        Field avroField = schema.getField(field.getFieldName());
        if (avroField != null) {
            return field.getFieldName();
        }

        for (final String alias : field.getAliases()) {
            avroField = schema.getField(alias);
            if (avroField != null) {
                return alias;
            }
        }

        return null;
    }

    public static Map<String, Object> convertAvroRecordToMap(final GenericRecord avroRecord, final RecordSchema recordSchema, final Charset charset) {
        final Map<String, Object> values = new HashMap<>(recordSchema.getFieldCount());

        for (final RecordField recordField : recordSchema.getFields()) {
            final String relevantFieldName = getMatchingFieldName(avroRecord, recordField);
            final Object value = (relevantFieldName == null) ? null : avroRecord.get(relevantFieldName);

            final String fieldName = recordField.getFieldName();
            try {
                final Field avroField = avroRecord.getSchema().getField(relevantFieldName);
                if (avroField == null) {
                    values.put(fieldName, null);
                    continue;
                }

                final Schema fieldSchema = avroField.schema();
                final Object rawValue = normalizeValue(value, fieldSchema, fieldName);

                final DataType desiredType = recordField.getDataType();
                final Object coercedValue = DataTypeUtils.convertType(rawValue, desiredType, fieldName, charset);

                values.put(fieldName, coercedValue);
            } catch (Exception ex) {
                logger.debug("fail to convert field {}", fieldName, ex );
                throw ex;
            }
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
    private static Object convertUnionFieldValue(final Object originalValue, final Schema fieldSchema, final Function<Schema, Object> conversion, final String fieldName) {
        boolean foundNonNull = false;

        // It is an extremely common case to have a UNION type because a field can be NULL or some other type. In this situation,
        // we will have two possible types, and one of them will be null. When this happens, we can be much more efficient by simply
        // determining the non-null type and converting to that.
        final List<Schema> schemaTypes = fieldSchema.getTypes();
        if (schemaTypes.size() == 2) {
            final Schema firstSchema = schemaTypes.get(0);
            final Schema secondSchema = schemaTypes.get(1);

            if (firstSchema.getType() == Type.NULL) {
                return conversion.apply(secondSchema);
            }
            if (secondSchema.getType() == Type.NULL) {
                return conversion.apply(firstSchema);
            }
        }

        final Optional<Schema> mostSuitableType = DataTypeUtils.findMostSuitableType(
                originalValue,
                getNonNullSubSchemas(fieldSchema),
                AvroTypeUtil::determineDataType
        );
        if (mostSuitableType.isPresent()) {
            return conversion.apply(mostSuitableType.get());
        }

        for (final Schema subSchema : fieldSchema.getTypes()) {
            if (subSchema.getType() == Type.NULL) {
                continue;
            }

            foundNonNull = true;
            final DataType desiredDataType = determineDataType(subSchema);
            try {
                final Object convertedValue = conversion.apply(subSchema);

                if (isCompatibleDataType(convertedValue, desiredDataType)) {
                    return convertedValue;
                }

                // For logical types those store with different type (e.g. BigDecimal as ByteBuffer), check compatibility using the original rawValue
                if (subSchema.getLogicalType() != null && DataTypeUtils.isCompatibleDataType(originalValue, desiredDataType)) {
                    return convertedValue;
                }
            } catch (Exception e) {
                // If failed with one of possible types, continue with the next available option.
                if (logger.isDebugEnabled()) {
                    logger.debug("Cannot convert value {} to type {}", originalValue, desiredDataType, e);
                }
            }
        }

        if (foundNonNull) {
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
                if (value instanceof Array || value instanceof List || value instanceof ByteBuffer) {
                    return true;
                }
                break;
            case MAP:
                if (value instanceof Map) {
                    return true;
                }
        }

        return DataTypeUtils.isCompatibleDataType(value, dataType);
    }


    /**
     * Convert an Avro object to a normal Java objects for further processing.
     * The counter-part method which convert a raw value to an Avro object is {@link #convertToAvroObject(Object, Schema, String, Charset)}
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
                    return java.sql.Date.valueOf(LocalDate.ofEpochDay((int) value));
                } else if (LOGICAL_TYPE_TIME_MILLIS.equals(logicalName)) {
                    // time-millis logical name means that the value is number of milliseconds since midnight.
                    return new Time((int) value);
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
                    return new Time(TimeUnit.MICROSECONDS.toMillis((long) value));
                } else if (LOGICAL_TYPE_TIMESTAMP_MILLIS.equals(logicalName)) {
                    return new Timestamp((long) value);
                } else if (LOGICAL_TYPE_TIMESTAMP_MICROS.equals(logicalName)) {
                    return new Timestamp(TimeUnit.MICROSECONDS.toMillis((long) value));
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
                final RecordSchema childSchema = createSchema(recordSchema, false);
                return new MapRecord(childSchema, values);
            case BYTES:
                final ByteBuffer bb = (ByteBuffer) value;
                final LogicalType logicalType = avroSchema.getLogicalType();
                if (logicalType != null && LOGICAL_TYPE_DECIMAL.equals(logicalType.getName())) {
                    return new Conversions.DecimalConversion().fromBytes(bb, avroSchema, logicalType);
                }
                return convertByteArray(bb.array());
            case FIXED:
                final GenericFixed fixed = (GenericFixed) value;
                final LogicalType fixedLogicalType = avroSchema.getLogicalType();
                if (fixedLogicalType != null && LOGICAL_TYPE_DECIMAL.equals(fixedLogicalType.getName())) {
                    final ByteBuffer fixedByteBuffer = ByteBuffer.wrap(fixed.bytes());
                    return new Conversions.DecimalConversion().fromBytes(fixedByteBuffer, avroSchema, fixedLogicalType);
                }
                return convertByteArray(fixed.bytes());
            case ENUM:
                return value.toString();
            case NULL:
                return null;
            case STRING:
                return value.toString();
            case ARRAY:
                if (value instanceof List) {
                    final List<?> list = (List<?>) value;
                    final Object[] valueArray = new Object[list.size()];
                    for (int i = 0; i < list.size(); i++) {
                        final Schema elementSchema = avroSchema.getElementType();
                        valueArray[i] = normalizeValue(list.get(i), elementSchema, fieldName + "[" + i + "]");
                    }
                    return valueArray;
                } else {
                    final GenericArray<?> array = (GenericArray<?>) value;
                    final Object[] valueArray = new Object[array.size()];
                    for (int i = 0; i < array.size(); i++) {
                        final Schema elementSchema = avroSchema.getElementType();
                        valueArray[i] = normalizeValue(array.get(i), elementSchema, fieldName + "[" + i + "]");
                    }
                    return valueArray;
                }
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

                return map;
        }

        return value;
    }

    private static int getLogicalTimeMillis(final Object value, final String format, final String fieldName) {
        final FieldConverter<Object, LocalTime> fieldConverter = StandardFieldConverterRegistry.getRegistry().getFieldConverter(LocalTime.class);
        final LocalTime localTime = fieldConverter.convertField(value, Optional.ofNullable(format), fieldName);

        final LocalTime midnightLocalTime = localTime.truncatedTo(ChronoUnit.DAYS);
        final Duration duration = Duration.between(midnightLocalTime, localTime);
        final long millisSinceMidnight = duration.toMillis();
        return (int) millisSinceMidnight;
    }
}
