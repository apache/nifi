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

package org.apache.nifi.serialization.record.util;

import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.field.FieldConverter;
import org.apache.nifi.serialization.record.field.StandardFieldConverterRegistry;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.DecimalDataType;
import org.apache.nifi.serialization.record.type.EnumDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;

public class DataTypeUtils {
    private static final Logger logger = LoggerFactory.getLogger(DataTypeUtils.class);

    // Regexes for parsing Floating-Point numbers
    private static final String OptionalSign  = "[\\-\\+]?";
    private static final String Infinity = "(Infinity)";
    private static final String NotANumber = "(NaN)";

    private static final String Base10Digits = "\\d+";
    private static final String Base10Decimal = "\\." + Base10Digits;
    private static final String OptionalBase10Decimal = "(\\.\\d*)?";

    private static final String Base10Exponent = "[eE]" + OptionalSign + Base10Digits;
    private static final String OptionalBase10Exponent = "(" + Base10Exponent + ")?";

    private static final String  doubleRegex =
        OptionalSign +
        "(" +
            Infinity + "|" +
            NotANumber + "|" +
            "(" + Base10Digits + OptionalBase10Decimal + ")" + "|" +
            "(" + Base10Digits + OptionalBase10Decimal + Base10Exponent + ")" + "|" +
            "(" + Base10Decimal + OptionalBase10Exponent + ")" +
        ")";

    private static final String decimalRegex =
        OptionalSign +
            "(" + Base10Digits + OptionalBase10Decimal + ")" + "|" +
            "(" + Base10Digits + OptionalBase10Decimal + Base10Exponent + ")" + "|" +
            "(" + Base10Decimal + OptionalBase10Exponent + ")";

    private static final Pattern FLOATING_POINT_PATTERN = Pattern.compile(doubleRegex);
    private static final Pattern DECIMAL_PATTERN = Pattern.compile(decimalRegex);

    private static final Optional<String> DEFAULT_DATE_FORMAT = Optional.of(RecordFieldType.DATE.getDefaultFormat());
    private static final Optional<String> DEFAULT_TIME_FORMAT = Optional.of(RecordFieldType.TIME.getDefaultFormat());
    private static final Optional<String> DEFAULT_TIMESTAMP_FORMAT = Optional.of(RecordFieldType.TIMESTAMP.getDefaultFormat());

    private static final int FLOAT_SIGNIFICAND_PRECISION = 24; // As specified in IEEE 754 binary32
    private static final int DOUBLE_SIGNIFICAND_PRECISION = 53; // As specified in IEEE 754 binary64

    private static final Long MAX_GUARANTEED_PRECISE_WHOLE_IN_FLOAT = Double.valueOf(Math.pow(2, FLOAT_SIGNIFICAND_PRECISION)).longValue();
    private static final Long MIN_GUARANTEED_PRECISE_WHOLE_IN_FLOAT = -MAX_GUARANTEED_PRECISE_WHOLE_IN_FLOAT;
    private static final Long MAX_GUARANTEED_PRECISE_WHOLE_IN_DOUBLE = Double.valueOf(Math.pow(2, DOUBLE_SIGNIFICAND_PRECISION)).longValue();
    private static final Long MIN_GUARANTEED_PRECISE_WHOLE_IN_DOUBLE = -MAX_GUARANTEED_PRECISE_WHOLE_IN_DOUBLE;

    private static final BigInteger MAX_FLOAT_VALUE_IN_BIGINT = BigInteger.valueOf(MAX_GUARANTEED_PRECISE_WHOLE_IN_FLOAT);
    private static final BigInteger MIN_FLOAT_VALUE_IN_BIGINT = BigInteger.valueOf(MIN_GUARANTEED_PRECISE_WHOLE_IN_FLOAT);
    private static final BigInteger MAX_DOUBLE_VALUE_IN_BIGINT = BigInteger.valueOf(MAX_GUARANTEED_PRECISE_WHOLE_IN_DOUBLE);
    private static final BigInteger MIN_DOUBLE_VALUE_IN_BIGINT = BigInteger.valueOf(MIN_GUARANTEED_PRECISE_WHOLE_IN_DOUBLE);

    private static final double MAX_FLOAT_VALUE_IN_DOUBLE = Float.valueOf(Float.MAX_VALUE).doubleValue();
    private static final double MIN_FLOAT_VALUE_IN_DOUBLE = -MAX_FLOAT_VALUE_IN_DOUBLE;

    private static final Map<RecordFieldType, Predicate<Object>> NUMERIC_VALIDATORS = new EnumMap<>(RecordFieldType.class);

    static {
        NUMERIC_VALIDATORS.put(RecordFieldType.BIGINT, value -> value instanceof BigInteger);
        NUMERIC_VALIDATORS.put(RecordFieldType.LONG, value -> value instanceof Long);
        NUMERIC_VALIDATORS.put(RecordFieldType.INT, value -> value instanceof Integer);
        NUMERIC_VALIDATORS.put(RecordFieldType.BYTE, value -> value instanceof Byte);
        NUMERIC_VALIDATORS.put(RecordFieldType.SHORT, value -> value instanceof Short);
        NUMERIC_VALIDATORS.put(RecordFieldType.DOUBLE, value -> value instanceof Double);
        NUMERIC_VALIDATORS.put(RecordFieldType.FLOAT, value -> value instanceof Float);
        NUMERIC_VALIDATORS.put(RecordFieldType.DECIMAL, value -> value instanceof BigDecimal);
    }

    public static Object convertType(final Object value, final DataType dataType, final String fieldName) {
        return convertType(value, dataType, fieldName, StandardCharsets.UTF_8);
    }

    public static Object convertType(final Object value, final DataType dataType, final String fieldName, final Charset charset) {
        return convertType(value, dataType, DEFAULT_DATE_FORMAT, DEFAULT_TIME_FORMAT, DEFAULT_TIMESTAMP_FORMAT, fieldName, charset);
    }

    private static String getDateFormat(final RecordFieldType fieldType, final Optional<String> dateFormat,
                                            final Optional<String> timeFormat, final Optional<String> timestampFormat) {
        return switch (fieldType) {
            case DATE -> dateFormat.orElse(null);
            case TIME -> timeFormat.orElse(null);
            case TIMESTAMP -> timestampFormat.orElse(null);
            default -> null;
        };
    }

    public static Object convertType(
            final Object value,
            final DataType dataType,
            final Optional<String> dateFormat,
            final Optional<String> timeFormat,
            final Optional<String> timestampFormat,
            final String fieldName
    ) {
        return convertType(value, dataType, dateFormat, timeFormat, timestampFormat, fieldName, StandardCharsets.UTF_8);
    }

    public static Object convertType(
            final Object value,
            final DataType dataType,
            final Optional<String> dateFormat,
            final Optional<String> timeFormat,
            final Optional<String> timestampFormat,
            final String fieldName,
            final Charset charset
    ) {

        if (value == null) {
            return null;
        }

        switch (dataType.getFieldType()) {
            case BIGINT:
                return toBigInt(value, fieldName);
            case BOOLEAN:
                return toBoolean(value, fieldName);
            case BYTE:
                return toByte(value, fieldName);
            case CHAR:
                return toCharacter(value, fieldName);
            case DATE:
                final FieldConverter<Object, LocalDate> localDateConverter = StandardFieldConverterRegistry.getRegistry().getFieldConverter(LocalDate.class);
                final LocalDate localDate = localDateConverter.convertField(value, dateFormat, fieldName);
                return localDate == null ? null : Date.valueOf(localDate);
            case DECIMAL:
                return toBigDecimal(value, fieldName);
            case DOUBLE:
                return toDouble(value, fieldName);
            case FLOAT:
                return toFloat(value, fieldName);
            case INT:
                return toInteger(value, fieldName);
            case LONG:
                return toLong(value, fieldName);
            case SHORT:
                return toShort(value, fieldName);
            case ENUM:
                return toEnum(value, (EnumDataType) dataType, fieldName);
            case STRING:
                final FieldConverter<Object, String> stringConverter = StandardFieldConverterRegistry.getRegistry().getFieldConverter(String.class);
                final String pattern = getDateFormat(dataType.getFieldType(), dateFormat, timeFormat, timestampFormat);
                return stringConverter.convertField(value, Optional.ofNullable(pattern), fieldName);
            case TIME:
                final FieldConverter<Object, Time> timeConverter = StandardFieldConverterRegistry.getRegistry().getFieldConverter(Time.class);
                return timeConverter.convertField(value, timeFormat, fieldName);
            case TIMESTAMP:
                final FieldConverter<Object, Timestamp> timestampConverter = StandardFieldConverterRegistry.getRegistry().getFieldConverter(Timestamp.class);
                return timestampConverter.convertField(value, timestampFormat, fieldName);
            case UUID:
                return toUUID(value);
            case ARRAY:
                return toArray(value, fieldName, ((ArrayDataType) dataType).getElementType(), charset);
            case MAP:
                return toMap(value, fieldName);
            case RECORD:
                final RecordDataType recordType = (RecordDataType) dataType;
                final RecordSchema childSchema = recordType.getChildSchema();
                return toRecord(value, childSchema, fieldName, charset);
            case CHOICE: {
                final ChoiceDataType choiceDataType = (ChoiceDataType) dataType;
                final DataType chosenDataType = chooseDataType(value, choiceDataType);
                if (chosenDataType == null) {
                    throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass()
                            + " for field " + fieldName + " to any of the following available Sub-Types for a Choice: " + choiceDataType.getPossibleSubTypes());
                }

                return convertType(value, chosenDataType, fieldName, charset);
            }
        }

        return null;
    }

    public static UUID toUUID(Object value) {
        switch (value) {
            case null -> {
                return null;
            }
            case UUID uuid -> {
                return uuid;
            }
            case String string -> {
                try {
                    return UUID.fromString(string);
                } catch (Exception ex) {
                    throw new IllegalTypeConversionException(String.format("Could not parse %s into a UUID", value), ex);
                }
            }
            case byte[] bytes -> {
                return uuidFromBytes(bytes);
            }
            case Byte[] array -> {
                byte[] converted = new byte[array.length];
                for (int x = 0; x < array.length; x++) {
                    converted[x] = array[x];
                }
                return uuidFromBytes(converted);
            }
            default -> throw new IllegalTypeConversionException(value.getClass() + " cannot be converted into a UUID");
        }
    }

    private static UUID uuidFromBytes(byte[] bytes) {
        try {
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            return new UUID(buffer.getLong(), buffer.getLong());
        } catch (Exception ex) {
            throw new IllegalTypeConversionException("Could not convert bytes to UUID");
        }
    }

    public static boolean isCompatibleDataType(final Object value, final DataType dataType) {
        return isCompatibleDataType(value, dataType, false);
    }

    public static boolean isCompatibleDataType(final Object value, final DataType dataType, final boolean strict) {
        return switch (dataType.getFieldType()) {
            case ARRAY -> isArrayTypeCompatible(value, ((ArrayDataType) dataType).getElementType(), strict);
            case BIGINT -> isBigIntTypeCompatible(value);
            case BOOLEAN -> isBooleanTypeCompatible(value);
            case BYTE -> isByteTypeCompatible(value);
            case CHAR -> isCharacterTypeCompatible(value);
            case DATE -> isDateTypeCompatible(value, dataType.getFormat());
            case DECIMAL -> isDecimalTypeCompatible(value);
            case DOUBLE -> isDoubleTypeCompatible(value);
            case FLOAT -> isFloatTypeCompatible(value);
            case INT -> isIntegerTypeCompatible(value);
            case LONG -> isLongTypeCompatible(value);
            case RECORD -> {
                final RecordSchema schema = ((RecordDataType) dataType).getChildSchema();
                yield isRecordTypeCompatible(schema, value, strict);
            }
            case SHORT -> isShortTypeCompatible(value);
            case TIME -> isTimeTypeCompatible(value, dataType.getFormat());
            case TIMESTAMP -> isTimestampTypeCompatible(value, dataType.getFormat());
            case STRING -> isStringTypeCompatible(value);
            case ENUM -> isEnumTypeCompatible(value, (EnumDataType) dataType);
            case MAP -> isMapTypeCompatible(value);
            case CHOICE -> {
                final DataType chosenDataType = chooseDataType(value, (ChoiceDataType) dataType);
                yield chosenDataType != null;
            }
            default -> false;
        };
    }

    public static DataType chooseDataType(final Object value, final ChoiceDataType choiceType) {
        Queue<DataType> possibleSubTypes = new LinkedList<>(choiceType.getPossibleSubTypes());
        List<DataType> compatibleSimpleSubTypes = new ArrayList<>();

        DataType subType;
        while ((subType = possibleSubTypes.poll()) != null) {
            if (subType instanceof ChoiceDataType) {
                possibleSubTypes.addAll(((ChoiceDataType) subType).getPossibleSubTypes());
            } else {
                if (isCompatibleDataType(value, subType)) {
                    compatibleSimpleSubTypes.add(subType);
                }
            }
        }

        int nrOfCompatibleSimpleSubTypes = compatibleSimpleSubTypes.size();

        final DataType chosenSimpleType;
        if (nrOfCompatibleSimpleSubTypes == 0) {
            chosenSimpleType = null;
        } else if (nrOfCompatibleSimpleSubTypes == 1) {
            chosenSimpleType = compatibleSimpleSubTypes.getFirst();
        } else {
            chosenSimpleType = findMostSuitableType(value, compatibleSimpleSubTypes, Function.identity())
                    .orElse(compatibleSimpleSubTypes.getFirst());
        }

        return chosenSimpleType;
    }

    public static <T> Optional<T> findMostSuitableType(Object value, List<T> types, Function<T, DataType> dataTypeMapper) {
        if (value instanceof String) {
            return findMostSuitableTypeByStringValue((String) value, types, dataTypeMapper);
        } else {
            DataType inferredDataType = inferDataType(value, null);

            if (inferredDataType != null && !inferredDataType.getFieldType().equals(RecordFieldType.STRING)) {
                for (T type : types) {
                    if (inferredDataType.equals(dataTypeMapper.apply(type))) {
                        return Optional.of(type);
                    }
                }

                for (T type : types) {
                    if (getWiderType(dataTypeMapper.apply(type), inferredDataType).isPresent()) {
                        return Optional.of(type);
                    }
                }
            }
        }

        return Optional.empty();
    }

    public static <T> Optional<T> findMostSuitableTypeByStringValue(String valueAsString, List<T> types, Function<T, DataType> dataTypeMapper) {
        // Sorting based on the RecordFieldType enum ordering looks appropriate here as we want simpler types
        //  first and the enum's ordering seems to reflect that
        types.sort(Comparator.comparing(type -> dataTypeMapper.apply(type).getFieldType()));

        for (T type : types) {
            try {
                if (isCompatibleDataType(valueAsString, dataTypeMapper.apply(type))) {
                    return Optional.of(type);
                }
            } catch (Exception e) {
                logger.error("Exception thrown while checking if '{}' is compatible with '{}'", valueAsString, type, e);
            }
        }

        return Optional.empty();
    }

    public static Record toRecord(final Object value, final RecordSchema recordSchema, final String fieldName) {
        return toRecord(value, recordSchema, fieldName, StandardCharsets.UTF_8);
    }

    public static Record toRecord(final Object value, final RecordSchema recordSchema, final String fieldName, final Charset charset) {
        switch (value) {
            case null -> {
                return null;
            }
            case Record record -> {
                return record;
            }
            case Map<?, ?> map -> {
                if (recordSchema == null) {
                    throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass()
                            + " to Record for field " + fieldName + " because the value is a Map but no Record Schema was provided");
                }

                final Map<String, Object> coercedValues = new LinkedHashMap<>();

                for (final Map.Entry<?, ?> entry : map.entrySet()) {
                    final Object keyValue = entry.getKey();
                    if (keyValue == null) {
                        continue;
                    }

                    final String key = keyValue.toString();
                    final Optional<DataType> desiredTypeOption = recordSchema.getDataType(key);
                    if (desiredTypeOption.isEmpty()) {
                        continue;
                    }

                    final Object rawValue = entry.getValue();
                    final Object coercedValue = convertType(rawValue, desiredTypeOption.get(), fieldName, charset);
                    coercedValues.put(key, coercedValue);
                }

                return new MapRecord(recordSchema, coercedValues);
            }
            default -> {
            }
        }

        throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to Record for field " + fieldName);
    }

    public static Record toRecord(final Object value, final String fieldName) {
        return toRecord(value, fieldName, false);
    }

    public static Record toRecord(final Object value, final String fieldName, final boolean recursive) {
        return toRecord(value, fieldName, StandardCharsets.UTF_8, recursive);
    }

    public static RecordSchema inferSchema(final Map<String, Object> values, final String fieldName, final Charset charset) {
        if (values == null) {
            return null;
        }

        final List<RecordField> inferredFieldTypes = new ArrayList<>();

        for (final Map.Entry<?, ?> entry : values.entrySet()) {
            final Object keyValue = entry.getKey();
            if (keyValue == null) {
                continue;
            }

            final String key = keyValue.toString();
            final Object rawValue = entry.getValue();
            final DataType inferredDataType = inferDataType(rawValue, RecordFieldType.STRING.getDataType());

            final RecordField recordField = new RecordField(key, inferredDataType, true);
            inferredFieldTypes.add(recordField);

            convertType(rawValue, inferredDataType, fieldName, charset);
        }

        return new SimpleRecordSchema(inferredFieldTypes);
    }

    public static Record toRecord(final Object value, final String fieldName, final Charset charset) {
        return toRecord(value, fieldName, charset, false);
    }

    private static Object convertNestedObject(final Object rawValue, final String key, final Charset charset) {
        final Object coercedValue;
        switch (rawValue) {
            case Map<?, ?> ignored -> coercedValue = toRecord(rawValue, key, charset, true);
            case Object[] rawArray -> {
                final List<Object> objList = new ArrayList<>(rawArray.length);
                for (final Object o : rawArray) {
                    objList.add(o instanceof Map<?, ?> ? toRecord(o, key, charset, true) : o);
                }
                coercedValue = objList.toArray();
            }
            case Collection<?> objCollection -> {
                // Records have ARRAY DataTypes, so convert any Collections
                final List<Object> objList = new ArrayList<>(objCollection.size());
                for (final Object o : objCollection) {
                    objList.add(o instanceof Map<?, ?> ? toRecord(o, key, charset, true) : o);
                }
                coercedValue = objList.toArray();
            }
            case null, default -> coercedValue = rawValue;
        }
        return coercedValue;
    }

    public static Record toRecord(final Object value, final String fieldName, final Charset charset, final boolean recursive) {
        if (value == null) {
            return null;
        }

        if (value instanceof Record record) {
            if (recursive) {
                record.getRawFieldNames().forEach(name -> {
                    final Object rawValue = record.getValue(name);
                    record.setValue(name, convertNestedObject(rawValue, name, charset));
                });
            }
            return record;
        }

        final List<RecordField> inferredFieldTypes = new ArrayList<>();
        if (value instanceof Map<?, ?> map) {
            final Map<String, Object> coercedValues = new LinkedHashMap<>();

            for (final Map.Entry<?, ?> entry : map.entrySet()) {
                final Object keyValue = entry.getKey();
                if (keyValue == null) {
                    continue;
                }

                final String key = keyValue.toString();
                final Object rawValue = entry.getValue();
                final DataType inferredDataType = inferDataType(rawValue, RecordFieldType.STRING.getDataType());

                final RecordField recordField = new RecordField(key, inferredDataType, true);
                inferredFieldTypes.add(recordField);

                final Object coercedValue = recursive
                        ? convertNestedObject(rawValue, key, charset)
                        : convertType(rawValue, inferredDataType, fieldName, charset);
                coercedValues.put(key, coercedValue);
            }

            final RecordSchema inferredSchema = new SimpleRecordSchema(inferredFieldTypes);
            return new MapRecord(inferredSchema, coercedValues);
        }

        throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to Record for field " + fieldName);
    }

    public static DataType inferDataType(final Object value, final DataType defaultType) {
        switch (value) {
            case null -> {
                return defaultType;
            }
            case String ignored -> {
                return RecordFieldType.STRING.getDataType();
            }
            case Record record -> {
                final RecordSchema schema = record.getSchema();
                return RecordFieldType.RECORD.getRecordDataType(schema);
            }
            case Number ignored1 -> {
                switch (value) {
                    case Long ignored2 -> {
                        return RecordFieldType.LONG.getDataType();
                    }
                    case Integer ignored3 -> {
                        return RecordFieldType.INT.getDataType();
                    }
                    case Short ignored4 -> {
                        return RecordFieldType.SHORT.getDataType();
                    }
                    case Byte ignored5 -> {
                        return RecordFieldType.BYTE.getDataType();
                    }
                    case Float ignored6 -> {
                        return RecordFieldType.FLOAT.getDataType();
                    }
                    case Double ignored7 -> {
                        return RecordFieldType.DOUBLE.getDataType();
                    }
                    case BigInteger ignored8 -> {
                        return RecordFieldType.BIGINT.getDataType();
                    }
                    case BigDecimal bigDecimal -> {
                        return RecordFieldType.DECIMAL.getDecimalDataType(bigDecimal.precision(), bigDecimal.scale());
                    }
                    default -> {
                    }
                }
            }
            case Boolean ignored9 -> {
                return RecordFieldType.BOOLEAN.getDataType();
            }
            case Time ignored10 -> {
                return RecordFieldType.TIME.getDataType();
            }
            case Timestamp ignored11 -> {
                return RecordFieldType.TIMESTAMP.getDataType();
            }
            case java.util.Date ignored12 -> {
                return RecordFieldType.DATE.getDataType();
            }
            case Character ignored13 -> {
                return RecordFieldType.CHAR.getDataType();
            }
            // A value of a Map could be either a Record or a Map type. In either case, it must have Strings as keys.
            case Map mapInstance -> {
                final Map<String, Object> map;
                // Only transform the map if the keys aren't strings
                boolean allStrings = true;
                for (final Object key : mapInstance.keySet()) {
                    if (!(key instanceof String)) {
                        allStrings = false;
                        break;
                    }
                }

                if (allStrings) {
                    map = (Map<String, Object>) value;
                } else {
                    final Map<?, ?> m = (Map<?, ?>) value;
                    map = new LinkedHashMap<>(m.size());
                    m.forEach((k, v) -> map.put(k == null ? null : k.toString(), v));
                }
                return inferRecordDataType(map);
            }
            default -> {
            }
        }

        if (value.getClass().isArray()) {
            DataType mergedDataType = null;

            int length = Array.getLength(value);
            for (int index = 0; index < length; index++) {
                final DataType inferredDataType = inferDataType(Array.get(value, index), RecordFieldType.STRING.getDataType());
                mergedDataType = mergeDataTypes(mergedDataType, inferredDataType);
            }

            if (mergedDataType == null) {
                mergedDataType = RecordFieldType.STRING.getDataType();
            }

            return RecordFieldType.ARRAY.getArrayDataType(mergedDataType);
        }

        if (value instanceof Iterable<?> iterable) {

            DataType mergedDataType = null;
            for (final Object arrayValue : iterable) {
                final DataType inferredDataType = inferDataType(arrayValue, RecordFieldType.STRING.getDataType());
                mergedDataType = mergeDataTypes(mergedDataType, inferredDataType);
            }

            if (mergedDataType == null) {
                mergedDataType = RecordFieldType.STRING.getDataType();
            }

            return RecordFieldType.ARRAY.getArrayDataType(mergedDataType);
        }

        return defaultType;
    }

    private static DataType inferRecordDataType(final Map<String, ?> map) {
        final List<RecordField> fields = new ArrayList<>(map.size());
        for (final Map.Entry<String, ?> entry : map.entrySet()) {
            final String key = entry.getKey();
            final Object value = entry.getValue();

            final DataType dataType = inferDataType(value, RecordFieldType.STRING.getDataType());
            final RecordField field = new RecordField(key, dataType, true);
            fields.add(field);
        }

        final RecordSchema schema = new SimpleRecordSchema(fields);
        return RecordFieldType.RECORD.getRecordDataType(schema);
    }

    /**
     * Check if the given record structured object compatible with the schema.
     * @param schema record schema, schema validation will not be performed if schema is null
     * @param value the record structured object, i.e. Record or Map
     * @param strict check for a strict match, i.e. all fields in the record should have a corresponding entry in the schema
     * @return True if the object is compatible with the schema
     */
    private static boolean isRecordTypeCompatible(RecordSchema schema, Object value, boolean strict) {

        if (value == null) {
            return false;
        }

        if (!(value instanceof Record) && !(value instanceof Map)) {
            return false;
        }

        if (schema == null) {
            return true;
        }

        if (strict) {
            if (value instanceof Record) {
                if (!schema.getFieldNames().containsAll(((Record) value).getRawFieldNames())) {
                    return false;
                }
            }
        }

        for (final RecordField childField : schema.getFields()) {
            final Object childValue;
            if (value instanceof Record) {
                childValue = ((Record) value).getValue(childField);
            } else {
                childValue = ((Map) value).get(childField.getFieldName());
            }

            if (childValue == null && !childField.isNullable()) {
                logger.debug("Value is not compatible with schema because field {} has a null value, which is not allowed in the schema", childField.getFieldName());
                return false;
            }
            if (childValue == null) {
                continue; // consider compatible
            }

            if (!isCompatibleDataType(childValue, childField.getDataType(), strict)) {
                return false;
            }
        }
        return true;
    }

    public static Object[] toArray(final Object value, final String fieldName, final DataType elementDataType) {
        return toArray(value, fieldName, elementDataType, StandardCharsets.UTF_8);
    }

    public static Object[] toArray(final Object value, final String fieldName, final DataType elementDataType, final Charset charset) {
        switch (value) {
            case null -> {
                return null;
            }
            case Object[] objects -> {
                return objects;
            }
            case String s when RecordFieldType.BYTE.getDataType().equals(elementDataType) -> {
                byte[] src = s.getBytes(charset);
                Byte[] dest = new Byte[src.length];
                for (int i = 0; i < src.length; i++) {
                    dest[i] = src[i];
                }
                return dest;
            }
            case byte[] src -> {
                Byte[] dest = new Byte[src.length];
                for (int i = 0; i < src.length; i++) {
                    dest[i] = src[i];
                }
                return dest;
            }
            case UUID uuid -> {
                ByteBuffer buffer = ByteBuffer.allocate(16);
                buffer.putLong(uuid.getMostSignificantBits());
                buffer.putLong(uuid.getLeastSignificantBits());
                Byte[] result = new Byte[16];
                byte[] array = buffer.array();
                for (int index = 0; index < array.length; index++) {
                    result[index] = array[index];
                }

                return result;
            }
            case List<?> list -> {
                return list.toArray();
            }
            default -> {
            }
        }

        try {
            if (value instanceof Blob blob) {
                long rawBlobLength = blob.length();
                if (rawBlobLength > Integer.MAX_VALUE) {
                    throw new IllegalTypeConversionException("Value of type " + value.getClass() + " too large to convert to Object Array for field " + fieldName);
                }
                int blobLength = (int) rawBlobLength;
                byte[] src = blob.getBytes(1, blobLength);
                Byte[] dest = new Byte[blobLength];
                for (int i = 0; i < src.length; i++) {
                    dest[i] = src[i];
                }
                return dest;
            } else {
                throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to Object Array for field " + fieldName);
            }
        } catch (IllegalTypeConversionException itce) {
            throw itce;
        } catch (Exception e) {
            throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to Object Array for field " + fieldName, e);
        }
    }

    public static boolean isArrayTypeCompatible(final Object value, final DataType elementDataType) {
        return isArrayTypeCompatible(value, elementDataType, false);
    }

    public static boolean isArrayTypeCompatible(final Object value, final DataType elementDataType, final boolean strict) {
        if (value == null) {
            return false;
        }
        // Either an object array (check the element type) or a String to be converted to byte[]
        if (value instanceof final Object[] array) {
            for (final Object element : array) {
                // Check each element to ensure its type is the same or can be coerced (if need be)
                if (!isCompatibleDataType(element, elementDataType, strict)) {
                    return false;
                }
            }

            return true;
        } else {
            return value instanceof String && RecordFieldType.BYTE.getDataType().equals(elementDataType);
        }
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> toMap(final Object value, final String fieldName) {
        switch (value) {
            case null -> {
                return null;
            }
            case Map<?, ?> original -> {
                boolean keysAreStrings = true;
                for (final Object key : original.keySet()) {
                    if (!(key instanceof String)) {
                        keysAreStrings = false;
                        break;
                    }
                }

                if (keysAreStrings) {
                    return (Map<String, Object>) value;
                }

                final Map<String, Object> transformed = new LinkedHashMap<>();
                for (final Map.Entry<?, ?> entry : original.entrySet()) {
                    final Object key = entry.getKey();
                    if (key == null) {
                        transformed.put(null, entry.getValue());
                    } else {
                        transformed.put(key.toString(), entry.getValue());
                    }
                }

                return transformed;
            }
            case Record record -> {
                final RecordSchema recordSchema = record.getSchema();
                if (recordSchema == null) {
                    throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type Record to Map for field " + fieldName
                            + " because Record does not have an associated Schema");
                }

                final Map<String, Object> map = new LinkedHashMap<>();
                for (final String recordFieldName : recordSchema.getFieldNames()) {
                    map.put(recordFieldName, record.getValue(recordFieldName));
                }

                return map;
            }
            default -> {
            }
        }

        throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to Map for field " + fieldName);
    }

    /**
     * Creates a native Java object from a given object of a specified type. Non-scalar (complex, nested, etc.) data types are processed iteratively/recursively, such that all
     * included objects are native Java objects, rather than Record API objects or implementation-specific objects.
     * @param value The object to be converted
     * @param dataType The type of the provided object
     * @return An object representing a native Java conversion of the given input object
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static Object convertRecordFieldtoObject(final Object value, final DataType dataType) {
        if (value == null) {
            return null;
        }

        DataType chosenDataType;
        if (dataType instanceof ChoiceDataType) {
            final DataType chosen = chooseDataType(value, (ChoiceDataType) dataType);
            chosenDataType = chosen != null ? chosen : dataType;
        } else {
            chosenDataType = dataType;
        }

        if (value instanceof Record record) {
            final RecordSchema recordSchema = record.getSchema();
            if (recordSchema == null) {
                throw new IllegalTypeConversionException("Cannot convert value of type Record to Map because Record does not have an associated Schema");
            }

            final Map<String, Object> recordMap = new LinkedHashMap<>(record.getRawFieldNames().size(), 1);
            for (final RecordField field : recordSchema.getFields()) {
                final String fieldName = field.getFieldName();
                final Object fieldValue = record.getValue(fieldName);
                if (field.getDataType() instanceof ChoiceDataType) {
                    final DataType chosen = chooseDataType(fieldValue, (ChoiceDataType) field.getDataType());
                    chosenDataType = chosen != null ? chosen : field.getDataType();
                } else {
                    chosenDataType = field.getDataType();
                }

                if (fieldValue == null) {
                    recordMap.put(fieldName, null);
                } else if (isScalarValue(chosenDataType, fieldValue)) {
                    recordMap.put(fieldName, fieldValue);
                } else if (chosenDataType instanceof RecordDataType) {
                    Record nestedRecord = (Record) fieldValue;
                    recordMap.put(fieldName, convertRecordFieldtoObject(nestedRecord, chosenDataType));
                } else if (chosenDataType instanceof MapDataType) {
                    recordMap.put(fieldName, convertRecordMapToJavaMap((Map) fieldValue, ((MapDataType) chosenDataType).getValueType()));
                } else if (chosenDataType instanceof ArrayDataType) {
                    recordMap.put(fieldName, convertRecordArrayToJavaArray((Object[]) fieldValue, ((ArrayDataType) chosenDataType).getElementType()));
                } else {
                    throw new IllegalTypeConversionException("Cannot convert value [" + fieldValue + "] of type " + chosenDataType
                            + " to Map for field " + fieldName + " because the type is not supported");
                }
            }
            return recordMap;
        } else if (value instanceof Map) {
            return convertRecordMapToJavaMap((Map) value, ((MapDataType) chosenDataType).getValueType());
        } else if (chosenDataType != null && isScalarValue(chosenDataType, value)) {
            return value;
        } else if (value instanceof Object[] && chosenDataType instanceof ArrayDataType) {
            // This is likely a Map whose values are represented as an array. Return a new array with each element converted to a Java object
            return convertRecordArrayToJavaArray((Object[]) value, ((ArrayDataType) chosenDataType).getElementType());
        }

        throw new IllegalTypeConversionException("Cannot convert value of class " + value.getClass().getName() + " because the type is not supported");
    }

    public static Map<String, Object> convertRecordMapToJavaMap(final Map<String, Object> map, final DataType valueDataType) {
        if (map == null) {
            return null;
        }

        final Map<String, Object> resultMap = new LinkedHashMap<>();
        for (final Map.Entry<String, Object> entry : map.entrySet()) {
            resultMap.put(entry.getKey(), convertRecordFieldtoObject(entry.getValue(), valueDataType));
        }
        return resultMap;
    }

    public static Object[] convertRecordArrayToJavaArray(final Object[] array, final DataType elementDataType) {
        if (array == null || array.length == 0) {
            return array;
        }

        final List<Object> objList = new ArrayList<>(array.length);
        boolean nonScalarConverted = false;
        for (final Object o : array) {
            if (isScalarValue(elementDataType, o)) {
                objList.add(o);
            } else {
                nonScalarConverted = true;
                objList.add(convertRecordFieldtoObject(o, elementDataType));
            }
        }

        return !nonScalarConverted ? array : objList.toArray();
    }

    public static boolean isMapTypeCompatible(final Object value) {
        return value != null && (value instanceof Map || value instanceof MapRecord);
    }

    private static String toString(final Object value, final Supplier<DateFormat> format, final Charset charset) {
        switch (value) {
            case null -> {
                return null;
            }
            case String s -> {
                return s;
            }
            case java.util.Date date when format == null -> {
                return String.valueOf(date.getTime());
            }
            case java.util.Date date -> {
                return formatDate(date, format);
            }
            case byte[] bytes -> {
                return new String(bytes, charset);
            }
            case Byte[] src -> {
                byte[] dest = new byte[src.length];
                for (int i = 0; i < src.length; i++) {
                    dest[i] = src[i];
                }
                return new String(dest, charset);
            }
            case Object[] o -> {
                if (o.length > 0) {

                    byte[] dest = new byte[o.length];
                    for (int i = 0; i < o.length; i++) {
                        dest[i] = (byte) o[i];
                    }
                    return new String(dest, charset);
                } else {
                    return ""; // Empty array = empty string
                }
            }
            case Clob clob -> {
                StringBuilder sb = new StringBuilder();
                char[] buffer = new char[32 * 1024]; // 32K default buffer

                try (Reader reader = clob.getCharacterStream()) {
                    int charsRead;
                    while ((charsRead = reader.read(buffer)) != -1) {
                        sb.append(buffer, 0, charsRead);
                    }
                    return sb.toString();
                } catch (Exception e) {
                    throw new IllegalTypeConversionException("Cannot convert value " + value + " of type " + value.getClass() + " to a valid String", e);
                }
            }
            default -> {
                return value.toString();
            }
        }
    }

    private static String formatDate(final java.util.Date date, final Supplier<DateFormat> formatSupplier) {
        final DateFormat dateFormat = formatSupplier.get();
        if (dateFormat == null) {
            return String.valueOf((date).getTime());
        }

        return dateFormat.format(date);
    }

    public static String toString(final Object value, final String format) {
        return toString(value, format, StandardCharsets.UTF_8);
    }

    public static String toString(final Object value, final String format, final Charset charset) {
        switch (value) {
            case null -> {
                return null;
            }
            case String s -> {
                return s;
            }
            case java.util.Date date when format == null -> {
                return String.valueOf(date.getTime());
            }
            default -> {
            }
        }

        final FieldConverter<Object, String> fieldConverter = StandardFieldConverterRegistry.getRegistry().getFieldConverter(String.class);
        switch (value) {
            case Date ignored -> {
                return fieldConverter.convertField(value, Optional.of(format), null);
            }
            case Time ignored1 -> {
                return fieldConverter.convertField(value, Optional.of(format), null);
            }
            case Timestamp ignored2 -> {
                return fieldConverter.convertField(value, Optional.of(format), null);
            }
            case java.util.Date ignored3 -> {
                return fieldConverter.convertField(value, Optional.of(format), null);
            }
            case Blob blob -> {
                StringBuilder sb = new StringBuilder();
                byte[] buffer = new byte[32 * 1024]; // 32K default buffer

                try (InputStream inStream = blob.getBinaryStream()) {
                    int bytesRead;
                    while ((bytesRead = inStream.read(buffer)) != -1) {
                        sb.append(new String(buffer, charset), 0, bytesRead);
                    }
                    return sb.toString();
                } catch (Exception e) {
                    throw new IllegalTypeConversionException("Cannot convert value " + value + " of type " + value.getClass() + " to a valid String", e);
                }
            }
            case Clob clob -> {
                StringBuilder sb = new StringBuilder();
                char[] buffer = new char[32 * 1024]; // 32K default buffer

                try (Reader reader = clob.getCharacterStream()) {
                    int charsRead;
                    while ((charsRead = reader.read(buffer)) != -1) {
                        sb.append(buffer, 0, charsRead);
                    }
                    return sb.toString();
                } catch (Exception e) {
                    throw new IllegalTypeConversionException("Cannot convert value " + value + " of type " + value.getClass() + " to a valid String", e);
                }
            }
            case Object[] objects -> {
                return Arrays.toString(objects);
            }
            case byte[] bytes -> {
                return new String(bytes, charset);
            }
            default -> {
                return value.toString();
            }
        }
    }

    public static boolean isStringTypeCompatible(final Object value) {
        return !(value instanceof Record);
    }

    public static boolean isEnumTypeCompatible(final Object value, final EnumDataType enumType) {
        return enumType.getEnums() != null && enumType.getEnums().contains(value);
    }

    public static Object toEnum(Object value, EnumDataType dataType, String fieldName) {
        if (dataType.getEnums() != null && dataType.getEnums().contains(value)) {
            return value.toString();
        }
        throw new IllegalTypeConversionException("Cannot convert value " + value + " of type " + dataType + " for field " + fieldName);
    }

    /**
     * Converts a java.sql.Date object in local time zone (typically coming from a java.sql.ResultSet and having 00:00:00 time part)
     * to UTC normalized form (storing the epoch corresponding to the UTC time with the same date/time as the input).
     *
     * @param dateLocalTZ java.sql.Date in local time zone
     * @return java.sql.Date in UTC normalized form
     */
    public static Date convertDateToUTC(Date dateLocalTZ) {
        ZonedDateTime zdtLocalTZ = ZonedDateTime.ofInstant(Instant.ofEpochMilli(dateLocalTZ.getTime()), ZoneId.systemDefault());
        ZonedDateTime zdtUTC = zdtLocalTZ.withZoneSameLocal(ZoneOffset.UTC);
        return new Date(zdtUTC.toInstant().toEpochMilli());
    }

    public static boolean isDateTypeCompatible(final Object value, final String format) {
        if (value == null) {
            return false;
        }

        if (value instanceof java.util.Date || value instanceof Number) {
            return true;
        }

        if (value instanceof String) {
            if (format == null) {
                return isInteger((String) value);
            }

            try {
                DateTimeFormatter.ofPattern(format).parse(value.toString());
                return true;
            } catch (final DateTimeParseException e) {
                return false;
            }
        }

        return false;
    }

    private static boolean isInteger(final String value) {
        if (value == null || value.isEmpty()) {
            return false;
        }

        for (int i = 0; i < value.length(); i++) {
            if (!Character.isDigit(value.charAt(i))) {
                return false;
            }
        }

        return true;
    }

    public static boolean isTimeTypeCompatible(final Object value, final String format) {
        return isDateTypeCompatible(value, format);
    }

    public static boolean isTimestampTypeCompatible(final Object value, final String format) {
        return isDateTypeCompatible(value, format);
    }

    public static BigInteger toBigInt(final Object value, final String fieldName) {
        switch (value) {
            case null -> {
                return null;
            }
            case BigInteger bigInteger -> {
                return bigInteger;
            }
            case Number number -> {
                return BigInteger.valueOf(number.longValue());
            }
            case String string -> {
                try {
                    return string.isBlank() ? null : new BigInteger(string);
                } catch (NumberFormatException nfe) {
                    throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to BigInteger for field " + fieldName
                            + ", value is not a valid representation of BigInteger", nfe);
                }
            }
            default -> throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to BigInteger for field " + fieldName);
        }
    }

    public static boolean isBigIntTypeCompatible(final Object value) {
        return isNumberTypeCompatible(value, DataTypeUtils::isIntegral);
    }

    public static boolean isDecimalTypeCompatible(final Object value) {
        return isNumberTypeCompatible(value, DataTypeUtils::isDecimal);
    }

    public static Boolean toBoolean(final Object value, final String fieldName) {
        switch (value) {
            case null -> {
                return null;
            }
            case Boolean b -> {
                return b;
            }
            case String string -> {
                if (string.equalsIgnoreCase("true")) {
                    return Boolean.TRUE;
                } else if (string.equalsIgnoreCase("false")) {
                    return Boolean.FALSE;
                }
            }
            default -> {
            }
        }
        throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to Boolean for field " + fieldName);
    }

    public static boolean isBooleanTypeCompatible(final Object value) {
        return switch (value) {
            case null -> false;
            case Boolean ignored -> true;
            case String string -> string.equalsIgnoreCase("true") || string.equalsIgnoreCase("false");
            default -> false;
        };
    }

    public static BigDecimal toBigDecimal(final Object value, final String fieldName) {
        switch (value) {
            case null -> {
                return null;
            }
            case BigDecimal bigDecimal -> {
                return bigDecimal;
            }
            case Number number -> {
                if (number instanceof Byte
                        || number instanceof Short
                        || number instanceof Integer
                        || number instanceof Long) {
                    return BigDecimal.valueOf(number.longValue());
                }

                switch (number) {
                    case BigInteger bigInteger -> {
                        return new BigDecimal(bigInteger);
                    }
                    case Float v -> {
                        return new BigDecimal(Float.toString(v));
                    }
                    case Double v -> {
                        return new BigDecimal(Double.toString(v));
                    }
                    default -> {
                    }
                }

            }
            default -> {
            }
        }

        if (value instanceof String string) {
            try {
                return string.isBlank() ? null : new BigDecimal(string);
            } catch (NumberFormatException nfe) {
                throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to BigDecimal for field " + fieldName
                        + ", value is not a valid representation of BigDecimal", nfe);
            }
        }

        throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to BigDecimal for field " + fieldName);
    }

    public static Double toDouble(final Object value, final String fieldName) {
        return switch (value) {
            case null -> null;
            case Number number -> number.doubleValue();
            case String string -> string.isBlank() ? null : Double.parseDouble(string);
            default ->
                    throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to Double for field " + fieldName);
        };

    }

    public static boolean isDoubleTypeCompatible(final Object value) {
        return isNumberTypeCompatible(value, DataTypeUtils::isDouble);
    }

    private static boolean isNumberTypeCompatible(final Object value, final Predicate<String> stringPredicate) {
        return switch (value) {
            case null -> false;
            case Number ignored -> true;
            case String s -> stringPredicate.test(s);
            default -> false;
        };

    }

    public static Float toFloat(final Object value, final String fieldName) {
        return switch (value) {
            case null -> null;
            case Number number -> number.floatValue();
            case String string -> string.isBlank() ? null : Float.parseFloat(string);
            default ->
                    throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to Float for field " + fieldName);
        };

    }

    public static boolean isFloatTypeCompatible(final Object value) {
        return isNumberTypeCompatible(value, DataTypeUtils::isFloatingPoint);
    }

    private static boolean isDecimal(final String value) {
        if (value == null || value.isEmpty()) {
            return false;
        }

        return DECIMAL_PATTERN.matcher(value).matches();
    }

    private static boolean isFloatingPoint(final String value) {
        if (value == null || value.isEmpty()) {
            return false;
        }

        if (!FLOATING_POINT_PATTERN.matcher(value).matches()) {
            return false;
        }

        // Just to ensure that the exponents are in range, etc.
        try {
            Float.parseFloat(value);
        } catch (final NumberFormatException nfe) {
            return false;
        }

        return true;
    }

    private static boolean isDouble(final String value) {
        if (value == null || value.isEmpty()) {
            return false;
        }

        if (!FLOATING_POINT_PATTERN.matcher(value).matches()) {
            return false;
        }

        // Just to ensure that the exponents are in range, etc.
        try {
            Double.parseDouble(value);
        } catch (final NumberFormatException nfe) {
            return false;
        }

        return true;
    }

    public static Long toLong(final Object value, final String fieldName) {
        return switch (value) {
            case null -> null;
            case Number number -> number.longValue();
            case String string -> string.isBlank() ? null : Long.parseLong(string);
            case java.util.Date date -> date.getTime();
            default ->
                    throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to Long for field " + fieldName);
        };
    }

    public static boolean isLongTypeCompatible(final Object value) {
        return switch (value) {
            case null -> false;
            case Number ignored -> true;
            case java.util.Date ignored1 -> true;
            case String s -> isIntegral(s, Long.MIN_VALUE, Long.MAX_VALUE);
            default -> false;
        };

    }

    /**
     * Check if the value is an integral.
     */
    private static boolean isIntegral(final String value) {
        if (value == null || value.isEmpty()) {
            return false;
        }

        int initialPosition = 0;
        final char firstChar = value.charAt(0);
        if (firstChar == '+' || firstChar == '-') {
            initialPosition = 1;

            if (value.length() == 1) {
                return false;
            }
        }

        for (int i = initialPosition; i < value.length(); i++) {
            if (!Character.isDigit(value.charAt(i))) {
                return false;
            }
        }

        return true;
    }

    /**
     * Check if the value is an integral within a value range.
     */
    private static boolean isIntegral(final String value, final long minValue, final long maxValue) {

        if (!isIntegral(value)) {
            return false;
        }

        try {
            final long longValue = Long.parseLong(value);
            return longValue >= minValue && longValue <= maxValue;
        } catch (final NumberFormatException nfe) {
            // In case the value actually exceeds the max value of a Long
            return false;
        }
    }

    public static Integer toInteger(final Object value, final String fieldName) {
        switch (value) {
            case null -> {
                return null;
            }
            case Number number -> {
                try {
                    return Math.toIntExact(number.longValue());
                } catch (ArithmeticException ae) {
                    throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to Integer for field " + fieldName
                            + " as it causes an arithmetic overflow (the value is too large, e.g.)", ae);
                }
            }
            case String string -> {
                return string.isBlank() ? null : Integer.parseInt(string);
            }
            default -> {
            }
        }

        throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to Integer for field " + fieldName);
    }

    public static boolean isIntegerTypeCompatible(final Object value) {
        if (value instanceof Number number) {
            try {
                Math.toIntExact(number.longValue());
                return true;
            } catch (ArithmeticException ae) {
                return false;
            }
        }
        return isNumberTypeCompatible(value, s -> isIntegral(s, Integer.MIN_VALUE, Integer.MAX_VALUE));
    }


    public static Short toShort(final Object value, final String fieldName) {
        return switch (value) {
            case null -> null;
            case Number number -> number.shortValue();
            case String string -> string.isBlank() ? null : Short.parseShort(string);
            default ->
                    throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to Short for field " + fieldName);
        };

    }

    public static boolean isShortTypeCompatible(final Object value) {
        return isNumberTypeCompatible(value, s -> isIntegral(s, Short.MIN_VALUE, Short.MAX_VALUE));
    }

    public static Byte toByte(final Object value, final String fieldName) {
        return switch (value) {
            case null -> null;
            case Number number -> number.byteValue();
            case String s -> Byte.parseByte(s);
            default ->
                    throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to Byte for field " + fieldName);
        };

    }

    public static boolean isByteTypeCompatible(final Object value) {
        return isNumberTypeCompatible(value, s -> isIntegral(s, Byte.MIN_VALUE, Byte.MAX_VALUE));
    }


    public static Character toCharacter(final Object value, final String fieldName) {
        switch (value) {
            case null -> {
                return null;
            }
            case Character c -> {
                return c;
            }
            case CharSequence charSeq -> {
                if (charSeq.isEmpty()) {
                    throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass()
                            + " to Character because it has a length of 0 for field " + fieldName);
                }

                return charSeq.charAt(0);
            }
            default -> {
            }
        }

        throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to Character for field " + fieldName);
    }

    public static boolean isCharacterTypeCompatible(final Object value) {
        return (value instanceof Character || (value instanceof CharSequence && !((CharSequence) value).isEmpty()));
    }

    public static RecordSchema merge(final RecordSchema thisSchema, final RecordSchema otherSchema) {
        if (thisSchema == null) {
            return otherSchema;
        }
        if (otherSchema == null) {
            return thisSchema;
        }
        if (thisSchema == otherSchema) {
            return thisSchema;
        }

        final List<RecordField> otherFields = otherSchema.getFields();
        if (otherFields.isEmpty()) {
            return thisSchema;
        }

        final List<RecordField> thisFields = thisSchema.getFields();
        if (thisFields.isEmpty()) {
            return otherSchema;
        }

        final Map<String, Integer> fieldIndices = new HashMap<>();
        final List<RecordField> fields = new ArrayList<>();
        for (int i = 0; i < thisFields.size(); i++) {
            final RecordField field = thisFields.get(i);

            final Integer index = i;

            fieldIndices.put(field.getFieldName(), index);
            for (final String alias : field.getAliases()) {
                fieldIndices.put(alias, index);
            }

            fields.add(field);
        }

        for (final RecordField otherField : otherFields) {
            Integer fieldIndex = fieldIndices.get(otherField.getFieldName());

            // Find the field in 'thisSchema' that corresponds to 'otherField',
            // if one exists.
            if (fieldIndex == null) {
                for (final String alias : otherField.getAliases()) {
                    fieldIndex = fieldIndices.get(alias);
                    if (fieldIndex != null) {
                        break;
                    }
                }
            }

            // If there is no field with the same name then just add 'otherField'.
            if (fieldIndex == null) {
                fields.add(otherField);
                continue;
            }

            // Merge the two fields, if necessary
            final RecordField thisField = fields.get(fieldIndex);
            if (isMergeRequired(thisField, otherField)) {
                final RecordField mergedField = merge(thisField, otherField);
                fields.set(fieldIndex, mergedField);
            }
        }

        return new SimpleRecordSchema(fields);
    }


    private static boolean isMergeRequired(final RecordField thisField, final RecordField otherField) {
        if (!thisField.getDataType().equals(otherField.getDataType())) {
            return true;
        }

        if (!thisField.getAliases().equals(otherField.getAliases())) {
            return true;
        }

        return !Objects.equals(thisField.getDefaultValue(), otherField.getDefaultValue());
    }

    public static RecordField merge(final RecordField thisField, final RecordField otherField) {
        final String fieldName = thisField.getFieldName();
        final Set<String> aliases = new HashSet<>();
        aliases.addAll(thisField.getAliases());
        aliases.addAll(otherField.getAliases());

        final Object defaultValue;
        if (thisField.getDefaultValue() == null && otherField.getDefaultValue() != null) {
            defaultValue = otherField.getDefaultValue();
        } else {
            defaultValue = thisField.getDefaultValue();
        }

        final DataType dataType = mergeDataTypes(thisField.getDataType(), otherField.getDataType());
        return new RecordField(fieldName, dataType, defaultValue, aliases, thisField.isNullable() || otherField.isNullable());
    }

    public static DataType mergeDataTypes(final DataType thisDataType, final DataType otherDataType) {
        if (thisDataType == null) {
            return otherDataType;
        }

        if (otherDataType == null) {
            return thisDataType;
        }

        if (thisDataType.equals(otherDataType)) {
            return thisDataType;
        } else {
            // If one type is 'wider' than the other (such as an INT and a LONG), just use the wider type (LONG, in this case),
            // rather than using a CHOICE of the two.
            final Optional<DataType> widerType = getWiderType(thisDataType, otherDataType);
            if (widerType.isPresent()) {
                return widerType.get();
            }

            final DataTypeSet dataTypeSet = new DataTypeSet();
            dataTypeSet.add(thisDataType);
            dataTypeSet.add(otherDataType);

            final List<DataType> possibleChildTypes = dataTypeSet.getTypes();
            possibleChildTypes.sort(Comparator.comparing(DataType::getFieldType));

            return RecordFieldType.CHOICE.getChoiceDataType(possibleChildTypes);
        }
    }

    public static Optional<DataType> getWiderType(final DataType thisDataType, final DataType otherDataType) {
        if (thisDataType == null) {
            return Optional.ofNullable(otherDataType);
        }
        if (otherDataType == null) {
            return Optional.of(thisDataType);
        }

        final RecordFieldType thisFieldType = thisDataType.getFieldType();
        final RecordFieldType otherFieldType = otherDataType.getFieldType();

        if (thisFieldType == RecordFieldType.ARRAY && otherFieldType == RecordFieldType.ARRAY) {
            // Check for array<null> and return the other (or empty if they are both array<null>). This happens if at some point we inferred an element type of null which
            // indicates an empty array, and then we inferred a non-null type for the same field in a different record. The non-null type should be used in that case.
            ArrayDataType thisArrayType = (ArrayDataType) thisDataType;
            ArrayDataType otherArrayType = (ArrayDataType) otherDataType;
            if (thisArrayType.getElementType() == null) {
                if (otherArrayType.getElementType() == null) {
                    return Optional.empty();
                } else {
                    return Optional.of(otherDataType);
                }
            } else {
                if (otherArrayType.getElementType() == null) {
                    return Optional.of(thisDataType);
                } else {
                    final Optional<DataType> widerElementType = getWiderType(thisArrayType.getElementType(), otherArrayType.getElementType());
                    if (widerElementType.isPresent()) {
                        return Optional.of(RecordFieldType.ARRAY.getArrayDataType(widerElementType.get()));
                    }
                    return Optional.empty();
                }
            }
        }

        final int thisIntTypeValue = getIntegerTypeValue(thisFieldType);
        final int otherIntTypeValue = getIntegerTypeValue(otherFieldType);
        final boolean thisIsInt = thisIntTypeValue > -1;
        final boolean otherIsInt = otherIntTypeValue > -1;

        if (thisIsInt && otherIsInt) {
            if (thisIntTypeValue > otherIntTypeValue) {
                return Optional.of(thisDataType);
            }

            return Optional.of(otherDataType);
        }

        final boolean otherIsDecimal = isDecimalType(otherFieldType);

        switch (thisFieldType) {
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
                if (otherIsDecimal) {
                    return Optional.of(otherDataType);
                }
                break;
            case FLOAT:
                if (otherFieldType == RecordFieldType.DOUBLE || otherFieldType == RecordFieldType.DECIMAL) {
                    return Optional.of(otherDataType);
                }
                if (otherFieldType == RecordFieldType.BYTE || otherFieldType == RecordFieldType.SHORT || otherFieldType == RecordFieldType.INT || otherFieldType == RecordFieldType.LONG) {
                    return Optional.of(thisDataType);
                }
                break;
            case DOUBLE:
                if (otherFieldType == RecordFieldType.DECIMAL) {
                    return Optional.of(otherDataType);
                }
                if (otherFieldType == RecordFieldType.BYTE || otherFieldType == RecordFieldType.SHORT || otherFieldType == RecordFieldType.INT || otherFieldType == RecordFieldType.LONG
                    || otherFieldType == RecordFieldType.FLOAT) {

                    return Optional.of(thisDataType);
                }
                break;
            case DECIMAL:
                if (otherFieldType == RecordFieldType.DOUBLE || otherFieldType == RecordFieldType.FLOAT || otherIsInt) {
                    return Optional.of(thisDataType);
                } else if (otherFieldType == RecordFieldType.DECIMAL) {
                    final DecimalDataType thisDecimalDataType = (DecimalDataType) thisDataType;
                    final DecimalDataType otherDecimalDataType = (DecimalDataType) otherDataType;

                    final int precision = Math.max(thisDecimalDataType.getPrecision(), otherDecimalDataType.getPrecision());
                    final int scale = Math.max(thisDecimalDataType.getScale(), otherDecimalDataType.getScale());
                    return Optional.of(RecordFieldType.DECIMAL.getDecimalDataType(precision, scale));
                }
                break;
            case CHAR:
            case UUID:
                if (otherFieldType == RecordFieldType.STRING) {
                    return Optional.of(otherDataType);
                }
                break;
            case STRING:
                if (otherFieldType == RecordFieldType.CHAR || otherFieldType == RecordFieldType.UUID) {
                    return Optional.of(thisDataType);
                }
                break;
            case RECORD:
                if (otherFieldType != RecordFieldType.RECORD)  {
                    return Optional.empty();
                }

                final RecordDataType thisRecordDataType = (RecordDataType) thisDataType;
                final RecordDataType otherRecordDataType = (RecordDataType) otherDataType;
                return getWiderRecordType(thisRecordDataType, otherRecordDataType);
        }

        return Optional.empty();
    }

    private static Optional<DataType> getWiderRecordType(final RecordDataType thisRecordDataType, final RecordDataType otherRecordDataType) {
        final RecordSchema thisSchema = thisRecordDataType.getChildSchema();
        final RecordSchema otherSchema = otherRecordDataType.getChildSchema();

        if (thisSchema == null && otherSchema != null) {
            return Optional.of(otherRecordDataType);
        } else if (thisSchema != null && otherSchema == null) {
            return Optional.of(thisRecordDataType);
        } else if (thisSchema == null && otherSchema == null) {
            return Optional.empty();
        }

        final Set<RecordField> thisFields = new HashSet<>(thisSchema.getFields());
        final Set<RecordField> otherFields = new HashSet<>(otherSchema.getFields());

        if (thisFields.containsAll(otherFields)) {
            return Optional.of(thisRecordDataType);
        }

        if (otherFields.containsAll(thisFields)) {
            return Optional.of(otherRecordDataType);
        }

        // Check if all fields in 'thisSchema' are equal to or wider than all fields in 'otherSchema'
        if (isRecordWider(thisSchema, otherSchema)) {
            return Optional.of(thisRecordDataType);
        }
        if (isRecordWider(otherSchema, thisSchema)) {
            return Optional.of(otherRecordDataType);
        }

        return Optional.empty();
    }

    public static boolean isRecordWider(final RecordSchema potentiallyWider, final RecordSchema potentiallyNarrower) {
        final List<RecordField> narrowerFields = potentiallyNarrower.getFields();

        for (final RecordField narrowerField : narrowerFields) {
            final Optional<RecordField> widerField = potentiallyWider.getField(narrowerField.getFieldName());
            if (widerField.isEmpty()) {
                return false;
            }

            if (widerField.get().getDataType().equals(narrowerField.getDataType())) {
                continue;
            }

            final Optional<DataType> widerType = getWiderType(widerField.get().getDataType(), narrowerField.getDataType());
            if (widerType.isEmpty()) {
                return false;
            }

            if (!widerType.get().equals(widerField.get().getDataType())) {
                return false;
            }
        }

        return true;
    }

    private static boolean isDecimalType(final RecordFieldType fieldType) {
        return switch (fieldType) {
            case FLOAT, DOUBLE, DECIMAL -> true;
            default -> false;
        };
    }

    private static int getIntegerTypeValue(final RecordFieldType fieldType) {
        return switch (fieldType) {
            case BIGINT -> 4;
            case LONG -> 3;
            case INT -> 2;
            case SHORT -> 1;
            case BYTE -> 0;
            default -> -1;
        };
    }

    /**
     * Converts the specified field data type into a java.sql.Types constant (INTEGER = 4, e.g.)
     *
     * @param dataType the DataType to be converted
     * @return the SQL type corresponding to the specified RecordFieldType
     */
    public static int getSQLTypeValue(final DataType dataType) {
        if (dataType == null) {
            return Types.NULL;
        }
        RecordFieldType fieldType = dataType.getFieldType();
        return switch (fieldType) {
            case BIGINT, LONG -> Types.BIGINT;
            case BOOLEAN -> Types.BOOLEAN;
            case BYTE -> Types.TINYINT;
            case CHAR -> Types.CHAR;
            case DATE -> Types.DATE;
            case DOUBLE -> Types.DOUBLE;
            case FLOAT -> Types.FLOAT;
            case DECIMAL -> Types.NUMERIC;
            case INT -> Types.INTEGER;
            case SHORT -> Types.SMALLINT;
            case STRING, UUID -> Types.VARCHAR;
            case ENUM -> Types.OTHER;
            case TIME -> Types.TIME;
            case TIMESTAMP -> Types.TIMESTAMP;
            case ARRAY -> Types.ARRAY;
            case MAP, RECORD -> Types.STRUCT;
            case CHOICE -> throw new IllegalTypeConversionException("Cannot convert CHOICE, type must be explicit");
        };
    }

    /**
     * Converts the specified java.sql.Types constant field data type (INTEGER = 4, e.g.) into a DataType
     *
     * @param sqlType the DataType to be converted
     * @return the SQL type corresponding to the specified RecordFieldType
     */
    public static DataType getDataTypeFromSQLTypeValue(final int sqlType) {
        return switch (sqlType) {
            case Types.BIGINT -> RecordFieldType.BIGINT.getDataType();
            case Types.BIT, Types.INTEGER -> RecordFieldType.INT.getDataType();
            case Types.BOOLEAN -> RecordFieldType.BOOLEAN.getDataType();
            case Types.TINYINT -> RecordFieldType.BYTE.getDataType();
            case Types.DATE -> RecordFieldType.DATE.getDataType();
            case Types.DOUBLE -> RecordFieldType.DOUBLE.getDataType();
            case Types.FLOAT -> RecordFieldType.FLOAT.getDataType();
            case Types.NUMERIC -> RecordFieldType.DECIMAL.getDataType();
            case Types.SMALLINT -> RecordFieldType.SHORT.getDataType();
            case Types.CHAR, Types.VARCHAR, Types.LONGNVARCHAR, Types.LONGVARCHAR, Types.NCHAR, Types.NVARCHAR,
                 Types.OTHER, Types.SQLXML, Types.CLOB -> RecordFieldType.STRING.getDataType();
            case Types.TIME -> RecordFieldType.TIME.getDataType();
            case Types.TIMESTAMP -> RecordFieldType.TIMESTAMP.getDataType();
            case Types.ARRAY -> RecordFieldType.ARRAY.getDataType();
            case Types.BINARY, Types.BLOB -> RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType());
            case Types.STRUCT -> RecordFieldType.RECORD.getDataType();
            default -> null;
        };
    }

    public static boolean isScalarValue(final DataType dataType, final Object value) {
        final RecordFieldType fieldType = dataType.getFieldType();

        final RecordFieldType chosenType;
        if (fieldType == RecordFieldType.CHOICE) {
            final ChoiceDataType choiceDataType = (ChoiceDataType) dataType;
            final DataType chosenDataType = chooseDataType(value, choiceDataType);
            if (chosenDataType == null) {
                return false;
            }

            chosenType = chosenDataType.getFieldType();
        } else {
            chosenType = fieldType;
        }

        return switch (chosenType) {
            case ARRAY, MAP, RECORD -> false;
            default -> true;
        };
    }

    public static Charset getCharset(String charsetName) {
        if (charsetName == null) {
            return StandardCharsets.UTF_8;
        } else {
            return Charset.forName(charsetName);
        }
    }

    /**
     * Returns true if the given value is an integer value and fits into a float variable without precision loss. This is
     * decided based on the numerical value of the input and the significant bytes used in the float.
     *
     * @param value The value to check.
     *
     * @return True in case of the value meets the conditions, false otherwise.
     */
    public static boolean isIntegerFitsToFloat(final Object value) {
        if (!(value instanceof Integer)) {
            return false;
        }

        final int intValue = (Integer) value;
        return MIN_GUARANTEED_PRECISE_WHOLE_IN_FLOAT <= intValue && intValue <= MAX_GUARANTEED_PRECISE_WHOLE_IN_FLOAT;
    }

    /**
     * Returns true if the given value is a long value and fits into a float variable without precision loss. This is
     * decided based on the numerical value of the input and the significant bytes used in the float.
     *
     * @param value The value to check.
     *
     * @return True in case of the value meets the conditions, false otherwise.
     */
    public static boolean isLongFitsToFloat(final Object value) {
        if (!(value instanceof Long)) {
            return false;
        }

        final long longValue = (Long) value;
        return MIN_GUARANTEED_PRECISE_WHOLE_IN_FLOAT <= longValue && longValue <= MAX_GUARANTEED_PRECISE_WHOLE_IN_FLOAT;
    }

    /**
     * Returns true if the given value is a long value and fits into a double variable without precision loss. This is
     * decided based on the numerical value of the input and the significant bytes used in the double.
     *
     * @param value The value to check.
     *
     * @return True in case of the value meets the conditions, false otherwise.
     */
    public static boolean isLongFitsToDouble(final Object value) {
        if (!(value instanceof Long)) {
            return false;
        }

        final long longValue = (Long) value;
        return MIN_GUARANTEED_PRECISE_WHOLE_IN_DOUBLE <= longValue && longValue <= MAX_GUARANTEED_PRECISE_WHOLE_IN_DOUBLE;
    }

    /**
     * Returns true if the given value is a BigInteger value and fits into a float variable without precision loss. This is
     * decided based on the numerical value of the input and the significant bytes used in the float.
     *
     * @param value The value to check.
     *
     * @return True in case of the value meets the conditions, false otherwise.
     */
    public static boolean isBigIntFitsToFloat(final Object value) {
        if (!(value instanceof BigInteger bigIntValue)) {
            return false;
        }

        return bigIntValue.compareTo(MIN_FLOAT_VALUE_IN_BIGINT) >= 0 && bigIntValue.compareTo(MAX_FLOAT_VALUE_IN_BIGINT) <= 0;
    }

    /**
     * Returns true if the given value is a BigInteger value and fits into a double variable without precision loss. This is
     * decided based on the numerical value of the input and the significant bytes used in the double.
     *
     * @param value The value to check.
     *
     * @return True in case of the value meets the conditions, false otherwise.
     */
    public static boolean isBigIntFitsToDouble(final Object value) {
        if (!(value instanceof BigInteger bigIntValue)) {
            return false;
        }

        return bigIntValue.compareTo(MIN_DOUBLE_VALUE_IN_BIGINT) >= 0 && bigIntValue.compareTo(MAX_DOUBLE_VALUE_IN_BIGINT) <= 0;
    }

    /**
     * Returns true in case the incoming value is a double which is within the range of float variable type.
     *
     * <p>
     * Note: the method only considers the covered range but not precision. The reason for this is that at this point the
     * double representation might already slightly differs from the original text value.
     * </p>
     *
     * @param value The value to check.
     *
     * @return True in case of the double value fits to float data type.
     */
    public static boolean isDoubleWithinFloatInterval(final Object value) {

        if (!(value instanceof Double doubleValue)) {
            return false;
        }

        return MIN_FLOAT_VALUE_IN_DOUBLE <= doubleValue && doubleValue <= MAX_FLOAT_VALUE_IN_DOUBLE;
    }

    /**
     * Checks if an incoming value satisfies the requirements of a given (numeric) type or any of it's narrow data type.
     *
     * @param value Incoming value.
     * @param fieldType The expected field type.
     *
     * @return Returns true if the incoming value satisfies the data type of any of it's narrow data types. Otherwise returns false. Only numeric data types are supported.
     */
    public static boolean isFittingNumberType(final Object value, final RecordFieldType fieldType) {
        if (NUMERIC_VALIDATORS.get(fieldType).test(value)) {
            return true;
        }

        for (final RecordFieldType recordFieldType : fieldType.getNarrowDataTypes()) {
            if (NUMERIC_VALIDATORS.get(recordFieldType).test(value)) {
                return true;
            }
        }

        return false;
    }
}
