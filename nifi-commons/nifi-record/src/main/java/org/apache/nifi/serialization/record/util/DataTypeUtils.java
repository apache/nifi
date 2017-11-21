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

import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;

public class DataTypeUtils {

    // Regexes for parsing Floting-Point numbers
    private static final String OptionalSign  = "[\\-\\+]?";
    private static final String Infinity = "(Infinity)";
    private static final String NotANumber = "(NaN)";

    private static final String Base10Digits  = "\\d+";
    private static final String Base10Decimal  = "\\." + Base10Digits;
    private static final String OptionalBase10Decimal  = Base10Decimal + "?";

    private static final String Base10Exponent      = "[eE]" + OptionalSign + Base10Digits;
    private static final String OptionalBase10Exponent = "(" + Base10Exponent + ")?";

    private static final String  doubleRegex =
        OptionalSign +
        "(" +
            Infinity + "|" +
            NotANumber + "|"+
            "(" + Base10Digits + Base10Decimal + ")" + "|" +
            "(" + Base10Digits + OptionalBase10Decimal + Base10Exponent + ")" + "|" +
            "(" + Base10Decimal + OptionalBase10Exponent + ")" +
        ")";

    private static final Pattern FLOATING_POINT_PATTERN = Pattern.compile(doubleRegex);

    private static final TimeZone gmt = TimeZone.getTimeZone("gmt");

    private static final Supplier<DateFormat> DEFAULT_DATE_FORMAT = () -> getDateFormat(RecordFieldType.DATE.getDefaultFormat());
    private static final Supplier<DateFormat> DEFAULT_TIME_FORMAT = () -> getDateFormat(RecordFieldType.TIME.getDefaultFormat());
    private static final Supplier<DateFormat> DEFAULT_TIMESTAMP_FORMAT = () -> getDateFormat(RecordFieldType.TIMESTAMP.getDefaultFormat());

    public static Object convertType(final Object value, final DataType dataType, final String fieldName) {
        return convertType(value, dataType, DEFAULT_DATE_FORMAT, DEFAULT_TIME_FORMAT, DEFAULT_TIMESTAMP_FORMAT, fieldName);
    }

    public static DateFormat getDateFormat(final RecordFieldType fieldType, final Supplier<DateFormat> dateFormat,
        final Supplier<DateFormat> timeFormat, final Supplier<DateFormat> timestampFormat) {
        switch (fieldType) {
            case DATE:
                return dateFormat.get();
            case TIME:
                return timeFormat.get();
            case TIMESTAMP:
                return timestampFormat.get();
        }

        return null;
    }

    public static Object convertType(final Object value, final DataType dataType, final Supplier<DateFormat> dateFormat, final Supplier<DateFormat> timeFormat,
        final Supplier<DateFormat> timestampFormat, final String fieldName) {

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
                return toDate(value, dateFormat, fieldName);
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
            case STRING:
                return toString(value, () -> getDateFormat(dataType.getFieldType(), dateFormat, timeFormat, timestampFormat));
            case TIME:
                return toTime(value, timeFormat, fieldName);
            case TIMESTAMP:
                return toTimestamp(value, timestampFormat, fieldName);
            case ARRAY:
                return toArray(value, fieldName);
            case MAP:
                return toMap(value, fieldName);
            case RECORD:
                final RecordDataType recordType = (RecordDataType) dataType;
                final RecordSchema childSchema = recordType.getChildSchema();
                return toRecord(value, childSchema, fieldName);
            case CHOICE: {
                final ChoiceDataType choiceDataType = (ChoiceDataType) dataType;
                final DataType chosenDataType = chooseDataType(value, choiceDataType);
                if (chosenDataType == null) {
                    throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass()
                        + " for field " + fieldName + " to any of the following available Sub-Types for a Choice: " + choiceDataType.getPossibleSubTypes());
                }

                return convertType(value, chosenDataType, fieldName);
            }
        }

        return null;
    }


    public static boolean isCompatibleDataType(final Object value, final DataType dataType) {
        switch (dataType.getFieldType()) {
            case ARRAY:
                return isArrayTypeCompatible(value);
            case BIGINT:
                return isBigIntTypeCompatible(value);
            case BOOLEAN:
                return isBooleanTypeCompatible(value);
            case BYTE:
                return isByteTypeCompatible(value);
            case CHAR:
                return isCharacterTypeCompatible(value);
            case DATE:
                return isDateTypeCompatible(value, dataType.getFormat());
            case DOUBLE:
                return isDoubleTypeCompatible(value);
            case FLOAT:
                return isFloatTypeCompatible(value);
            case INT:
                return isIntegerTypeCompatible(value);
            case LONG:
                return isLongTypeCompatible(value);
            case RECORD:
                return isRecordTypeCompatible(value);
            case SHORT:
                return isShortTypeCompatible(value);
            case TIME:
                return isTimeTypeCompatible(value, dataType.getFormat());
            case TIMESTAMP:
                return isTimestampTypeCompatible(value, dataType.getFormat());
            case STRING:
                return isStringTypeCompatible(value);
            case MAP:
                return isMapTypeCompatible(value);
            case CHOICE: {
                final DataType chosenDataType = chooseDataType(value, (ChoiceDataType) dataType);
                return chosenDataType != null;
            }
        }

        return false;
    }

    public static DataType chooseDataType(final Object value, final ChoiceDataType choiceType) {
        for (final DataType subType : choiceType.getPossibleSubTypes()) {
            if (isCompatibleDataType(value, subType)) {
                if (subType.getFieldType() == RecordFieldType.CHOICE) {
                    return chooseDataType(value, (ChoiceDataType) subType);
                }

                return subType;
            }
        }

        return null;
    }

    public static Record toRecord(final Object value, final RecordSchema recordSchema, final String fieldName) {
        if (value == null) {
            return null;
        }

        if (value instanceof Record) {
            return ((Record) value);
        }

        if (value instanceof Map) {
            if (recordSchema == null) {
                throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass()
                    + " to Record for field " + fieldName + " because the value is a Map but no Record Schema was provided");
            }

            final Map<?, ?> map = (Map<?, ?>) value;
            final Map<String, Object> coercedValues = new HashMap<>();

            for (final Map.Entry<?, ?> entry : map.entrySet()) {
                final Object keyValue = entry.getKey();
                if (keyValue == null) {
                    continue;
                }

                final String key = keyValue.toString();
                final Optional<DataType> desiredTypeOption = recordSchema.getDataType(key);
                if (!desiredTypeOption.isPresent()) {
                    continue;
                }

                final Object rawValue = entry.getValue();
                final Object coercedValue = convertType(rawValue, desiredTypeOption.get(), fieldName);
                coercedValues.put(key, coercedValue);
            }

            return new MapRecord(recordSchema, coercedValues);
        }

        throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to Record for field " + fieldName);
    }

    public static boolean isRecordTypeCompatible(final Object value) {
        return value != null && value instanceof Record;
    }

    public static Object[] toArray(final Object value, final String fieldName) {
        if (value == null) {
            return null;
        }

        if (value instanceof Object[]) {
            return (Object[]) value;
        }

        throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to Object Array for field " + fieldName);
    }

    public static boolean isArrayTypeCompatible(final Object value) {
        return value != null && value instanceof Object[];
    }

    @SuppressWarnings("unchecked")
    public static Map<String, Object> toMap(final Object value, final String fieldName) {
        if (value == null) {
            return null;
        }

        if (value instanceof Map) {
            final Map<?, ?> original = (Map<?, ?>) value;

            boolean keysAreStrings = true;
            for (final Object key : original.keySet()) {
                if (!(key instanceof String)) {
                    keysAreStrings = false;
                }
            }

            if (keysAreStrings) {
                return (Map<String, Object>) value;
            }

            final Map<String, Object> transformed = new HashMap<>();
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

        if (value instanceof Record) {
            final Record record = (Record) value;
            final RecordSchema recordSchema = record.getSchema();
            if (recordSchema == null) {
                throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type Record to Map for field " + fieldName
                    + " because Record does not have an associated Schema");
            }

            final Map<String, Object> map = new HashMap<>();
            for (final String recordFieldName : recordSchema.getFieldNames()) {
                map.put(recordFieldName, record.getValue(recordFieldName));
            }

            return map;
        }

        throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to Map for field " + fieldName);
    }

    public static boolean isMapTypeCompatible(final Object value) {
        return value != null && value instanceof Map;
    }


    public static String toString(final Object value, final Supplier<DateFormat> format) {
        if (value == null) {
            return null;
        }

        if (value instanceof String) {
            return (String) value;
        }

        if (format == null && value instanceof java.util.Date) {
            return String.valueOf(((java.util.Date) value).getTime());
        }

        if (value instanceof java.util.Date) {
            return formatDate((java.util.Date) value, format);
        }

        return value.toString();
    }

    private static String formatDate(final java.util.Date date, final Supplier<DateFormat> formatSupplier) {
        final DateFormat dateFormat = formatSupplier.get();
        if (dateFormat == null) {
            return String.valueOf((date).getTime());
        }

        return dateFormat.format(date);
    }

    public static String toString(final Object value, final String format) {
        if (value == null) {
            return null;
        }

        if (value instanceof String) {
            return (String) value;
        }

        if (format == null && value instanceof java.util.Date) {
            return String.valueOf(((java.util.Date) value).getTime());
        }

        if (value instanceof java.sql.Date) {
            return getDateFormat(format).format((java.util.Date) value);
        }
        if (value instanceof java.sql.Time) {
            return getDateFormat(format).format((java.util.Date) value);
        }
        if (value instanceof java.sql.Timestamp) {
            return getDateFormat(format).format((java.util.Date) value);
        }
        if (value instanceof java.util.Date) {
            return getDateFormat(format).format((java.util.Date) value);
        }

        if (value instanceof Object[]) {
            return Arrays.toString((Object[]) value);
        }

        return value.toString();
    }

    public static boolean isStringTypeCompatible(final Object value) {
        return value != null;
    }

    public static java.sql.Date toDate(final Object value, final Supplier<DateFormat> format, final String fieldName) {
        if (value == null) {
            return null;
        }

        if (value instanceof Date) {
            return (Date) value;
        }

        if (value instanceof Number) {
            final long longValue = ((Number) value).longValue();
            return new Date(longValue);
        }

        if (value instanceof String) {
            try {
                final String string = ((String) value).trim();
                if (string.isEmpty()) {
                    return null;
                }

                if (format == null) {
                    return new Date(Long.parseLong(string));
                }

                final DateFormat dateFormat = format.get();
                if (dateFormat == null) {
                    return new Date(Long.parseLong(string));
                }
                final java.util.Date utilDate = dateFormat.parse(string);
                return new Date(utilDate.getTime());
            } catch (final ParseException | NumberFormatException e) {
                throw new IllegalTypeConversionException("Could not convert value [" + value
                    + "] of type java.lang.String to Date because the value is not in the expected date format: " + format + " for field " + fieldName);
            }
        }

        throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to Date for field " + fieldName);
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
                getDateFormat(format).parse((String) value);
                return true;
            } catch (final ParseException e) {
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

    public static Time toTime(final Object value, final Supplier<DateFormat> format, final String fieldName) {
        if (value == null) {
            return null;
        }

        if (value instanceof Time) {
            return (Time) value;
        }

        if (value instanceof Number) {
            final long longValue = ((Number) value).longValue();
            return new Time(longValue);
        }

        if (value instanceof String) {
            try {
                final String string = ((String) value).trim();
                if (string.isEmpty()) {
                    return null;
                }

                if (format == null) {
                    return new Time(Long.parseLong(string));
                }

                final DateFormat dateFormat = format.get();
                if (dateFormat == null) {
                    return new Time(Long.parseLong(string));
                }
                final java.util.Date utilDate = dateFormat.parse(string);
                return new Time(utilDate.getTime());
            } catch (final ParseException e) {
                throw new IllegalTypeConversionException("Could not convert value [" + value
                    + "] of type java.lang.String to Time for field " + fieldName + " because the value is not in the expected date format: " + format);
            }
        }

        throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to Time for field " + fieldName);
    }

    public static DateFormat getDateFormat(final String format) {
        if (format == null) {
            return null;
        }
        final DateFormat df = new SimpleDateFormat(format);
        df.setTimeZone(gmt);
        return df;
    }

    public static boolean isTimeTypeCompatible(final Object value, final String format) {
        return isDateTypeCompatible(value, format);
    }

    public static Timestamp toTimestamp(final Object value, final Supplier<DateFormat> format, final String fieldName) {
        if (value == null) {
            return null;
        }

        if (value instanceof Timestamp) {
            return (Timestamp) value;
        }

        if (value instanceof Number) {
            final long longValue = ((Number) value).longValue();
            return new Timestamp(longValue);
        }

        if (value instanceof String) {
            try {
                final String string = ((String) value).trim();
                if (string.isEmpty()) {
                    return null;
                }

                if (format == null) {
                    return new Timestamp(Long.parseLong(string));
                }

                final DateFormat dateFormat = format.get();
                if (dateFormat == null) {
                    return new Timestamp(Long.parseLong(string));
                }
                final java.util.Date utilDate = dateFormat.parse(string);
                return new Timestamp(utilDate.getTime());
            } catch (final ParseException e) {
                throw new IllegalTypeConversionException("Could not convert value [" + value
                    + "] of type java.lang.String to Timestamp for field " + fieldName + " because the value is not in the expected date format: " + format);
            }
        }

        throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to Timestamp for field " + fieldName);
    }

    public static boolean isTimestampTypeCompatible(final Object value, final String format) {
        return isDateTypeCompatible(value, format);
    }


    public static BigInteger toBigInt(final Object value, final String fieldName) {
        if (value == null) {
            return null;
        }

        if (value instanceof BigInteger) {
            return (BigInteger) value;
        }
        if (value instanceof Long) {
            return BigInteger.valueOf((Long) value);
        }

        throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to BigInteger for field " + fieldName);
    }

    public static boolean isBigIntTypeCompatible(final Object value) {
        return value == null && (value instanceof BigInteger || value instanceof Long);
    }

    public static Boolean toBoolean(final Object value, final String fieldName) {
        if (value == null) {
            return null;
        }

        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        if (value instanceof String) {
            final String string = (String) value;
            if (string.equalsIgnoreCase("true")) {
                return Boolean.TRUE;
            } else if (string.equalsIgnoreCase("false")) {
                return Boolean.FALSE;
            }
        }

        throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to Boolean for field " + fieldName);
    }

    public static boolean isBooleanTypeCompatible(final Object value) {
        if (value == null) {
            return false;
        }
        if (value instanceof Boolean) {
            return true;
        }
        if (value instanceof String) {
            final String string = (String) value;
            return string.equalsIgnoreCase("true") || string.equalsIgnoreCase("false");
        }
        return false;
    }

    public static Double toDouble(final Object value, final String fieldName) {
        if (value == null) {
            return null;
        }

        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }

        if (value instanceof String) {
            return Double.parseDouble((String) value);
        }

        throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to Double for field " + fieldName);
    }

    public static boolean isDoubleTypeCompatible(final Object value) {
        return isNumberTypeCompatible(value, s -> isDouble(s));
    }

    private static boolean isNumberTypeCompatible(final Object value, final Predicate<String> stringPredicate) {
        if (value == null) {
            return false;
        }

        if (value instanceof Number) {
            return true;
        }

        if (value instanceof String) {
            return stringPredicate.test((String) value);
        }

        return false;
    }

    public static Float toFloat(final Object value, final String fieldName) {
        if (value == null) {
            return null;
        }

        if (value instanceof Number) {
            return ((Number) value).floatValue();
        }

        if (value instanceof String) {
            return Float.parseFloat((String) value);
        }

        throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to Float for field " + fieldName);
    }

    public static boolean isFloatTypeCompatible(final Object value) {
        return isNumberTypeCompatible(value, s -> isFloatingPoint(s));
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
        if (value == null) {
            return null;
        }

        if (value instanceof Number) {
            return ((Number) value).longValue();
        }

        if (value instanceof String) {
            return Long.parseLong((String) value);
        }

        if (value instanceof java.util.Date) {
            return ((java.util.Date) value).getTime();
        }

        throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to Long for field " + fieldName);
    }

    public static boolean isLongTypeCompatible(final Object value) {
        if (value == null) {
            return false;
        }

        if (value instanceof Number) {
            return true;
        }

        if (value instanceof java.util.Date) {
            return true;
        }

        if (value instanceof String) {
            return isIntegral((String) value, Long.MIN_VALUE, Long.MAX_VALUE);
        }

        return false;
    }

    private static boolean isIntegral(final String value, final long minValue, final long maxValue) {
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

        try {
            final long longValue = Long.parseLong(value);
            return longValue >= minValue && longValue <= maxValue;
        } catch (final NumberFormatException nfe) {
            // In case the value actually exceeds the max value of a Long
            return false;
        }
    }


    public static Integer toInteger(final Object value, final String fieldName) {
        if (value == null) {
            return null;
        }

        if (value instanceof Number) {
            return ((Number) value).intValue();
        }

        if (value instanceof String) {
            return Integer.parseInt((String) value);
        }

        throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to Integer for field " + fieldName);
    }

    public static boolean isIntegerTypeCompatible(final Object value) {
        return isNumberTypeCompatible(value, s -> isIntegral(s, Integer.MIN_VALUE, Integer.MAX_VALUE));
    }


    public static Short toShort(final Object value, final String fieldName) {
        if (value == null) {
            return null;
        }

        if (value instanceof Number) {
            return ((Number) value).shortValue();
        }

        if (value instanceof String) {
            return Short.parseShort((String) value);
        }

        throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to Short for field " + fieldName);
    }

    public static boolean isShortTypeCompatible(final Object value) {
        return isNumberTypeCompatible(value, s -> isIntegral(s, Short.MIN_VALUE, Short.MAX_VALUE));
    }

    public static Byte toByte(final Object value, final String fieldName) {
        if (value == null) {
            return null;
        }

        if (value instanceof Number) {
            return ((Number) value).byteValue();
        }

        if (value instanceof String) {
            return Byte.parseByte((String) value);
        }

        throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to Byte for field " + fieldName);
    }

    public static boolean isByteTypeCompatible(final Object value) {
        return isNumberTypeCompatible(value, s -> isIntegral(s, Byte.MIN_VALUE, Byte.MAX_VALUE));
    }


    public static Character toCharacter(final Object value, final String fieldName) {
        if (value == null) {
            return null;
        }

        if (value instanceof Character) {
            return ((Character) value);
        }

        if (value instanceof CharSequence) {
            final CharSequence charSeq = (CharSequence) value;
            if (charSeq.length() == 0) {
                throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass()
                    + " to Character because it has a length of 0 for field " + fieldName);
            }

            return charSeq.charAt(0);
        }

        throw new IllegalTypeConversionException("Cannot convert value [" + value + "] of type " + value.getClass() + " to Character for field " + fieldName);
    }

    public static boolean isCharacterTypeCompatible(final Object value) {
        return value != null && (value instanceof Character || (value instanceof CharSequence && ((CharSequence) value).length() > 0));
    }

    public static RecordSchema merge(final RecordSchema thisSchema, final RecordSchema otherSchema) {
        if (thisSchema == null) {
            return otherSchema;
        }
        if (otherSchema == null) {
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

            final Integer index = Integer.valueOf(i);

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

        if (!Objects.equals(thisField.getDefaultValue(), otherField.getDefaultValue())) {
            return true;
        }

        return false;
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

        final DataType dataType;
        if (thisField.getDataType().equals(otherField.getDataType())) {
            dataType = thisField.getDataType();
        } else {
            dataType = RecordFieldType.CHOICE.getChoiceDataType(thisField.getDataType(), otherField.getDataType());
        }

        return new RecordField(fieldName, dataType, defaultValue, aliases);
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

        switch (chosenType) {
            case ARRAY:
            case MAP:
            case RECORD:
                return false;
        }

        return true;
    }
}
