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

package org.apache.nifi.serialization.record;

import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.serialization.record.util.IllegalTypeConversionException;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.DateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.DoubleAdder;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestDataTypeUtils {
    private static final ZoneId SYSTEM_DEFAULT_ZONE_ID = ZoneOffset.systemDefault();

    private static final String ISO_8601_YEAR_MONTH_DAY = "2000-01-01";

    private static final String CUSTOM_MONTH_DAY_YEAR = "01-01-2000";

    private static final String CUSTOM_MONTH_DAY_YEAR_PATTERN = "MM-dd-yyyy";

    private static final String DATE_FIELD = "date";

    /**
     * This is a unit test to verify conversion java Date objects to Timestamps. Support for this was
     * required in order to help the MongoDB packages handle date/time logical types in the Record API.
     */
    @Test
    public void testDateToTimestamp() {
        java.util.Date date = new java.util.Date();
        Timestamp ts = DataTypeUtils.toTimestamp(date, null, null);

        assertNotNull(ts);
        assertEquals(ts.getTime(), date.getTime(), "Times didn't match");

        java.sql.Date sDate = new java.sql.Date(date.getTime());
        ts = DataTypeUtils.toTimestamp(date, null, null);
        assertNotNull(ts);
        assertEquals(ts.getTime(), sDate.getTime(), "Times didn't match");
    }

    /*
     * This was a bug in NiFi 1.8 where converting from a Timestamp to a Date with the record path API
     * would throw an exception.
     */
    @Test
    public void testTimestampToDate() {
        java.util.Date date = new java.util.Date();
        Timestamp ts = DataTypeUtils.toTimestamp(date, null, null);
        assertNotNull(ts);

        java.sql.Date output = DataTypeUtils.toDate(ts, null, null);
        assertNotNull(output);
        assertEquals(output.getTime(), ts.getTime(), "Timestamps didn't match");
    }

    @Test
    public void testConvertRecordMapToJavaMap() {
        assertNull(DataTypeUtils.convertRecordMapToJavaMap(null, null));
        assertNull(DataTypeUtils.convertRecordMapToJavaMap(null, RecordFieldType.MAP.getDataType()));
        Map<String,Object> resultMap = DataTypeUtils.convertRecordMapToJavaMap(new HashMap<>(), RecordFieldType.MAP.getDataType());
        assertNotNull(resultMap);
        assertTrue(resultMap.isEmpty());

        int[] intArray = {3,2,1};

        Map<String,Object> inputMap = new HashMap<String,Object>() {{
            put("field1", "hello");
            put("field2", 1);
            put("field3", intArray);
        }};

        resultMap = DataTypeUtils.convertRecordMapToJavaMap(inputMap, RecordFieldType.STRING.getDataType());
        assertNotNull(resultMap);
        assertFalse(resultMap.isEmpty());
        assertEquals("hello", resultMap.get("field1"));
        assertEquals(1, resultMap.get("field2"));
        assertTrue(resultMap.get("field3") instanceof int[]);
        assertNull(resultMap.get("field4"));

    }

    @Test
    public void testConvertRecordArrayToJavaArray() {
        assertNull(DataTypeUtils.convertRecordArrayToJavaArray(null, null));
        assertNull(DataTypeUtils.convertRecordArrayToJavaArray(null, RecordFieldType.STRING.getDataType()));
        String[] stringArray = {"Hello", "World!"};
        Object[] resultArray = DataTypeUtils.convertRecordArrayToJavaArray(stringArray, RecordFieldType.STRING.getDataType());
        assertNotNull(resultArray);
        for(Object o : resultArray) {
            assertTrue(o instanceof String);
        }
    }

    @Test
    public void testConvertArrayOfRecordsToJavaArray() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("stringField", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("intField", RecordFieldType.INT.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        final Map<String, Object> values1 = new HashMap<>();
        values1.put("stringField", "hello");
        values1.put("intField", 5);
        final Record inputRecord1 = new MapRecord(schema, values1);

        final Map<String, Object> values2 = new HashMap<>();
        values2.put("stringField", "world");
        values2.put("intField", 50);
        final Record inputRecord2 = new MapRecord(schema, values2);

        Object[] recordArray = {inputRecord1, inputRecord2};
        Object resultObj = DataTypeUtils.convertRecordFieldtoObject(recordArray, RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.RECORD.getRecordDataType(schema)));
        assertNotNull(resultObj);
        assertTrue(resultObj instanceof Object[]);
        Object[] resultArray = (Object[]) resultObj;
        for(Object o : resultArray) {
            assertTrue(o instanceof Map);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testConvertRecordFieldToObject() {
        assertNull(DataTypeUtils.convertRecordFieldtoObject(null, null));
        assertNull(DataTypeUtils.convertRecordFieldtoObject(null, RecordFieldType.MAP.getDataType()));

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("defaultOfHello", RecordFieldType.STRING.getDataType(), "hello"));
        fields.add(new RecordField("noDefault", RecordFieldType.CHOICE.getChoiceDataType(RecordFieldType.STRING.getDataType())));
        fields.add(new RecordField("intField", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("intArray", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.INT.getDataType())));

        // Map of Records with Arrays
        List<RecordField> nestedRecordFields = new ArrayList<>();
        nestedRecordFields.add(new RecordField("a", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.INT.getDataType())));
        nestedRecordFields.add(new RecordField("b", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType())));
        RecordSchema nestedRecordSchema = new SimpleRecordSchema(nestedRecordFields);

        fields.add(new RecordField("complex", RecordFieldType.MAP.getMapDataType(RecordFieldType.RECORD.getRecordDataType(nestedRecordSchema))));

        final RecordSchema schema = new SimpleRecordSchema(fields);
        final Map<String, Object> values = new HashMap<>();
        values.put("noDefault", "world");
        values.put("intField", 5);
        values.put("intArray", new Integer[] {3,2,1});
        final Map<String, Object> complexValues = new HashMap<>();

        final Map<String, Object> complexValueRecord1 = new HashMap<>();
        complexValueRecord1.put("a",new Integer[] {3,2,1});
        complexValueRecord1.put("b",new Integer[] {5,4,3});

        final Map<String, Object> complexValueRecord2 = new HashMap<>();
        complexValueRecord2.put("a",new String[] {"hello","world!"});
        complexValueRecord2.put("b",new String[] {"5","4","3"});

        complexValues.put("complex1", DataTypeUtils.toRecord(complexValueRecord1, nestedRecordSchema, "complex1", StandardCharsets.UTF_8));
        complexValues.put("complex2", DataTypeUtils.toRecord(complexValueRecord2, nestedRecordSchema, "complex2", StandardCharsets.UTF_8));

        values.put("complex", complexValues);
        final Record inputRecord = new MapRecord(schema, values);

        Object o = DataTypeUtils.convertRecordFieldtoObject(inputRecord, RecordFieldType.RECORD.getRecordDataType(schema));
        assertTrue(o instanceof Map);
        Map<String,Object> outputMap = (Map<String,Object>) o;
        assertEquals("hello", outputMap.get("defaultOfHello"));
        assertEquals("world", outputMap.get("noDefault"));
        o = outputMap.get("intField");
        assertEquals(5,o);
        o = outputMap.get("intArray");
        assertTrue(o instanceof Integer[]);
        Integer[] intArray = (Integer[])o;
        assertEquals(3, intArray.length);
        assertEquals((Integer)3, intArray[0]);
        o = outputMap.get("complex");
        assertTrue(o instanceof Map);
        Map<String,Object> nestedOutputMap = (Map<String,Object>)o;
        o = nestedOutputMap.get("complex1");
        assertTrue(o instanceof Map);
        Map<String,Object> complex1 = (Map<String,Object>)o;
        o = complex1.get("a");
        assertTrue(o instanceof Integer[]);
        assertEquals((Integer)2, ((Integer[])o)[1]);
        o = complex1.get("b");
        assertTrue(o instanceof Integer[]);
        assertEquals((Integer)3, ((Integer[])o)[2]);
        o = nestedOutputMap.get("complex2");
        assertTrue(o instanceof Map);
        Map<String,Object> complex2 = (Map<String,Object>)o;
        o = complex2.get("a");
        assertTrue(o instanceof String[]);
        assertEquals("hello", ((String[])o)[0]);
        o = complex2.get("b");
        assertTrue(o instanceof String[]);
        assertEquals("4", ((String[])o)[1]);

    }

    @Test
    public void testToArray() {
        final List<String> list = Arrays.asList("Seven", "Eleven", "Thirteen");

        final Object[] array = DataTypeUtils.toArray(list, "list", null);

        assertEquals(list.size(), array.length);
        for (int i = 0; i < list.size(); i++) {
            assertEquals(list.get(i), array[i]);
        }
    }

    @Test
    public void testStringToBytes() {
        Object bytes = DataTypeUtils.convertType("Hello", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType()),null, StandardCharsets.UTF_8);
        assertTrue(bytes instanceof Byte[]);
        assertNotNull(bytes);
        Byte[] b = (Byte[]) bytes;
        assertEquals((long) 72, (long) b[0], "Conversion from String to byte[] failed");  // H
        assertEquals((long) 101, (long) b[1], "Conversion from String to byte[] failed" ); // e
        assertEquals((long) 108, (long) b[2], "Conversion from String to byte[] failed" ); // l
        assertEquals((long) 108, (long) b[3], "Conversion from String to byte[] failed" ); // l
        assertEquals((long) 111, (long) b[4], "Conversion from String to byte[] failed" ); // o
    }

    @Test
    public void testBytesToString() {
        Object s = DataTypeUtils.convertType("Hello".getBytes(StandardCharsets.UTF_16), RecordFieldType.STRING.getDataType(),null, StandardCharsets.UTF_16);
        assertNotNull(s);
        assertTrue(s instanceof String);
        assertEquals("Hello", s, "Conversion from byte[] to String failed");
    }

    @Test
    public void testBytesToBytes() {
        Object b = DataTypeUtils.convertType("Hello".getBytes(StandardCharsets.UTF_16), RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType()),null, StandardCharsets.UTF_16);
        assertNotNull(b);
        assertTrue(b instanceof Byte[]);
        assertEquals((Object) "Hello".getBytes(StandardCharsets.UTF_16)[0], ((Byte[]) b)[0], "Conversion from byte[] to String failed at char 0");
    }

    @Test
    public void testConvertToBigDecimalWhenInputIsValid() {
        // given
        final BigDecimal expectedValue = BigDecimal.valueOf(12L);

        // when & then
        whenExpectingValidBigDecimalConversion(expectedValue, BigDecimal.valueOf(12L));
        whenExpectingValidBigDecimalConversion(expectedValue, (byte) 12);
        whenExpectingValidBigDecimalConversion(expectedValue, (short) 12);
        whenExpectingValidBigDecimalConversion(expectedValue, 12);
        whenExpectingValidBigDecimalConversion(expectedValue, 12L);
        whenExpectingValidBigDecimalConversion(expectedValue, BigInteger.valueOf(12L));
        whenExpectingValidBigDecimalConversion(expectedValue, 12F);
        whenExpectingValidBigDecimalConversion(expectedValue, 12.000F);
        whenExpectingValidBigDecimalConversion(expectedValue, 12D);
        whenExpectingValidBigDecimalConversion(expectedValue, 12.000D);
        whenExpectingValidBigDecimalConversion(expectedValue, "12");
        whenExpectingValidBigDecimalConversion(expectedValue, "12.000");
    }

    @Test
    public void testConvertToBigDecimalWhenNullInput() {
        assertNull(DataTypeUtils.convertType(null, RecordFieldType.DECIMAL.getDecimalDataType(30, 10), null, StandardCharsets.UTF_8));
    }

    @Test
    public void testConvertToBigDecimalWhenInputStringIsInvalid() {
        assertThrows(IllegalTypeConversionException.class, () -> DataTypeUtils.convertType("test", RecordFieldType.DECIMAL.getDecimalDataType(30, 10),
                null, StandardCharsets.UTF_8));
    }

    @Test
    public void testConvertToBigDecimalWhenUnsupportedType() {
        assertThrows(IllegalTypeConversionException.class, () -> DataTypeUtils.convertType(new ArrayList<Double>(), RecordFieldType.DECIMAL.getDecimalDataType(30, 10),
                null, StandardCharsets.UTF_8));
    }

    @Test
    public void testConvertToBigDecimalWhenUnsupportedNumberType() {
        assertThrows(IllegalTypeConversionException.class, () -> DataTypeUtils.convertType(new DoubleAdder(), RecordFieldType.DECIMAL.getDecimalDataType(30, 10),
                null, StandardCharsets.UTF_8));
    }

    @Test
    public void testCompatibleDataTypeBigDecimal() {
        // given
        final DataType dataType = RecordFieldType.DECIMAL.getDecimalDataType(30, 10);

        // when & then
        assertTrue(DataTypeUtils.isCompatibleDataType(new BigDecimal("1.2345678901234567890"), dataType));
        assertTrue(DataTypeUtils.isCompatibleDataType(new BigInteger("12345678901234567890"), dataType));
        assertTrue(DataTypeUtils.isCompatibleDataType(1234567890123456789L, dataType));
        assertTrue(DataTypeUtils.isCompatibleDataType(1, dataType));
        assertTrue(DataTypeUtils.isCompatibleDataType((byte) 1, dataType));
        assertTrue(DataTypeUtils.isCompatibleDataType((short) 1, dataType));
        assertTrue(DataTypeUtils.isCompatibleDataType("1.2345678901234567890", dataType));
        assertTrue(DataTypeUtils.isCompatibleDataType(3.1F, dataType));
        assertTrue(DataTypeUtils.isCompatibleDataType(3.0D, dataType));
        assertFalse(DataTypeUtils.isCompatibleDataType("1234567XYZ", dataType));
        assertFalse(DataTypeUtils.isCompatibleDataType(new Long[]{1L, 2L}, dataType));
    }

    @Test
    public void testInferDataTypeWithBigDecimal() {
        assertEquals(RecordFieldType.DECIMAL.getDecimalDataType(3, 1), DataTypeUtils.inferDataType(BigDecimal.valueOf(12.3D), null));
    }

    @Test
    public void testIsBigDecimalTypeCompatible() {
        assertTrue(DataTypeUtils.isDecimalTypeCompatible((byte) 13));
        assertTrue(DataTypeUtils.isDecimalTypeCompatible((short) 13));
        assertTrue(DataTypeUtils.isDecimalTypeCompatible(12));
        assertTrue(DataTypeUtils.isDecimalTypeCompatible(12L));
        assertTrue(DataTypeUtils.isDecimalTypeCompatible(BigInteger.valueOf(12L)));
        assertTrue(DataTypeUtils.isDecimalTypeCompatible(12.123F));
        assertTrue(DataTypeUtils.isDecimalTypeCompatible(12.123D));
        assertTrue(DataTypeUtils.isDecimalTypeCompatible(BigDecimal.valueOf(12.123D)));
        assertTrue(DataTypeUtils.isDecimalTypeCompatible("123"));

        assertFalse(DataTypeUtils.isDecimalTypeCompatible(null));
        assertFalse(DataTypeUtils.isDecimalTypeCompatible("test"));
        assertFalse(DataTypeUtils.isDecimalTypeCompatible(new ArrayList<>()));
        // Decimal handling does not support NaN and Infinity as the underlying BigDecimal is unable to parse
        assertFalse(DataTypeUtils.isDecimalTypeCompatible("NaN"));
        assertFalse(DataTypeUtils.isDecimalTypeCompatible("Infinity"));
    }

    @Test
    public void testGetSQLTypeValueWithBigDecimal() {
        assertEquals(Types.NUMERIC, DataTypeUtils.getSQLTypeValue(RecordFieldType.DECIMAL.getDecimalDataType(30, 10)));
    }

    @Test
    public void testGetDataTypeFromSQLTypeValue() {
        assertEquals(RecordFieldType.STRING.getDataType(), DataTypeUtils.getDataTypeFromSQLTypeValue(Types.CLOB));
        assertEquals(RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType()), DataTypeUtils.getDataTypeFromSQLTypeValue(Types.BLOB));
        assertEquals(RecordFieldType.STRING.getDataType(), DataTypeUtils.getDataTypeFromSQLTypeValue(Types.CHAR));
    }

    @Test
    public void testChooseDataTypeWhenExpectedIsBigDecimal() {
        // GIVEN
        final List<DataType> dataTypes = Arrays.asList(
                RecordFieldType.FLOAT.getDataType(),
                RecordFieldType.DOUBLE.getDataType(),
                RecordFieldType.DECIMAL.getDecimalDataType(2, 1),
                RecordFieldType.DECIMAL.getDecimalDataType(20, 10)
        );

        final Object value = new BigDecimal("1.2");
        final DataType expected = RecordFieldType.DECIMAL.getDecimalDataType(2, 1);

        // WHEN
        // THEN
        testChooseDataTypeAlsoReverseTypes(value, dataTypes, expected);
    }

    @Test
    public void testFloatingPointCompatibility() {
        final String[] prefixes = new String[] {"", "-", "+"};
        final String[] exponents = new String[] {"e0", "e1", "e-1", "E0", "E1", "E-1"};
        final String[] decimals = new String[] {"", ".0", ".1", "."};

        for (final String prefix : prefixes) {
            for (final String decimal : decimals) {
                for (final String exp : exponents) {
                    String toTest = prefix + "100" + decimal + exp;
                    assertTrue(DataTypeUtils.isFloatTypeCompatible(toTest), toTest + " not valid float");
                    assertTrue(DataTypeUtils.isDoubleTypeCompatible(toTest), toTest + " not valid double");

                    Double.parseDouble(toTest); // ensure we can actually parse it
                    Float.parseFloat(toTest);

                    if (decimal.length() > 1) {
                        toTest = prefix + decimal + exp;
                        assertTrue(DataTypeUtils.isFloatTypeCompatible(toTest), toTest + " not valid float");
                        assertTrue(DataTypeUtils.isDoubleTypeCompatible(toTest), toTest + " not valid double");
                        Double.parseDouble(toTest); // ensure we can actually parse it
                        Float.parseFloat(toTest);
                    }
                }
            }
        }
    }

    @Test
    public void testIsCompatibleDataTypeMap() {
        Map<String,Object> testMap = new HashMap<>();
        testMap.put("Hello", "World");
        assertTrue(DataTypeUtils.isCompatibleDataType(testMap, RecordFieldType.RECORD.getDataType()));
    }

    @Test
    public void testIsCompatibleDataTypeBigint() {
        final DataType dataType = RecordFieldType.BIGINT.getDataType();
        assertTrue(DataTypeUtils.isCompatibleDataType(new BigInteger("12345678901234567890"), dataType));
        assertTrue(DataTypeUtils.isCompatibleDataType(1234567890123456789L, dataType));
        assertTrue(DataTypeUtils.isCompatibleDataType(1, dataType));
        assertTrue(DataTypeUtils.isCompatibleDataType((short) 1, dataType));
        assertTrue(DataTypeUtils.isCompatibleDataType("12345678901234567890", dataType));
        assertTrue(DataTypeUtils.isCompatibleDataType(3.1f, dataType));
        assertTrue(DataTypeUtils.isCompatibleDataType(3.0, dataType));
        assertFalse(DataTypeUtils.isCompatibleDataType("1234567XYZ", dataType));
        assertFalse(DataTypeUtils.isCompatibleDataType(new Long[]{1L, 2L}, dataType));
    }

    @Test
    public void testIsCompatibleDataTypeInteger() {
        final DataType dataType = RecordFieldType.INT.getDataType();
        assertTrue(DataTypeUtils.isCompatibleDataType(new Integer("1234567"), dataType));
        assertTrue(DataTypeUtils.isCompatibleDataType("1234567", dataType));
        assertFalse(DataTypeUtils.isCompatibleDataType(new BigInteger("12345678901234567890"), dataType));
        assertFalse(DataTypeUtils.isCompatibleDataType(1234567890123456789L, dataType));
        assertTrue(DataTypeUtils.isCompatibleDataType(1, dataType));
        assertTrue(DataTypeUtils.isCompatibleDataType((short) 1, dataType));
        assertFalse(DataTypeUtils.isCompatibleDataType("12345678901234567890", dataType));
        assertTrue(DataTypeUtils.isCompatibleDataType(3.1f, dataType));
        assertTrue(DataTypeUtils.isCompatibleDataType(3.0, dataType));
        assertFalse(DataTypeUtils.isCompatibleDataType("1234567XYZ", dataType));
        assertFalse(DataTypeUtils.isCompatibleDataType(new Long[]{1L, 2L}, dataType));
    }

    @Test
    public void testIsCompatibleDataTypeArrayDifferentElementTypes() {
        Object[] array = new Object[]{"2", 1};
        assertTrue(DataTypeUtils.isCompatibleDataType(array, RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.INT.getDataType())));
        array = new Object[]{Collections.singletonMap("hello", "world"), 1};
        assertFalse(DataTypeUtils.isCompatibleDataType(array, RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.INT.getDataType())));
    }

    @Test
    public void testConvertDataTypeBigint() {
        final Function<Object, BigInteger> toBigInteger = v -> (BigInteger) DataTypeUtils.convertType(v, RecordFieldType.BIGINT.getDataType(), "field");
        assertEquals(new BigInteger("12345678901234567890"), toBigInteger.apply(new BigInteger("12345678901234567890")));
        assertEquals(new BigInteger("1234567890123456789"), toBigInteger.apply(1234567890123456789L));
        assertEquals(new BigInteger("1"), toBigInteger.apply(1));
        assertEquals(new BigInteger("1"), toBigInteger.apply((short) 1));
        // Decimals are truncated.
        assertEquals(new BigInteger("3"), toBigInteger.apply(3.4f));
        assertEquals(new BigInteger("3"), toBigInteger.apply(3.9f));
        assertEquals(new BigInteger("12345678901234567890"), toBigInteger.apply("12345678901234567890"));
        Exception e = null;
        try {
            toBigInteger.apply("1234567XYZ");
        } catch (IllegalTypeConversionException itce) {
            e = itce;
        }
        assertNotNull(e);
    }

    @Test
    public void testFindMostSuitableTypeByStringValueShouldReturnEvenWhenOneTypeThrowsException() {
        String valueAsString = "value";

        String nonMatchingType = "nonMatchingType";
        String throwsExceptionType = "throwsExceptionType";
        String matchingType = "matchingType";

        List<String> types = Arrays.asList(
                nonMatchingType,
                throwsExceptionType,
                matchingType
        );
        Optional<String> expected = Optional.of(matchingType);

        AtomicBoolean exceptionThrown = new AtomicBoolean(false);

        Function<String, DataType> dataTypeMapper = type -> {
            if (type.equals(nonMatchingType)) {
                return RecordFieldType.BOOLEAN.getDataType();
            } else if (type.equals(throwsExceptionType)) {
                return new DataType(RecordFieldType.DATE, null) {
                    @Override
                    public String getFormat() {
                        exceptionThrown.set(true);
                        throw new RuntimeException("maching error");
                    }
                };
            } else if (type.equals(matchingType)) {
                return RecordFieldType.STRING.getDataType();
            }

            return null;
        };

        Optional<String> actual = DataTypeUtils.findMostSuitableTypeByStringValue(valueAsString, types, dataTypeMapper);
        assertTrue(exceptionThrown.get(), "Exception not thrown during test as intended.");
        assertEquals(expected, actual);
    }

    @Test
    public void testChooseDataTypeWhenInt_vs_INT_FLOAT_ThenShouldReturnINT() {
        // GIVEN
        List<DataType> dataTypes = Arrays.asList(
                RecordFieldType.INT.getDataType(),
                RecordFieldType.FLOAT.getDataType()
        );

        Object value = 1;
        DataType expected = RecordFieldType.INT.getDataType();

        // WHEN
        // THEN
        testChooseDataTypeAlsoReverseTypes(value, dataTypes, expected);
    }

    @Test
    public void testChooseDataTypeWhenFloat_vs_INT_FLOAT_ThenShouldReturnFLOAT() {
        // GIVEN
        List<DataType> dataTypes = Arrays.asList(
                RecordFieldType.INT.getDataType(),
                RecordFieldType.FLOAT.getDataType()
        );

        Object value = 1.5f;
        DataType expected = RecordFieldType.FLOAT.getDataType();

        // WHEN
        // THEN
        testChooseDataTypeAlsoReverseTypes(value, dataTypes, expected);
    }

    @Test
    public void testChooseDataTypeWhenHasChoiceThenShouldReturnSingleMatchingFromChoice() {
        // GIVEN
        List<DataType> dataTypes = Arrays.asList(
                RecordFieldType.INT.getDataType(),
                RecordFieldType.DOUBLE.getDataType(),
                RecordFieldType.CHOICE.getChoiceDataType(
                        RecordFieldType.FLOAT.getDataType(),
                        RecordFieldType.STRING.getDataType()
                )
        );

        Object value = 1.5f;
        DataType expected = RecordFieldType.FLOAT.getDataType();

        // WHEN
        // THEN
        testChooseDataTypeAlsoReverseTypes(value, dataTypes, expected);
    }

    private <E> void testChooseDataTypeAlsoReverseTypes(Object value, List<DataType> dataTypes, DataType expected) {
        testChooseDataType(dataTypes, value, expected);
        Collections.reverse(dataTypes);
        testChooseDataType(dataTypes, value, expected);
    }

    private void testChooseDataType(List<DataType> dataTypes, Object value, DataType expected) {
        // GIVEN
        ChoiceDataType choiceDataType = (ChoiceDataType) RecordFieldType.CHOICE.getChoiceDataType(dataTypes.toArray(new DataType[dataTypes.size()]));

        // WHEN
        DataType actual = DataTypeUtils.chooseDataType(value, choiceDataType);

        // THEN
        assertEquals(expected, actual);
    }

    @Test
    public void testInferTypeWithMapStringKeys() {
        Map<String, String> map = new LinkedHashMap<>();
        map.put("a", "Hello");
        map.put("b", "World");

        RecordDataType expected = (RecordDataType)RecordFieldType.RECORD.getRecordDataType(new SimpleRecordSchema(Arrays.asList(
                new RecordField("a", RecordFieldType.STRING.getDataType()),
                new RecordField("b", RecordFieldType.STRING.getDataType())
        )));

        DataType actual = DataTypeUtils.inferDataType(map, null);
        assertEquals(expected, actual);
    }

    @Test
    public void testInferTypeWithMapNonStringKeys() {
        Map<Integer, String> map = new LinkedHashMap<>();
        map.put(1, "Hello");
        map.put(2, "World");

        RecordDataType expected = (RecordDataType)RecordFieldType.RECORD.getRecordDataType(new SimpleRecordSchema(Arrays.asList(
                new RecordField("1", RecordFieldType.STRING.getDataType()),
                new RecordField("2", RecordFieldType.STRING.getDataType())
        )));

        DataType actual = DataTypeUtils.inferDataType(map, null);
        assertEquals(expected, actual);
    }

    @Test
    public void testFindMostSuitableTypeWithBoolean() {
        testFindMostSuitableType(true, RecordFieldType.BOOLEAN.getDataType());
    }

    @Test
    public void testFindMostSuitableTypeWithByte() {
        testFindMostSuitableType(Byte.valueOf((byte) 123), RecordFieldType.BYTE.getDataType());
    }

    @Test
    public void testFindMostSuitableTypeWithShort() {
        testFindMostSuitableType(Short.valueOf((short) 123), RecordFieldType.SHORT.getDataType());
    }

    @Test
    public void testFindMostSuitableTypeWithInt() {
        testFindMostSuitableType(123, RecordFieldType.INT.getDataType());
    }

    @Test
    public void testFindMostSuitableTypeWithLong() {
        testFindMostSuitableType(123L, RecordFieldType.LONG.getDataType());
    }

    @Test
    public void testFindMostSuitableTypeWithBigInt() {
        testFindMostSuitableType(BigInteger.valueOf(123L), RecordFieldType.BIGINT.getDataType());
    }

    @Test
    public void testFindMostSuitableTypeWithBigDecimal() {
        testFindMostSuitableType(BigDecimal.valueOf(123.456D), RecordFieldType.DECIMAL.getDecimalDataType(6, 3));
    }

    @Test
    public void testFindMostSuitableTypeWithFloat() {
        testFindMostSuitableType(12.3F, RecordFieldType.FLOAT.getDataType());
    }

    @Test
    public void testFindMostSuitableTypeWithDouble() {
        testFindMostSuitableType(12.3, RecordFieldType.DOUBLE.getDataType());
    }

    @Test
    public void testFindMostSuitableTypeWithDate() {
        testFindMostSuitableType("1111-11-11", RecordFieldType.DATE.getDataType());
    }

    @Test
    public void testFindMostSuitableTypeWithTime() {
        testFindMostSuitableType("11:22:33", RecordFieldType.TIME.getDataType());
    }

    @Test
    public void testFindMostSuitableTypeWithTimeStamp() {
        testFindMostSuitableType("1111-11-11 11:22:33", RecordFieldType.TIMESTAMP.getDataType());
    }

    @Test
    public void testFindMostSuitableTypeWithChar() {
        testFindMostSuitableType('a', RecordFieldType.CHAR.getDataType());
    }

    @Test
    public void testFindMostSuitableTypeWithStringShouldReturnChar() {
        testFindMostSuitableType("abc", RecordFieldType.CHAR.getDataType());
    }

    @Test
    public void testFindMostSuitableTypeWithString() {
        testFindMostSuitableType("abc", RecordFieldType.STRING.getDataType(), RecordFieldType.CHAR.getDataType());
    }

    @Test
    public void testFindMostSuitableTypeWithArray() {
        testFindMostSuitableType(new int[]{1, 2, 3}, RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.INT.getDataType()));
    }

    private void testFindMostSuitableType(Object value, DataType expected, DataType... filtered) {
        List<DataType> filteredOutDataTypes = Arrays.stream(filtered).collect(Collectors.toList());

        // GIVEN
        List<DataType> unexpectedTypes = Arrays.stream(RecordFieldType.values())
                .flatMap(recordFieldType -> {
                    Stream<DataType> dataTypeStream;

                    if (RecordFieldType.ARRAY.equals(recordFieldType)) {
                        dataTypeStream = Arrays.stream(RecordFieldType.values()).map(elementType -> RecordFieldType.ARRAY.getArrayDataType(elementType.getDataType()));
                    } else {
                        dataTypeStream = Stream.of(recordFieldType.getDataType());
                    }

                    return dataTypeStream;
                })
                .filter(dataType -> !dataType.equals(expected))
                .filter(dataType -> !filteredOutDataTypes.contains(dataType))
                .collect(Collectors.toList());

        IntStream.rangeClosed(0, unexpectedTypes.size()).forEach(insertIndex -> {
            List<DataType> allTypes = new LinkedList<>(unexpectedTypes);
            allTypes.add(insertIndex, expected);

            // WHEN
            Optional<DataType> actual = DataTypeUtils.findMostSuitableType(value, allTypes, Function.identity());

            // THEN
            assertEquals(Optional.ofNullable(expected), actual);
        });
    }

    private void whenExpectingValidBigDecimalConversion(final BigDecimal expectedValue, final Object incomingValue) {
        // Checking indirect conversion
        final String failureMessage = "Conversion from " + incomingValue.getClass().getSimpleName() + " to " + expectedValue.getClass().getSimpleName() + " failed, when ";
        final BigDecimal indirectResult = whenExpectingValidConversion(expectedValue, incomingValue, RecordFieldType.DECIMAL.getDecimalDataType(30, 10));
        // In some cases, direct equality check comes with false negative as the changing representation brings in
        // insignificant changes what might break the comparison. For example 12F will be represented as "12.0"
        assertEquals(0, expectedValue.compareTo(indirectResult), failureMessage + "indirect");

        // Checking direct conversion
        final BigDecimal directResult = DataTypeUtils.toBigDecimal(incomingValue, "field");
        assertEquals(0, expectedValue.compareTo(directResult), failureMessage + "direct");

    }

    private <T> T whenExpectingValidConversion(final T expectedValue, final Object incomingValue, final DataType dataType) {
        final Object result = DataTypeUtils.convertType(incomingValue, dataType, null, StandardCharsets.UTF_8);
        assertNotNull(result);
        assertTrue(expectedValue.getClass().isInstance(result));
        return (T) result;
    }

    @Test
    public void testIsIntegerFitsToFloat() {
        final int maxRepresentableInt = Double.valueOf(Math.pow(2, 24)).intValue();

        assertTrue(DataTypeUtils.isIntegerFitsToFloat(0));
        assertTrue(DataTypeUtils.isIntegerFitsToFloat(9));
        assertTrue(DataTypeUtils.isIntegerFitsToFloat(maxRepresentableInt));
        assertTrue(DataTypeUtils.isIntegerFitsToFloat(-1 * maxRepresentableInt));

        assertFalse(DataTypeUtils.isIntegerFitsToFloat("test"));
        assertFalse(DataTypeUtils.isIntegerFitsToFloat(9L));
        assertFalse(DataTypeUtils.isIntegerFitsToFloat(9.0));
        assertFalse(DataTypeUtils.isIntegerFitsToFloat(Integer.MAX_VALUE));
        assertFalse(DataTypeUtils.isIntegerFitsToFloat(Integer.MIN_VALUE));
        assertFalse(DataTypeUtils.isIntegerFitsToFloat(maxRepresentableInt + 1));
        assertFalse(DataTypeUtils.isIntegerFitsToFloat(-1 * maxRepresentableInt - 1));
    }

    @Test
    public void testIsLongFitsToFloat() {
        final long maxRepresentableLong = Double.valueOf(Math.pow(2, 24)).longValue();

        assertTrue(DataTypeUtils.isLongFitsToFloat(0L));
        assertTrue(DataTypeUtils.isLongFitsToFloat(9L));
        assertTrue(DataTypeUtils.isLongFitsToFloat(maxRepresentableLong));
        assertTrue(DataTypeUtils.isLongFitsToFloat(-1L * maxRepresentableLong));

        assertFalse(DataTypeUtils.isLongFitsToFloat("test"));
        assertFalse(DataTypeUtils.isLongFitsToFloat(9));
        assertFalse(DataTypeUtils.isLongFitsToFloat(9.0));
        assertFalse(DataTypeUtils.isLongFitsToFloat(Long.MAX_VALUE));
        assertFalse(DataTypeUtils.isLongFitsToFloat(Long.MIN_VALUE));
        assertFalse(DataTypeUtils.isLongFitsToFloat(maxRepresentableLong + 1L));
        assertFalse(DataTypeUtils.isLongFitsToFloat(-1L * maxRepresentableLong - 1L));
    }

    @Test
    public void testIsLongFitsToDouble() {
        final long maxRepresentableLong = Double.valueOf(Math.pow(2, 53)).longValue();

        assertTrue(DataTypeUtils.isLongFitsToDouble(0L));
        assertTrue(DataTypeUtils.isLongFitsToDouble(9L));
        assertTrue(DataTypeUtils.isLongFitsToDouble(maxRepresentableLong));
        assertTrue(DataTypeUtils.isLongFitsToDouble(-1L * maxRepresentableLong));

        assertFalse(DataTypeUtils.isLongFitsToDouble("test"));
        assertFalse(DataTypeUtils.isLongFitsToDouble(9));
        assertFalse(DataTypeUtils.isLongFitsToDouble(9.0));
        assertFalse(DataTypeUtils.isLongFitsToDouble(Long.MAX_VALUE));
        assertFalse(DataTypeUtils.isLongFitsToDouble(Long.MIN_VALUE));
        assertFalse(DataTypeUtils.isLongFitsToDouble(maxRepresentableLong + 1L));
        assertFalse(DataTypeUtils.isLongFitsToDouble(-1L * maxRepresentableLong - 1L));
    }

    @Test
    public void testIsBigIntFitsToFloat() {
        final BigInteger maxRepresentableBigInt = BigInteger.valueOf(Double.valueOf(Math.pow(2, 24)).longValue());

        assertTrue(DataTypeUtils.isBigIntFitsToFloat(BigInteger.valueOf(0L)));
        assertTrue(DataTypeUtils.isBigIntFitsToFloat(BigInteger.valueOf(8L)));
        assertTrue(DataTypeUtils.isBigIntFitsToFloat(maxRepresentableBigInt));
        assertTrue(DataTypeUtils.isBigIntFitsToFloat(maxRepresentableBigInt.negate()));

        assertFalse(DataTypeUtils.isBigIntFitsToFloat("test"));
        assertFalse(DataTypeUtils.isBigIntFitsToFloat(9));
        assertFalse(DataTypeUtils.isBigIntFitsToFloat(9.0));
        assertFalse(DataTypeUtils.isBigIntFitsToFloat(new BigInteger(String.join("", Collections.nCopies(100, "1")))));
        assertFalse(DataTypeUtils.isBigIntFitsToFloat(new BigInteger(String.join("", Collections.nCopies(100, "1"))).negate()));
    }

    @Test
    public void testIsBigIntFitsToDouble() {
        final BigInteger maxRepresentableBigInt = BigInteger.valueOf(Double.valueOf(Math.pow(2, 53)).longValue());

        assertTrue(DataTypeUtils.isBigIntFitsToDouble(BigInteger.valueOf(0L)));
        assertTrue(DataTypeUtils.isBigIntFitsToDouble(BigInteger.valueOf(8L)));
        assertTrue(DataTypeUtils.isBigIntFitsToDouble(maxRepresentableBigInt));
        assertTrue(DataTypeUtils.isBigIntFitsToDouble(maxRepresentableBigInt.negate()));

        assertFalse(DataTypeUtils.isBigIntFitsToDouble("test"));
        assertFalse(DataTypeUtils.isBigIntFitsToDouble(9));
        assertFalse(DataTypeUtils.isBigIntFitsToDouble(9.0));
        assertFalse(DataTypeUtils.isBigIntFitsToDouble(new BigInteger(String.join("", Collections.nCopies(100, "1")))));
        assertFalse(DataTypeUtils.isBigIntFitsToDouble(new BigInteger(String.join("", Collections.nCopies(100, "1"))).negate()));
    }

    @Test
    public void testIsDoubleWithinFloatInterval() {
        assertTrue(DataTypeUtils.isDoubleWithinFloatInterval(0D));
        assertTrue(DataTypeUtils.isDoubleWithinFloatInterval(0.1D));
        assertTrue(DataTypeUtils.isDoubleWithinFloatInterval((double) Float.MAX_VALUE));
        assertTrue(DataTypeUtils.isDoubleWithinFloatInterval((double) Float.MIN_VALUE));
        assertTrue(DataTypeUtils.isDoubleWithinFloatInterval((double) -1 * Float.MAX_VALUE));
        assertTrue(DataTypeUtils.isDoubleWithinFloatInterval((double) -1 * Float.MIN_VALUE));


        assertFalse(DataTypeUtils.isDoubleWithinFloatInterval("test"));
        assertFalse(DataTypeUtils.isDoubleWithinFloatInterval(9));
        assertFalse(DataTypeUtils.isDoubleWithinFloatInterval(9.0F));
        assertFalse(DataTypeUtils.isDoubleWithinFloatInterval(Double.MAX_VALUE));
        assertFalse(DataTypeUtils.isDoubleWithinFloatInterval((double) -1 * Double.MAX_VALUE));
    }

    @Test
    public void testIsFittingNumberType() {
        // Byte
        assertTrue(DataTypeUtils.isFittingNumberType((byte) 9, RecordFieldType.BYTE));
        assertFalse(DataTypeUtils.isFittingNumberType((short)9, RecordFieldType.BYTE));
        assertFalse(DataTypeUtils.isFittingNumberType(9, RecordFieldType.BYTE));
        assertFalse(DataTypeUtils.isFittingNumberType(9L, RecordFieldType.BYTE));
        assertFalse(DataTypeUtils.isFittingNumberType(BigInteger.valueOf(9L), RecordFieldType.BYTE));

        // Short
        assertTrue(DataTypeUtils.isFittingNumberType((byte) 9, RecordFieldType.SHORT));
        assertTrue(DataTypeUtils.isFittingNumberType((short)9, RecordFieldType.SHORT));
        assertFalse(DataTypeUtils.isFittingNumberType(9, RecordFieldType.SHORT));
        assertFalse(DataTypeUtils.isFittingNumberType(9L, RecordFieldType.SHORT));
        assertFalse(DataTypeUtils.isFittingNumberType(BigInteger.valueOf(9L), RecordFieldType.SHORT));

        // Integer
        assertTrue(DataTypeUtils.isFittingNumberType((byte) 9, RecordFieldType.INT));
        assertTrue(DataTypeUtils.isFittingNumberType((short)9, RecordFieldType.INT));
        assertTrue(DataTypeUtils.isFittingNumberType(9, RecordFieldType.INT));
        assertFalse(DataTypeUtils.isFittingNumberType(9L, RecordFieldType.INT));
        assertFalse(DataTypeUtils.isFittingNumberType(BigInteger.valueOf(9L), RecordFieldType.INT));

        // Long
        assertTrue(DataTypeUtils.isFittingNumberType((byte) 9, RecordFieldType.LONG));
        assertTrue(DataTypeUtils.isFittingNumberType((short)9, RecordFieldType.LONG));
        assertTrue(DataTypeUtils.isFittingNumberType(9, RecordFieldType.LONG));
        assertTrue(DataTypeUtils.isFittingNumberType(9L, RecordFieldType.LONG));
        assertFalse(DataTypeUtils.isFittingNumberType(BigInteger.valueOf(9L), RecordFieldType.LONG));

        // Bigint
        assertTrue(DataTypeUtils.isFittingNumberType((byte) 9, RecordFieldType.BIGINT));
        assertTrue(DataTypeUtils.isFittingNumberType((short)9, RecordFieldType.BIGINT));
        assertTrue(DataTypeUtils.isFittingNumberType(9, RecordFieldType.BIGINT));
        assertTrue(DataTypeUtils.isFittingNumberType(9L, RecordFieldType.BIGINT));
        assertTrue(DataTypeUtils.isFittingNumberType(BigInteger.valueOf(9L), RecordFieldType.BIGINT));

        // Float
        assertTrue(DataTypeUtils.isFittingNumberType(9F, RecordFieldType.FLOAT));
        assertFalse(DataTypeUtils.isFittingNumberType(9D, RecordFieldType.FLOAT));
        assertFalse(DataTypeUtils.isFittingNumberType(9, RecordFieldType.FLOAT));

        // Double
        assertTrue(DataTypeUtils.isFittingNumberType(9F, RecordFieldType.DOUBLE));
        assertTrue(DataTypeUtils.isFittingNumberType(9D, RecordFieldType.DOUBLE));
        assertFalse(DataTypeUtils.isFittingNumberType(9, RecordFieldType.DOUBLE));
    }

    @Test
    public void testConvertDateToUTC() {
        int year = 2021;
        int month = 1;
        int dayOfMonth = 25;

        Date dateLocalTZ = new Date(ZonedDateTime.of(LocalDateTime.of(year, month, dayOfMonth,0,0,0), ZoneId.systemDefault()).toInstant().toEpochMilli());

        Date dateUTC = DataTypeUtils.convertDateToUTC(dateLocalTZ);

        ZonedDateTime zdt = ZonedDateTime.ofInstant(Instant.ofEpochMilli(dateUTC.getTime()), ZoneId.of("UTC"));
        assertEquals(year, zdt.getYear());
        assertEquals(month, zdt.getMonthValue());
        assertEquals(dayOfMonth, zdt.getDayOfMonth());
        assertEquals(0, zdt.getHour());
        assertEquals(0, zdt.getMinute());
        assertEquals(0, zdt.getSecond());
        assertEquals(0, zdt.getNano());
    }

    /**
     * Convert String to java.sql.Date using implicit default DateFormat with GMT Time Zone
     *
     * Running this method on a system with a time zone other than GMT should return the same year-month-day
     */
    @Test
    public void testConvertTypeStringToDateDefaultTimeZoneFormat() {
        final Object converted = DataTypeUtils.convertType(ISO_8601_YEAR_MONTH_DAY, RecordFieldType.DATE.getDataType(), DATE_FIELD);
        assertTrue(converted instanceof java.sql.Date, "Converted value is not java.sql.Date");
        assertEquals(ISO_8601_YEAR_MONTH_DAY, converted.toString());
    }

    /**
     * Convert String to java.sql.Date using custom pattern DateFormat with configured GMT Time Zone
     */
    @Test
    public void testConvertTypeStringToDateConfiguredTimeZoneFormat() {
        final DateFormat dateFormat = DataTypeUtils.getDateFormat(CUSTOM_MONTH_DAY_YEAR_PATTERN, "GMT");
        final Object converted = DataTypeUtils.convertType(CUSTOM_MONTH_DAY_YEAR, RecordFieldType.DATE.getDataType(), () -> dateFormat, null, null,"date");
        assertTrue(converted instanceof java.sql.Date, "Converted value is not java.sql.Date");
        assertEquals(ISO_8601_YEAR_MONTH_DAY, converted.toString());
    }

    /**
     * Convert String to java.sql.Date using custom pattern DateFormat with system default Time Zone
     */
    @Test
    public void testConvertTypeStringToDateConfiguredSystemDefaultTimeZoneFormat() {
        final DateFormat dateFormat = DataTypeUtils.getDateFormat(CUSTOM_MONTH_DAY_YEAR_PATTERN, TimeZone.getDefault().getID());
        final Object converted = DataTypeUtils.convertType(CUSTOM_MONTH_DAY_YEAR, RecordFieldType.DATE.getDataType(), () -> dateFormat, null, null,"date");
        assertTrue(converted instanceof java.sql.Date, "Converted value is not java.sql.Date");
        assertEquals(ISO_8601_YEAR_MONTH_DAY, converted.toString());
    }

    @Test
    public void testToLocalDateFromString() {
        assertToLocalDateEquals(ISO_8601_YEAR_MONTH_DAY, ISO_8601_YEAR_MONTH_DAY);
    }

    @Test
    public void testToLocalDateFromSqlDate() {
        assertToLocalDateEquals(ISO_8601_YEAR_MONTH_DAY, java.sql.Date.valueOf(ISO_8601_YEAR_MONTH_DAY));
    }

    @Test
    public void testToLocalDateFromUtilDate() {
        final LocalDate localDate = LocalDate.parse(ISO_8601_YEAR_MONTH_DAY);
        final long epochMillis = toEpochMilliSystemDefaultZone(localDate);
        assertToLocalDateEquals(ISO_8601_YEAR_MONTH_DAY, new java.util.Date(epochMillis));
    }

    @Test
    public void testToLocalDateFromNumberEpochMillis() {
        final LocalDate localDate = LocalDate.parse(ISO_8601_YEAR_MONTH_DAY);
        final long epochMillis = toEpochMilliSystemDefaultZone(localDate);
        assertToLocalDateEquals(ISO_8601_YEAR_MONTH_DAY, epochMillis);
    }

    private long toEpochMilliSystemDefaultZone(final LocalDate localDate) {
        final LocalTime localTime = LocalTime.of(0, 0);
        final Instant instantSystemDefaultZone = ZonedDateTime.of(localDate, localTime, SYSTEM_DEFAULT_ZONE_ID).toInstant();
        return instantSystemDefaultZone.toEpochMilli();
    }

    private void assertToLocalDateEquals(final String expected, final Object value) {
        final DateTimeFormatter systemDefaultZoneFormatter = DataTypeUtils.getDateTimeFormatter(RecordFieldType.DATE.getDefaultFormat(), SYSTEM_DEFAULT_ZONE_ID);
        final LocalDate localDate = DataTypeUtils.toLocalDate(value, () -> systemDefaultZoneFormatter, DATE_FIELD);
        assertEquals(expected, localDate.toString(), String.format("Value Class [%s] to LocalDate not matched", value.getClass()));
    }
}
