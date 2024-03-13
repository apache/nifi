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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TestMapRecord {

    @Test
    void testIncorporateInactiveFieldsWithUpdate() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("string", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("number", RecordFieldType.INT.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);
        final Map<String, Object> values = new HashMap<>();
        final Record record = new MapRecord(schema, values);
        record.setValue("number", "value");
        record.incorporateInactiveFields();

        final RecordSchema updatedSchema = record.getSchema();
        final DataType dataType = updatedSchema.getDataType("number").orElseThrow();
        assertSame(RecordFieldType.CHOICE, dataType.getFieldType());

        final ChoiceDataType choiceDataType = (ChoiceDataType) dataType;
        final List<DataType> subTypes = choiceDataType.getPossibleSubTypes();
        assertEquals(2, subTypes.size());
        assertTrue(subTypes.contains(RecordFieldType.INT.getDataType()));
        assertTrue(subTypes.contains(RecordFieldType.STRING.getDataType()));
    }

    @Test
    void testIncorporateInactiveFieldsWithConflict() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("string", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("number", RecordFieldType.INT.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);
        final Map<String, Object> values = new HashMap<>();
        final Record record = new MapRecord(schema, values);
        record.setValue("new", 8);
        record.incorporateInactiveFields();

        record.setValue("new", "eight");
        record.incorporateInactiveFields();

        final DataType dataType = record.getSchema().getDataType("new").orElseThrow();
        assertSame(RecordFieldType.CHOICE, dataType.getFieldType());

        final ChoiceDataType choiceDataType = (ChoiceDataType) dataType;
        final List<DataType> subTypes = choiceDataType.getPossibleSubTypes();
        assertEquals(2, subTypes.size());
        assertTrue(subTypes.contains(RecordFieldType.INT.getDataType()));
        assertTrue(subTypes.contains(RecordFieldType.STRING.getDataType()));
    }

    @Test
    void testDefaultValue() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("noDefault", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("defaultOfHello", RecordFieldType.STRING.getDataType(), "hello"));

        final RecordSchema schema = new SimpleRecordSchema(fields);
        final Map<String, Object> values = new HashMap<>();
        final Record record = new MapRecord(schema, values);

        assertNull(record.getValue("noDefault"));
        assertEquals("hello", record.getValue("defaultOfHello"));
    }

    @Test
    void testDefaultValueInGivenField() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("noDefault", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("defaultOfHello", RecordFieldType.STRING.getDataType(), "hello"));

        final RecordSchema schema = new SimpleRecordSchema(fields);
        final Map<String, Object> values = new HashMap<>();
        final Record record = new MapRecord(schema, values);

        assertNull(record.getValue("noDefault"));
        assertEquals("hello", record.getValue("defaultOfHello"));

        final RecordField newField = new RecordField("noDefault", RecordFieldType.STRING.getDataType(), "new");
        assertEquals("new", record.getValue(newField));
    }

    @Test
    void testIllegalDefaultValue() {
        new RecordField("hello", RecordFieldType.STRING.getDataType(), 84);
        new RecordField("hello", RecordFieldType.STRING.getDataType(), (Object) null);
        new RecordField("hello", RecordFieldType.INT.getDataType(), 84);
        new RecordField("hello", RecordFieldType.INT.getDataType(), (Object) null);

        assertThrows(IllegalArgumentException.class, () -> new RecordField("hello", RecordFieldType.INT.getDataType(), "foo"));
    }

    private Set<String> set(final String... values) {
        return new LinkedHashSet<>(Arrays.asList(values));
    }

    @Test
    void testAliasOneValue() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("foo", RecordFieldType.STRING.getDataType(), null, set("bar", "baz")));

        final RecordSchema schema = new SimpleRecordSchema(fields);
        final Map<String, Object> values = new HashMap<>();
        values.put("bar", 1);

        final Record record = new MapRecord(schema, values);
        assertEquals(1, record.getValue("foo"));
        assertEquals(1, record.getValue("bar"));
        assertEquals(1, record.getValue("baz"));
    }

    @Test
    void testAliasConflictingValues() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("foo", RecordFieldType.STRING.getDataType(), null, set("bar", "baz")));

        final RecordSchema schema = new SimpleRecordSchema(fields);
        final Map<String, Object> values = new HashMap<>();
        values.put("bar", 1);
        values.put("foo", null);

        final Record record = new MapRecord(schema, values);
        assertEquals(1, record.getValue("foo"));
        assertEquals(1, record.getValue("bar"));
        assertEquals(1, record.getValue("baz"));
    }

    @Test
    void testAliasConflictingAliasValues() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("foo", RecordFieldType.STRING.getDataType(), null, set("bar", "baz")));

        final RecordSchema schema = new SimpleRecordSchema(fields);
        final Map<String, Object> values = new LinkedHashMap<>();
        values.put("baz", 1);
        values.put("bar", 33);

        final Record record = new MapRecord(schema, values);
        assertEquals(33, record.getValue("foo"));
        assertEquals(33, record.getValue("bar"));
        assertEquals(33, record.getValue("baz"));
    }

    @Test
    void testAliasInGivenField() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("foo", RecordFieldType.STRING.getDataType(), null, set("bar", "baz")));

        final RecordSchema schema = new SimpleRecordSchema(fields);
        final Map<String, Object> values = new HashMap<>();
        values.put("bar", 33);

        final Record record = new MapRecord(schema, values);
        assertEquals(33, record.getValue("foo"));
        assertEquals(33, record.getValue("bar"));
        assertEquals(33, record.getValue("baz"));

        final RecordField noAlias = new RecordField("hello", RecordFieldType.STRING.getDataType());
        assertNull(record.getValue(noAlias));

        final RecordField withAlias = new RecordField("hello", RecordFieldType.STRING.getDataType(), null, set("baz"));
        assertEquals(33, record.getValue(withAlias));
        assertEquals("33", record.getAsString(withAlias, withAlias.getDataType().getFormat()));
    }


    @Test
    void testDefaultValueWithAliasValue() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("foo", RecordFieldType.STRING.getDataType(), "hello", set("bar", "baz")));

        final RecordSchema schema = new SimpleRecordSchema(fields);
        final Map<String, Object> values = new LinkedHashMap<>();
        values.put("baz", 1);
        values.put("bar", 33);

        final Record record = new MapRecord(schema, values);
        assertEquals(33, record.getValue("foo"));
        assertEquals(33, record.getValue("bar"));
        assertEquals(33, record.getValue("baz"));
    }

    @Test
    void testDefaultValueWithAliasesDefined() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("foo", RecordFieldType.STRING.getDataType(), "hello", set("bar", "baz")));

        final RecordSchema schema = new SimpleRecordSchema(fields);
        final Map<String, Object> values = new HashMap<>();
        final Record record = new MapRecord(schema, values);
        assertEquals("hello", record.getValue("foo"));
        assertEquals("hello", record.getValue("bar"));
        assertEquals("hello", record.getValue("baz"));
    }

    @Test
    void testNestedSchema() {
        final String FOO_TEST_VAL = "test!";
        final String NESTED_RECORD_VALUE = "Hello, world!";

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("foo", RecordFieldType.STRING.getDataType(), null, set("bar", "baz")));
        List<RecordField> nestedFields = new ArrayList<>();
        nestedFields.add(new RecordField("test", RecordFieldType.STRING.getDataType()));
        RecordSchema nestedSchema = new SimpleRecordSchema(nestedFields);
        RecordDataType nestedType = new RecordDataType(nestedSchema);
        fields.add(new RecordField("nested", nestedType));
        fields.add(new RecordField("list", new ArrayDataType(nestedType)));
        RecordSchema fullSchema = new SimpleRecordSchema(fields);

        Map<String, Object> nestedValues = new HashMap<>();
        nestedValues.put("test", NESTED_RECORD_VALUE);
        Record nestedRecord = new MapRecord(nestedSchema, nestedValues);
        Map<String, Object> values = new HashMap<>();
        values.put("foo", FOO_TEST_VAL);
        values.put("nested", nestedRecord);

        List<Record> list = new ArrayList<>();
        for (int x = 0; x < 5; x++) {
            list.add(new MapRecord(nestedSchema, nestedValues));
        }
        values.put("list", list);

        Record record = new MapRecord(fullSchema, values);

        Map<String, Object> fullConversion = ((MapRecord)record).toMap(true);
        assertEquals(FOO_TEST_VAL, fullConversion.get("foo"));
        assertTrue(fullConversion.get("nested") instanceof Map);

        Map<String, Object> nested = (Map<String, Object>)fullConversion.get("nested");
        assertEquals(1, nested.size());
        assertEquals(NESTED_RECORD_VALUE, nested.get("test"));

        assertTrue(fullConversion.get("list") instanceof List);
        List recordList = (List) fullConversion.get("list");
        assertEquals(5, recordList.size());
        for (Object rec : recordList) {
            assertTrue(rec instanceof Map);
            Map<String, Object> map = (Map<String, Object>)rec;
            assertEquals(1, map.size());
            assertEquals(NESTED_RECORD_VALUE, map.get("test"));
        }
    }

    @ParameterizedTest
    @MethodSource("provideTimestamps")
    void testGettingLocalDateAndOffsetDateTime(final Object input,
        final String format,
        final long expectedDate,
        final long expectedDateTime) {

        final List<RecordField> fields = new ArrayList<>();
        final String timestampFieldName = "timestamp";
        fields.add(new RecordField(timestampFieldName, RecordFieldType.TIMESTAMP.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);
        final HashMap<String, Object> item = new HashMap<>();
        item.put(timestampFieldName, input);
        final MapRecord testRecord = new MapRecord(schema, item);

        final LocalDate localDate = testRecord.getAsLocalDate(timestampFieldName, format);
        final Instant instantDate = localDate.atStartOfDay().atZone(ZoneId.systemDefault()).toInstant();
        assertEquals(expectedDate, instantDate.toEpochMilli());

        final OffsetDateTime offsetDateTime = testRecord.getAsOffsetDateTime(timestampFieldName, format);
        final Instant instantDateTime = offsetDateTime.toInstant();
        assertEquals(expectedDateTime, instantDateTime.toEpochMilli());
    }

    private static Stream<Arguments> provideTimestamps() {
        return Stream.of(
            Arguments.of("2022-01-01 12:34:56.789", "yyyy-MM-dd HH:mm:ss.SSS", 1640995200000L, 1641040496789L),
            Arguments.of("01/02/2022 12:34:56", "MM/dd/yyyy HH:mm:ss", 1641081600000L, 1641126896000L),
            Arguments.of("03-01-2022 12:34:56", "dd-MM-yyyy HH:mm:ss", 1641168000000L, 1641213296000L),
            Arguments.of("01/01/22 12:34:56", "MM/dd/yy HH:mm:ss", 1640995200000L, 1641040496000L),
            Arguments.of("02-01-22 12:34:56", "dd-MM-yy HH:mm:ss", 1641081600000L, 1641126896000L),
            Arguments.of("2022-01-03T12:34:56.789", "yyyy-MM-dd'T'HH:mm:ss.SSS", 1641168000000L, 1641213296789L),
            Arguments.of("Sat, 01 Jan 2022 12:34:56 GMT", "EEE, dd MMM yyyy HH:mm:ss zzz", 1640995200000L, 1641040496000L),
            Arguments.of("2022-Jan-02 12:34:56.789", "yyyy-MMM-dd HH:mm:ss.SSS", 1641081600000L, 1641126896789L),
            Arguments.of("20220103 12:34:56", "yyyyMMdd HH:mm:ss", 1641168000000L, 1641213296000L),
            Arguments.of("01/01/2022 12:34:56 AM", "MM/dd/yyyy hh:mm:ss a", 1640995200000L, 1640997296000L),
            Arguments.of("02-01-2022 12:34:56 AM", "dd-MM-yyyy hh:mm:ss a", 1641081600000L, 1641083696000L),
            Arguments.of("2022 01 03 12:34:56.789", "yyyy MM dd HH:mm:ss.SSS", 1641168000000L, 1641213296789L),
            Arguments.of("2022-01-01 12:34:56.789-05:00", "yyyy-MM-dd HH:mm:ss.SSSXXX", 1640995200000L, 1641040496789L),
            Arguments.of(1641040496789L, null, 1640995200000L, 1641040496789L),
            Arguments.of("2020-02-29 23:59:59.999", "yyyy-MM-dd HH:mm:ss.SSS", 1582934400000L, 1583020799999L), // leap year
            Arguments.of("2024-03-10 02:00:00.000", "yyyy-MM-dd HH:mm:ss.SSS", 1710028800000L, 1710036000000L) // DST transition
        );
    }
}
