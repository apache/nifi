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
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.serialization.record.util.TriFunction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestMapRecord {

    private static final String ISO_LOCAL_DATE = "yyyy-MM-dd";
    private static final String ISO_LOCAL_DATE_TIME = "yyyy-MM-dd'T'HH:mm:ss.SSS";
    private static final String ISO_OFFSET_DATE_TIME = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";

    private static final List<RecordField> STRING_NUMBER_FIELDS = List.of(
        new RecordField("string", RecordFieldType.STRING.getDataType()),
        new RecordField("number", RecordFieldType.INT.getDataType())
    );


    @Test
    void testRenameClearsSerializedForm() {
        final Map<String, Object> values = new HashMap<>(Map.of("string", "hello", "number", 8));
        final RecordSchema schema = new SimpleRecordSchema(STRING_NUMBER_FIELDS);
        final Record record = new MapRecord(schema, values, SerializedForm.of("Hello there", "text/unit-test"));

        assertTrue(record.getSerializedForm().isPresent());
        record.rename(record.getSchema().getField("string").get(), "newString");
        assertFalse(record.getSerializedForm().isPresent());
    }

    @Test
    void testRemoveClearsSerializedForm() {
        final Map<String, Object> values = new HashMap<>(Map.of("string", "hello", "number", 8));
        final RecordSchema schema = new SimpleRecordSchema(STRING_NUMBER_FIELDS);
        final Record record = new MapRecord(schema, values, SerializedForm.of("Hello there", "text/unit-test"));

        assertTrue(record.getSerializedForm().isPresent());
        record.rename(record.getSchema().getField("string").get(), "newString");
        assertFalse(record.getSerializedForm().isPresent());
    }

    @Test
    void testRenameRemoveInvalidFieldsToNotClearSerializedForm() {
        final Map<String, Object> values = new HashMap<>(Map.of("string", "hello", "number", 8));
        final RecordSchema schema = new SimpleRecordSchema(STRING_NUMBER_FIELDS);
        final Record record = new MapRecord(schema, values, SerializedForm.of("Hello there", "text/unit-test"));

        assertTrue(record.getSerializedForm().isPresent());

        final RecordField invalidField = new RecordField("Other Field", RecordFieldType.STRING.getDataType());
        assertFalse(record.rename(invalidField, "newString"));
        assertTrue(record.getSerializedForm().isPresent());

        record.remove(invalidField);
        assertTrue(record.getSerializedForm().isPresent());
    }

    @Test
    void testIncorporateInactiveFieldsWithUpdate() {
        final Map<String, Object> values = new HashMap<>(Map.of("string", "hello", "number", 8));
        final RecordSchema schema = new SimpleRecordSchema(STRING_NUMBER_FIELDS);
        final Record record = new MapRecord(schema, values, SerializedForm.of("Hello there", "text/unit-test"));

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
        final Map<String, Object> values = new HashMap<>(Map.of("string", "hello", "number", 8));
        final RecordSchema schema = new SimpleRecordSchema(STRING_NUMBER_FIELDS);
        final Record record = new MapRecord(schema, values, SerializedForm.of("Hello there", "text/unit-test"));

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

        Map<String, Object> fullConversion = ((MapRecord) record).toMap(true);
        assertEquals(FOO_TEST_VAL, fullConversion.get("foo"));
        assertInstanceOf(Map.class, fullConversion.get("nested"));

        Map<String, Object> nested = (Map<String, Object>) fullConversion.get("nested");
        assertEquals(1, nested.size());
        assertEquals(NESTED_RECORD_VALUE, nested.get("test"));

        assertInstanceOf(List.class, fullConversion.get("list"));
        List recordList = (List) fullConversion.get("list");
        assertEquals(5, recordList.size());
        for (Object rec : recordList) {
            assertInstanceOf(Map.class, rec);
            Map<String, Object> map = (Map<String, Object>) rec;
            assertEquals(1, map.size());
            assertEquals(NESTED_RECORD_VALUE, map.get("test"));
        }
    }

    @ParameterizedTest
    @MethodSource("provideLocalDates")
    void testGettingLocalDate(final String input, final String format, LocalDate expectedDate) {
        executeDateTimeTest(input, format, expectedDate, MapRecord::getAsLocalDate);
    }

    @ParameterizedTest
    @MethodSource("provideLocalDateTimes")
    void testGettingLocalDateTime(final String input, final String format, LocalDateTime expectedDateTime) {
        executeDateTimeTest(input, format, expectedDateTime, MapRecord::getAsLocalDateTime);
    }

    @ParameterizedTest
    @MethodSource("provideOffsetDateTimes")
    void testGettingOffsetDateTime(final String input, final String format, OffsetDateTime expectedOffsetDateTime) {
        executeDateTimeTest(input, format, expectedOffsetDateTime, MapRecord::getAsOffsetDateTime);
    }

    private <T> void executeDateTimeTest(final String input,
        final String format,
        final Object expectedDateTime,
        TriFunction<MapRecord, String, String, T> dateTimeFunction) {
        // create a `MapRecord` from the input
        final List<RecordField> fields = new ArrayList<>();
        final String timestampFieldName = "timestamp";
        fields.add(new RecordField(timestampFieldName, RecordFieldType.TIMESTAMP.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);
        final HashMap<String, Object> item = new HashMap<>();
        item.put(timestampFieldName, input);
        final MapRecord testRecord = new MapRecord(schema, item);

        // apply the datetime function to the record and compare
        final T actualDateTime = dateTimeFunction.apply(testRecord, timestampFieldName, format);
        assertEquals(expectedDateTime, actualDateTime);
    }

    private static Stream<Arguments> provideLocalDates() {
        return Stream.of(
            Arguments.of("2022-01-01", ISO_LOCAL_DATE, LocalDate.parse("2022-01-01")),
            Arguments.of("2022-01-01T12:34:56.789", ISO_LOCAL_DATE_TIME, LocalDate.parse("2022-01-01")),
            Arguments.of("2017-06-23T01:02:03.456", ISO_LOCAL_DATE_TIME, LocalDate.parse("2017-06-23")),
            Arguments.of("2020-02-29T23:59:59.999", ISO_LOCAL_DATE_TIME, LocalDate.parse("2020-02-29")), // leap year
            Arguments.of("2024-03-10T02:00:00.000", ISO_LOCAL_DATE_TIME, LocalDate.parse("2024-03-10")), // DST transition
            // test minimum and maximum values
            Arguments.of("0001-01-01T00:00:00.000", ISO_LOCAL_DATE_TIME, LocalDate.parse("0001-01-01")),
            Arguments.of("9999-12-31T23:59:59.999", ISO_LOCAL_DATE_TIME, LocalDate.parse("9999-12-31"))
        );
    }

    private static Stream<Arguments> provideLocalDateTimes() {
        return Stream.of(
            Arguments.of("2022-01-01T12:34:56.789", ISO_LOCAL_DATE_TIME, LocalDateTime.parse("2022-01-01T12:34:56.789")),
            Arguments.of("2017-06-23T01:02:03.456", ISO_LOCAL_DATE_TIME, LocalDateTime.parse("2017-06-23T01:02:03.456")),
            Arguments.of("2020-02-29T23:59:59.999", ISO_LOCAL_DATE_TIME, LocalDateTime.parse("2020-02-29T23:59:59.999")), // leap year
            Arguments.of("2024-03-10T02:00:00.000", ISO_LOCAL_DATE_TIME, LocalDateTime.parse("2024-03-10T02:00:00.000")), // DST transition
            // test minimum and maximum values
            Arguments.of("0001-01-01T00:00:00.000", ISO_LOCAL_DATE_TIME, LocalDateTime.parse("0001-01-01T00:00:00.000")),
            Arguments.of("9999-12-31T23:59:59.999", ISO_LOCAL_DATE_TIME, LocalDateTime.parse("9999-12-31T23:59:59.999"))
        );
    }

    private static Stream<Arguments> provideOffsetDateTimes() {
        return Stream.of(
            Arguments.of("2022-01-01T12:34:56.789+00:00", ISO_OFFSET_DATE_TIME, OffsetDateTime.parse("2022-01-01T12:34:56.789+00:00")),
            Arguments.of("2017-06-23T01:02:03.456+00:00", ISO_OFFSET_DATE_TIME, OffsetDateTime.parse("2017-06-23T01:02:03.456+00:00")),
            Arguments.of("2020-02-29T23:59:59.999+00:00", ISO_OFFSET_DATE_TIME, OffsetDateTime.parse("2020-02-29T23:59:59.999+00:00")), // leap year
            Arguments.of("2024-03-10T02:00:00.000+00:00", ISO_OFFSET_DATE_TIME, OffsetDateTime.parse("2024-03-10T02:00:00.000+00:00")), // DST transition
            // test minimum and maximum values
            Arguments.of("0001-01-01T00:00:00.000+00:00", ISO_OFFSET_DATE_TIME, OffsetDateTime.parse("0001-01-01T00:00:00.000+00:00")),
            Arguments.of("9999-12-31T23:59:59.999+00:00", ISO_OFFSET_DATE_TIME, OffsetDateTime.parse("9999-12-31T23:59:59.999+00:00"))
        );
    }
}
