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
package org.apache.nifi.processors.snowflake.snowpipe.streaming.converter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.junit.jupiter.api.Test;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ObjectLogicalDataTypeConverterTest {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final ObjectLogicalDataTypeConverter converter = new ObjectLogicalDataTypeConverter();

    @Test
    void testMapOfMapOfMap() throws JsonProcessingException {
        final MapRecord root = generateMapOfMapOfMap();

        final String val = ((MapRecord) ((MapRecord) root.toMap().get("child1"))
            .toMap().get("child2"))
            .toMap().get("child3")
            .toString();


        assertEquals("val", val);

        final Object converted = converter.getConvertedObject(root, LogicalDataType.OBJECT);
        final int nesting = testNesting(converted);
        assertEquals(4, nesting);

        final String expected = """
            {
              "child1" : {
                "child2" : {
                  "child3" : "val"
                }
              }
            }""";
        assertJsonEquals(expected, converted);
    }

    @Test
    void testArrayOfArrayOfMap() throws JsonProcessingException {
        final Object[] root = generateArrayOfArrayOfMapRecord();
        final String value = ((MapRecord) ((Object[]) root[0])[0]).getValue("key").toString();
        assertEquals("val", value);

        final Object converted = converter.getConvertedObject(root, LogicalDataType.ARRAY);
        final int nesting = testNesting(converted);
        assertEquals(4, nesting);

        final String expected = """
            [ [ {
              "key" : "val"
            } ] ]""";
        assertJsonEquals(expected, converted);
    }

    @Test
    void testMapOfListOfMapOfListOfMap() throws JsonProcessingException {
        final MapRecord root = generateMapOfListOfMapOfListOfMap();

        final String value = (String) ((MapRecord) ((Object[]) ((MapRecord) ((Object[]) root.toMap().get("list"))
            [0]).toMap().get("list"))[0]).toMap().get("key");
        assertEquals("val", value);

        final Object converted = converter.getConvertedObject(root, LogicalDataType.OBJECT);
        final int nesting = testNesting(converted);
        assertEquals(6, nesting);

        final String expected = """
            {
              "list" : [ {
                "list" : [ {
                  "key" : "val"
                } ]
              } ]
            }""";
        assertJsonEquals(expected, converted);
    }

    @Test
    void testMapOfJavaSqlTypes() {
        final LocalDate date = LocalDate.of(2025, 1, 1);
        final LocalTime time = LocalTime.of(12, 0, 0);
        final LocalDateTime dateTime = LocalDateTime.of(2025, 1, 1, 12, 0, 0, 123456789);

        final MapRecord root = new MapRecord(new SimpleRecordSchema(List.of(
            new RecordField("date", RecordFieldType.DATE.getDataType()),
            new RecordField("time", RecordFieldType.TIME.getDataType()),
            new RecordField("timestamp", RecordFieldType.TIMESTAMP.getDataType()))),
            Map.of(
                "date", Date.valueOf(date),
                "time", Time.valueOf(time),
                "timestamp", Timestamp.valueOf(dateTime)
            ));

        final Map<String, Object> converted = getObjectMap(root);

        assertEquals(date, converted.get("date"));
        assertEquals(time, converted.get("time"));
        assertEquals(dateTime, converted.get("timestamp"));
    }

    @Test
    void testMapOfArrayJavaSqlTypes() {
        final LocalDate date = LocalDate.of(2025, 1, 1);
        final LocalTime time = LocalTime.of(12, 0, 0);
        final LocalDateTime dateTime = LocalDateTime.of(2025, 1, 1, 12, 0, 0, 123456789);

        final MapRecord root = new MapRecord(new SimpleRecordSchema(List.of(
            new RecordField("date", new ArrayDataType(RecordFieldType.DATE.getDataType())),
            new RecordField("time", new ArrayDataType(RecordFieldType.TIME.getDataType())),
            new RecordField("timestamp", new ArrayDataType(RecordFieldType.TIMESTAMP.getDataType())))),
            Map.of(
                "date", new Object[]{Date.valueOf(date)},
                "time", new Object[]{Time.valueOf(time)},
                "timestamp", new Object[]{Timestamp.valueOf(dateTime)}
            ));

        final Map<String, Object> converted = getObjectMap(root);

        assertEquals(date, nestedElementOf(converted.get("date")));
        assertEquals(time, nestedElementOf(converted.get("time")));
        assertEquals(dateTime, nestedElementOf(converted.get("timestamp")));
    }

    @Test
    void testIntToLocalDate() {
        final int number = 1;

        final Object converted = converter.getConvertedObject(number, LogicalDataType.DATE);

        final LocalDate expected = LocalDate.of(1970, 1, 1);
        assertEquals(expected, converted);
    }

    @Test
    void testLongMillisecondsToLocalDate() {
        final long milliseconds = 31536000001L;

        final Object converted = converter.getConvertedObject(milliseconds, LogicalDataType.DATE);

        final LocalDate expected = LocalDate.of(1971, 1, 1);
        assertEquals(expected, converted);
    }

    @Test
    void testLongMicrosecondsToLocalDate() {
        final long microseconds = 31536000000001L;

        final Object converted = converter.getConvertedObject(microseconds, LogicalDataType.DATE);

        final LocalDate expected = LocalDate.of(1971, 1, 1);
        assertEquals(expected, converted);
    }

    @Test
    void testLongNanosecondsToLocalDate() {
        final long nanoseconds = 31536000000000001L;

        final Object converted = converter.getConvertedObject(nanoseconds, LogicalDataType.DATE);

        final LocalDate expected = LocalDate.of(1971, 1, 1);
        assertEquals(expected, converted);
    }

    @Test
    void testIntToLocalTime() {
        final int number = 1;

        final Object converted = converter.getConvertedObject(number, LogicalDataType.TIME);

        final LocalTime expected = LocalTime.of(0, 0, 1);
        assertEquals(expected, converted);
    }

    @Test
    void testLongMillisecondsToLocalTime() {
        final long milliseconds = 31536000001L;

        final Object converted = converter.getConvertedObject(milliseconds, LogicalDataType.TIME);

        final int expectedNanoseconds = 1000000;
        final LocalTime expected = LocalTime.of(0, 0, 0, expectedNanoseconds);
        assertEquals(expected, converted);
    }

    @Test
    void testLongMicrosecondsToLocalTime() {
        final long microseconds = 31536000000001L;

        final Object converted = converter.getConvertedObject(microseconds, LogicalDataType.TIME);

        final int expectedNanoseconds = 1000;
        final LocalTime expected = LocalTime.of(0, 0, 0, expectedNanoseconds);
        assertEquals(expected, converted);
    }

    @Test
    void testLongNanosecondsToLocalTime() {
        final long nanoseconds = 31536000000000001L;

        final Object converted = converter.getConvertedObject(nanoseconds, LogicalDataType.TIME);

        final int expectedNanoseconds = 1;
        final LocalTime expected = LocalTime.of(0, 0, 0, expectedNanoseconds);
        assertEquals(expected, converted);
    }

    @Test
    void testIntToInstantTimestampTimeZone() {
        final int number = 1;

        final Object converted = converter.getConvertedObject(number, LogicalDataType.TIMESTAMP_TZ);

        final Instant expected = Instant.ofEpochSecond(1, 0);
        assertEquals(expected, converted);
    }

    @Test
    void testLongMillisecondsToInstantTimestampTimeZone() {
        final long milliseconds = 31536000001L;

        final Object converted = converter.getConvertedObject(milliseconds, LogicalDataType.TIMESTAMP_TZ);

        final Instant expected = Instant.ofEpochMilli(milliseconds);
        assertEquals(expected, converted);
    }

    @Test
    void testLongMicrosecondsToInstantTimestampTimeZone() {
        final long microseconds = 31536000000001L;

        final Object converted = converter.getConvertedObject(microseconds, LogicalDataType.TIMESTAMP_TZ);

        final int expectedNanoseconds = 1000;
        final OffsetDateTime expectedDateTime = OffsetDateTime.of(1971, 1, 1, 0, 0, 0, expectedNanoseconds, ZoneOffset.UTC);

        final Instant expected = expectedDateTime.toInstant();
        assertEquals(expected, converted);
    }

    @Test
    void testLongNanosecondsToInstantTimestampTimeZone() {
        final long nanoseconds = 31536000000000001L;

        final Object converted = converter.getConvertedObject(nanoseconds, LogicalDataType.TIMESTAMP_TZ);

        final int expectedNanoseconds = 1;
        final OffsetDateTime expectedDateTime = OffsetDateTime.of(1971, 1, 1, 0, 0, 0, expectedNanoseconds, ZoneOffset.UTC);

        final Instant expected = expectedDateTime.toInstant();
        assertEquals(expected, converted);
    }

    private Object nestedElementOf(final Object date) {
        return ((Object[]) date)[0];
    }

    private MapRecord generateMapOfMapOfMap() {
        // Define schemas
        final RecordField child3Field = new RecordField("child3", RecordFieldType.STRING.getDataType());
        final RecordSchema child3Schema = new SimpleRecordSchema(Collections.singletonList(child3Field));

        final RecordField child2Field = new RecordField("child2", RecordFieldType.RECORD.getRecordDataType(child3Schema));
        final RecordSchema child2Schema = new SimpleRecordSchema(Collections.singletonList(child2Field));

        final RecordField child1Field = new RecordField("child1", RecordFieldType.RECORD.getRecordDataType(child2Schema));
        final RecordSchema rootSchema = new SimpleRecordSchema(Collections.singletonList(child1Field));

        // Create the nested structure
        final MapRecord child3Record = new MapRecord(child3Schema, Map.of("child3", "val"));
        final MapRecord child2Record = new MapRecord(child2Schema, Map.of("child2", child3Record));
        return new MapRecord(rootSchema, Map.of("child1", child2Record));
    }

    private Object[] generateArrayOfArrayOfMapRecord() {
        final RecordField keyField = new RecordField("key", RecordFieldType.STRING.getDataType());
        final RecordSchema mapRecordSchema = new SimpleRecordSchema(Collections.singletonList(keyField));
        final MapRecord mapRecord = new MapRecord(mapRecordSchema, Map.of("key", "val"));

        return new Object[]{
            new Object[]{
                mapRecord
            }
        };
    }

    private MapRecord generateMapOfListOfMapOfListOfMap() {
        final RecordField innermostField = new RecordField("key", RecordFieldType.STRING.getDataType());
        final RecordSchema innermostSchema = new SimpleRecordSchema(Collections.singletonList(innermostField));

        final RecordField listOfInnermostField = new RecordField("list", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.RECORD.getRecordDataType(innermostSchema)));
        final RecordSchema listOfInnermostSchema = new SimpleRecordSchema(Collections.singletonList(listOfInnermostField));

        final RecordField mapOfListField = new RecordField("map", RecordFieldType.RECORD.getRecordDataType(listOfInnermostSchema));
        final RecordSchema mapOfListSchema = new SimpleRecordSchema(Collections.singletonList(mapOfListField));

        final RecordField listOfMapField = new RecordField("list", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.RECORD.getRecordDataType(mapOfListSchema)));
        final RecordSchema listOfMapSchema = new SimpleRecordSchema(Collections.singletonList(listOfMapField));

        final MapRecord innermostRecord = new MapRecord(innermostSchema, Map.of("key", "val"));
        final Object[] listOfInnermost = new Object[]{innermostRecord};
        final MapRecord mapOfListRecord = new MapRecord(listOfInnermostSchema, Map.of("list", listOfInnermost));
        final Object[] listOfMap = new Object[]{mapOfListRecord};
        return new MapRecord(listOfMapSchema, Map.of("list", listOfMap));
    }

    private int testNesting(final Object object) {
        final int nodes = 1;

        if (object instanceof Map<?, ?> map) {
            return nodes + testMap(map);
        } else if (object instanceof Object[] array) {
            return nodes + testArray(array);
        }
        return nodes;
    }

    private int testMap(final Map<?, ?> inputMap) {
        int nodes = 1;
        for (final Map.Entry<?, ?> entry : inputMap.entrySet()) {
            final Object value = entry.getValue();
            if (value instanceof Map<?, ?> map) {
                nodes += testMap(map);
            } else if (value instanceof Object[] array) {
                nodes += testArray(array);
            }
        }
        return nodes;
    }

    private int testArray(final Object[] inputArray) {
        int nodes = 1;
        for (final Object object : inputArray) {
            if (object instanceof Map<?, ?> map) {
                nodes += testMap(map);
            } else if (object instanceof Object[] array) {
                nodes += testArray(array);
            }
        }
        return nodes;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getObjectMap(final Object input) {
        return (Map<String, Object>) converter.getConvertedObject(input, LogicalDataType.OBJECT);
    }

    private void assertJsonEquals(final String expected, final Object actual) throws JsonProcessingException {
        assertEquals(expected.replaceAll("\\s", ""), objectMapper.writeValueAsString(actual));
    }
}
