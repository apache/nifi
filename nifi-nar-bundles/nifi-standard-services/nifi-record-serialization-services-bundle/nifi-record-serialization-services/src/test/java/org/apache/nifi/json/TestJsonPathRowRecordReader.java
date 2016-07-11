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

package org.apache.nifi.json;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.jayway.jsonpath.JsonPath;

public class TestJsonPathRowRecordReader {
    private final LinkedHashMap<String, JsonPath> allJsonPaths = new LinkedHashMap<>();

    @Before
    public void populateJsonPaths() {
        allJsonPaths.clear();

        allJsonPaths.put("id", JsonPath.compile("$.id"));
        allJsonPaths.put("name", JsonPath.compile("$.name"));
        allJsonPaths.put("balance", JsonPath.compile("$.balance"));
        allJsonPaths.put("address", JsonPath.compile("$.address"));
        allJsonPaths.put("city", JsonPath.compile("$.city"));
        allJsonPaths.put("state", JsonPath.compile("$.state"));
        allJsonPaths.put("zipCode", JsonPath.compile("$.zipCode"));
        allJsonPaths.put("country", JsonPath.compile("$.country"));
    }

    @Test
    public void testReadArray() throws IOException, MalformedRecordException {
        try (final InputStream in = new FileInputStream(new File("src/test/resources/json/bank-account-array.json"));
            final JsonPathRowRecordReader reader = new JsonPathRowRecordReader(allJsonPaths, Collections.emptyMap(), in, Mockito.mock(ComponentLog.class))) {

            final RecordSchema schema = reader.getSchema();

            final List<String> fieldNames = schema.getFieldNames();
            final List<String> expectedFieldNames = Arrays.asList(new String[] {"id", "name", "balance", "address", "city", "state", "zipCode", "country"});
            assertEquals(expectedFieldNames, fieldNames);

            final List<RecordFieldType> dataTypes = schema.getDataTypes().stream().map(dt -> dt.getFieldType()).collect(Collectors.toList());
            final List<RecordFieldType> expectedTypes = Arrays.asList(new RecordFieldType[] {RecordFieldType.INT, RecordFieldType.STRING,
                RecordFieldType.DOUBLE, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING});
            assertEquals(expectedTypes, dataTypes);

            final Object[] firstRecordValues = reader.nextRecord().getValues();
            Assert.assertArrayEquals(new Object[] {1, "John Doe", 4750.89, "123 My Street", "My City", "MS", "11111", "USA"}, firstRecordValues);

            final Object[] secondRecordValues = reader.nextRecord().getValues();
            Assert.assertArrayEquals(new Object[] {2, "Jane Doe", 4820.09, "321 Your Street", "Your City", "NY", "33333", "USA"}, secondRecordValues);

            assertNull(reader.nextRecord());
        }
    }

    @Test
    public void testSingleJsonElement() throws IOException, MalformedRecordException {
        try (final InputStream in = new FileInputStream(new File("src/test/resources/json/single-bank-account.json"));
            final JsonPathRowRecordReader reader = new JsonPathRowRecordReader(allJsonPaths, Collections.emptyMap(), in, Mockito.mock(ComponentLog.class))) {

            final RecordSchema schema = reader.getSchema();

            final List<String> fieldNames = schema.getFieldNames();
            final List<String> expectedFieldNames = Arrays.asList(new String[] {"id", "name", "balance", "address", "city", "state", "zipCode", "country"});
            assertEquals(expectedFieldNames, fieldNames);

            final List<RecordFieldType> dataTypes = schema.getDataTypes().stream().map(dt -> dt.getFieldType()).collect(Collectors.toList());
            final List<RecordFieldType> expectedTypes = Arrays.asList(new RecordFieldType[] {RecordFieldType.INT, RecordFieldType.STRING,
                RecordFieldType.DOUBLE, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING});
            assertEquals(expectedTypes, dataTypes);

            final Object[] firstRecordValues = reader.nextRecord().getValues();
            Assert.assertArrayEquals(new Object[] {1, "John Doe", 4750.89, "123 My Street", "My City", "MS", "11111", "USA"}, firstRecordValues);

            assertNull(reader.nextRecord());
        }
    }



    @Test
    public void testElementWithNestedData() throws IOException, MalformedRecordException {
        final LinkedHashMap<String, JsonPath> jsonPaths = new LinkedHashMap<>(allJsonPaths);
        jsonPaths.put("account", JsonPath.compile("$.account"));

        try (final InputStream in = new FileInputStream(new File("src/test/resources/json/single-element-nested.json"));
            final JsonPathRowRecordReader reader = new JsonPathRowRecordReader(jsonPaths, Collections.emptyMap(), in, Mockito.mock(ComponentLog.class))) {

            final RecordSchema schema = reader.getSchema();

            final List<String> fieldNames = schema.getFieldNames();
            final List<String> expectedFieldNames = Arrays.asList(new String[] {"id", "name", "balance", "address", "city", "state", "zipCode", "country", "account"});
            assertEquals(expectedFieldNames, fieldNames);

            final List<RecordFieldType> dataTypes = schema.getDataTypes().stream().map(dt -> dt.getFieldType()).collect(Collectors.toList());
            final List<RecordFieldType> expectedTypes = Arrays.asList(new RecordFieldType[] {RecordFieldType.INT, RecordFieldType.STRING, RecordFieldType.STRING,
                RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.RECORD});
            assertEquals(expectedTypes, dataTypes);

            final Object[] firstRecordValues = reader.nextRecord().getValues();
            final Object[] simpleElements = Arrays.copyOfRange(firstRecordValues, 0, firstRecordValues.length - 1);
            Assert.assertArrayEquals(new Object[] {1, "John Doe", null, "123 My Street", "My City", "MS", "11111", "USA"}, simpleElements);

            final Object lastElement = firstRecordValues[firstRecordValues.length - 1];
            assertTrue(lastElement instanceof Record);
            final Record record = (Record) lastElement;
            assertEquals(42, record.getValue("id"));
            assertEquals(4750.89D, record.getValue("balance"));

            assertNull(reader.nextRecord());
        }
    }

    @Test
    public void testElementWithNestedArray() throws IOException, MalformedRecordException {
        final LinkedHashMap<String, JsonPath> jsonPaths = new LinkedHashMap<>(allJsonPaths);
        jsonPaths.put("accounts", JsonPath.compile("$.accounts"));

        try (final InputStream in = new FileInputStream(new File("src/test/resources/json/single-element-nested-array.json"));
            final JsonPathRowRecordReader reader = new JsonPathRowRecordReader(jsonPaths, Collections.emptyMap(), in, Mockito.mock(ComponentLog.class))) {

            final RecordSchema schema = reader.getSchema();

            final List<String> fieldNames = schema.getFieldNames();
            final List<String> expectedFieldNames = Arrays.asList(new String[] {
                "id", "name", "balance", "address", "city", "state", "zipCode", "country", "accounts"});
            assertEquals(expectedFieldNames, fieldNames);

            final List<RecordFieldType> dataTypes = schema.getDataTypes().stream().map(dt -> dt.getFieldType()).collect(Collectors.toList());
            final List<RecordFieldType> expectedTypes = Arrays.asList(new RecordFieldType[] {RecordFieldType.INT, RecordFieldType.STRING, RecordFieldType.STRING,
                RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.ARRAY});
            assertEquals(expectedTypes, dataTypes);

            final Object[] firstRecordValues = reader.nextRecord().getValues();
            final Object[] nonArrayValues = Arrays.copyOfRange(firstRecordValues, 0, firstRecordValues.length - 1);
            Assert.assertArrayEquals(new Object[] {1, "John Doe", null, "123 My Street", "My City", "MS", "11111", "USA"}, nonArrayValues);

            final Object lastRecord = firstRecordValues[firstRecordValues.length - 1];
            assertTrue(Object[].class.isAssignableFrom(lastRecord.getClass()));

            final Object[] array = (Object[]) lastRecord;
            assertEquals(2, array.length);
            final Object firstElement = array[0];
            assertTrue(firstElement instanceof Map);

            final Map<?, ?> firstMap = (Map<?, ?>) firstElement;
            assertEquals(42, firstMap.get("id"));
            assertEquals(4750.89D, firstMap.get("balance"));

            final Object secondElement = array[1];
            assertTrue(secondElement instanceof Map);
            final Map<?, ?> secondMap = (Map<?, ?>) secondElement;
            assertEquals(43, secondMap.get("id"));
            assertEquals(48212.38D, secondMap.get("balance"));

            assertNull(reader.nextRecord());
        }
    }

    @Test
    public void testReadArrayDifferentSchemas() throws IOException, MalformedRecordException {
        try (final InputStream in = new FileInputStream(new File("src/test/resources/json/bank-account-array-different-schemas.json"));
            final JsonPathRowRecordReader reader = new JsonPathRowRecordReader(allJsonPaths, Collections.emptyMap(), in, Mockito.mock(ComponentLog.class))) {

            final RecordSchema schema = reader.getSchema();

            final List<String> fieldNames = schema.getFieldNames();
            final List<String> expectedFieldNames = Arrays.asList(new String[] {"id", "name", "balance", "address", "city", "state", "zipCode", "country"});
            assertEquals(expectedFieldNames, fieldNames);

            final List<RecordFieldType> dataTypes = schema.getDataTypes().stream().map(dt -> dt.getFieldType()).collect(Collectors.toList());
            final List<RecordFieldType> expectedTypes = Arrays.asList(new RecordFieldType[] {RecordFieldType.INT, RecordFieldType.STRING,
                RecordFieldType.DOUBLE, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING});
            assertEquals(expectedTypes, dataTypes);

            final Object[] firstRecordValues = reader.nextRecord().getValues();
            Assert.assertArrayEquals(new Object[] {1, "John Doe", 4750.89, "123 My Street", "My City", "MS", "11111", "USA"}, firstRecordValues);

            final Object[] secondRecordValues = reader.nextRecord().getValues();
            Assert.assertArrayEquals(new Object[] {2, "Jane Doe", 4820.09, "321 Your Street", "Your City", "NY", "33333", null}, secondRecordValues);

            final Object[] thirdRecordValues = reader.nextRecord().getValues();
            Assert.assertArrayEquals(new Object[] {3, "Jake Doe", 4751.89, "124 My Street", "My City", "MS", "11111", "USA"}, thirdRecordValues);

            assertNull(reader.nextRecord());
        }
    }

    @Test
    public void testReadArrayDifferentSchemasWithOverride() throws IOException, MalformedRecordException {
        final LinkedHashMap<String, JsonPath> jsonPaths = new LinkedHashMap<>(allJsonPaths);
        jsonPaths.put("address2", JsonPath.compile("$.address2"));
        final Map<String, DataType> typeOverrides = Collections.singletonMap("address2", RecordFieldType.STRING.getDataType());

        try (final InputStream in = new FileInputStream(new File("src/test/resources/json/bank-account-array-different-schemas.json"));
            final JsonPathRowRecordReader reader = new JsonPathRowRecordReader(jsonPaths, typeOverrides, in, Mockito.mock(ComponentLog.class))) {
            final RecordSchema schema = reader.getSchema();

            final List<String> fieldNames = schema.getFieldNames();
            final List<String> expectedFieldNames = Arrays.asList(new String[] {"id", "name", "balance", "address", "city", "state", "zipCode", "country", "address2"});
            assertEquals(expectedFieldNames, fieldNames);

            final List<RecordFieldType> dataTypes = schema.getDataTypes().stream().map(dt -> dt.getFieldType()).collect(Collectors.toList());
            final List<RecordFieldType> expectedTypes = Arrays.asList(new RecordFieldType[] {RecordFieldType.INT, RecordFieldType.STRING, RecordFieldType.DOUBLE, RecordFieldType.STRING,
                RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING});
            assertEquals(expectedTypes, dataTypes);

            final Object[] firstRecordValues = reader.nextRecord().getValues();
            Assert.assertArrayEquals(new Object[] {1, "John Doe", 4750.89, "123 My Street", "My City", "MS", "11111", "USA", null}, firstRecordValues);

            final Object[] secondRecordValues = reader.nextRecord().getValues();
            Assert.assertArrayEquals(new Object[] {2, "Jane Doe", 4820.09, "321 Your Street", "Your City", "NY", "33333", null, null}, secondRecordValues);

            final Object[] thirdRecordValues = reader.nextRecord().getValues();
            Assert.assertArrayEquals(new Object[] {3, "Jake Doe", 4751.89, "124 My Street", "My City", "MS", "11111", "USA", "Apt. #12"}, thirdRecordValues);

            assertNull(reader.nextRecord());
        }
    }

    @Test
    public void testPrimitiveTypeArrays() throws IOException, MalformedRecordException {
        final LinkedHashMap<String, JsonPath> jsonPaths = new LinkedHashMap<>(allJsonPaths);
        jsonPaths.put("accountIds", JsonPath.compile("$.accountIds"));

        try (final InputStream in = new FileInputStream(new File("src/test/resources/json/primitive-type-array.json"));
            final JsonPathRowRecordReader reader = new JsonPathRowRecordReader(jsonPaths, Collections.emptyMap(), in, Mockito.mock(ComponentLog.class))) {

            final RecordSchema schema = reader.getSchema();

            final List<String> fieldNames = schema.getFieldNames();
            final List<String> expectedFieldNames = Arrays.asList(new String[] {"id", "name", "balance", "address", "city", "state", "zipCode", "country", "accountIds"});
            assertEquals(expectedFieldNames, fieldNames);

            final List<RecordFieldType> dataTypes = schema.getDataTypes().stream().map(dt -> dt.getFieldType()).collect(Collectors.toList());
            final List<RecordFieldType> expectedTypes = Arrays.asList(new RecordFieldType[] {RecordFieldType.INT, RecordFieldType.STRING, RecordFieldType.DOUBLE, RecordFieldType.STRING,
                RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.ARRAY});
            assertEquals(expectedTypes, dataTypes);

            final Object[] firstRecordValues = reader.nextRecord().getValues();

            final Object[] nonArrayValues = Arrays.copyOfRange(firstRecordValues, 0, firstRecordValues.length - 1);
            Assert.assertArrayEquals(new Object[] {1, "John Doe", 4750.89D, "123 My Street", "My City", "MS", "11111", "USA"}, nonArrayValues);

            final Object lastRecord = firstRecordValues[firstRecordValues.length - 1];
            assertNotNull(lastRecord);
            assertTrue(Object[].class.isAssignableFrom(lastRecord.getClass()));

            final Object[] array = (Object[]) lastRecord;
            Assert.assertArrayEquals(new Object[] {1, 2, 3}, array);

            assertNull(reader.nextRecord());
            assertNull(reader.nextRecord());
        }
    }
}
