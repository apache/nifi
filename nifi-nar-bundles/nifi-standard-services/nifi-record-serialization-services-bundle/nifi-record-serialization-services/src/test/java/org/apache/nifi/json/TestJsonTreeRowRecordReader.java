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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
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
import org.junit.Test;
import org.mockito.Mockito;

public class TestJsonTreeRowRecordReader {

    @Test
    public void testReadArray() throws IOException, MalformedRecordException {
        try (final InputStream in = new FileInputStream(new File("src/test/resources/json/bank-account-array.json"));
            final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(in, Mockito.mock(ComponentLog.class), Collections.emptyMap())) {

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
            final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(in, Mockito.mock(ComponentLog.class), Collections.emptyMap())) {

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
        try (final InputStream in = new FileInputStream(new File("src/test/resources/json/single-element-nested.json"));
            final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(in, Mockito.mock(ComponentLog.class), Collections.emptyMap())) {

            final RecordSchema schema = reader.getSchema();

            final List<String> fieldNames = schema.getFieldNames();
            final List<String> expectedFieldNames = Arrays.asList(new String[] {"id", "name", "address", "city", "state", "zipCode", "country", "account"});
            assertEquals(expectedFieldNames, fieldNames);

            final List<RecordFieldType> dataTypes = schema.getDataTypes().stream().map(dt -> dt.getFieldType()).collect(Collectors.toList());
            final List<RecordFieldType> expectedTypes = Arrays.asList(new RecordFieldType[] {RecordFieldType.INT, RecordFieldType.STRING,
                RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.RECORD});
            assertEquals(expectedTypes, dataTypes);

            final Object[] firstRecordValues = reader.nextRecord().getValues();
            final Object[] allButLast = Arrays.copyOfRange(firstRecordValues, 0, firstRecordValues.length - 1);
            Assert.assertArrayEquals(new Object[] {1, "John Doe", "123 My Street", "My City", "MS", "11111", "USA"}, allButLast);

            final Object last = firstRecordValues[firstRecordValues.length - 1];
            assertTrue(Record.class.isAssignableFrom(last.getClass()));
            final Record record = (Record) last;
            assertEquals(42, record.getValue("id"));
            assertEquals(4750.89, record.getValue("balance"));

            assertNull(reader.nextRecord());
        }
    }

    @Test
    public void testElementWithNestedArray() throws IOException, MalformedRecordException {
        try (final InputStream in = new FileInputStream(new File("src/test/resources/json/single-element-nested-array.json"));
            final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(in, Mockito.mock(ComponentLog.class), Collections.emptyMap())) {

            final RecordSchema schema = reader.getSchema();

            final List<String> fieldNames = schema.getFieldNames();
            final List<String> expectedFieldNames = Arrays.asList(new String[] {
                "id", "name", "address", "city", "state", "zipCode", "country", "accounts"});
            assertEquals(expectedFieldNames, fieldNames);

            final List<RecordFieldType> dataTypes = schema.getDataTypes().stream().map(dt -> dt.getFieldType()).collect(Collectors.toList());
            final List<RecordFieldType> expectedTypes = Arrays.asList(new RecordFieldType[] {RecordFieldType.INT, RecordFieldType.STRING,
                RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.ARRAY});
            assertEquals(expectedTypes, dataTypes);

            final Object[] firstRecordValues = reader.nextRecord().getValues();
            final Object[] nonArrayValues = Arrays.copyOfRange(firstRecordValues, 0, firstRecordValues.length - 1);
            Assert.assertArrayEquals(new Object[] {1, "John Doe", "123 My Street", "My City", "MS", "11111", "USA"}, nonArrayValues);

            final Object lastRecord = firstRecordValues[firstRecordValues.length - 1];
            assertTrue(Object[].class.isAssignableFrom(lastRecord.getClass()));

            assertNull(reader.nextRecord());
        }
    }

    @Test
    public void testReadArrayDifferentSchemas() throws IOException, MalformedRecordException {
        try (final InputStream in = new FileInputStream(new File("src/test/resources/json/bank-account-array-different-schemas.json"));
            final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(in, Mockito.mock(ComponentLog.class), Collections.emptyMap())) {

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
        final Map<String, DataType> overrides = new HashMap<>();
        overrides.put("address2", RecordFieldType.STRING.getDataType());

        try (final InputStream in = new FileInputStream(new File("src/test/resources/json/bank-account-array-different-schemas.json"));
            final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(in, Mockito.mock(ComponentLog.class), overrides)) {

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
    public void testReadArrayDifferentSchemasWithOptionalElementOverridden() throws IOException, MalformedRecordException {
        final Map<String, DataType> overrides = new HashMap<>();
        overrides.put("balance", RecordFieldType.DOUBLE.getDataType());

        try (final InputStream in = new FileInputStream(new File("src/test/resources/json/bank-account-array-optional-balance.json"));
            final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(in, Mockito.mock(ComponentLog.class), overrides)) {

            final RecordSchema schema = reader.getSchema();

            final List<String> fieldNames = schema.getFieldNames();
            final List<String> expectedFieldNames = Arrays.asList(new String[] {"id", "name", "balance", "address", "city", "state", "zipCode", "country"});
            assertEquals(expectedFieldNames, fieldNames);

            final List<RecordFieldType> dataTypes = schema.getDataTypes().stream().map(dt -> dt.getFieldType()).collect(Collectors.toList());
            final List<RecordFieldType> expectedTypes = Arrays.asList(new RecordFieldType[] {RecordFieldType.INT, RecordFieldType.STRING, RecordFieldType.DOUBLE, RecordFieldType.STRING,
                RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING});
            assertEquals(expectedTypes, dataTypes);

            final Object[] firstRecordValues = reader.nextRecord().getValues();
            Assert.assertArrayEquals(new Object[] {1, "John Doe", 4750.89, "123 My Street", "My City", "MS", "11111", "USA"}, firstRecordValues);

            final Object[] secondRecordValues = reader.nextRecord().getValues();
            Assert.assertArrayEquals(new Object[] {2, "Jane Doe", null, "321 Your Street", "Your City", "NY", "33333", "USA"}, secondRecordValues);

            final Object[] thirdRecordValues = reader.nextRecord().getValues();
            Assert.assertArrayEquals(new Object[] {3, "Jimmy Doe", null, "321 Your Street", "Your City", "NY", "33333", "USA"}, thirdRecordValues);

            assertNull(reader.nextRecord());
        }
    }


    @Test
    public void testReadUnicodeCharacters() throws IOException, MalformedRecordException {
        try (final InputStream in = new FileInputStream(new File("src/test/resources/json/json-with-unicode.json"));
            final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(in, Mockito.mock(ComponentLog.class), Collections.emptyMap())) {

            final Object[] firstRecordValues = reader.nextRecord().getValues();

            final Object secondValue = firstRecordValues[1];
            assertTrue(secondValue instanceof Long);
            assertEquals(832036744985577473L, secondValue);

            final Object unicodeValue = firstRecordValues[2];
            assertEquals("\u3061\u3083\u6ce3\u304d\u305d\u3046", unicodeValue);

            assertNull(reader.nextRecord());
        }
    }

}
