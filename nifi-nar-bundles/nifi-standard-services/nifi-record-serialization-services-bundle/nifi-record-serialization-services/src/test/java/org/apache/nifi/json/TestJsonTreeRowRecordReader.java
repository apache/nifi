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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

public class TestJsonTreeRowRecordReader {
    private final String dateFormat = RecordFieldType.DATE.getDefaultFormat();
    private final String timeFormat = RecordFieldType.TIME.getDefaultFormat();
    private final String timestampFormat = RecordFieldType.TIMESTAMP.getDefaultFormat();

    private List<RecordField> getDefaultFields() {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("balance", RecordFieldType.DOUBLE.getDataType()));
        fields.add(new RecordField("address", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("city", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("state", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("zipCode", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("country", RecordFieldType.STRING.getDataType()));
        return fields;
    }

    private RecordSchema getAccountSchema() {
        final List<RecordField> accountFields = new ArrayList<>();
        accountFields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        accountFields.add(new RecordField("balance", RecordFieldType.DOUBLE.getDataType()));

        final RecordSchema accountSchema = new SimpleRecordSchema(accountFields);
        return accountSchema;
    }

    @Test
    @Ignore("Intended only for manual testing to determine performance before/after modifications")
    public void testPerformanceOnLocalFile() throws IOException, MalformedRecordException {
        final RecordSchema schema = new SimpleRecordSchema(Collections.emptyList());

        final File file = new File("/devel/nifi/nifi-assembly/target/nifi-1.2.0-SNAPSHOT-bin/nifi-1.2.0-SNAPSHOT/prov/16812193969219289");
        final byte[] data = Files.readAllBytes(file.toPath());

        final ComponentLog logger = Mockito.mock(ComponentLog.class);

        int recordCount = 0;
        final int iterations = 1000;

        for (int j = 0; j < 5; j++) {
            final long start = System.nanoTime();
            for (int i = 0; i < iterations; i++) {
                try (final InputStream in = new ByteArrayInputStream(data);
                    final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(in, logger, schema, dateFormat, timeFormat, timestampFormat)) {
                    while (reader.nextRecord() != null) {
                        recordCount++;
                    }
                }
            }
            final long nanos = System.nanoTime() - start;
            final long millis = TimeUnit.NANOSECONDS.toMillis(nanos);
            System.out.println("Took " + millis + " millis to read " + recordCount + " records");
        }
    }

    @Test
    @Ignore("Intended only for manual testing to determine performance before/after modifications")
    public void testPerformanceOnIndividualMessages() throws IOException, MalformedRecordException {
        final RecordSchema schema = new SimpleRecordSchema(Collections.emptyList());

        final File file = new File("/devel/nifi/nifi-assembly/target/nifi-1.2.0-SNAPSHOT-bin/nifi-1.2.0-SNAPSHOT/1.prov.json");
        final byte[] data = Files.readAllBytes(file.toPath());

        final ComponentLog logger = Mockito.mock(ComponentLog.class);

        int recordCount = 0;
        final int iterations = 1_000_000;

        for (int j = 0; j < 5; j++) {
            final long start = System.nanoTime();
            for (int i = 0; i < iterations; i++) {
                try (final InputStream in = new ByteArrayInputStream(data);
                    final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(in, logger, schema, dateFormat, timeFormat, timestampFormat)) {
                    while (reader.nextRecord() != null) {
                        recordCount++;
                    }
                }
            }
            final long nanos = System.nanoTime() - start;
            final long millis = TimeUnit.NANOSECONDS.toMillis(nanos);
            System.out.println("Took " + millis + " millis to read " + recordCount + " records");
        }
    }

    @Test
    public void testReadArray() throws IOException, MalformedRecordException {
        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());

        try (final InputStream in = new FileInputStream(new File("src/test/resources/json/bank-account-array.json"));
            final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(in, Mockito.mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat)) {

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
    public void testReadRawRecordIncludesFieldsNotInSchema() throws IOException, MalformedRecordException {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        try (final InputStream in = new FileInputStream(new File("src/test/resources/json/bank-account-array.json"));
            final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(in, Mockito.mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat)) {

            final Record schemaValidatedRecord = reader.nextRecord();
            assertEquals(1, schemaValidatedRecord.getValue("id"));
            assertEquals("John Doe", schemaValidatedRecord.getValue("name"));
            assertNull(schemaValidatedRecord.getValue("balance"));
        }

        try (final InputStream in = new FileInputStream(new File("src/test/resources/json/bank-account-array.json"));
            final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(in, Mockito.mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat)) {

            final Record rawRecord = reader.nextRecord(false, false);
            assertEquals(1, rawRecord.getValue("id"));
            assertEquals("John Doe", rawRecord.getValue("name"));
            assertEquals(4750.89, rawRecord.getValue("balance"));
            assertEquals("123 My Street", rawRecord.getValue("address"));
            assertEquals("My City", rawRecord.getValue("city"));
            assertEquals("MS", rawRecord.getValue("state"));
            assertEquals("11111", rawRecord.getValue("zipCode"));
            assertEquals("USA", rawRecord.getValue("country"));
        }
    }


    @Test
    public void testReadRawRecordTypeCoercion() throws IOException, MalformedRecordException {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        try (final InputStream in = new FileInputStream(new File("src/test/resources/json/bank-account-array.json"));
            final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(in, Mockito.mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat)) {

            final Record schemaValidatedRecord = reader.nextRecord();
            assertEquals("1", schemaValidatedRecord.getValue("id")); // will be coerced into a STRING as per the schema
            assertEquals("John Doe", schemaValidatedRecord.getValue("name"));
            assertNull(schemaValidatedRecord.getValue("balance"));

            assertEquals(2, schemaValidatedRecord.getRawFieldNames().size());
        }

        try (final InputStream in = new FileInputStream(new File("src/test/resources/json/bank-account-array.json"));
            final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(in, Mockito.mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat)) {

            final Record rawRecord = reader.nextRecord(false, false);
            assertEquals(1, rawRecord.getValue("id")); // will return raw value of (int) 1
            assertEquals("John Doe", rawRecord.getValue("name"));
            assertEquals(4750.89, rawRecord.getValue("balance"));
            assertEquals("123 My Street", rawRecord.getValue("address"));
            assertEquals("My City", rawRecord.getValue("city"));
            assertEquals("MS", rawRecord.getValue("state"));
            assertEquals("11111", rawRecord.getValue("zipCode"));
            assertEquals("USA", rawRecord.getValue("country"));

            assertEquals(8, rawRecord.getRawFieldNames().size());
        }
    }


    @Test
    public void testSingleJsonElement() throws IOException, MalformedRecordException {
        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());

        try (final InputStream in = new FileInputStream(new File("src/test/resources/json/single-bank-account.json"));
            final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(in, Mockito.mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat)) {

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
    public void testSingleJsonElementWithChoiceFields() throws IOException, MalformedRecordException {
        // Wraps default fields by Choice data type to test mapping to a Choice type.
        final List<RecordField> choiceFields = getDefaultFields().stream()
                .map(f -> new RecordField(f.getFieldName(), RecordFieldType.CHOICE.getChoiceDataType(f.getDataType()))).collect(Collectors.toList());
        final RecordSchema schema = new SimpleRecordSchema(choiceFields);

        try (final InputStream in = new FileInputStream(new File("src/test/resources/json/single-bank-account.json"));
             final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(in, Mockito.mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat)) {

            final List<String> fieldNames = schema.getFieldNames();
            final List<String> expectedFieldNames = Arrays.asList(new String[] {"id", "name", "balance", "address", "city", "state", "zipCode", "country"});
            assertEquals(expectedFieldNames, fieldNames);

            final List<RecordFieldType> expectedTypes = Arrays.asList(new RecordFieldType[] {RecordFieldType.INT, RecordFieldType.STRING,
                    RecordFieldType.DOUBLE, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING});
            final List<RecordField> fields = schema.getFields();
            for (int i = 0; i < schema.getFields().size(); i++) {
                assertTrue(fields.get(i).getDataType() instanceof ChoiceDataType);
                final ChoiceDataType choiceDataType = (ChoiceDataType) fields.get(i).getDataType();
                assertEquals(expectedTypes.get(i), choiceDataType.getPossibleSubTypes().get(0).getFieldType());
            }

            final Object[] firstRecordValues = reader.nextRecord().getValues();
            Assert.assertArrayEquals(new Object[] {1, "John Doe", 4750.89, "123 My Street", "My City", "MS", "11111", "USA"}, firstRecordValues);

            assertNull(reader.nextRecord());
        }
    }

    @Test
    public void testElementWithNestedData() throws IOException, MalformedRecordException {
        final DataType accountType = RecordFieldType.RECORD.getRecordDataType(getAccountSchema());
        final List<RecordField> fields = getDefaultFields();
        fields.add(new RecordField("account", accountType));
        fields.remove(new RecordField("balance", RecordFieldType.DOUBLE.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        try (final InputStream in = new FileInputStream(new File("src/test/resources/json/single-element-nested.json"));
            final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(in, Mockito.mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat)) {

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
        final DataType accountRecordType = RecordFieldType.RECORD.getRecordDataType(getAccountSchema());
        final DataType accountsType = RecordFieldType.ARRAY.getArrayDataType(accountRecordType);

        final List<RecordField> fields = getDefaultFields();
        fields.add(new RecordField("accounts", accountsType));
        fields.remove(new RecordField("balance", RecordFieldType.DOUBLE.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        try (final InputStream in = new FileInputStream(new File("src/test/resources/json/single-element-nested-array.json"));
            final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(in, Mockito.mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat)) {

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
        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());

        try (final InputStream in = new FileInputStream(new File("src/test/resources/json/bank-account-array-different-schemas.json"));
            final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(in, Mockito.mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat)) {

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

        final List<RecordField> fields = getDefaultFields();
        fields.add(new RecordField("address2", RecordFieldType.STRING.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        try (final InputStream in = new FileInputStream(new File("src/test/resources/json/bank-account-array-different-schemas.json"));
            final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(in, Mockito.mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat)) {

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
        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());

        try (final InputStream in = new FileInputStream(new File("src/test/resources/json/bank-account-array-optional-balance.json"));
            final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(in, Mockito.mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat)) {

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

        final List<RecordField> fromFields = new ArrayList<>();
        fromFields.add(new RecordField("id", RecordFieldType.LONG.getDataType()));
        fromFields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        final RecordSchema fromSchema = new SimpleRecordSchema(fromFields);
        final DataType fromType = RecordFieldType.RECORD.getRecordDataType(fromSchema);

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("created_at", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("id", RecordFieldType.LONG.getDataType()));
        fields.add(new RecordField("unicode", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("from", fromType));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        try (final InputStream in = new FileInputStream(new File("src/test/resources/json/json-with-unicode.json"));
            final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(in, Mockito.mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat)) {

            final Object[] firstRecordValues = reader.nextRecord().getValues();

            final Object secondValue = firstRecordValues[1];
            assertTrue(secondValue instanceof Long);
            assertEquals(832036744985577473L, secondValue);

            final Object unicodeValue = firstRecordValues[2];
            assertEquals("\u3061\u3083\u6ce3\u304d\u305d\u3046", unicodeValue);

            assertNull(reader.nextRecord());
        }
    }

    @Test
    public void testIncorrectSchema() throws IOException, MalformedRecordException {
        final DataType accountType = RecordFieldType.RECORD.getRecordDataType(getAccountSchema());
        final List<RecordField> fields = getDefaultFields();
        fields.add(new RecordField("account", accountType));
        fields.remove(new RecordField("balance", RecordFieldType.DOUBLE.getDataType()));

        final RecordSchema schema = new SimpleRecordSchema(fields);

        try (final InputStream in = new FileInputStream(new File("src/test/resources/json/single-bank-account-wrong-field-type.json"));
            final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(in, Mockito.mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat)) {

            reader.nextRecord().getValues();
            Assert.fail("Was able to read record with invalid schema.");

        } catch (final MalformedRecordException mre) {
            final String msg = mre.getCause().getMessage();
            assertTrue(msg.contains("account.balance"));
            assertTrue(msg.contains("true"));
            assertTrue(msg.contains("Double"));
            assertTrue(msg.contains("Boolean"));
        }
    }
}
