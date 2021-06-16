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

import org.apache.avro.Schema;
import org.apache.commons.io.FileUtils;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schema.inference.InferSchemaAccessStrategy;
import org.apache.nifi.schema.inference.TimeValueInference;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.util.EqualsWrapper;
import org.apache.nifi.util.MockComponentLog;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class TestJsonTreeRowRecordReader {
    private final String dateFormat = RecordFieldType.DATE.getDefaultFormat();
    private final String timeFormat = RecordFieldType.TIME.getDefaultFormat();
    private final String timestampFormat = RecordFieldType.TIMESTAMP.getDefaultFormat();

    private List<RecordField> getDefaultFields() {
        return getFields(RecordFieldType.DOUBLE.getDataType());
    }

    private List<RecordField> getFields(final DataType balanceDataType) {
        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.INT.getDataType()));
        fields.add(new RecordField("name", RecordFieldType.STRING.getDataType()));
        fields.add(new RecordField("balance", balanceDataType));
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
    public void testReadChoiceOfStringOrArrayOfRecords() throws IOException, MalformedRecordException {
        final File schemaFile = new File("src/test/resources/json/choice-of-string-or-array-record.avsc");
        final File jsonFile = new File("src/test/resources/json/choice-of-string-or-array-record.json");

        final Schema avroSchema = new Schema.Parser().parse(schemaFile);
        final RecordSchema recordSchema = AvroTypeUtil.createSchema(avroSchema);

        try (final InputStream fis = new FileInputStream(jsonFile);
            final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(fis, new MockComponentLog("id", "id"), recordSchema, dateFormat, timeFormat, timestampFormat)) {

            final Record record = reader.nextRecord();
            final Object[] fieldsArray = record.getAsArray("fields");
            assertEquals(2, fieldsArray.length);

            final Object firstElement = fieldsArray[0];
            assertTrue(firstElement instanceof Record);
            assertEquals("string", ((Record) firstElement).getAsString("type"));

            final Object secondElement = fieldsArray[1];
            assertTrue(secondElement instanceof Record);
            final Object[] typeArray = ((Record) secondElement).getAsArray("type");
            assertEquals(1, typeArray.length);

            final Object firstType = typeArray[0];
            assertTrue(firstType instanceof Record);
            final Record firstTypeRecord = (Record) firstType;
            assertEquals("string", firstTypeRecord.getAsString("type"));
        }

    }

    @Test
    @Ignore("Intended only for manual testing to determine performance before/after modifications")
    public void testPerformanceOnLocalFile() throws IOException, MalformedRecordException {
        final RecordSchema schema = new SimpleRecordSchema(Collections.emptyList());

        final File file = new File("/devel/nifi/nifi-assembly/target/nifi-1.2.0-SNAPSHOT-bin/nifi-1.2.0-SNAPSHOT/prov/16812193969219289");
        final byte[] data = Files.readAllBytes(file.toPath());

        final ComponentLog logger = mock(ComponentLog.class);

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

        final ComponentLog logger = mock(ComponentLog.class);

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
    public void testChoiceOfRecordTypes() throws IOException, MalformedRecordException {
        final Schema avroSchema = new Schema.Parser().parse(new File("src/test/resources/json/record-choice.avsc"));
        final RecordSchema recordSchema = AvroTypeUtil.createSchema(avroSchema);

        try (final InputStream in = new FileInputStream(new File("src/test/resources/json/elements-for-record-choice.json"));
            final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(in, mock(ComponentLog.class), recordSchema, dateFormat, timeFormat, timestampFormat)) {

            // evaluate first record
            final Record firstRecord = reader.nextRecord();
            assertNotNull(firstRecord);
            final RecordSchema firstOuterSchema = firstRecord.getSchema();
            assertEquals(Arrays.asList("id", "child"), firstOuterSchema.getFieldNames());
            assertEquals("1234", firstRecord.getValue("id"));

            // record should have a schema that indicates that the 'child' is a CHOICE of 2 different record types
            assertTrue(firstOuterSchema.getDataType("child").get().getFieldType() == RecordFieldType.CHOICE);
            final List<DataType> firstSubTypes = ((ChoiceDataType) firstOuterSchema.getDataType("child").get()).getPossibleSubTypes();
            assertEquals(2, firstSubTypes.size());
            assertEquals(2L, firstSubTypes.stream().filter(type -> type.getFieldType() == RecordFieldType.RECORD).count());

            // child record should have a schema with "id" as the only field
            final Object childObject = firstRecord.getValue("child");
            assertTrue(childObject instanceof Record);
            final Record firstChildRecord = (Record) childObject;
            final RecordSchema firstChildSchema = firstChildRecord.getSchema();

            assertEquals(Arrays.asList("id"), firstChildSchema.getFieldNames());

            // evaluate second record
            final Record secondRecord = reader.nextRecord();
            assertNotNull(secondRecord);

            final RecordSchema secondOuterSchema = secondRecord.getSchema();
            assertEquals(Arrays.asList("id", "child"), secondOuterSchema.getFieldNames());
            assertEquals("1234", secondRecord.getValue("id"));

            // record should have a schema that indicates that the 'child' is a CHOICE of 2 different record types
            assertTrue(secondOuterSchema.getDataType("child").get().getFieldType() == RecordFieldType.CHOICE);
            final List<DataType> secondSubTypes = ((ChoiceDataType) secondOuterSchema.getDataType("child").get()).getPossibleSubTypes();
            assertEquals(2, secondSubTypes.size());
            assertEquals(2L, secondSubTypes.stream().filter(type -> type.getFieldType() == RecordFieldType.RECORD).count());

            // child record should have a schema with "name" as the only field
            final Object secondChildObject = secondRecord.getValue("child");
            assertTrue(secondChildObject instanceof Record);
            final Record secondChildRecord = (Record) secondChildObject;
            final RecordSchema secondChildSchema = secondChildRecord.getSchema();

            assertEquals(Arrays.asList("name"), secondChildSchema.getFieldNames());

            assertNull(reader.nextRecord());
        }

    }

    @Test
    public void testReadArray() throws IOException, MalformedRecordException {
        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());

        try (final InputStream in = new FileInputStream(new File("src/test/resources/json/bank-account-array.json"));
            final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(in, mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat)) {

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
    public void testReadOneLinePerJSON() throws IOException, MalformedRecordException {
        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());

        try (final InputStream in = new FileInputStream(new File("src/test/resources/json/bank-account-oneline.json"));
             final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(in, mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat)) {

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
    public void testReadMultilineJSON() throws IOException, MalformedRecordException {
        final List<RecordField> fields = getFields(RecordFieldType.DECIMAL.getDecimalDataType(30, 10));
        final RecordSchema schema = new SimpleRecordSchema(fields);

        try (final InputStream in = new FileInputStream(new File("src/test/resources/json/bank-account-multiline.json"));
             final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(in, mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat)) {

            final List<String> fieldNames = schema.getFieldNames();
            final List<String> expectedFieldNames = Arrays.asList(new String[] {"id", "name", "balance", "address", "city", "state", "zipCode", "country"});
            assertEquals(expectedFieldNames, fieldNames);

            final List<RecordFieldType> dataTypes = schema.getDataTypes().stream().map(dt -> dt.getFieldType()).collect(Collectors.toList());
            final List<RecordFieldType> expectedTypes = Arrays.asList(new RecordFieldType[] {RecordFieldType.INT, RecordFieldType.STRING,
                    RecordFieldType.DECIMAL, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING, RecordFieldType.STRING});
            assertEquals(expectedTypes, dataTypes);

            final Object[] firstRecordValues = reader.nextRecord().getValues();
            Assert.assertArrayEquals(new Object[] {1, "John Doe", BigDecimal.valueOf(4750.89), "123 My Street", "My City", "MS", "11111", "USA"}, firstRecordValues);

            final Object[] secondRecordValues = reader.nextRecord().getValues();
            Assert.assertArrayEquals(new Object[] {2, "Jane Doe", BigDecimal.valueOf(4820.09), "321 Your Street", "Your City", "NY", "33333", "USA"}, secondRecordValues);

            assertNull(reader.nextRecord());
        }
    }

    @Test
    public void testReadMultilineArrays() throws IOException, MalformedRecordException {
        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());

        try (final InputStream in = new FileInputStream(new File("src/test/resources/json/bank-account-multiarray.json"));
             final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(in, mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat)) {

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

            final Object[] thirdRecordValues = reader.nextRecord().getValues();
            Assert.assertArrayEquals(new Object[] {3, "Maria Doe", 4750.89, "123 My Street", "My City", "ME", "11111", "USA"}, thirdRecordValues);

            final Object[] fourthRecordValues = reader.nextRecord().getValues();
            Assert.assertArrayEquals(new Object[] {4, "Xi Doe", 4820.09, "321 Your Street", "Your City", "NV", "33333", "USA"}, fourthRecordValues);

            assertNull(reader.nextRecord());
        }
    }

    @Test
    public void testReadMixedJSON() throws IOException, MalformedRecordException {
        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());

        try (final InputStream in = new FileInputStream(new File("src/test/resources/json/bank-account-mixed.json"));
             final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(in, mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat)) {

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

            final Object[] thirdRecordValues = reader.nextRecord().getValues();
            Assert.assertArrayEquals(new Object[] {3, "Maria Doe", 4750.89, "123 My Street", "My City", "ME", "11111", "USA"}, thirdRecordValues);

            final Object[] fourthRecordValues = reader.nextRecord().getValues();
            Assert.assertArrayEquals(new Object[] {4, "Xi Doe", 4820.09, "321 Your Street", "Your City", "NV", "33333", "USA"}, fourthRecordValues);


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
            final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(in, mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat)) {

            final Record schemaValidatedRecord = reader.nextRecord(true, true);
            assertEquals(1, schemaValidatedRecord.getValue("id"));
            assertEquals("John Doe", schemaValidatedRecord.getValue("name"));
            assertNull(schemaValidatedRecord.getValue("balance"));
        }

        try (final InputStream in = new FileInputStream(new File("src/test/resources/json/bank-account-array.json"));
            final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(in, mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat)) {

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
            final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(in, mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat)) {

            final Record schemaValidatedRecord = reader.nextRecord(true, true);
            assertEquals("1", schemaValidatedRecord.getValue("id")); // will be coerced into a STRING as per the schema
            assertEquals("John Doe", schemaValidatedRecord.getValue("name"));
            assertNull(schemaValidatedRecord.getValue("balance"));

            assertEquals(2, schemaValidatedRecord.getRawFieldNames().size());
        }

        try (final InputStream in = new FileInputStream(new File("src/test/resources/json/bank-account-array.json"));
            final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(in, mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat)) {

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
    public void testTimestampCoercedFromString() throws IOException, MalformedRecordException {
        final List<RecordField> recordFields = Collections.singletonList(new RecordField("timestamp", RecordFieldType.TIMESTAMP.getDataType()));
        final RecordSchema schema = new SimpleRecordSchema(recordFields);

        for (final boolean coerceTypes : new boolean[] {true, false}) {
            try (final InputStream in = new FileInputStream(new File("src/test/resources/json/timestamp.json"));
                 final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(in, mock(ComponentLog.class), schema, dateFormat, timeFormat, "yyyy/MM/dd HH:mm:ss")) {

                final Record record = reader.nextRecord(coerceTypes, false);
                final Object value = record.getValue("timestamp");
                assertTrue("With coerceTypes set to " + coerceTypes + ", value is not a Timestamp", value instanceof java.sql.Timestamp);
            }
        }
    }

    @Test
    public void testSingleJsonElement() throws IOException, MalformedRecordException {
        final RecordSchema schema = new SimpleRecordSchema(getDefaultFields());

        try (final InputStream in = new FileInputStream(new File("src/test/resources/json/single-bank-account.json"));
            final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(in, mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat)) {

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
             final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(in, mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat)) {

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
            final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(in, mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat)) {

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
            final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(in, mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat)) {

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
            final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(in, mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat)) {

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
            final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(in, mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat)) {

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
            final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(in, mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat)) {

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
            final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(in, mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat)) {

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
            final JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(in, mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat)) {

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

    @Test
    public void testMergeOfSimilarRecords() throws Exception {
        // GIVEN
        String jsonPath = "src/test/resources/json/similar-records.json";

        RecordSchema expectedSchema = new SimpleRecordSchema(Arrays.asList(
            new RecordField("integer", RecordFieldType.INT.getDataType()),
            new RecordField("boolean", RecordFieldType.BOOLEAN.getDataType()),
            new RecordField("booleanOrString", RecordFieldType.CHOICE.getChoiceDataType(
                RecordFieldType.BOOLEAN.getDataType(),
                RecordFieldType.STRING.getDataType()
            )),
            new RecordField("string", RecordFieldType.STRING.getDataType())
        ));

        List<Object> expected = Arrays.asList(
            new MapRecord(expectedSchema, new HashMap<String, Object>(){{
                put("integer", 1);
                put("boolean", true);
                put("booleanOrString", true);
            }}),
            new MapRecord(expectedSchema, new HashMap<String, Object>(){{
                put("integer", 2);
                put("string", "stringValue2");
                put("booleanOrString", "booleanOrStringValue2");
            }})
        );

        // WHEN
        // THEN
        testReadRecords(jsonPath, expected);
    }

    @Test
    public void testChoiceOfEmbeddedSimilarRecords() throws Exception {
        // GIVEN
        String jsonPath = "src/test/resources/json/choice-of-embedded-similar-records.json";

        SimpleRecordSchema expectedRecordSchema1 = new SimpleRecordSchema(Arrays.asList(
            new RecordField("integer", RecordFieldType.INT.getDataType()),
            new RecordField("boolean", RecordFieldType.BOOLEAN.getDataType())
        ));
        SimpleRecordSchema expectedRecordSchema2 = new SimpleRecordSchema(Arrays.asList(
            new RecordField("integer", RecordFieldType.INT.getDataType()),
            new RecordField("string", RecordFieldType.STRING.getDataType())
        ));
        RecordSchema expectedRecordChoiceSchema = new SimpleRecordSchema(Arrays.asList(
            new RecordField("record", RecordFieldType.CHOICE.getChoiceDataType(
                RecordFieldType.RECORD.getRecordDataType(expectedRecordSchema1),
                RecordFieldType.RECORD.getRecordDataType(expectedRecordSchema2)
            ))
        ));

        List<Object> expected = Arrays.asList(
            new MapRecord(expectedRecordChoiceSchema, new HashMap<String, Object>(){{
                put("record", new MapRecord(expectedRecordSchema1, new HashMap<String, Object>(){{
                    put("integer", 1);
                    put("boolean", true);
                }}));
            }}),
            new MapRecord(expectedRecordChoiceSchema, new HashMap<String, Object>(){{
                put("record", new MapRecord(expectedRecordSchema2, new HashMap<String, Object>(){{
                    put("integer", 2);
                    put("string", "stringValue2");
                }}));
            }})
        );

        // WHEN
        // THEN
        testReadRecords(jsonPath, expected);
    }

    @Test
    public void testChoiceOfEmbeddedArraysAndSingleRecords() throws Exception {
        // GIVEN
        String jsonPath = "src/test/resources/json/choice-of-embedded-arrays-and-single-records.json";

        SimpleRecordSchema expectedRecordSchema1 = new SimpleRecordSchema(Arrays.asList(
            new RecordField("integer", RecordFieldType.INT.getDataType())
        ));
        SimpleRecordSchema expectedRecordSchema2 = new SimpleRecordSchema(Arrays.asList(
            new RecordField("integer", RecordFieldType.INT.getDataType()),
            new RecordField("boolean", RecordFieldType.BOOLEAN.getDataType())
        ));
        SimpleRecordSchema expectedRecordSchema3 = new SimpleRecordSchema(Arrays.asList(
            new RecordField("integer", RecordFieldType.INT.getDataType()),
            new RecordField("string", RecordFieldType.STRING.getDataType())
        ));
        SimpleRecordSchema expectedRecordSchema4 = new SimpleRecordSchema(Arrays.asList(
            new RecordField("integer", RecordFieldType.INT.getDataType()),
            new RecordField("string", RecordFieldType.STRING.getDataType())
        ));
        RecordSchema expectedRecordChoiceSchema = new SimpleRecordSchema(Arrays.asList(
            new RecordField("record", RecordFieldType.CHOICE.getChoiceDataType(
                RecordFieldType.RECORD.getRecordDataType(expectedRecordSchema1),
                RecordFieldType.RECORD.getRecordDataType(expectedRecordSchema3),
                RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.RECORD.getRecordDataType(expectedRecordSchema2)),
                RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.RECORD.getRecordDataType(expectedRecordSchema4))
            ))
        ));

        List<Object> expected = Arrays.asList(
            new MapRecord(expectedRecordChoiceSchema, new HashMap<String, Object>(){{
                put("record", new MapRecord(expectedRecordSchema1, new HashMap<String, Object>(){{
                    put("integer", 1);
                }}));
            }}),
            new MapRecord(expectedRecordChoiceSchema, new HashMap<String, Object>(){{
                put("record", new Object[]{
                    new MapRecord(expectedRecordSchema2, new HashMap<String, Object>() {{
                        put("integer", 21);
                        put("boolean", true);
                    }}),
                    new MapRecord(expectedRecordSchema2, new HashMap<String, Object>() {{
                        put("integer", 22);
                        put("boolean", false);
                    }})
                });
            }}),
            new MapRecord(expectedRecordChoiceSchema, new HashMap<String, Object>(){{
                put("record", new MapRecord(expectedRecordSchema3, new HashMap<String, Object>(){{
                    put("integer", 3);
                    put("string", "stringValue3");
                }}));
            }}),
            new MapRecord(expectedRecordChoiceSchema, new HashMap<String, Object>(){{
                put("record", new Object[]{
                    new MapRecord(expectedRecordSchema4, new HashMap<String, Object>() {{
                        put("integer", 41);
                        put("string", "stringValue41");
                    }}),
                    new MapRecord(expectedRecordSchema4, new HashMap<String, Object>() {{
                        put("integer", 42);
                        put("string", "stringValue42");
                    }})
                });
            }})
        );

        // WHEN
        // THEN
        testReadRecords(jsonPath, expected);
    }

    @Test
    public void testChoiceOfMergedEmbeddedArraysAndSingleRecords() throws Exception {
        // GIVEN
        String jsonPath = "src/test/resources/json/choice-of-merged-embedded-arrays-and-single-records.json";

        SimpleRecordSchema expectedRecordSchema1 = new SimpleRecordSchema(Arrays.asList(
            new RecordField("integer", RecordFieldType.INT.getDataType()),
            new RecordField("boolean", RecordFieldType.BOOLEAN.getDataType())
        ));
        SimpleRecordSchema expectedRecordSchema2 = new SimpleRecordSchema(Arrays.asList(
            new RecordField("integer", RecordFieldType.INT.getDataType()),
            new RecordField("boolean", RecordFieldType.BOOLEAN.getDataType())
        ));
        SimpleRecordSchema expectedRecordSchema3 = new SimpleRecordSchema(Arrays.asList(
            new RecordField("integer", RecordFieldType.INT.getDataType()),
            new RecordField("string", RecordFieldType.STRING.getDataType())
        ));
        SimpleRecordSchema expectedRecordSchema4 = new SimpleRecordSchema(Arrays.asList(
            new RecordField("integer", RecordFieldType.INT.getDataType()),
            new RecordField("string", RecordFieldType.STRING.getDataType()),
            new RecordField("boolean", RecordFieldType.BOOLEAN.getDataType())
        ));
        RecordSchema expectedRecordChoiceSchema = new SimpleRecordSchema(Arrays.asList(
            new RecordField("record", RecordFieldType.CHOICE.getChoiceDataType(
                RecordFieldType.RECORD.getRecordDataType(expectedRecordSchema1),
                RecordFieldType.RECORD.getRecordDataType(expectedRecordSchema3),
                RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.RECORD.getRecordDataType(expectedRecordSchema2)),
                RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.RECORD.getRecordDataType(expectedRecordSchema4))
            ))
        ));

        List<Object> expected = Arrays.asList(
            new MapRecord(expectedRecordChoiceSchema, new HashMap<String, Object>(){{
                put("record", new MapRecord(expectedRecordSchema1, new HashMap<String, Object>(){{
                    put("integer", 1);
                    put("boolean", false);
                }}));
            }}),
            new MapRecord(expectedRecordChoiceSchema, new HashMap<String, Object>(){{
                put("record", new Object[]{
                    new MapRecord(expectedRecordSchema2, new HashMap<String, Object>() {{
                        put("integer", 21);
                        put("boolean", true);
                    }}),
                    new MapRecord(expectedRecordSchema2, new HashMap<String, Object>() {{
                        put("integer", 22);
                        put("boolean", false);
                    }})
                });
            }}),
            new MapRecord(expectedRecordChoiceSchema, new HashMap<String, Object>(){{
                put("record", new MapRecord(expectedRecordSchema3, new HashMap<String, Object>(){{
                    put("integer", 3);
                    put("string", "stringValue3");
                }}));
            }}),
            new MapRecord(expectedRecordChoiceSchema, new HashMap<String, Object>(){{
                put("record", new Object[]{
                    new MapRecord(expectedRecordSchema4, new HashMap<String, Object>() {{
                        put("integer", 41);
                        put("string", "stringValue41");
                    }}),
                    new MapRecord(expectedRecordSchema4, new HashMap<String, Object>() {{
                        put("integer", 42);
                        put("string", "stringValue42");
                    }}),
                    new MapRecord(expectedRecordSchema4, new HashMap<String, Object>() {{
                        put("integer", 43);
                        put("boolean", false);
                    }})
                });
            }})
        );

        // WHEN
        // THEN
        testReadRecords(jsonPath, expected);
    }

    @Test
    public void testChoseSuboptimalSchemaWhenDataHasExtraFields() throws Exception {
        // GIVEN
        String jsonPath = "src/test/resources/json/choice-of-different-arrays-with-extra-fields.json";

        SimpleRecordSchema recordSchema1 = new SimpleRecordSchema(Arrays.asList(
            new RecordField("integer", RecordFieldType.INT.getDataType()),
            new RecordField("boolean", RecordFieldType.BOOLEAN.getDataType())
        ));
        SimpleRecordSchema recordSchema2 = new SimpleRecordSchema(Arrays.asList(
            new RecordField("integer", RecordFieldType.INT.getDataType()),
            new RecordField("string", RecordFieldType.STRING.getDataType())
        ));

        RecordSchema recordChoiceSchema = new SimpleRecordSchema(Arrays.asList(
            new RecordField("record", RecordFieldType.CHOICE.getChoiceDataType(
                RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.RECORD.getRecordDataType(recordSchema1)),
                RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.RECORD.getRecordDataType(recordSchema2))
            ))
        ));

        RecordSchema schema = new SimpleRecordSchema(Arrays.asList(
            new RecordField("dataCollection", RecordFieldType.ARRAY.getArrayDataType(
                RecordFieldType.RECORD.getRecordDataType(recordChoiceSchema)
            )
        )));

        SimpleRecordSchema expectedChildSchema1 = new SimpleRecordSchema(Arrays.asList(
            new RecordField("integer", RecordFieldType.INT.getDataType()),
            new RecordField("boolean", RecordFieldType.BOOLEAN.getDataType())
        ));
        SimpleRecordSchema expectedChildSchema2 = new SimpleRecordSchema(Arrays.asList(
            new RecordField("integer", RecordFieldType.INT.getDataType()),
            new RecordField("string", RecordFieldType.STRING.getDataType())
        ));
        RecordSchema expectedRecordChoiceSchema = new SimpleRecordSchema(Arrays.asList(
            new RecordField("record", RecordFieldType.CHOICE.getChoiceDataType(
                RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.RECORD.getRecordDataType(expectedChildSchema1)),
                RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.RECORD.getRecordDataType(expectedChildSchema2))
            ))
        ));

        // Since the actual arrays have records with either (INT, BOOLEAN, STRING) or (INT, STRING, STRING)
        //  while the explicit schema defines only (INT, BOOLEAN) and (INT, STRING) we can't tell which record schema to chose
        //  so we take the first one (INT, BOOLEAN) - as best effort - for both cases
        SimpleRecordSchema expectedSelectedRecordSchemaForRecordsInBothArrays = expectedChildSchema1;

        List<Object> expected = Arrays.asList(
            new MapRecord(expectedRecordChoiceSchema, new HashMap<String, Object>(){{
                put("record", new Object[]{
                    new MapRecord(expectedSelectedRecordSchemaForRecordsInBothArrays, new HashMap<String, Object>() {{
                        put("integer", 11);
                        put("boolean", true);
                        put("extraString", "extraStringValue11");
                    }}),
                    new MapRecord(expectedSelectedRecordSchemaForRecordsInBothArrays, new HashMap<String, Object>() {{
                        put("integer", 12);
                        put("boolean", false);
                        put("extraString", "extraStringValue12");
                    }})
                });
            }}),
            new MapRecord(expectedRecordChoiceSchema, new HashMap<String, Object>(){{
                put("record", new Object[]{
                    new MapRecord(expectedSelectedRecordSchemaForRecordsInBothArrays, new HashMap<String, Object>() {{
                        put("integer", 21);
                        put("extraString", "extraStringValue21");
                        put("string", "stringValue21");
                    }}),
                    new MapRecord(expectedSelectedRecordSchemaForRecordsInBothArrays, new HashMap<String, Object>() {{
                        put("integer", 22);
                        put("extraString", "extraStringValue22");
                        put("string", "stringValue22");
                    }})
                });
            }})
        );

        // WHEN
        // THEN
        testReadRecords(jsonPath, schema, expected);
    }

    private void testReadRecords(String jsonPath, List<Object> expected) throws IOException, MalformedRecordException {
        // GIVEN
        final File jsonFile = new File(jsonPath);

        try (
            InputStream jsonStream = new ByteArrayInputStream(FileUtils.readFileToByteArray(jsonFile));
        ) {
            RecordSchema schema = inferSchema(jsonStream);

            // WHEN
            // THEN
            testReadRecords(jsonStream, schema, expected);
        }
    }

    private void testReadRecords(String jsonPath, RecordSchema schema, List<Object> expected) throws IOException, MalformedRecordException {
        // GIVEN
        final File jsonFile = new File(jsonPath);

        try (
            InputStream jsonStream = new ByteArrayInputStream(FileUtils.readFileToByteArray(jsonFile));
        ) {
            // WHEN
            // THEN
            testReadRecords(jsonStream, schema, expected);
        }
    }

    private void testReadRecords(InputStream jsonStream, RecordSchema schema, List<Object> expected) throws IOException, MalformedRecordException {
        // GIVEN
        try (
            JsonTreeRowRecordReader reader = new JsonTreeRowRecordReader(jsonStream, mock(ComponentLog.class), schema, dateFormat, timeFormat, timestampFormat);
        ) {
            // WHEN
            List<Object> actual = new ArrayList<>();
            Record record;
            while ((record = reader.nextRecord()) != null) {
                List<Object> dataCollection = Arrays.asList((Object[]) record.getValue("dataCollection"));
                actual.addAll(dataCollection);
            }

            // THEN
            List<Function<Object, Object>> propertyProviders = Arrays.asList(
                _object -> ((Record)_object).getSchema(),
                _object -> Arrays.stream(((Record)_object).getValues()).map(value -> {
                    if (value != null && value.getClass().isArray()) {
                        return Arrays.asList((Object[]) value);
                    } else {
                        return value;
                    }
                }).collect(Collectors.toList())
            );

            List<EqualsWrapper<Object>> wrappedExpected = EqualsWrapper.wrapList(expected, propertyProviders);
            List<EqualsWrapper<Object>> wrappedActual = EqualsWrapper.wrapList(actual, propertyProviders);

            assertEquals(wrappedExpected, wrappedActual);
        }
    }

    private RecordSchema inferSchema(InputStream jsonStream) throws IOException {
        RecordSchema schema = new InferSchemaAccessStrategy<>(
            (__, inputStream) -> new JsonRecordSource(inputStream),
            new JsonSchemaInference(new TimeValueInference(null, null, null)),
            mock(ComponentLog.class)
        ).getSchema(Collections.emptyMap(), jsonStream, null);

        jsonStream.reset();

        return schema;
    }
}
