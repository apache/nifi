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
package org.apache.nifi.processors.standard;

import org.apache.avro.Schema;
import org.apache.nifi.avro.AvroReader;
import org.apache.nifi.avro.AvroRecordSetWriter;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.processors.standard.faker.FakerMethodHolder;
import org.apache.nifi.processors.standard.faker.FakerUtils;
import org.apache.nifi.processors.standard.faker.PredefinedRecordSchema;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class TestGenerateRecord {

    private TestRunner testRunner;
    private GenerateRecord processor;

    @BeforeEach
    public void setup() {
        processor = new GenerateRecord();
        testRunner = TestRunners.newTestRunner(processor);
    }

    @Test
    public void testGenerateNoNullableFields() throws Exception {

        // Set all Faker properties
        for (Map.Entry<String, FakerMethodHolder> fakerProperty : FakerUtils.getDatatypeFunctionMap().entrySet()) {
            testRunner.setProperty(fakerProperty.getKey(), fakerProperty.getKey());
        }

        final Map<String, String> recordFields = processor.getFields(testRunner.getProcessContext());
        final RecordSchema outputSchema = processor.generateRecordSchema(recordFields, false);
        final MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1, false, outputSchema);
        testRunner.addControllerService("record-writer", recordWriter);
        testRunner.enableControllerService(recordWriter);
        testRunner.setProperty(GenerateRecord.RECORD_WRITER, "record-writer");
        testRunner.setProperty(GenerateRecord.NULLABLE_FIELDS, "false");
        testRunner.setProperty(GenerateRecord.NULL_PERCENTAGE, "100"); // This should be ignored
        testRunner.setProperty(GenerateRecord.NUM_RECORDS, "3");

        testRunner.run();
        testRunner.assertTransferCount(GenerateRecord.REL_SUCCESS, 1);
        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(GenerateRecord.REL_SUCCESS).getFirst();
        final String output = flowFile.getContent();
        for (String line : output.split(System.lineSeparator())) {
            // A null value would not be output so a comma would be the last character on the line
            if (line.endsWith(",")) {
                fail(line + "should not end with a value");
            }
        }
    }

    @Test
    public void testGenerateNullableFieldsZeroNullPercentage() throws Exception {
        // Set all Faker properties
        for (Map.Entry<String, FakerMethodHolder> fakerProperty : FakerUtils.getDatatypeFunctionMap().entrySet()) {
            testRunner.setProperty(fakerProperty.getKey(), fakerProperty.getKey());
        }

        final Map<String, String> recordFields = processor.getFields(testRunner.getProcessContext());
        final RecordSchema outputSchema = processor.generateRecordSchema(recordFields, true);
        final MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1, false, outputSchema);
        testRunner.addControllerService("record-writer", recordWriter);
        testRunner.enableControllerService(recordWriter);
        testRunner.setProperty(GenerateRecord.RECORD_WRITER, "record-writer");
        testRunner.setProperty(GenerateRecord.NULLABLE_FIELDS, "true");
        testRunner.setProperty(GenerateRecord.NULL_PERCENTAGE, "0");
        testRunner.setProperty(GenerateRecord.NUM_RECORDS, "3");

        testRunner.run();
        testRunner.assertTransferCount(GenerateRecord.REL_SUCCESS, 1);
        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(GenerateRecord.REL_SUCCESS).getFirst();
        final String output = flowFile.getContent();
        for (String line : output.split(System.lineSeparator())) {
            // A null value would not be output so a comma would be the last character on the line
            if (line.endsWith(",")) {
                fail(line + "should not end with a value");
            }
        }
    }

    @Test
    public void testGenerateNullableFieldsOneHundredNullPercentage() throws Exception {
        // Set all Faker properties
        for (Map.Entry<String, FakerMethodHolder> fakerProperty : FakerUtils.getDatatypeFunctionMap().entrySet()) {
            testRunner.setProperty(fakerProperty.getKey(), fakerProperty.getKey());
        }

        final Map<String, String> recordFields = processor.getFields(testRunner.getProcessContext());
        final RecordSchema outputSchema = processor.generateRecordSchema(recordFields, true);
        final MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1, false, outputSchema);
        testRunner.addControllerService("record-writer", recordWriter);
        testRunner.enableControllerService(recordWriter);
        testRunner.setProperty(GenerateRecord.RECORD_WRITER, "record-writer");
        testRunner.setProperty(GenerateRecord.NULLABLE_FIELDS, "true");
        testRunner.setProperty(GenerateRecord.NULL_PERCENTAGE, "100");
        testRunner.setProperty(GenerateRecord.NUM_RECORDS, "1");

        testRunner.run();
        testRunner.assertTransferCount(GenerateRecord.REL_SUCCESS, 1);
        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(GenerateRecord.REL_SUCCESS).getFirst();
        // null values should cause all fields to be empty in the output
        // create a string of commas whose number equals the number of fields in the datatypeFunctionMap (size - 1 copies)
        flowFile.assertContentEquals(String.join("", Collections.nCopies(FakerUtils.getDatatypeFunctionMap().size() - 1, ",")) + "\n");
    }

    // Tests that the special FT_* types in FakerUtils are supported by the processor.
    // Note: FT_BOOL is defined but not added to createFakerPropertyList(), so we exclude it.
    @Test
    public void testFieldsReturnValue() throws Exception {

List<Field> fieldTypeFields = Arrays.stream(GenerateRecord.class.getFields()).filter((field) -> field.getName().startsWith("FT_")).toList();
        for (Field field : fieldTypeFields) {
            field.setAccessible(true);
            testRunner.setProperty(field.getName().toLowerCase(Locale.ROOT), ((AllowableValue) field.get(null)).getValue());
        }

        final Map<String, String> recordFields = processor.getFields(testRunner.getProcessContext());
        final RecordSchema outputSchema = processor.generateRecordSchema(recordFields, true);
        final MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1, false, outputSchema);
        testRunner.addControllerService("record-writer", recordWriter);
        testRunner.enableControllerService(recordWriter);
        testRunner.setProperty(GenerateRecord.RECORD_WRITER, "record-writer");
        testRunner.setProperty(GenerateRecord.NULLABLE_FIELDS, "true");
        testRunner.setProperty(GenerateRecord.NULL_PERCENTAGE, "100");
        testRunner.setProperty(GenerateRecord.NUM_RECORDS, "1");

        testRunner.run();
        testRunner.assertTransferCount(GenerateRecord.REL_SUCCESS, 1);
    }

    @Test
    public void testGenerateNoNullableFieldsSchemaText() throws Exception {

        String schemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestGenerateRecord/nested_no_nullable.avsc")));
        final Schema avroSchema = new Schema.Parser().parse(schemaText);
        final RecordSchema outputSchema = AvroTypeUtil.createSchema(avroSchema);

        final MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1, false, outputSchema);
        testRunner.addControllerService("record-writer", recordWriter);
        testRunner.enableControllerService(recordWriter);
        testRunner.setProperty(GenerateRecord.RECORD_WRITER, "record-writer");
        testRunner.setProperty(GenerateRecord.SCHEMA_TEXT, schemaText);
        testRunner.setProperty(GenerateRecord.NULLABLE_FIELDS, "true"); // Should be ignored
        testRunner.setProperty(GenerateRecord.NULL_PERCENTAGE, "0");
        testRunner.setProperty(GenerateRecord.NUM_RECORDS, "30");

        testRunner.run();
        testRunner.assertTransferCount(GenerateRecord.REL_SUCCESS, 1);
        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(GenerateRecord.REL_SUCCESS).getFirst();
        final String output = flowFile.getContent();
        for (String line : output.split(System.lineSeparator())) {
            // A null value would not be output so a comma would be the last character on the line
            if (line.contains(",,")) {
                fail(line + "should not contain null values");
            }
        }
    }

    @Test
    public void testGenerateNullableFieldsZeroNullPercentageSchemaText() throws Exception {
        String schemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestGenerateRecord/nested_nullable.avsc")));
        final AvroRecordSetWriter recordWriter = new AvroRecordSetWriter();
        testRunner.addControllerService("record-writer", recordWriter);
        testRunner.enableControllerService(recordWriter);
        testRunner.setProperty(GenerateRecord.RECORD_WRITER, "record-writer");
        testRunner.setProperty(GenerateRecord.SCHEMA_TEXT, schemaText);
        testRunner.setProperty(GenerateRecord.NULLABLE_FIELDS, "false"); // Should be ignored
        testRunner.setProperty(GenerateRecord.NULL_PERCENTAGE, "0");
        testRunner.setProperty(GenerateRecord.NUM_RECORDS, "3");

        testRunner.run();
        testRunner.assertTransferCount(GenerateRecord.REL_SUCCESS, 1);
        final MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(GenerateRecord.REL_SUCCESS).getFirst();

        final AvroReader avroReader = new AvroReader();
        testRunner.addControllerService("avroReader", avroReader);
        testRunner.enableControllerService(avroReader);
        final byte[] validFlowFileBytes = flowFile.toByteArray();
        try (
                final ByteArrayInputStream resultContentStream = new ByteArrayInputStream(validFlowFileBytes);
                final RecordReader recordReader = avroReader.createRecordReader(flowFile.getAttributes(), resultContentStream, validFlowFileBytes.length, testRunner.getLogger())
        ) {
            // Check correct schema
            final RecordSchema resultSchema = recordReader.getSchema();

            final Optional<RecordField> systemField = resultSchema.getField("System");
            assertTrue(systemField.isPresent());
            assertEquals(RecordFieldType.RECORD, systemField.get().getDataType().getFieldType());
            RecordDataType systemRecordType = (RecordDataType) systemField.get().getDataType();
            RecordSchema systemSchema = systemRecordType.getChildSchema();

            final Optional<RecordField> providerField = systemSchema.getField("Provider");
            assertTrue(providerField.isPresent());
            assertEquals(RecordFieldType.RECORD, providerField.get().getDataType().getFieldType());
            RecordDataType providerRecordType = (RecordDataType) providerField.get().getDataType();
            RecordSchema providerSchema = providerRecordType.getChildSchema();

            final Optional<RecordField> guidField = providerSchema.getField("Guid");
            assertTrue(guidField.isPresent());
            assertEquals(RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType()), guidField.get().getDataType());

            // Check object type, class etc.
            final org.apache.nifi.serialization.record.Record record = recordReader.nextRecord();
            assertNotNull(record);
            final Object systemObject = record.getValue("System");
            assertNotNull(systemObject);
            assertInstanceOf(Record.class, systemObject);
            final org.apache.nifi.serialization.record.Record systemRecord = (org.apache.nifi.serialization.record.Record) systemObject;
            final Object providerObject = systemRecord.getValue("Provider");
            assertNotNull(providerObject);
            assertInstanceOf(Record.class, providerObject);
            final org.apache.nifi.serialization.record.Record providerRecord = (org.apache.nifi.serialization.record.Record) providerObject;
            final Object guidObject = providerRecord.getValue("Guid");
            assertNotNull(guidObject);
            assertInstanceOf(Object[].class, guidObject);
            // Check for array of Byte objects if not empty
            Object[] guidArray = (Object[]) guidObject;
            if (guidArray.length > 0) {
                assertInstanceOf(Byte.class, guidArray[0]);
            }

        }
    }

    @Test
    public void testGenerateNullableFieldsOneHundredNullPercentageSchemaText() throws Exception {
        String schemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestGenerateRecord/nested_nullable.avsc")));
        final Schema avroSchema = new Schema.Parser().parse(schemaText);
        final RecordSchema outputSchema = AvroTypeUtil.createSchema(avroSchema);

        final MockRecordWriter recordWriter = new MockRecordWriter(null, true, -1, false, outputSchema);
        testRunner.addControllerService("record-writer", recordWriter);
        testRunner.enableControllerService(recordWriter);
        testRunner.setProperty(GenerateRecord.RECORD_WRITER, "record-writer");
        testRunner.setProperty(GenerateRecord.SCHEMA_TEXT, schemaText);
        testRunner.setProperty(GenerateRecord.NULLABLE_FIELDS, "true");
        testRunner.setProperty(GenerateRecord.NULL_PERCENTAGE, "100");
        testRunner.setProperty(GenerateRecord.NUM_RECORDS, "1");

        testRunner.run();
        testRunner.assertTransferCount(GenerateRecord.REL_SUCCESS, 1);
        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(GenerateRecord.REL_SUCCESS).getFirst();
        // null values should cause all fields to be empty in the output (2 top-level record fields in this case
        flowFile.assertContentEquals(",\n");
    }

    @Test
    void testMigrateProperties() {
        final Map<String, String> expectedRenamed = Map.of(
                "record-writer", GenerateRecord.RECORD_WRITER.getName(),
                "number-of-records", GenerateRecord.NUM_RECORDS.getName(),
                "nullable-fields", GenerateRecord.NULLABLE_FIELDS.getName(),
                "null-percentage", GenerateRecord.NULL_PERCENTAGE.getName(),
                "schema-text", GenerateRecord.SCHEMA_TEXT.getName()
        );

        final PropertyMigrationResult propertyMigrationResult = testRunner.migrateProperties();
        assertEquals(expectedRenamed, propertyMigrationResult.getPropertiesRenamed());
    }

    @Test
    public void testValidationFailsWithNoSchemaConfiguration() throws InitializationException {
        final MockRecordWriter recordWriter = new MockRecordWriter(null, true);
        testRunner.addControllerService("record-writer", recordWriter);
        testRunner.enableControllerService(recordWriter);
        testRunner.setProperty(GenerateRecord.RECORD_WRITER, "record-writer");
        testRunner.setProperty(GenerateRecord.NUM_RECORDS, "1");

        testRunner.assertNotValid();
    }

    @Test
    public void testValidationFailsWithMultipleSchemaConfigurations() throws Exception {
        String schemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestGenerateRecord/nested_nullable.avsc")));

        final MockRecordWriter recordWriter = new MockRecordWriter(null, true);
        testRunner.addControllerService("record-writer", recordWriter);
        testRunner.enableControllerService(recordWriter);
        testRunner.setProperty(GenerateRecord.RECORD_WRITER, "record-writer");
        testRunner.setProperty(GenerateRecord.NUM_RECORDS, "1");

        // Set both Schema Text and Predefined Schema - should be invalid
        testRunner.setProperty(GenerateRecord.SCHEMA_TEXT, schemaText);
        testRunner.setProperty(GenerateRecord.PREDEFINED_SCHEMA, PredefinedRecordSchema.PERSON.name());

        testRunner.assertNotValid();
    }

    @Test
    public void testValidationFailsWithPredefinedSchemaAndDynamicProperties() throws Exception {
        final MockRecordWriter recordWriter = new MockRecordWriter(null, true);
        testRunner.addControllerService("record-writer", recordWriter);
        testRunner.enableControllerService(recordWriter);
        testRunner.setProperty(GenerateRecord.RECORD_WRITER, "record-writer");
        testRunner.setProperty(GenerateRecord.NUM_RECORDS, "1");

        // Set both Predefined Schema and dynamic property - should be invalid
        testRunner.setProperty(GenerateRecord.PREDEFINED_SCHEMA, PredefinedRecordSchema.PERSON.name());
        testRunner.setProperty("myField", "Address.fullAddress");

        testRunner.assertNotValid();
    }

    @Test
    public void testPredefinedSchemaPerson() throws Exception {
        // Field names aligned with schema.org/Person and schema.org/PostalAddress
        testPredefinedSchema(PredefinedRecordSchema.PERSON, 5,
                "identifier", "givenName", "familyName", "email", "telephone", "birthDate", "age", "active", "address");
    }

    @Test
    public void testPredefinedSchemaOrder() throws Exception {
        // Field names aligned with schema.org/Order
        testPredefinedSchema(PredefinedRecordSchema.ORDER, 5,
                "orderNumber", "customer", "customerName", "customerEmail", "orderDate", "orderTime",
                "orderDelivery", "totalPrice", "priceCurrency", "orderStatus", "isGift", "itemCount", "orderedItem");
    }

    @Test
    public void testPredefinedSchemaEvent() throws Exception {
        // Field names aligned with schema.org/Event
        testPredefinedSchema(PredefinedRecordSchema.EVENT, 5,
                "identifier", "additionalType", "startDate", "startTime", "endDate", "organizer",
                "eventStatus", "description", "isAccessibleForFree", "attendeeCount", "duration", "keywords", "additionalProperty");
    }

    @Test
    public void testPredefinedSchemaSensor() throws Exception {
        // Field names aligned with schema.org conventions (GeoCoordinates for location)
        testPredefinedSchema(PredefinedRecordSchema.SENSOR, 5,
                "identifier", "additionalType", "manufacturer", "dateCreated", "temperature",
                "humidity", "pressure", "batteryLevel", "signalStrength", "isActive", "geo");
    }

    @Test
    public void testPredefinedSchemaProduct() throws Exception {
        // Field names aligned with schema.org/Product
        testPredefinedSchema(PredefinedRecordSchema.PRODUCT, 5,
                "identifier", "sku", "name", "description", "category", "brand", "price",
                "priceCurrency", "availability", "inventoryLevel", "ratingValue", "reviewCount", "dateCreated",
                "dateModified", "keywords", "additionalProperty");
    }

    @Test
    public void testPredefinedSchemaStockTrade() throws Exception {
        // Field names aligned with schema.org conventions
        testPredefinedSchema(PredefinedRecordSchema.STOCK_TRADE, 5,
                "identifier", "tickerSymbol", "name", "exchange", "actionType", "dateCreated",
                "price", "orderQuantity", "totalPrice", "priceCurrency", "bidPrice", "askPrice",
                "highPrice", "lowPrice", "marketCap", "isSettled");
    }

    @Test
    public void testPredefinedSchemaCompleteExample() throws Exception {
        // Field names aligned with schema.org conventions
        testPredefinedSchema(PredefinedRecordSchema.COMPLETE_EXAMPLE, 3,
                "identifier", "isActive", "score", "count", "ratingValue", "price", "balance", "initial",
                "flags", "position", "dateCreated", "lastLogin", "dateModified", "keywords",
                "scores", "additionalProperty", "person", "orderedItem");
    }

    private void testPredefinedSchema(PredefinedRecordSchema predefinedSchema, int numRecords, String... expectedFields) throws Exception {
        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        testRunner.addControllerService("json-writer", jsonWriter);
        testRunner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.INHERIT_RECORD_SCHEMA);
        testRunner.enableControllerService(jsonWriter);

        testRunner.setProperty(GenerateRecord.RECORD_WRITER, "json-writer");
        testRunner.setProperty(GenerateRecord.PREDEFINED_SCHEMA, predefinedSchema.name());
        testRunner.setProperty(GenerateRecord.NULLABLE_FIELDS, "true");
        testRunner.setProperty(GenerateRecord.NULL_PERCENTAGE, "0");
        testRunner.setProperty(GenerateRecord.NUM_RECORDS, String.valueOf(numRecords));

        testRunner.assertValid();
        testRunner.run();

        testRunner.assertTransferCount(GenerateRecord.REL_SUCCESS, 1);
        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(GenerateRecord.REL_SUCCESS).get(0);

        // Verify record count attribute
        flowFile.assertAttributeEquals("record.count", String.valueOf(numRecords));
        flowFile.assertAttributeEquals("mime.type", "application/json");

        // Verify content is valid JSON and contains expected fields
        final String content = flowFile.getContent();
        assertNotNull(content);
        assertTrue(content.startsWith("["), "Content should be a JSON array");

        // Verify all expected fields are present in the output
        for (String field : expectedFields) {
            assertTrue(content.contains("\"" + field + "\""),
                    "Expected field '" + field + "' not found in output for schema " + predefinedSchema.name());
        }

        // Parse and verify records using JsonTreeReader
        final JsonTreeReader jsonReader = new JsonTreeReader();
        testRunner.addControllerService("json-reader", jsonReader);
        testRunner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, "infer-schema");
        testRunner.enableControllerService(jsonReader);

        final byte[] contentBytes = flowFile.toByteArray();
        try (final ByteArrayInputStream inputStream = new ByteArrayInputStream(contentBytes);
             final RecordReader recordReader = jsonReader.createRecordReader(flowFile.getAttributes(), inputStream, contentBytes.length, testRunner.getLogger())) {

            int recordCount = 0;
            Record record;
            while ((record = recordReader.nextRecord()) != null) {
                recordCount++;
                // Verify each expected field exists in the record
                for (String field : expectedFields) {
                    assertTrue(record.getSchema().getFieldNames().contains(field),
                            "Record schema should contain field '" + field + "' for schema " + predefinedSchema.name());
                }
            }
            assertEquals(numRecords, recordCount, "Should have generated " + numRecords + " records");
        }
    }

    @Test
    public void testPredefinedSchemaWithNullPercentage() throws Exception {
        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        testRunner.addControllerService("json-writer", jsonWriter);
        testRunner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.INHERIT_RECORD_SCHEMA);
        testRunner.enableControllerService(jsonWriter);

        testRunner.setProperty(GenerateRecord.RECORD_WRITER, "json-writer");
        testRunner.setProperty(GenerateRecord.PREDEFINED_SCHEMA, PredefinedRecordSchema.PERSON.name());
        testRunner.setProperty(GenerateRecord.NULLABLE_FIELDS, "true");
        testRunner.setProperty(GenerateRecord.NULL_PERCENTAGE, "100");
        testRunner.setProperty(GenerateRecord.NUM_RECORDS, "1");

        testRunner.assertValid();
        testRunner.run();

        testRunner.assertTransferCount(GenerateRecord.REL_SUCCESS, 1);
        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(GenerateRecord.REL_SUCCESS).get(0);

        // With 100% null percentage, all nullable fields should be null
        final String content = flowFile.getContent();
        assertNotNull(content);
        // The content should contain null values
        assertTrue(content.contains("null"), "With 100% null percentage, output should contain null values");
    }
}
