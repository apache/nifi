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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.avro.Schema;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.processors.standard.faker.FakerMethodHolder;
import org.apache.nifi.processors.standard.faker.FakerUtils;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

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
        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(GenerateRecord.REL_SUCCESS).get(0);
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
        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(GenerateRecord.REL_SUCCESS).get(0);
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
        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(GenerateRecord.REL_SUCCESS).get(0);
        // null values should cause all fields to be empty in the output
        // create a string of commas whose number equals the number of fields in the datatypeFunctionMap (size - 1 copies)
        flowFile.assertContentEquals(String.join("", Collections.nCopies(FakerUtils.getDatatypeFunctionMap().size() - 1, ",")) + "\n");
    }

    // Tests that the remaining fields are supported by the processor.
    @Test
    public void testFieldsReturnValue() throws Exception {

        List<Field> fieldTypeFields = Arrays.stream(GenerateRecord.class.getFields()).filter((field) -> field.getName().startsWith("FT_")).collect(Collectors.toList());
        for (Field field : fieldTypeFields) {
            testRunner.setProperty(field.getName().toLowerCase(Locale.ROOT), ((AllowableValue) field.get(processor)).getValue());
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
        testRunner.setProperty(GenerateRecord.NUM_RECORDS, "3");

        testRunner.run();
        testRunner.assertTransferCount(GenerateRecord.REL_SUCCESS, 1);
        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(GenerateRecord.REL_SUCCESS).get(0);
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
        final JsonRecordSetWriter recordWriter = new JsonRecordSetWriter();
        testRunner.addControllerService("record-writer", recordWriter);
        testRunner.enableControllerService(recordWriter);
        testRunner.setProperty(GenerateRecord.RECORD_WRITER, "record-writer");
        testRunner.setProperty(GenerateRecord.SCHEMA_TEXT, schemaText);
        testRunner.setProperty(GenerateRecord.NULLABLE_FIELDS, "false"); // Should be ignored
        testRunner.setProperty(GenerateRecord.NULL_PERCENTAGE, "0");
        testRunner.setProperty(GenerateRecord.NUM_RECORDS, "3");

        testRunner.run();
        testRunner.assertTransferCount(GenerateRecord.REL_SUCCESS, 1);
        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(GenerateRecord.REL_SUCCESS).get(0);
        final String output = flowFile.getContent();
        final JsonFactory jsonFactory = new JsonFactory();
        try (JsonParser jsonParser = jsonFactory.createParser(output)) {
            jsonParser.setCodec(new ObjectMapper());
            JsonNode recordArray = jsonParser.readValueAsTree();
            assertTrue(recordArray instanceof ArrayNode);
            JsonNode recordNode = recordArray.get(0);
            JsonNode systemNode = recordNode.get("System");
            assertNotNull(systemNode);
            JsonNode providerNode = systemNode.get("Provider");
            assertNotNull(providerNode);
            JsonNode guidNode = providerNode.get("Guid");
            assertNotNull(guidNode);
            assertNotNull(guidNode.asText());
            JsonNode nameNode = providerNode.get("Name");
            assertNotNull(nameNode);
            assertNotNull(nameNode.asText());
            JsonNode eventIdNode = systemNode.get("EventID");
            assertNotNull(eventIdNode);
            eventIdNode.asInt(); // This would throw a NullPointerException if the value was null
            JsonNode eventDataNode = recordNode.get("EventData");
            assertNotNull(eventDataNode);
            JsonNode dataNode = eventDataNode.get("Data");
            assertNotNull(dataNode);
            assertTrue(dataNode instanceof ArrayNode);
            assertTrue(dataNode.size() <= 10 && dataNode.size() >= 0);
            for (int i = 0; i < dataNode.size(); i++) {
                JsonNode dataElementNode = dataNode.get(i);
                assertNotNull(dataElementNode);
                JsonNode dataElementNameNode = dataElementNode.get("Name");
                assertNotNull(dataElementNameNode);
                assertNotNull(dataElementNameNode.asText());
                JsonNode dataElementDataNode = dataElementNode.get("DataElement");
                assertNotNull(dataElementDataNode);
                assertNotNull(dataElementDataNode.asText());
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
        testRunner.setProperty(GenerateRecord.NULLABLE_FIELDS, "false"); // Should be ignored
        testRunner.setProperty(GenerateRecord.NULL_PERCENTAGE, "100");
        testRunner.setProperty(GenerateRecord.NUM_RECORDS, "1");

        testRunner.run();
        testRunner.assertTransferCount(GenerateRecord.REL_SUCCESS, 1);
        MockFlowFile flowFile = testRunner.getFlowFilesForRelationship(GenerateRecord.REL_SUCCESS).get(0);
        // null values should cause all fields to be empty in the output (2 top-level record fields in this case
        flowFile.assertContentEquals(",\n");
    }
}