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
package org.apache.nifi.processors.jolt;

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.jolt.util.JoltTransformStrategy;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.schema.inference.SchemaInferenceUtil;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.StringUtils;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisabledOnOs(OS.WINDOWS) //The pretty printed json comparisons don't work on windows
public class TestJoltTransformRecord {

    final static String CHAINR_SPEC_PATH = "src/test/resources/specs/chainrSpec.json";
    static String chainrSpecContents;
    private TestRunner runner;
    private JoltTransformRecord processor;
    private MockRecordParser parser;
    private JsonRecordSetWriter writer;

    @BeforeAll
    static void setUpBeforeAll() throws Exception {
        chainrSpecContents = Files.readString(Paths.get(CHAINR_SPEC_PATH));
    }

    @BeforeEach
    public void setup() throws Exception {
        processor = new JoltTransformRecord();
        runner = TestRunners.newTestRunner(processor);
        parser = new MockRecordParser();
        runner.addControllerService("parser", parser);
        runner.enableControllerService(parser);
        runner.setProperty(JoltTransformRecord.RECORD_READER, "parser");
        writer = new JsonRecordSetWriter();
        runner.addControllerService("writer", writer);
        runner.setProperty(writer, "Schema Write Strategy", "full-schema-attribute");
        runner.setProperty(JoltTransformRecord.RECORD_WRITER, "writer");
        // Each test must set the Schema Access strategy and Schema, and enable the writer CS
    }

    @Test
    public void testRelationshipsCreated() throws IOException {
        generateTestData(1, null);
        final String outputSchemaText = Files.readString(Paths.get("src/test/resources/TestJoltTransformRecord/chainrOutputSchema.avsc"));
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(writer, JsonRecordSetWriter.PRETTY_PRINT_JSON, "true");
        runner.enableControllerService(writer);
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, chainrSpecContents);
        runner.enqueue(new byte[0]);
        Set<Relationship> relationships = processor.getRelationships();
        assertTrue(relationships.contains(JoltTransformRecord.REL_FAILURE));
        assertTrue(relationships.contains(JoltTransformRecord.REL_SUCCESS));
        assertTrue(relationships.contains(JoltTransformRecord.REL_ORIGINAL));
        assertEquals(3, relationships.size());
    }

    @Test
    public void testRelationshipsCreatedFromFile() throws IOException {
        generateTestData(1, null);
        final String outputSchemaText = Files.readString(Paths.get("src/test/resources/TestJoltTransformRecord/chainrOutputSchema.avsc"));
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(writer, JsonRecordSetWriter.PRETTY_PRINT_JSON, "true");
        runner.enableControllerService(writer);
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, CHAINR_SPEC_PATH);
        runner.enqueue(new byte[0]);
        Set<Relationship> relationships = processor.getRelationships();
        assertTrue(relationships.contains(JoltTransformRecord.REL_FAILURE));
        assertTrue(relationships.contains(JoltTransformRecord.REL_SUCCESS));
        assertTrue(relationships.contains(JoltTransformRecord.REL_ORIGINAL));
        assertEquals(3, relationships.size());
    }

    @Test
    public void testInvalidJOLTSpec() throws IOException {
        generateTestData(1, null);
        final String outputSchemaText = Files.readString(Paths.get("src/test/resources/TestJoltTransformRecord/shiftrOutputSchema.avsc"));
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(writer, JsonRecordSetWriter.PRETTY_PRINT_JSON, "true");
        runner.enableControllerService(writer);
        String spec = "[{}]";
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, spec);
        runner.assertNotValid();

        runner.setProperty(JoltTransformRecord.JOLT_SPEC, chainrSpecContents);
        runner.assertValid();
    }

    @Test
    public void testIncorrectJOLTSpec() {
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, chainrSpecContents);
        runner.setProperty(JoltTransformRecord.JOLT_TRANSFORM, JoltTransformStrategy.SHIFTR);
        runner.assertNotValid();
    }

    @Test
    public void testSpecIsNotSet() {
        runner.setProperty(JoltTransformRecord.JOLT_TRANSFORM, JoltTransformStrategy.SHIFTR);
        runner.assertNotValid();
    }

    @Test
    public void testSpecIsEmpty() throws IOException {
        generateTestData(1, null);
        final String outputSchemaText = Files.readString(Paths.get("src/test/resources/TestJoltTransformRecord/shiftrOutputSchema.avsc"));
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(writer, JsonRecordSetWriter.PRETTY_PRINT_JSON, "true");
        runner.enableControllerService(writer);
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, StringUtils.EMPTY);
        runner.setProperty(JoltTransformRecord.JOLT_TRANSFORM, JoltTransformStrategy.SHIFTR);
        runner.assertNotValid();
    }

    @Test
    public void testSpecNotRequired() throws IOException {
        generateTestData(1, null);
        final String outputSchemaText = Files.readString(Paths.get("src/test/resources/TestJoltTransformRecord/shiftrOutputSchema.avsc"));
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(writer, JsonRecordSetWriter.PRETTY_PRINT_JSON, "true");
        runner.enableControllerService(writer);
        runner.setProperty(JoltTransformRecord.JOLT_TRANSFORM, JoltTransformStrategy.SORTR);
        runner.assertValid();
    }

    @Test
    public void testNoFlowFileContent() throws IOException {
        generateTestData(1, null);
        final String outputSchemaText = Files.readString(Paths.get("src/test/resources/TestJoltTransformRecord/chainrOutputSchema.avsc"));
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(writer, JsonRecordSetWriter.PRETTY_PRINT_JSON, "true");
        runner.enableControllerService(writer);
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, chainrSpecContents);
        runner.run();
        runner.assertQueueEmpty();
        runner.assertTransferCount(JoltTransformRecord.REL_FAILURE, 0);
        runner.assertTransferCount(JoltTransformRecord.REL_SUCCESS, 0);
    }

    @Test
    public void testNoRecords() throws IOException {
        generateTestData(0, null);
        final String outputSchemaText = Files.readString(Paths.get("src/test/resources/TestJoltTransformRecord/chainrOutputSchema.avsc"));
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(writer, JsonRecordSetWriter.PRETTY_PRINT_JSON, "true");
        runner.enableControllerService(writer);
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, chainrSpecContents);
        runner.enqueue("{}");
        runner.run();
        runner.assertQueueEmpty();
        runner.assertTransferCount(JoltTransformRecord.REL_FAILURE, 0);
        runner.assertTransferCount(JoltTransformRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(JoltTransformRecord.REL_ORIGINAL, 1);
    }

    @Test
    public void testInvalidFlowFileContent() throws IOException {
        generateTestData(1, null);

        final String outputSchemaText = Files.readString(Paths.get("src/test/resources/TestJoltTransformRecord/chainrOutputSchema.avsc"));

        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(writer, JsonRecordSetWriter.PRETTY_PRINT_JSON, "true");
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, chainrSpecContents);

        runner.enableControllerService(writer);
        parser.failAfter(0);
        runner.enqueue("invalid json");

        runner.run();
        runner.assertAllFlowFilesTransferred(JoltTransformRecord.REL_FAILURE);
    }

    @Test
    public void testCustomTransformationWithNoModule() throws IOException {
        generateTestData(1, null);
        final String outputSchemaText = Files.readString(Paths.get("src/test/resources/TestJoltTransformRecord/chainrOutputSchema.avsc"));
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(writer, JsonRecordSetWriter.PRETTY_PRINT_JSON, "true");
        runner.enableControllerService(writer);
        final String spec = Files.readString(Paths.get("src/test/resources/specs/customChainrSpec.json"));
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, spec);
        runner.setProperty(JoltTransformRecord.CUSTOM_CLASS, "TestCustomJoltTransform");
        runner.setProperty(JoltTransformRecord.JOLT_TRANSFORM, JoltTransformStrategy.CUSTOMR);
        runner.assertValid();
    }

    @Test
    public void testCustomTransformationWithMissingClassName() throws IOException {
        generateTestData(1, null);
        final String outputSchemaText = Files.readString(Paths.get("src/test/resources/TestJoltTransformRecord/chainrOutputSchema.avsc"));
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(writer, JsonRecordSetWriter.PRETTY_PRINT_JSON, "true");
        runner.enableControllerService(writer);
        final String customJarPath = "src/test/resources/TestJoltTransformRecord/TestCustomJoltTransform.jar";
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, chainrSpecContents);
        runner.setProperty(JoltTransformRecord.MODULES, customJarPath);
        runner.setProperty(JoltTransformRecord.JOLT_TRANSFORM, JoltTransformStrategy.CUSTOMR);
        runner.enqueue(new byte[0]);
        runner.assertNotValid();
    }

    @Test
    public void testCustomTransformationWithInvalidClassPath() {
        final String customJarPath = "src/test/resources/TestJoltTransformRecord/FakeCustomJar.jar";
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, chainrSpecContents);
        runner.setProperty(JoltTransformRecord.CUSTOM_CLASS, "TestCustomJoltTransform");
        runner.setProperty(JoltTransformRecord.MODULES, customJarPath);
        runner.setProperty(JoltTransformRecord.JOLT_TRANSFORM, JoltTransformStrategy.CUSTOMR);
        runner.enqueue(new byte[0]);
        runner.assertNotValid();
    }

    @Test
    public void testCustomTransformationWithInvalidClassName() {
        final String customJarPath = "src/test/resources/TestJoltTransformRecord/TestCustomJoltTransform.jar";
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, chainrSpecContents);
        runner.setProperty(JoltTransformRecord.CUSTOM_CLASS, "FakeCustomJoltTransform");
        runner.setProperty(JoltTransformRecord.MODULES, customJarPath);
        runner.setProperty(JoltTransformRecord.JOLT_TRANSFORM, JoltTransformStrategy.CUSTOMR);
        runner.enqueue(new byte[0]);
        runner.assertNotValid();
    }

    @ParameterizedTest(name = "{index} {1}")
    @MethodSource("getChainrArguments")
    public void testTransformInputWithChainr(Path specPath, String description) throws IOException {
        generateTestData(1, null);
        final String outputSchemaText = Files.readString(Paths.get("src/test/resources/TestJoltTransformRecord/chainrOutputSchema.avsc"));
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(writer, JsonRecordSetWriter.PRETTY_PRINT_JSON, "true");
        runner.enableControllerService(writer);
        final String spec = Files.readString(specPath);
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, spec);
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertTransferCount(JoltTransformRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(JoltTransformRecord.REL_ORIGINAL, 1);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(JoltTransformRecord.REL_SUCCESS).get(0);
        transformed.assertAttributeExists(CoreAttributes.MIME_TYPE.key());
        transformed.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/json");
        assertEquals(Files.readString(Paths.get("src/test/resources/TestJoltTransformRecord/chainrOutput.json")),
                new String(transformed.toByteArray()));
    }

    @Test
    public void testTransformInputWithShiftr() throws IOException {
        generateTestData(1, null);
        final String outputSchemaText = Files.readString(Paths.get("src/test/resources/TestJoltTransformRecord/shiftrOutputSchema.avsc"));
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(writer, JsonRecordSetWriter.PRETTY_PRINT_JSON, "true");
        runner.enableControllerService(writer);
        final String spec = Files.readString(Paths.get("src/test/resources/specs/shiftrSpec.json"));
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, spec);
        runner.setProperty(JoltTransformRecord.JOLT_TRANSFORM, JoltTransformStrategy.SHIFTR);
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertTransferCount(JoltTransformRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(JoltTransformRecord.REL_ORIGINAL, 1);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(JoltTransformRecord.REL_SUCCESS).get(0);
        transformed.assertAttributeExists(CoreAttributes.MIME_TYPE.key());
        transformed.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/json");
        assertEquals(Files.readString(Paths.get("src/test/resources/TestJoltTransformRecord/shiftrOutput.json")),
                new String(transformed.toByteArray()));
    }

    String addAccentedChars(String input) {
        return input.replace("\"primary\"", "\"primaryÄÖÜ\"");
    }

    @Test
    public void testTransformInputWithShiftrAccentedChars() throws IOException {
        generateTestData(1, null);
        final String outputSchemaText = Files.readString(Paths.get("src/test/resources/TestJoltTransformRecord/shiftrOutputSchema.avsc"));
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(writer, JsonRecordSetWriter.PRETTY_PRINT_JSON, "true");
        runner.enableControllerService(writer);
        final String spec = Files.readString(Paths.get("src/test/resources/specs/shiftrSpec.json"));
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, spec);
        runner.setProperty(JoltTransformRecord.JOLT_TRANSFORM, JoltTransformStrategy.SHIFTR);
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertTransferCount(JoltTransformRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(JoltTransformRecord.REL_ORIGINAL, 1);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(JoltTransformRecord.REL_SUCCESS).get(0);
        transformed.assertAttributeExists(CoreAttributes.MIME_TYPE .key());
        transformed.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/json");
        assertEquals(Files.readString(Paths.get("src/test/resources/TestJoltTransformRecord/shiftrOutput.json")),
                new String(transformed.toByteArray()));
    }

    @Test
    public void testTransformInputWithShiftrMultipleOutputRecords() throws IOException {
        RecordField aField = new RecordField("a", RecordFieldType.INT.getDataType());
        RecordField bField = new RecordField("b", RecordFieldType.INT.getDataType());
        RecordField cField = new RecordField("c", RecordFieldType.INT.getDataType());
        List<RecordField> abcFields = Arrays.asList(aField, bField, cField);
        RecordSchema xSchema = new SimpleRecordSchema(abcFields);
        RecordField xRecord = new RecordField("x", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.RECORD.getRecordDataType(xSchema)));
        parser.addSchemaField(xRecord);

        final Record record1 = new MapRecord(xSchema, new HashMap<String, Object>() {{
            put("a", 1);
            put("b", 2);
            put("c", 3);
        }});
        final Record record2 = new MapRecord(xSchema, new HashMap<String, Object>() {{
            put("a", 11);
            put("b", 21);
            put("c", 31);
        }});
        final Record record3 = new MapRecord(xSchema, new HashMap<String, Object>() {{
            put("a", 21);
            put("b", 2);
            put("c", 3);
        }});
        final Object[] recordArray1 = new Object[]{record1, record2, record3};
        parser.addRecord((Object) recordArray1);

        final Record record4 = new MapRecord(xSchema, new HashMap<String, Object>() {{
            put("a", 100);
            put("b", 200);
            put("c", 300);
        }});
        final Record record5 = new MapRecord(xSchema, new HashMap<String, Object>() {{
            put("a", 101);
            put("b", 201);
            put("c", 301);
        }});
        final Object[] recordArray2 = new Object[]{record4, record5};
        parser.addRecord((Object) recordArray2);

        final String outputSchemaText = Files.readString(Paths.get("src/test/resources/TestJoltTransformRecord/shiftrOutputSchemaMultipleOutputRecords.avsc"));
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(writer, JsonRecordSetWriter.PRETTY_PRINT_JSON, "true");
        runner.enableControllerService(writer);
        final String spec = Files.readString(Paths.get("src/test/resources/TestJoltTransformRecord/shiftrSpecMultipleOutputRecords.json"));
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, spec);
        runner.setProperty(JoltTransformRecord.JOLT_TRANSFORM, JoltTransformStrategy.SHIFTR);
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertTransferCount(JoltTransformRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(JoltTransformRecord.REL_ORIGINAL, 1);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(JoltTransformRecord.REL_SUCCESS).get(0);
        transformed.assertAttributeExists(CoreAttributes.MIME_TYPE.key());
        transformed.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/json");
        assertEquals(Files.readString(Paths.get("src/test/resources/TestJoltTransformRecord/shiftrOutputMultipleOutputRecords.json")),
                new String(transformed.toByteArray()));
    }

    @Test
    public void testTransformInputWithShiftrFromFile() throws IOException {
        generateTestData(1, null);
        final String outputSchemaText = Files.readString(Paths.get("src/test/resources/TestJoltTransformRecord/shiftrOutputSchema.avsc"));
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(writer, JsonRecordSetWriter.PRETTY_PRINT_JSON, "true");
        runner.enableControllerService(writer);
        final String spec = "./src/test/resources/specs/shiftrSpec.json";
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, spec);
        runner.setProperty(JoltTransformRecord.JOLT_TRANSFORM, JoltTransformStrategy.SHIFTR);
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertTransferCount(JoltTransformRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(JoltTransformRecord.REL_ORIGINAL, 1);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(JoltTransformRecord.REL_SUCCESS).get(0);
        transformed.assertAttributeExists(CoreAttributes.MIME_TYPE.key());
        transformed.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/json");
        assertEquals(Files.readString(Paths.get("src/test/resources/TestJoltTransformRecord/shiftrOutput.json")),
                new String(transformed.toByteArray()));
    }

    @Test
    public void testTransformInputWithDefaultr() throws IOException {
        generateTestData(1, null);
        final String outputSchemaText = Files.readString(Paths.get("src/test/resources/TestJoltTransformRecord/defaultrOutputSchema.avsc"));
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(writer, JsonRecordSetWriter.PRETTY_PRINT_JSON, "true");
        runner.enableControllerService(writer);
        final String spec = Files.readString(Paths.get("src/test/resources/specs/defaultrSpec.json"));
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, spec);
        runner.setProperty(JoltTransformRecord.JOLT_TRANSFORM, JoltTransformStrategy.DEFAULTR);
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertTransferCount(JoltTransformRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(JoltTransformRecord.REL_ORIGINAL, 1);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(JoltTransformRecord.REL_SUCCESS).get(0);
        assertEquals(Files.readString(Paths.get("src/test/resources/TestJoltTransformRecord/defaultrOutput.json")),
                new String(transformed.toByteArray()));
    }

    @Test
    public void testTransformInputWithRemovr() throws IOException {
        generateTestData(1, null);
        final String outputSchemaText = Files.readString(Paths.get("src/test/resources/TestJoltTransformRecord/removrOutputSchema.avsc"));
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(writer, JsonRecordSetWriter.PRETTY_PRINT_JSON, "true");
        runner.enableControllerService(writer);
        final String spec = Files.readString(Paths.get("src/test/resources/specs/removrSpec.json"));
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, spec);
        runner.setProperty(JoltTransformRecord.JOLT_TRANSFORM, JoltTransformStrategy.REMOVR);
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertTransferCount(JoltTransformRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(JoltTransformRecord.REL_ORIGINAL, 1);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(JoltTransformRecord.REL_SUCCESS).get(0);
        assertEquals(Files.readString(Paths.get("src/test/resources/TestJoltTransformRecord/removrOutput.json")),
                new String(transformed.toByteArray()));

    }

    @Test
    public void testTransformInputWithCardinality() throws IOException {
        generateTestData(1, null);
        final String outputSchemaText = Files.readString(Paths.get("src/test/resources/TestJoltTransformRecord/cardrOutputSchema.avsc"));
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(writer, JsonRecordSetWriter.PRETTY_PRINT_JSON, "true");
        runner.enableControllerService(writer);
        final String spec = Files.readString(Paths.get("src/test/resources/specs/cardrSpec.json"));
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, spec);
        runner.setProperty(JoltTransformRecord.JOLT_TRANSFORM, JoltTransformStrategy.CARDINALITY);
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertTransferCount(JoltTransformRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(JoltTransformRecord.REL_ORIGINAL, 1);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(JoltTransformRecord.REL_SUCCESS).get(0);
        assertEquals(Files.readString(Paths.get("src/test/resources/TestJoltTransformRecord/cardrOutput.json")),
                new String(transformed.toByteArray()));

    }

    @Test
    public void testTransformInputWithSortr() throws IOException {
        generateTestData(1, null);
        final String outputSchemaText = Files.readString(Paths.get("src/test/resources/TestJoltTransformRecord/sortrOutputSchema.avsc"));
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(writer, JsonRecordSetWriter.PRETTY_PRINT_JSON, "true");
        runner.enableControllerService(writer);
        runner.setProperty(JoltTransformRecord.JOLT_TRANSFORM, JoltTransformStrategy.SORTR);
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertTransferCount(JoltTransformRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(JoltTransformRecord.REL_ORIGINAL, 1);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(JoltTransformRecord.REL_SUCCESS).get(0);
        transformed.assertAttributeExists(CoreAttributes.MIME_TYPE.key());
        transformed.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/json");
        assertEquals(Files.readString(Paths.get("src/test/resources/TestJoltTransformRecord/sortrOutput.json")),
                new String(transformed.toByteArray()));
    }

    @Test
    public void testTransformInputWithDefaultrExpressionLanguage() throws IOException {
        generateTestData(1, null);
        final String outputSchemaText = Files.readString(Paths.get("src/test/resources/TestJoltTransformRecord/defaultrELOutputSchema.avsc"));
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(writer, JsonRecordSetWriter.PRETTY_PRINT_JSON, "true");
        runner.enableControllerService(writer);
        final String spec = Files.readString(Paths.get("src/test/resources/specs/defaultrELSpec.json"));
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, spec);
        runner.setProperty(JoltTransformRecord.JOLT_TRANSFORM, JoltTransformStrategy.DEFAULTR);
        runner.setEnvironmentVariableValue("quota", "5");
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertTransferCount(JoltTransformRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(JoltTransformRecord.REL_ORIGINAL, 1);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(JoltTransformRecord.REL_SUCCESS).get(0);
        assertEquals(Files.readString(Paths.get("src/test/resources/TestJoltTransformRecord/defaultrELOutput.json")),
                new String(transformed.toByteArray()));

    }

    @Test
    public void testTransformInputWithModifierDefault() throws IOException {
        generateTestData(1, null);
        // Input schema = output schema, just modifying values
        final String outputSchemaText = Files.readString(Paths.get("src/test/resources/TestJoltTransformRecord/inputSchema.avsc"));
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(writer, JsonRecordSetWriter.PRETTY_PRINT_JSON, "true");
        runner.enableControllerService(writer);
        final String spec = Files.readString(Paths.get("src/test/resources/specs/modifierDefaultSpec.json"));
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, spec);
        runner.setProperty(JoltTransformRecord.JOLT_TRANSFORM, JoltTransformStrategy.MODIFIER_DEFAULTR);
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertTransferCount(JoltTransformRecord.REL_SUCCESS, 1);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(JoltTransformRecord.REL_SUCCESS).get(0);
        assertEquals(Files.readString(Paths.get("src/test/resources/TestJoltTransformRecord/modifierDefaultOutput.json")),
                new String(transformed.toByteArray()));
    }

    @Test
    public void testTransformInputWithModifierDefine() throws IOException {
        generateTestData(1, null);
        final String outputSchemaText = Files.readString(Paths.get("src/test/resources/TestJoltTransformRecord/modifierDefineOutputSchema.avsc"));
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(writer, JsonRecordSetWriter.PRETTY_PRINT_JSON, "true");
        runner.enableControllerService(writer);
        final String spec = Files.readString(Paths.get("src/test/resources/specs/modifierDefineSpec.json"));
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, spec);
        runner.setProperty(JoltTransformRecord.JOLT_TRANSFORM, JoltTransformStrategy.MODIFIER_DEFAULTR);
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertTransferCount(JoltTransformRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(JoltTransformRecord.REL_ORIGINAL, 1);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(JoltTransformRecord.REL_SUCCESS).get(0);
        assertEquals(Files.readString(Paths.get("src/test/resources/TestJoltTransformRecord/modifierDefineOutput.json")),
                new String(transformed.toByteArray()));
    }

    @Test
    public void testTransformInputWithModifierOverwrite() throws IOException {
        generateTestData(1, null);
        final String outputSchemaText = Files.readString(Paths.get("src/test/resources/TestJoltTransformRecord/modifierOverwriteOutputSchema.avsc"));
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(writer, JsonRecordSetWriter.PRETTY_PRINT_JSON, "true");
        runner.enableControllerService(writer);
        final String spec = Files.readString(Paths.get("src/test/resources/specs/modifierOverwriteSpec.json"));
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, spec);
        runner.setProperty(JoltTransformRecord.JOLT_TRANSFORM, JoltTransformStrategy.MODIFIER_DEFAULTR);
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertTransferCount(JoltTransformRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(JoltTransformRecord.REL_ORIGINAL, 1);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(JoltTransformRecord.REL_SUCCESS).get(0);
        assertEquals(Files.readString(Paths.get("src/test/resources/TestJoltTransformRecord/modifierOverwriteOutput.json")),
                new String(transformed.toByteArray()));
    }

    @Test
    public void testTransformInputWithSortrPopulatedSpec() throws IOException {
        generateTestData(1, null);
        final String outputSchemaText = Files.readString(Paths.get("src/test/resources/TestJoltTransformRecord/sortrOutputSchema.avsc"));
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(writer, JsonRecordSetWriter.PRETTY_PRINT_JSON, "true");
        runner.enableControllerService(writer);
        runner.setProperty(JoltTransformRecord.JOLT_TRANSFORM, JoltTransformStrategy.SORTR);
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, "abcd");
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertTransferCount(JoltTransformRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(JoltTransformRecord.REL_ORIGINAL, 1);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(JoltTransformRecord.REL_SUCCESS).get(0);
        transformed.assertAttributeExists(CoreAttributes.MIME_TYPE.key());
        transformed.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/json");
        assertEquals(Files.readString(Paths.get("src/test/resources/TestJoltTransformRecord/sortrOutput.json")),
                new String(transformed.toByteArray()));
    }

    @Test
    public void testTransformInputCustomTransformationIgnored() throws IOException {
        generateTestData(1, null);
        final String outputSchemaText = Files.readString(Paths.get("src/test/resources/TestJoltTransformRecord/defaultrOutputSchema.avsc"));
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(writer, JsonRecordSetWriter.PRETTY_PRINT_JSON, "true");
        runner.enableControllerService(writer);
        final String customJarPath = "src/test/resources/TestJoltTransformRecord/TestCustomJoltTransform.jar";
        final String spec = Files.readString(Paths.get("src/test/resources/specs/defaultrSpec.json"));
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, spec);
        runner.setProperty(JoltTransformRecord.CUSTOM_CLASS, "TestCustomJoltTransform");
        runner.setProperty(JoltTransformRecord.MODULES, customJarPath);
        runner.setProperty(JoltTransformRecord.JOLT_TRANSFORM, JoltTransformStrategy.DEFAULTR);
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertTransferCount(JoltTransformRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(JoltTransformRecord.REL_ORIGINAL, 1);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(JoltTransformRecord.REL_SUCCESS).get(0);
        transformed.assertAttributeExists(CoreAttributes.MIME_TYPE.key());
        transformed.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/json");
        assertEquals(Files.readString(Paths.get("src/test/resources/TestJoltTransformRecord/defaultrOutput.json")),
                new String(transformed.toByteArray()));
    }

    @Test
    public void testExpressionLanguageJarFile() throws IOException {
        generateTestData(1, null);
        final String outputSchemaText = Files.readString(Paths.get("src/test/resources/TestJoltTransformRecord/defaultrOutputSchema.avsc"));
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(writer, JsonRecordSetWriter.PRETTY_PRINT_JSON, "true");
        runner.enableControllerService(writer);
        URL t = getClass().getResource("/TestJoltTransformRecord/TestCustomJoltTransform.jar");
        assertNotNull(t);
        final String customJarPath = t.getPath();
        final String spec = Files.readString(Paths.get("src/test/resources/specs/customChainrSpec.json"));
        final String customJoltTransform = "TestCustomJoltTransform";
        final String customClass = "TestCustomJoltTransform";
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, "${JOLT_SPEC}");
        runner.setProperty(JoltTransformRecord.MODULES, customJarPath);
        runner.setProperty(JoltTransformRecord.CUSTOM_CLASS, "${CUSTOM_CLASS}");
        runner.setProperty(JoltTransformRecord.JOLT_TRANSFORM, JoltTransformStrategy.CUSTOMR);
        runner.setEnvironmentVariableValue("CUSTOM_JAR", customJarPath);
        Map<String, String> customSpecs = new HashMap<>();
        customSpecs.put("JOLT_SPEC", spec);
        customSpecs.put("CUSTOM_JOLT_CLASS", customJoltTransform);
        customSpecs.put("CUSTOM_CLASS", customClass);
        runner.enqueue(new byte[0], customSpecs);
        runner.assertValid();
    }

    @Test
    public void testJoltSpecEL() throws IOException {
        generateTestData(1, null);
        final String outputSchemaText = Files.readString(Paths.get("src/test/resources/TestJoltTransformRecord/defaultrOutputSchema.avsc"));

        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(writer, JsonRecordSetWriter.PRETTY_PRINT_JSON, "true");
        runner.enableControllerService(writer);

        runner.setProperty(JoltTransformRecord.JOLT_SPEC, "${joltSpec}");
        runner.setProperty(JoltTransformRecord.JOLT_TRANSFORM, JoltTransformStrategy.DEFAULTR);

        final Map<String, String> attributes = Collections.singletonMap("joltSpec",
                "{\"RatingRange\":5,\"rating\":{\"*\":{\"MaxLabel\":\"High\",\"MinLabel\":\"Low\",\"DisplayType\":\"NORMAL\"}}}");
        runner.enqueue(new byte[0], attributes);

        runner.run();
        runner.assertTransferCount(JoltTransformRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(JoltTransformRecord.REL_ORIGINAL, 1);

        final MockFlowFile transformed = runner.getFlowFilesForRelationship(JoltTransformRecord.REL_SUCCESS).get(0);
        assertEquals(Files.readString(Paths.get("src/test/resources/TestJoltTransformRecord/defaultrOutput.json")),
                new String(transformed.toByteArray()));
    }

    @Test
    public void testJoltSpecInvalidEL() {
        final TestRunner runner = TestRunners.newTestRunner(new JoltTransformRecord());
        final String spec = "${joltSpec:nonExistingFunction()}";
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, spec);
        runner.enqueue(new byte[0]);
        runner.assertNotValid();
    }

    @Test
    public void testJoltComplexChoiceField() throws Exception {
        final JsonTreeReader reader = new JsonTreeReader();
        runner.addControllerService("reader", reader);
        runner.setProperty(reader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaInferenceUtil.INFER_SCHEMA);
        runner.enableControllerService(reader);
        runner.setProperty(JoltTransformRecord.RECORD_READER, "reader");

        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.INHERIT_RECORD_SCHEMA);
        runner.setProperty(writer, JsonRecordSetWriter.PRETTY_PRINT_JSON, "true");
        runner.enableControllerService(writer);

        final String flattenSpec = Files.readString(Paths.get("src/test/resources/TestJoltTransformRecord/flattenSpec.json"));
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, flattenSpec);
        runner.setProperty(JoltTransformRecord.JOLT_TRANSFORM, JoltTransformStrategy.CHAINR);

        final String inputJson = Files.readString(Paths.get("src/test/resources/TestJoltTransformRecord/input.json"));
        runner.enqueue(inputJson);

        runner.run();
        runner.assertTransferCount(JoltTransformRecord.REL_SUCCESS, 1);
        runner.assertTransferCount(JoltTransformRecord.REL_ORIGINAL, 1);

        final MockFlowFile transformed = runner.getFlowFilesForRelationship(JoltTransformRecord.REL_SUCCESS).get(0);
        assertEquals(Files.readString(Paths.get("src/test/resources/TestJoltTransformRecord/flattenedOutput.json")),
                new String(transformed.toByteArray()));
    }

    private static Stream<Arguments> getChainrArguments() {
        return Stream.of(
                Arguments.of(Paths.get(CHAINR_SPEC_PATH), "has no single line comments"),
                Arguments.of(Paths.get("src/test/resources/specs/chainrSpecWithSingleLineComment.json"), "has a single line comment"));
    }

    private void generateTestData(int numRecords, final BiFunction<Integer, MockRecordParser, Void> recordGenerator) {
        if (recordGenerator == null) {
            final RecordSchema primarySchema = new SimpleRecordSchema(Arrays.asList(
                    new RecordField("value", RecordFieldType.INT.getDataType())));
            final RecordSchema seriesSchema = new SimpleRecordSchema(Arrays.asList(
                    new RecordField("value", RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.INT.getDataType()))));
            final RecordSchema qualitySchema = new SimpleRecordSchema(Arrays.asList(
                    new RecordField("value", RecordFieldType.INT.getDataType())));
            final RecordSchema ratingSchema = new SimpleRecordSchema(Arrays.asList(
                    new RecordField("primary", RecordFieldType.RECORD.getDataType()),
                    new RecordField("series", RecordFieldType.RECORD.getDataType()),
                    new RecordField("quality", RecordFieldType.RECORD.getDataType())
            ));
            parser.addSchemaField("rating", RecordFieldType.RECORD);

            for (int i = 0; i < numRecords; i++) {
                final int index = i;

                final Record primaryRecord = new MapRecord(primarySchema, new HashMap<String, Object>() {{
                    put("value", (10 * index) + 3);
                }});
                final Record seriesRecord = new MapRecord(seriesSchema, new HashMap<String, Object>() {{
                    put("value", new Integer[]{(10 * index) + 5, (10 * index) + 4});
                }});
                final Record qualityRecord = new MapRecord(qualitySchema, new HashMap<String, Object>() {{
                    put("value", 3);
                }});


                Record ratingRecord = new MapRecord(ratingSchema, new HashMap<String, Object>() {{
                    put("primary", primaryRecord);
                    put("series", seriesRecord);
                    put("quality", qualityRecord);
                }});

                parser.addRecord(ratingRecord);
            }
        } else {
            recordGenerator.apply(numRecords, parser);
        }
    }
}
