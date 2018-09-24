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
package org.apache.nifi.processors.jolt.record;

import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
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
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestJoltTransformRecord {

    private TestRunner runner;
    private JoltTransformRecord processor;
    private MockRecordParser parser;
    private JsonRecordSetWriter writer;

    @Before
    public void setup() throws Exception {
        processor = new JoltTransformRecord();
        runner = TestRunners.newTestRunner(processor);
        parser = new MockRecordParser();
        try {
            runner.addControllerService("parser", parser);
        } catch (InitializationException e) {
            throw new IOException(e);
        }
        runner.enableControllerService(parser);
        runner.setProperty(JoltTransformRecord.RECORD_READER, "parser");
        writer = new JsonRecordSetWriter();
        try {
            runner.addControllerService("writer", writer);
        } catch (InitializationException e) {
            throw new IOException(e);
        }
        runner.setProperty(writer, "Schema Write Strategy", "full-schema-attribute");
        runner.setProperty(JoltTransformRecord.RECORD_WRITER, "writer");
        // Each test must set the Schema Access strategy and Schema, and enable the writer CS
    }

    @Test
    public void testRelationshipsCreated() throws IOException {
        generateTestData(1, null);
        final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/chainrOutputSchema.avsc")));
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(writer, "Pretty Print JSON", "true");
        runner.enableControllerService(writer);
        final String spec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/chainrSpec.json")));
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, spec);
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
        final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/shiftrOutputSchema.avsc")));
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(writer, "Pretty Print JSON", "true");
        runner.enableControllerService(writer);
        final String spec = "[{}]";
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, spec);
        runner.assertNotValid();
    }

    @Test
    public void testIncorrectJOLTSpec() throws IOException {
        final String chainrSpec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/chainrSpec.json")));
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, chainrSpec);
        runner.setProperty(JoltTransformRecord.JOLT_TRANSFORM, JoltTransformRecord.SHIFTR);
        runner.assertNotValid();
    }

    @Test
    public void testSpecIsNotSet() {
        runner.setProperty(JoltTransformRecord.JOLT_TRANSFORM, JoltTransformRecord.SHIFTR);
        runner.assertNotValid();
    }

    @Test
    public void testSpecIsEmpty() throws IOException {
        generateTestData(1, null);
        final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/shiftrOutputSchema.avsc")));
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(writer, "Pretty Print JSON", "true");
        runner.enableControllerService(writer);
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, StringUtils.EMPTY);
        runner.setProperty(JoltTransformRecord.JOLT_TRANSFORM, JoltTransformRecord.SHIFTR);
        runner.assertNotValid();
    }

    @Test
    public void testSpecNotRequired() throws IOException {
        generateTestData(1, null);
        final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/shiftrOutputSchema.avsc")));
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(writer, "Pretty Print JSON", "true");
        runner.enableControllerService(writer);
        runner.setProperty(JoltTransformRecord.JOLT_TRANSFORM, JoltTransformRecord.SORTR);
        runner.assertValid();
    }

    @Test
    public void testNoFlowFileContent() throws IOException {
        generateTestData(1, null);
        final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/chainrOutputSchema.avsc")));
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(writer, "Pretty Print JSON", "true");
        runner.enableControllerService(writer);
        final String spec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/chainrSpec.json")));
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, spec);
        runner.run();
        runner.assertQueueEmpty();
        runner.assertTransferCount(JoltTransformRecord.REL_FAILURE, 0);
        runner.assertTransferCount(JoltTransformRecord.REL_SUCCESS, 0);
    }

    @Test
    public void testInvalidFlowFileContent() throws IOException {
        generateTestData(1, null);
        final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/chainrOutputSchema.avsc")));
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(writer, "Pretty Print JSON", "true");
        runner.enableControllerService(writer);
        final String spec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/chainrSpec.json")));
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, spec);
        parser.failAfter(0);
        runner.enqueue("invalid json");
        runner.run();
        runner.assertAllFlowFilesTransferred(JoltTransformRecord.REL_FAILURE);
    }

    @Test
    public void testCustomTransformationWithNoModule() throws IOException {
        generateTestData(1, null);
        final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/chainrOutputSchema.avsc")));
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(writer, "Pretty Print JSON", "true");
        runner.enableControllerService(writer);
        final String spec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/customChainrSpec.json")));
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, spec);
        runner.setProperty(JoltTransformRecord.CUSTOM_CLASS, "TestCustomJoltTransform");
        runner.setProperty(JoltTransformRecord.JOLT_TRANSFORM, JoltTransformRecord.CUSTOMR);
        runner.assertNotValid();
    }

    @Test
    public void testCustomTransformationWithMissingClassName() throws IOException {
        generateTestData(1, null);
        final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/chainrOutputSchema.avsc")));
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(writer, "Pretty Print JSON", "true");
        runner.enableControllerService(writer);
        final String customJarPath = "src/test/resources/TestJoltTransformRecord/TestCustomJoltTransform.jar";
        final String spec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/chainrSpec.json")));
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, spec);
        runner.setProperty(JoltTransformRecord.MODULES, customJarPath);
        runner.setProperty(JoltTransformRecord.JOLT_TRANSFORM, JoltTransformRecord.CUSTOMR);
        runner.enqueue(new byte[0]);
        runner.assertNotValid();
    }

    @Test
    public void testCustomTransformationWithInvalidClassPath() throws IOException {
        final String customJarPath = "src/test/resources/TestJoltTransformRecord/FakeCustomJar.jar";
        final String spec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/chainrSpec.json")));
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, spec);
        runner.setProperty(JoltTransformRecord.CUSTOM_CLASS, "TestCustomJoltTransform");
        runner.setProperty(JoltTransformRecord.MODULES, customJarPath);
        runner.setProperty(JoltTransformRecord.JOLT_TRANSFORM, JoltTransformRecord.CUSTOMR);
        runner.enqueue(new byte[0]);
        runner.assertNotValid();
    }

    @Test
    public void testCustomTransformationWithInvalidClassName() throws IOException {
        final String customJarPath = "src/test/resources/TestJoltTransformRecord/TestCustomJoltTransform.jar";
        final String spec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/chainrSpec.json")));
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, spec);
        runner.setProperty(JoltTransformRecord.CUSTOM_CLASS, "FakeCustomJoltTransform");
        runner.setProperty(JoltTransformRecord.MODULES, customJarPath);
        runner.setProperty(JoltTransformRecord.JOLT_TRANSFORM, JoltTransformRecord.CUSTOMR);
        runner.enqueue(new byte[0]);
        runner.assertNotValid();
    }

    @Test
    public void testTransformInputWithChainr() throws IOException {
        generateTestData(1, null);
        final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/chainrOutputSchema.avsc")));
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(writer, "Pretty Print JSON", "true");
        runner.enableControllerService(writer);
        final String spec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/chainrSpec.json")));
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, spec);
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertTransferCount(JoltTransformRecord.REL_SUCCESS, 1);
runner.assertTransferCount(JoltTransformRecord.REL_ORIGINAL, 1);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(JoltTransformRecord.REL_SUCCESS).get(0);
        transformed.assertAttributeExists(CoreAttributes.MIME_TYPE.key());
        transformed.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/json");
        assertEquals(new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/chainrOutput.json"))),
                new String(transformed.toByteArray()));
    }

    @Test
    public void testTransformInputWithShiftr() throws IOException {
        generateTestData(1, null);
        final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/shiftrOutputSchema.avsc")));
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(writer, "Pretty Print JSON", "true");
        runner.enableControllerService(writer);
        final String spec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/shiftrSpec.json")));
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, spec);
        runner.setProperty(JoltTransformRecord.JOLT_TRANSFORM, JoltTransformRecord.SHIFTR);
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertTransferCount(JoltTransformRecord.REL_SUCCESS, 1);
runner.assertTransferCount(JoltTransformRecord.REL_ORIGINAL, 1);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(JoltTransformRecord.REL_SUCCESS).get(0);
        transformed.assertAttributeExists(CoreAttributes.MIME_TYPE.key());
        transformed.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/json");
        assertEquals(new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/shiftrOutput.json"))),
                new String(transformed.toByteArray()));

    }

    @Test
    public void testTransformInputWithDefaultr() throws IOException {
        generateTestData(1, null);
        final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/defaultrOutputSchema.avsc")));
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(writer, "Pretty Print JSON", "true");
        runner.enableControllerService(writer);
        final String spec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/defaultrSpec.json")));
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, spec);
        runner.setProperty(JoltTransformRecord.JOLT_TRANSFORM, JoltTransformRecord.DEFAULTR);
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertTransferCount(JoltTransformRecord.REL_SUCCESS, 1);
runner.assertTransferCount(JoltTransformRecord.REL_ORIGINAL, 1);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(JoltTransformRecord.REL_SUCCESS).get(0);
        assertEquals(new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/defaultrOutput.json"))),
                new String(transformed.toByteArray()));
    }

    @Test
    public void testTransformInputWithRemovr() throws IOException {
        generateTestData(1, null);
        final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/removrOutputSchema.avsc")));
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(writer, "Pretty Print JSON", "true");
        runner.enableControllerService(writer);
        final String spec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/removrSpec.json")));
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, spec);
        runner.setProperty(JoltTransformRecord.JOLT_TRANSFORM, JoltTransformRecord.REMOVR);
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertTransferCount(JoltTransformRecord.REL_SUCCESS, 1);
runner.assertTransferCount(JoltTransformRecord.REL_ORIGINAL, 1);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(JoltTransformRecord.REL_SUCCESS).get(0);
        assertEquals(new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/removrOutput.json"))),
                new String(transformed.toByteArray()));

    }

    @Test
    public void testTransformInputWithCardinality() throws IOException {
        generateTestData(1, null);
        final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/cardrOutputSchema.avsc")));
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(writer, "Pretty Print JSON", "true");
        runner.enableControllerService(writer);
        final String spec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/cardrSpec.json")));
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, spec);
        runner.setProperty(JoltTransformRecord.JOLT_TRANSFORM, JoltTransformRecord.CARDINALITY);
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertTransferCount(JoltTransformRecord.REL_SUCCESS, 1);
runner.assertTransferCount(JoltTransformRecord.REL_ORIGINAL, 1);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(JoltTransformRecord.REL_SUCCESS).get(0);
        assertEquals(new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/cardrOutput.json"))),
                new String(transformed.toByteArray()));

    }

    @Test
    public void testTransformInputWithSortr() throws IOException {
        generateTestData(1, null);
        final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/sortrOutputSchema.avsc")));
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(writer, "Pretty Print JSON", "true");
        runner.enableControllerService(writer);
        runner.setProperty(JoltTransformRecord.JOLT_TRANSFORM, JoltTransformRecord.SORTR);
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertTransferCount(JoltTransformRecord.REL_SUCCESS, 1);
runner.assertTransferCount(JoltTransformRecord.REL_ORIGINAL, 1);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(JoltTransformRecord.REL_SUCCESS).get(0);
        transformed.assertAttributeExists(CoreAttributes.MIME_TYPE.key());
        transformed.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/json");
        assertEquals(new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/sortrOutput.json"))),
                new String(transformed.toByteArray()));
    }

    @Test
    public void testTransformInputWithDefaultrExpressionLanguage() throws IOException {
        generateTestData(1, null);
        final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/defaultrELOutputSchema.avsc")));
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(writer, "Pretty Print JSON", "true");
        runner.enableControllerService(writer);
        final String spec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/defaultrELSpec.json")));
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, spec);
        runner.setProperty(JoltTransformRecord.JOLT_TRANSFORM, JoltTransformRecord.DEFAULTR);
        runner.setVariable("quota", "5");
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertTransferCount(JoltTransformRecord.REL_SUCCESS, 1);
runner.assertTransferCount(JoltTransformRecord.REL_ORIGINAL, 1);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(JoltTransformRecord.REL_SUCCESS).get(0);
        assertEquals(new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/defaultrELOutput.json"))),
                new String(transformed.toByteArray()));

    }

    @Test
    public void testTransformInputWithModifierDefault() throws IOException {
        generateTestData(1, null);
        // Input schema = output schema, just modifying values
        final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/inputSchema.avsc")));
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(writer, "Pretty Print JSON", "true");
        runner.enableControllerService(writer);
        final String spec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/modifierDefaultSpec.json")));
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, spec);
        runner.setProperty(JoltTransformRecord.JOLT_TRANSFORM, JoltTransformRecord.MODIFIER_DEFAULTR);
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertTransferCount(JoltTransformRecord.REL_SUCCESS, 1);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(JoltTransformRecord.REL_SUCCESS).get(0);
        assertEquals(new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/modifierDefaultOutput.json"))),
                new String(transformed.toByteArray()));
    }

    @Test
    public void testTransformInputWithModifierDefine() throws IOException {
        generateTestData(1, null);
        final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/modifierDefineOutputSchema.avsc")));
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(writer, "Pretty Print JSON", "true");
        runner.enableControllerService(writer);
        final String spec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/modifierDefineSpec.json")));
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, spec);
        runner.setProperty(JoltTransformRecord.JOLT_TRANSFORM, JoltTransformRecord.MODIFIER_DEFAULTR);
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertTransferCount(JoltTransformRecord.REL_SUCCESS, 1);
runner.assertTransferCount(JoltTransformRecord.REL_ORIGINAL, 1);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(JoltTransformRecord.REL_SUCCESS).get(0);
        assertEquals(new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/modifierDefineOutput.json"))),
                new String(transformed.toByteArray()));
    }

    @Test
    public void testTransformInputWithModifierOverwrite() throws IOException {
        generateTestData(1, null);
        final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/modifierOverwriteOutputSchema.avsc")));
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(writer, "Pretty Print JSON", "true");
        runner.enableControllerService(writer);
        final String spec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/modifierOverwriteSpec.json")));
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, spec);
        runner.setProperty(JoltTransformRecord.JOLT_TRANSFORM, JoltTransformRecord.MODIFIER_DEFAULTR);
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertTransferCount(JoltTransformRecord.REL_SUCCESS, 1);
runner.assertTransferCount(JoltTransformRecord.REL_ORIGINAL, 1);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(JoltTransformRecord.REL_SUCCESS).get(0);
        assertEquals(new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/modifierOverwriteOutput.json"))),
                new String(transformed.toByteArray()));
    }

    @Test
    public void testTransformInputWithSortrPopulatedSpec() throws IOException {
        generateTestData(1, null);
        final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/sortrOutputSchema.avsc")));
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(writer, "Pretty Print JSON", "true");
        runner.enableControllerService(writer);
        runner.setProperty(JoltTransformRecord.JOLT_TRANSFORM, JoltTransformRecord.SORTR);
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, "abcd");
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertTransferCount(JoltTransformRecord.REL_SUCCESS, 1);
runner.assertTransferCount(JoltTransformRecord.REL_ORIGINAL, 1);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(JoltTransformRecord.REL_SUCCESS).get(0);
        transformed.assertAttributeExists(CoreAttributes.MIME_TYPE.key());
        transformed.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/json");
        assertEquals(new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/sortrOutput.json"))),
                new String(transformed.toByteArray()));
    }

    @Test
    public void testTransformInputCustomTransformationIgnored() throws IOException {
        generateTestData(1, null);
        final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/defaultrOutputSchema.avsc")));
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(writer, "Pretty Print JSON", "true");
        runner.enableControllerService(writer);
        final String customJarPath = "src/test/resources/TestJoltTransformRecord/TestCustomJoltTransform.jar";
        final String spec = new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/defaultrSpec.json")));
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, spec);
        runner.setProperty(JoltTransformRecord.CUSTOM_CLASS, "TestCustomJoltTransform");
        runner.setProperty(JoltTransformRecord.MODULES, customJarPath);
        runner.setProperty(JoltTransformRecord.JOLT_TRANSFORM, JoltTransformRecord.DEFAULTR);
        runner.enqueue(new byte[0]);
        runner.run();
        runner.assertTransferCount(JoltTransformRecord.REL_SUCCESS, 1);
runner.assertTransferCount(JoltTransformRecord.REL_ORIGINAL, 1);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(JoltTransformRecord.REL_SUCCESS).get(0);
        transformed.assertAttributeExists(CoreAttributes.MIME_TYPE.key());
        transformed.assertAttributeEquals(CoreAttributes.MIME_TYPE.key(), "application/json");
        assertEquals(new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/defaultrOutput.json"))),
                new String(transformed.toByteArray()));
    }

    @Test
    public void testJoltSpecEL() throws IOException {
        generateTestData(1, null);
        final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/defaultrOutputSchema.avsc")));
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(writer, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(writer, "Pretty Print JSON", "true");
        runner.enableControllerService(writer);
        final String spec = "${joltSpec}";
        runner.setProperty(JoltTransformRecord.JOLT_SPEC, spec);
        runner.setProperty(JoltTransformRecord.JOLT_TRANSFORM, JoltTransformRecord.DEFAULTR);
        final Map<String, String> attributes = Collections.singletonMap("joltSpec",
                "{\"RatingRange\":5,\"rating\":{\"*\":{\"MaxLabel\":\"High\",\"MinLabel\":\"Low\",\"DisplayType\":\"NORMAL\"}}}");
        runner.enqueue(new byte[0], attributes);
        runner.run();
        runner.assertTransferCount(JoltTransformRecord.REL_SUCCESS, 1);
runner.assertTransferCount(JoltTransformRecord.REL_ORIGINAL, 1);
        final MockFlowFile transformed = runner.getFlowFilesForRelationship(JoltTransformRecord.REL_SUCCESS).get(0);
        assertEquals(new String(Files.readAllBytes(Paths.get("src/test/resources/TestJoltTransformRecord/defaultrOutput.json"))),
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

                Record primaryRecord = new MapRecord(primarySchema, new HashMap<String, Object>() {{
                    put("value", (10 * index) + 3);
                }});
                Record seriesRecord = new MapRecord(seriesSchema, new HashMap<String, Object>() {{
                    put("value", new Integer[]{(10 * index) + 5, (10 * index) + 4});
                }});
                Record qualityRecord = new MapRecord(qualitySchema, new HashMap<String, Object>() {{
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
