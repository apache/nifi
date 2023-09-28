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
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.serialization.SchemaRegistryRecordSetWriter;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class TestRenameRecordField {
    private final Path INPUT_FILES = Paths.get("src/test/resources/TestRenameRecordField/input");
    private final Path OUTPUT_FILES = Paths.get("src/test/resources/TestRenameRecordField/output");
    private TestRunner runner;


    @BeforeEach
    public void setup() throws InitializationException {
        runner = TestRunners.newTestRunner(RenameRecordField.class);

        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        runner.addControllerService("writer", jsonWriter);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.INHERIT_RECORD_SCHEMA);
        runner.setProperty(jsonWriter, JsonRecordSetWriter.SUPPRESS_NULLS, JsonRecordSetWriter.NEVER_SUPPRESS);
        runner.setProperty(jsonWriter, JsonRecordSetWriter.PRETTY_PRINT_JSON, "true");
        runner.setProperty(jsonWriter, jsonWriter.getSchemaWriteStrategyDescriptor(), SchemaRegistryRecordSetWriter.AVRO_SCHEMA_ATTRIBUTE);
        runner.enableControllerService(jsonWriter);
        runner.setProperty(AbstractRecordProcessor.RECORD_WRITER, "writer");

        final JsonTreeReader reader = new JsonTreeReader();
        runner.addControllerService("reader", reader);
        runner.enableControllerService(reader);
        runner.setProperty(AbstractRecordProcessor.RECORD_READER, "reader");
    }

    @Test
    public void testRenameFieldStaticValue() throws IOException {
        runner.setProperty("/application", "favoriteApplication");
        runner.enqueue(INPUT_FILES.resolve("simple-person.json"));
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractRecordProcessor.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(AbstractRecordProcessor.REL_SUCCESS).get(0);
        assertContentsEqual(out, OUTPUT_FILES.resolve("testRenameFieldStaticValue.json"));
    }

    @Test
    public void testRenameFieldUsingAttribute() throws IOException {
        runner.setProperty("/application", "${desiredKey}");
        runner.enqueue(INPUT_FILES.resolve("simple-person.json"), Collections.singletonMap("desiredKey", "favorite"));
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractRecordProcessor.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(AbstractRecordProcessor.REL_SUCCESS).get(0);
        assertContentsEqual(out, OUTPUT_FILES.resolve("testRenameFieldUsingAttribute.json"));
    }

    @Test
    public void testRenameMultipleFields() throws IOException {
        runner.setProperty("/application", "app");
        runner.setProperty("/name", "${nameKey}");
        runner.enqueue(INPUT_FILES.resolve("simple-person.json"), Collections.singletonMap("nameKey", "full_name"));
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractRecordProcessor.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(AbstractRecordProcessor.REL_SUCCESS).get(0);
        assertContentsEqual(out, OUTPUT_FILES.resolve("testRenameMultipleFields.json"));

        final String outputSchemaText = out.getAttribute("avro.schema");
        final Schema outputSchema = new Schema.Parser().parse(outputSchemaText);
        assertEquals(3, outputSchema.getFields().size());
        assertNull(outputSchema.getField("application"));
        assertNotNull(outputSchema.getField("app"));
        assertNull(outputSchema.getField("name"));
        assertNotNull(outputSchema.getField("full_name"));
    }

    @Test
    public void testRenameArray() throws IOException {
        runner.setProperty("/addresses", "addrs");
        runner.enqueue(INPUT_FILES.resolve("complex-person.json"));
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractRecordProcessor.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(AbstractRecordProcessor.REL_SUCCESS).get(0);
        assertContentsEqual(out, OUTPUT_FILES.resolve("testRenameArray.json"));
    }


    @Test
    public void testNestedPath() throws IOException {
        runner.setProperty("/addresses[*]/street", "streetAddress");
        runner.enqueue(INPUT_FILES.resolve("complex-person.json"));
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractRecordProcessor.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(AbstractRecordProcessor.REL_SUCCESS).get(0);
        assertContentsEqual(out, OUTPUT_FILES.resolve("testNestedPath.json"));
    }

    @Test
    public void testNamingConflict() throws IOException {
        runner.setProperty("/application", "name");
        runner.enqueue(INPUT_FILES.resolve("simple-person.json"));
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractRecordProcessor.REL_FAILURE, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(AbstractRecordProcessor.REL_FAILURE).get(0);
        // Output should be unchanged.
        assertContentsEqual(out, INPUT_FILES.resolve("simple-person.json"));
    }

    @Test
    public void testReferencingFieldName() throws IOException {
        runner.setProperty("/*", "UPDATED_${field.name}");
        runner.enqueue(INPUT_FILES.resolve("simple-person.json"));
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractRecordProcessor.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(AbstractRecordProcessor.REL_SUCCESS).get(0);
        assertContentsEqual(out, OUTPUT_FILES.resolve("testReferencingFieldName.json"));
    }

    @Test
    public void testRecursivelyReferencingAllFields() throws IOException {
        runner.setProperty("//*", "${field.name:toUpper()}");
        runner.enqueue(INPUT_FILES.resolve("complex-person.json"));
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractRecordProcessor.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(AbstractRecordProcessor.REL_SUCCESS).get(0);
        assertContentsEqual(out, OUTPUT_FILES.resolve("testRecursivelyReferencingAllFields.json"));
    }

    @Test
    public void testRecursivelyReferencingFieldName() throws IOException {
        runner.setProperty("//name", "${field.name:toUpper()}");
        runner.enqueue(INPUT_FILES.resolve("complex-person.json"));
        runner.run();

        runner.assertAllFlowFilesTransferred(AbstractRecordProcessor.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(AbstractRecordProcessor.REL_SUCCESS).get(0);
        assertContentsEqual(out, OUTPUT_FILES.resolve("testRecursivelyReferencingFieldName.json"));
    }

    private void assertContentsEqual(final MockFlowFile flowFile, final Path expectedContent) throws IOException {
        final String flowFileContent = flowFile.getContent();
        final String fileContent = new String(Files.readAllBytes(expectedContent));
        assertEquals(flowFileContent.replace("\r", ""), fileContent.replace("\r", ""));
    }
}
