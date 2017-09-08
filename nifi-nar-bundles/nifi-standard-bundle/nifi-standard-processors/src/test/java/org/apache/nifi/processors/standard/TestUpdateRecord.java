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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;

import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

public class TestUpdateRecord {

    private TestRunner runner;
    private MockRecordParser readerService;
    private MockRecordWriter writerService;

    @Before
    public void setup() throws InitializationException {
        readerService = new MockRecordParser();
        writerService = new MockRecordWriter("header", false);

        runner = TestRunners.newTestRunner(UpdateRecord.class);
        runner.addControllerService("reader", readerService);
        runner.enableControllerService(readerService);
        runner.addControllerService("writer", writerService);
        runner.enableControllerService(writerService);

        runner.setProperty(UpdateRecord.RECORD_READER, "reader");
        runner.setProperty(UpdateRecord.RECORD_WRITER, "writer");

        readerService.addSchemaField("name", RecordFieldType.STRING);
        readerService.addSchemaField("age", RecordFieldType.INT);
    }


    @Test
    public void testLiteralReplacementValue() {
        runner.setProperty("/name", "Jane Doe");
        runner.enqueue("");

        readerService.addRecord("John Doe", 35);
        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0);
        out.assertContentEquals("header\nJane Doe,35\n");
    }

    @Test
    public void testLiteralReplacementValueExpressionLanguage() {
        runner.setProperty("/name", "${newName}");
        runner.enqueue("", Collections.singletonMap("newName", "Jane Doe"));

        readerService.addRecord("John Doe", 35);
        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0);
        out.assertContentEquals("header\nJane Doe,35\n");
    }

    @Test
    public void testRecordPathReplacementValue() {
        runner.setProperty("/name", "/age");
        runner.setProperty(UpdateRecord.REPLACEMENT_VALUE_STRATEGY, UpdateRecord.RECORD_PATH_VALUES.getValue());
        runner.enqueue("");

        readerService.addRecord("John Doe", 35);
        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0);
        out.assertContentEquals("header\n35,35\n");
    }

    @Test
    public void testInvalidRecordPathUsingExpressionLanguage() {
        runner.setProperty("/name", "${recordPath}");
        runner.setProperty(UpdateRecord.REPLACEMENT_VALUE_STRATEGY, UpdateRecord.RECORD_PATH_VALUES.getValue());
        runner.enqueue("", Collections.singletonMap("recordPath", "hello"));

        readerService.addRecord("John Doe", 35);
        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_FAILURE, 1);
    }

    @Test
    public void testReplaceWithMissingRecordPath() throws InitializationException {
        readerService = new MockRecordParser();
        readerService.addSchemaField("name", RecordFieldType.STRING);
        readerService.addSchemaField("siblings", RecordFieldType.ARRAY);
        runner.addControllerService("reader", readerService);
        runner.enableControllerService(readerService);

        runner.setProperty("/name", "/siblings[0]/name");
        runner.setProperty(UpdateRecord.REPLACEMENT_VALUE_STRATEGY, UpdateRecord.RECORD_PATH_VALUES.getValue());

        runner.enqueue("", Collections.singletonMap("recordPath", "hello"));

        readerService.addRecord("John Doe", null);
        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        final MockFlowFile mff = runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0);
        mff.assertContentEquals("header\n,\n");
    }

    @Test
    public void testRelativePath() throws InitializationException {
        readerService = new MockRecordParser();
        readerService.addSchemaField("name", RecordFieldType.STRING);
        readerService.addSchemaField("nickname", RecordFieldType.STRING);
        runner.addControllerService("reader", readerService);
        runner.enableControllerService(readerService);

        runner.setProperty("/name", "../nickname");
        runner.setProperty(UpdateRecord.REPLACEMENT_VALUE_STRATEGY, UpdateRecord.RECORD_PATH_VALUES.getValue());

        runner.enqueue("", Collections.singletonMap("recordPath", "hello"));

        readerService.addRecord("John Doe", "Johnny");
        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        final MockFlowFile mff = runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0);
        mff.assertContentEquals("header\nJohnny,Johnny\n");
    }

    @Test
    public void testChangingSchema() throws InitializationException, IOException {
        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("reader", jsonReader);

        final String inputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/schema/person-with-name-record.avsc")));
        final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/schema/person-with-name-string.avsc")));

        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_TEXT, inputSchemaText);
        runner.enableControllerService(jsonReader);

        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        runner.addControllerService("writer", jsonWriter);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(jsonWriter, "Pretty Print JSON", "true");
        runner.setProperty(jsonWriter, "Schema Write Strategy", "full-schema-attribute");
        runner.enableControllerService(jsonWriter);

        runner.enqueue(Paths.get("src/test/resources/TestUpdateRecord/input/person.json"));
        runner.setProperty("/name", "/name/first");
        runner.setProperty(UpdateRecord.REPLACEMENT_VALUE_STRATEGY, UpdateRecord.RECORD_PATH_VALUES);

        runner.run();
        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        final String expectedOutput = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/output/person-with-firstname.json")));
        runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0).assertContentEquals(expectedOutput);
    }

    @Test
    public void testAddFieldNotInInputRecord() throws InitializationException, IOException {
        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("reader", jsonReader);

        final String inputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/schema/person-with-name-record.avsc")));
        final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/schema/person-with-name-string-fields.avsc")));

        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_TEXT, inputSchemaText);
        runner.enableControllerService(jsonReader);

        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        runner.addControllerService("writer", jsonWriter);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(jsonWriter, "Pretty Print JSON", "true");
        runner.setProperty(jsonWriter, "Schema Write Strategy", "full-schema-attribute");
        runner.enableControllerService(jsonWriter);

        runner.enqueue(Paths.get("src/test/resources/TestUpdateRecord/input/person.json"));
        runner.setProperty("/firstName", "/name/first");
        runner.setProperty("/lastName", "/name/last");
        runner.setProperty(UpdateRecord.REPLACEMENT_VALUE_STRATEGY, UpdateRecord.RECORD_PATH_VALUES);

        runner.run();
        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        final String expectedOutput = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/output/person-with-firstname-lastname.json")));
        runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0).assertContentEquals(expectedOutput);
    }

    @Test
    public void testFieldValuesInEL() throws InitializationException, IOException {
        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("reader", jsonReader);

        final String inputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/schema/person-with-name-record.avsc")));
        final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/schema/person-with-name-record.avsc")));

        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_TEXT, inputSchemaText);
        runner.enableControllerService(jsonReader);

        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        runner.addControllerService("writer", jsonWriter);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(jsonWriter, "Pretty Print JSON", "true");
        runner.setProperty(jsonWriter, "Schema Write Strategy", "full-schema-attribute");
        runner.enableControllerService(jsonWriter);

        runner.enqueue(Paths.get("src/test/resources/TestUpdateRecord/input/person.json"));
        runner.setProperty("/name/last", "${field.value:toUpper()}");
        runner.setProperty(UpdateRecord.REPLACEMENT_VALUE_STRATEGY, UpdateRecord.LITERAL_VALUES);

        runner.run();
        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        final String expectedOutput = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/output/person-with-capital-lastname.json")));
        runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0).assertContentEquals(expectedOutput);
    }

    @Test
    public void testSetRootPathAbsoluteWithMultipleValues() throws InitializationException, IOException {
        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("reader", jsonReader);

        final String inputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/schema/person-with-name-record.avsc")));
        final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/schema/name-fields-only.avsc")));

        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_TEXT, inputSchemaText);
        runner.enableControllerService(jsonReader);

        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        runner.addControllerService("writer", jsonWriter);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(jsonWriter, "Pretty Print JSON", "true");
        runner.setProperty(jsonWriter, "Schema Write Strategy", "full-schema-attribute");
        runner.enableControllerService(jsonWriter);

        runner.enqueue(Paths.get("src/test/resources/TestUpdateRecord/input/person.json"));
        runner.setProperty("/", "/name/*");
        runner.setProperty(UpdateRecord.REPLACEMENT_VALUE_STRATEGY, UpdateRecord.RECORD_PATH_VALUES);

        runner.run();
        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        final String expectedOutput = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/output/name-fields-only.json")));
        runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0).assertContentEquals(expectedOutput);
    }

    @Test
    public void testSetRootPathAbsoluteWithSingleValue() throws InitializationException, IOException {
        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("reader", jsonReader);

        final String inputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/schema/person-with-name-record.avsc")));
        final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/schema/name-fields-only.avsc")));

        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_TEXT, inputSchemaText);
        runner.enableControllerService(jsonReader);

        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        runner.addControllerService("writer", jsonWriter);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(jsonWriter, "Pretty Print JSON", "true");
        runner.setProperty(jsonWriter, "Schema Write Strategy", "full-schema-attribute");
        runner.enableControllerService(jsonWriter);

        runner.enqueue(Paths.get("src/test/resources/TestUpdateRecord/input/person.json"));
        runner.setProperty("/", "/name");
        runner.setProperty(UpdateRecord.REPLACEMENT_VALUE_STRATEGY, UpdateRecord.RECORD_PATH_VALUES);

        runner.run();
        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        final String expectedOutput = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/output/name-fields-only.json")));
        runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0).assertContentEquals(expectedOutput);
    }


    @Test
    public void testSetRootPathRelativeWithMultipleValues() throws InitializationException, IOException {
        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("reader", jsonReader);

        final String inputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/schema/person-with-name-record.avsc")));
        final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/schema/name-fields-only.avsc")));

        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_TEXT, inputSchemaText);
        runner.enableControllerService(jsonReader);

        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        runner.addControllerService("writer", jsonWriter);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(jsonWriter, "Pretty Print JSON", "true");
        runner.setProperty(jsonWriter, "Schema Write Strategy", "full-schema-attribute");
        runner.enableControllerService(jsonWriter);

        runner.enqueue(Paths.get("src/test/resources/TestUpdateRecord/input/person.json"));
        runner.setProperty("/name/..", "/name/*");
        runner.setProperty(UpdateRecord.REPLACEMENT_VALUE_STRATEGY, UpdateRecord.RECORD_PATH_VALUES);

        runner.run();
        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        final String expectedOutput = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/output/name-fields-only.json")));
        runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0).assertContentEquals(expectedOutput);
    }

    @Test
    public void testSetRootPathRelativeWithSingleValue() throws InitializationException, IOException {
        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("reader", jsonReader);

        final String inputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/schema/person-with-name-record.avsc")));
        final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/schema/name-fields-only.avsc")));

        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_TEXT, inputSchemaText);
        runner.enableControllerService(jsonReader);

        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        runner.addControllerService("writer", jsonWriter);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(jsonWriter, "Pretty Print JSON", "true");
        runner.setProperty(jsonWriter, "Schema Write Strategy", "full-schema-attribute");
        runner.enableControllerService(jsonWriter);

        runner.enqueue(Paths.get("src/test/resources/TestUpdateRecord/input/person.json"));
        runner.setProperty("/name/..", "/name");
        runner.setProperty(UpdateRecord.REPLACEMENT_VALUE_STRATEGY, UpdateRecord.RECORD_PATH_VALUES);

        runner.run();
        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        final String expectedOutput = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/output/name-fields-only.json")));
        runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0).assertContentEquals(expectedOutput);
    }

    @Test
    public void testSetAbsolutePathWithAnotherRecord() throws InitializationException, IOException {
        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("reader", jsonReader);

        final String inputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/schema/person-with-name-and-mother.avsc")));
        final String outputSchemaText = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/schema/person-with-name-and-mother.avsc")));

        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_TEXT, inputSchemaText);
        runner.enableControllerService(jsonReader);

        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        runner.addControllerService("writer", jsonWriter);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_TEXT, outputSchemaText);
        runner.setProperty(jsonWriter, "Pretty Print JSON", "true");
        runner.setProperty(jsonWriter, "Schema Write Strategy", "full-schema-attribute");
        runner.enableControllerService(jsonWriter);

        runner.enqueue(Paths.get("src/test/resources/TestUpdateRecord/input/person.json"));
        runner.setProperty("/name", "/mother");
        runner.setProperty(UpdateRecord.REPLACEMENT_VALUE_STRATEGY, UpdateRecord.RECORD_PATH_VALUES);

        runner.run();
        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        final String expectedOutput = new String(Files.readAllBytes(Paths.get("src/test/resources/TestUpdateRecord/output/name-and-mother-same.json")));
        runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0).assertContentEquals(expectedOutput);
    }

}
