/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.processors.standard;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.schema.inference.SchemaInferenceUtil;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class TestRemoveRecordField {

    private TestRunner runner;

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Before
    public void setup() throws InitializationException {
        runner = TestRunners.newTestRunner(RemoveRecordField.class);

        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        runner.addControllerService("writer", jsonWriter);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.INHERIT_RECORD_SCHEMA);
        runner.setProperty(jsonWriter, "suppress-nulls", "never-suppress");
        runner.setProperty(jsonWriter, "Pretty Print JSON", "true");
        runner.setProperty(jsonWriter, "Schema Write Strategy", "full-schema-attribute");
        runner.enableControllerService(jsonWriter);
        runner.setProperty(AbstractRecordProcessor.RECORD_WRITER, "writer");
    }

    @Test
    public void testNotRootPath() throws InitializationException, IOException {
        setUpJsonReader(null);

        runner.setProperty("root_path", "/");
        runner.assertNotValid();
    }

    @Test
    public void testNotRootPathEL() throws InitializationException, IOException {
        // must have at least 1 dynamic property to be valid
        runner.assertNotValid();

        setUpJsonReader(null);

        runner.setProperty("root_path", "${remove.path}");
        runner.assertValid();

        runner.enqueue(
                "{}".getBytes(StandardCharsets.UTF_8),
                Collections.singletonMap("remove.path", "/")
        );

        runner.run();
        runner.assertAllFlowFilesTransferred(AbstractRecordProcessor.REL_FAILURE, 1);
        runner.getFlowFilesForRelationship(AbstractRecordProcessor.REL_FAILURE).get(0)
                .assertAttributeEquals("record.error.message", "org.apache.nifi.processor.exception.ProcessException Thrown");
    }

    @Test
    public void testRemoveSimpleFieldWithSchemaInference() throws InitializationException, IOException {
        final String inputContent = "src/test/resources/TestRemoveRecordField/input/complex-person.json";
        final String outputContent = "src/test/resources/TestRemoveRecordField/output/complex-person-no-dateOfBirth.json";
        final String fieldToRemove = "/dateOfBirth";

        executeRemovalTest(null, inputContent, null, outputContent, fieldToRemove);
    }

    @Test
    public void testRemoveSimpleFieldThatIsMissingFromOneRecord() throws InitializationException, IOException {
        final String inputSchema = "src/test/resources/TestRemoveRecordField/input_schema/complex-person.avsc";
        final String inputContent = "src/test/resources/TestRemoveRecordField/input/complex-person.json";
        final String outputSchema = "src/test/resources/TestRemoveRecordField/output_schema/complex-person-no-dateOfBirth.avsc";
        final String outputContent = "src/test/resources/TestRemoveRecordField/output/complex-person-no-dateOfBirth.json";
        final String fieldToRemove = "/dateOfBirth";

        executeRemovalTest(inputSchema, inputContent, outputSchema, outputContent, fieldToRemove);
    }

    @Test
    public void testRemoveComplexFieldThatIsMissingFromOneRecord() throws InitializationException, IOException {
        final String inputSchema = "src/test/resources/TestRemoveRecordField/input_schema/complex-person.avsc";
        final String inputFlowFile = "src/test/resources/TestRemoveRecordField/input/complex-person.json";
        final String outputSchema = "src/test/resources/TestRemoveRecordField/output_schema/complex-person-no-workAddress.avsc";
        final String outputFlowFile = "src/test/resources/TestRemoveRecordField/output/complex-person-no-workAddress.json";
        final String fieldToRemove = "/workAddress";

        executeRemovalTest(inputSchema, inputFlowFile, outputSchema, outputFlowFile, fieldToRemove);
    }

    @Test
    public void testRemoveFieldFrom3LevelDeepStructure() throws InitializationException, IOException {
        final String inputSchema = "src/test/resources/TestRemoveRecordField/input_schema/complex-person.avsc";
        final String inputFlowFile = "src/test/resources/TestRemoveRecordField/input/complex-person.json";
        final String outputSchema = "src/test/resources/TestRemoveRecordField/output_schema/complex-person-no-workAddress-building-letter.avsc";
        final String outputFlowFile = "src/test/resources/TestRemoveRecordField/output/complex-person-no-workAddress-building-letter.json";
        final String fieldToRemove = "/workAddress/building/letter";

        executeRemovalTest(inputSchema, inputFlowFile, outputSchema, outputFlowFile, fieldToRemove);
    }

    @Test
    public void testRemoveNestedFieldFromDeepStructureWithRelativePath() throws InitializationException, IOException {
        final String inputSchema = "src/test/resources/TestRemoveRecordField/input_schema/complex-person.avsc";
        final String inputFlowFile = "src/test/resources/TestRemoveRecordField/input/complex-person.json";
        final String outputSchema = "src/test/resources/TestRemoveRecordField/output_schema/complex-person-no-workAddress-building-letter.avsc";
        final String outputFlowFile = "src/test/resources/TestRemoveRecordField/output/complex-person-no-workAddress-building-letter.json";
        final String fieldToRemove = "/workAddress//letter";

        executeRemovalTest(inputSchema, inputFlowFile, outputSchema, outputFlowFile, fieldToRemove);
    }

    @Test
    public void testRemoveFieldFromNonExistentParent() throws InitializationException, IOException {
        final String inputSchema = "src/test/resources/TestRemoveRecordField/input_schema/complex-person.avsc";
        final String inputFlowFile = "src/test/resources/TestRemoveRecordField/input/complex-person.json";
        final String outputSchema = "src/test/resources/TestRemoveRecordField/output_schema/complex-person.avsc";
        final String outputFlowFile = "src/test/resources/TestRemoveRecordField/output/complex-person.json";
        final String fieldToRemove = "/workAddress/nonExistent/letter";

        executeRemovalTest(inputSchema, inputFlowFile, outputSchema, outputFlowFile, fieldToRemove);
    }

    @Test
    public void testRemoveNonExistentField() throws InitializationException, IOException {
        final String inputSchema = "src/test/resources/TestRemoveRecordField/input_schema/complex-person.avsc";
        final String inputFlowFile = "src/test/resources/TestRemoveRecordField/input/complex-person.json";
        final String outputSchema = "src/test/resources/TestRemoveRecordField/output_schema/complex-person.avsc";
        final String outputFlowFile = "src/test/resources/TestRemoveRecordField/output/complex-person.json";
        final String fieldToRemove = "/nonExistentField";

        executeRemovalTest(inputSchema, inputFlowFile, outputSchema, outputFlowFile, fieldToRemove);
    }

    @Test
    public void testRemoveMultipleFields() throws InitializationException, IOException {
        final String inputSchema = "src/test/resources/TestRemoveRecordField/input_schema/complex-person.avsc";
        final String inputFlowFile = "src/test/resources/TestRemoveRecordField/input/complex-person.json";
        final String outputSchema = "src/test/resources/TestRemoveRecordField/output_schema/complex-person-multiple-fields-removed.avsc";
        final String outputFlowFile = "src/test/resources/TestRemoveRecordField/output/complex-person-multiple-fields-removed.json";
        final String fieldToRemove1 = "/name";
        final String fieldToRemove2 = "/dateOfBirth";
        final String fieldToRemove3 = "/workAddress/building";

        executeRemovalTest(inputSchema, inputFlowFile, outputSchema, outputFlowFile, fieldToRemove1, fieldToRemove2, fieldToRemove3);
    }

    @Test
    public void testRemoveEntireArrayFieldWithSchemaInference() throws InitializationException, IOException {
        final String inputFlowFile = "src/test/resources/TestRemoveRecordField/input/address-array.json";
        final String outputFlowFile = "src/test/resources/TestRemoveRecordField/output/address-array-removed.json";
        final String fieldToRemove = "/addresses";

        executeRemovalTest(null, inputFlowFile, null, outputFlowFile, fieldToRemove);
    }

    @Test
    public void testRemoveEntireArrayField() throws InitializationException, IOException {
        final String inputSchema = "src/test/resources/TestRemoveRecordField/input_schema/address-array.avsc";
        final String inputFlowFile = "src/test/resources/TestRemoveRecordField/input/address-array.json";
        final String outputSchema = "src/test/resources/TestRemoveRecordField/output_schema/address-array-field-removed.avsc";
        final String outputFlowFile = "src/test/resources/TestRemoveRecordField/output/address-array-removed.json";
        final String fieldToRemove = "/addresses";

        executeRemovalTest(inputSchema, inputFlowFile, outputSchema, outputFlowFile, fieldToRemove);
    }

    @Test
    public void testRemoveOneElementFromArray() throws InitializationException, IOException {
        final String inputSchema = "src/test/resources/TestRemoveRecordField/input_schema/address-array.avsc";
        final String inputFlowFile = "src/test/resources/TestRemoveRecordField/input/address-array.json";
        final String outputSchema = "src/test/resources/TestRemoveRecordField/output_schema/address-array.avsc";
        final String outputFlowFile = "src/test/resources/TestRemoveRecordField/output/address-array-one-element-removed.json";
        final String fieldToRemove = "/addresses[1]";

        executeRemovalTest(inputSchema, inputFlowFile, outputSchema, outputFlowFile, fieldToRemove);
    }

    @Test
    public void testRemoveAllElementsFromArrayAsterisk() throws InitializationException, IOException {
        final String inputSchema = "src/test/resources/TestRemoveRecordField/input_schema/address-array.avsc";
        final String inputFlowFile = "src/test/resources/TestRemoveRecordField/input/address-array.json";
        final String outputSchema = "src/test/resources/TestRemoveRecordField/output_schema/address-array.avsc";
        final String outputFlowFile = "src/test/resources/TestRemoveRecordField/output/address-array-empty.json";
        final String fieldToRemove = "/addresses[*]";

        executeRemovalTest(inputSchema, inputFlowFile, outputSchema, outputFlowFile, fieldToRemove);
    }

    @Test
    public void testRemoveAllElementsFromArrayIndexRange() throws InitializationException, IOException {
        final String inputSchema = "src/test/resources/TestRemoveRecordField/input_schema/address-array.avsc";
        final String inputFlowFile = "src/test/resources/TestRemoveRecordField/input/address-array.json";
        final String outputSchema = "src/test/resources/TestRemoveRecordField/output_schema/address-array.avsc";
        final String outputFlowFile = "src/test/resources/TestRemoveRecordField/output/address-array-empty.json";
        final String fieldToRemove = "/addresses[0..-1]";

        executeRemovalTest(inputSchema, inputFlowFile, outputSchema, outputFlowFile, fieldToRemove);
    }

    @Test
    public void testRemoveFieldFromOneArrayElement() throws InitializationException, IOException {
        // The schema must not be modified.
        final String inputSchema = "src/test/resources/TestRemoveRecordField/input_schema/address-array.avsc";
        final String inputFlowFile = "src/test/resources/TestRemoveRecordField/input/address-array.json";
        final String outputSchema = "src/test/resources/TestRemoveRecordField/output_schema/address-array.avsc";
        final String outputFlowFile = "src/test/resources/TestRemoveRecordField/output/address-array-zip-removed-from-one-element.json";
        final String fieldToRemove = "/addresses[1]/zip";

        executeRemovalTest(inputSchema, inputFlowFile, outputSchema, outputFlowFile, fieldToRemove);
    }

    @Test
    public void testRemoveFieldFromAllArrayElements() throws InitializationException, IOException {
        // The schema must be modified.
        final String inputSchema = "src/test/resources/TestRemoveRecordField/input_schema/address-array.avsc";
        final String inputFlowFile = "src/test/resources/TestRemoveRecordField/input/address-array.json";
        final String outputSchema = "src/test/resources/TestRemoveRecordField/output_schema/address-array-zip-removed.avsc";
        final String outputFlowFile = "src/test/resources/TestRemoveRecordField/output/address-array-zip-removed-from-all-elements.json";
        final String fieldToRemove = "/addresses[*]/zip";

        executeRemovalTest(inputSchema, inputFlowFile, outputSchema, outputFlowFile, fieldToRemove);
    }

    @Test
    public void testMapRemoveOneItem() throws InitializationException, IOException {
        // The schema must not be modified.
        final String inputSchema = "src/test/resources/TestRemoveRecordField/input_schema/bookshelf-map.avsc";
        final String inputFlowFile = "src/test/resources/TestRemoveRecordField/input/bookshelf-map.json";
        final String outputSchema = "src/test/resources/TestRemoveRecordField/output_schema/bookshelf-map.avsc";
        final String outputFlowFile = "src/test/resources/TestRemoveRecordField/output/bookshelf-map-one-item-removed.json";
        final String fieldToRemove = "/books['ISBN_2222222222222']";

        executeRemovalTest(inputSchema, inputFlowFile, outputSchema, outputFlowFile, fieldToRemove);
    }

    @Test
    public void testMapRemoveFieldFromOneItem() throws InitializationException, IOException {
        // The schema must not be modified.
        final String inputSchema = "src/test/resources/TestRemoveRecordField/input_schema/bookshelf-map.avsc";
        final String inputFlowFile = "src/test/resources/TestRemoveRecordField/input/bookshelf-map.json";
        final String outputSchema = "src/test/resources/TestRemoveRecordField/output_schema/bookshelf-map.avsc";
        final String outputFlowFile = "src/test/resources/TestRemoveRecordField/output/bookshelf-map-field-removed-from-one-element.json";
        final String fieldToRemove = "/books['ISBN_2222222222222']/author";

        executeRemovalTest(inputSchema, inputFlowFile, outputSchema, outputFlowFile, fieldToRemove);
    }

    @Test
    public void testMapRemoveFieldFromAllItems() throws InitializationException, IOException {
        // The schema must be modified.
        final String inputSchema = "src/test/resources/TestRemoveRecordField/input_schema/bookshelf-map.avsc";
        final String inputFlowFile = "src/test/resources/TestRemoveRecordField/input/bookshelf-map.json";
        final String outputSchema = "src/test/resources/TestRemoveRecordField/output_schema/bookshelf-map-author-field-removed.avsc";
        final String outputFlowFile = "src/test/resources/TestRemoveRecordField/output/bookshelf-map-field-removed-from-all-elements.json";
        final String fieldToRemove = "/books[*]/author";

        executeRemovalTest(inputSchema, inputFlowFile, outputSchema, outputFlowFile, fieldToRemove);
    }

    @Test
    public void testMapRemoveAllItems() throws InitializationException, IOException {
        // The schema must not be modified.
        final String inputSchema = "src/test/resources/TestRemoveRecordField/input_schema/bookshelf-map.avsc";
        final String inputFlowFile = "src/test/resources/TestRemoveRecordField/input/bookshelf-map.json";
        final String outputSchema = "src/test/resources/TestRemoveRecordField/output_schema/bookshelf-map.avsc";
        final String outputFlowFile = "src/test/resources/TestRemoveRecordField/output/bookshelf-map-all-items-removed.json";
        final String fieldToRemove = "/books[*]";

        executeRemovalTest(inputSchema, inputFlowFile, outputSchema, outputFlowFile, fieldToRemove);
    }

    @Test
    public void testChoiceRemoveFieldFromPossibleSubtypeWithSchemaInference() throws InitializationException, IOException {
        final String inputFlowFile = "src/test/resources/TestRemoveRecordField/input/name-choice.json";
        final String outputFlowFile = "src/test/resources/TestRemoveRecordField/output/name-choice-firstName-removed.json";
        final String fieldToRemove = "/name/firstName";

        executeRemovalTest(null, inputFlowFile, null, outputFlowFile, fieldToRemove);
    }

    @Test
    public void testChoiceRemoveFieldFromPossibleSubtype() throws InitializationException, IOException {
        // The schema must be modified.
        final String inputSchema = "src/test/resources/TestRemoveRecordField/input_schema/name-choice.avsc";
        final String inputFlowFile = "src/test/resources/TestRemoveRecordField/input/name-choice.json";
        final String outputSchema = "src/test/resources/TestRemoveRecordField/output_schema/name-choice-firstName-removed.avsc";
        final String outputFlowFile = "src/test/resources/TestRemoveRecordField/output/name-choice-firstName-removed.json";
        final String fieldToRemove = "/name/firstName";

        executeRemovalTest(inputSchema, inputFlowFile, outputSchema, outputFlowFile, fieldToRemove);
    }

    @Test
    public void testChoiceRemoveFieldFromChoiceInChoice() throws InitializationException, IOException {
        // The schema must be modified.
        final String inputSchema = "src/test/resources/TestRemoveRecordField/input_schema/address-choice.avsc";
        final String inputFlowFile = "src/test/resources/TestRemoveRecordField/input/address-choice.json";
        final String outputSchema = "src/test/resources/TestRemoveRecordField/output_schema/address-choice-letter-removed-from-building.avsc";
        final String outputFlowFile = "src/test/resources/TestRemoveRecordField/output/address-choice-letter-removed-from-building.json";
        final String fieldToRemove = "/address/building/letter";

        executeRemovalTest(inputSchema, inputFlowFile, outputSchema, outputFlowFile, fieldToRemove);
    }

    private void executeRemovalTest(final String inputSchema, final String inputFlowFile, final String outputSchema, final String outputFlowFile, final String... fieldsToRemove)
            throws IOException, InitializationException {
        executePreparation(inputSchema, inputFlowFile, fieldsToRemove);

        runner.run();

        assertOutput(outputSchema, outputFlowFile);
    }

    private void executePreparation(final String inputSchema, final String inputFlowFile, final String... fieldsToRemove) throws IOException, InitializationException {
        setUpJsonReader(inputSchema);

        final String fieldName = "field-to-remove-";
        final AtomicInteger counter = new AtomicInteger(0);
        final Map<String, String> properties = Arrays.stream(fieldsToRemove)
                .collect(Collectors.toMap(f -> fieldName + counter.incrementAndGet(), f -> f));
        setUpRunner(inputFlowFile, properties);
    }

    private void assertOutput(final String outputSchema, final String outputContent) throws IOException {
        runner.assertAllFlowFilesTransferred(AbstractRecordProcessor.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(AbstractRecordProcessor.REL_SUCCESS).get(0);

        final JsonNode expectedOutput = OBJECT_MAPPER.readTree(Paths.get(outputContent).toFile());
        final JsonNode output = OBJECT_MAPPER.readTree(out.getContent());
        assertThat(output, is(expectedOutput));

        if (outputSchema != null) {
            final JsonNode expectedSchema = OBJECT_MAPPER.readTree(Paths.get(outputSchema).toFile());
            final JsonNode schema = OBJECT_MAPPER.readTree(out.getAttribute("avro.schema"));
            assertThat(expectedSchema, is(schema));
        } else {
            out.assertAttributeExists("avro.schema");
            out.assertAttributeNotEquals("avro.schema", "");
        }
    }

    private void setUpJsonReader(final String schemaFilePath) throws IOException, InitializationException {
        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("reader", jsonReader);
        if (schemaFilePath == null) {
            runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaInferenceUtil.INFER_SCHEMA);
        } else {
            final String inputSchemaText = new String(Files.readAllBytes(Paths.get(schemaFilePath)));
            runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
            runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_TEXT, inputSchemaText);
        }
        runner.enableControllerService(jsonReader);
        runner.setProperty(AbstractRecordProcessor.RECORD_READER, "reader");
    }

    private void setUpRunner(final String flowFilePath, final Map<String, String> properties) throws IOException {
        runner.enqueue(Paths.get(flowFilePath));
        properties.forEach((propertyName,propertyValue) -> runner.setProperty(propertyName, propertyValue));
    }
}
