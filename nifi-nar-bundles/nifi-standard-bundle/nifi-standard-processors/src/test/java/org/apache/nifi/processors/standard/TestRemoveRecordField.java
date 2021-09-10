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

import org.apache.nifi.json.JsonRecordSetWriter;
import org.apache.nifi.json.JsonTreeReader;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaAccessUtils;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class TestRemoveRecordField {

    private TestRunner runner;

    @Before
    public void setup() throws InitializationException {
        runner = TestRunners.newTestRunner(RemoveRecordField.class);
    }

    @Test
    public void testRemoveSimpleFieldThatIsMissingFromOneRecord() throws InitializationException, IOException {
        // GIVEN
        String inputSchema = "src/test/resources/TestRemoveRecordField/input_schema/complex-person.avsc";
        String inputFlowFile = "src/test/resources/TestRemoveRecordField/input/complex-person.json";
        String outPutSchema = "src/test/resources/TestRemoveRecordField/output_schema/complex-person-no-dateOfBirth.avsc";
        String outPutFlowFile = "src/test/resources/TestRemoveRecordField/output/complex-person-no-dateOfBirth.json";
        String fieldToRemove = "/dateOfBirth";

        executeRemovalTest(inputSchema, inputFlowFile, outPutSchema, outPutFlowFile, fieldToRemove);
    }

    @Test
    public void testRemoveComplexFieldThatIsMissingFromOneRecord() throws InitializationException, IOException {
        // GIVEN
        String inputSchema = "src/test/resources/TestRemoveRecordField/input_schema/complex-person.avsc";
        String inputFlowFile = "src/test/resources/TestRemoveRecordField/input/complex-person.json";
        String outPutSchema = "src/test/resources/TestRemoveRecordField/output_schema/complex-person-no-workAddress.avsc";
        String outPutFlowFile = "src/test/resources/TestRemoveRecordField/output/complex-person-no-workAddress.json";
        String fieldToRemove = "/workAddress";

        executeRemovalTest(inputSchema, inputFlowFile, outPutSchema, outPutFlowFile, fieldToRemove);
    }

    @Test
    public void testRemoveFieldFromDeepStructure() throws InitializationException, IOException {
        // GIVEN
        String inputSchema = "src/test/resources/TestRemoveRecordField/input_schema/complex-person.avsc";
        String inputFlowFile = "src/test/resources/TestRemoveRecordField/input/complex-person.json";
        String outPutSchema = "src/test/resources/TestRemoveRecordField/output_schema/complex-person-no-workAddress-zip.avsc";
        String outPutFlowFile = "src/test/resources/TestRemoveRecordField/output/complex-person-no-workAddress-zip.json";
        String fieldToRemove = "/workAddress/zip";

        executeRemovalTest(inputSchema, inputFlowFile, outPutSchema, outPutFlowFile, fieldToRemove);
    }

    @Test
    public void testRemoveFieldFromDeepStructureWithRelativePath() throws InitializationException, IOException {
        // GIVEN
        String inputSchema = "src/test/resources/TestRemoveRecordField/input_schema/complex-person.avsc";
        String inputFlowFile = "src/test/resources/TestRemoveRecordField/input/complex-person.json";
        String outPutSchema = "src/test/resources/TestRemoveRecordField/output_schema/complex-person-no-zip.avsc";
        String outPutFlowFile = "src/test/resources/TestRemoveRecordField/output/complex-person-no-zip.json";
        String fieldToRemove = "//zip";

        executeRemovalTest(inputSchema, inputFlowFile, outPutSchema, outPutFlowFile, fieldToRemove);
    }

    @Test
    public void testRemoveFieldFrom3LevelDeepStructure() throws InitializationException, IOException {
        // GIVEN
        String inputSchema = "src/test/resources/TestRemoveRecordField/input_schema/complex-person.avsc";
        String inputFlowFile = "src/test/resources/TestRemoveRecordField/input/complex-person.json";
        String outPutSchema = "src/test/resources/TestRemoveRecordField/output_schema/complex-person-no-workAddress-building-letter.avsc";
        String outPutFlowFile = "src/test/resources/TestRemoveRecordField/output/complex-person-no-workAddress-building-letter.json";
        String fieldToRemove = "/workAddress/building/letter";

        executeRemovalTest(inputSchema, inputFlowFile, outPutSchema, outPutFlowFile, fieldToRemove);
    }

    @Test
    public void testRemoveComplexFieldFromDeepStructureWithRelativePath() throws InitializationException, IOException {
        // GIVEN
        String inputSchema = "src/test/resources/TestRemoveRecordField/input_schema/complex-person.avsc";
        String inputFlowFile = "src/test/resources/TestRemoveRecordField/input/complex-person.json";
        String outPutSchema = "src/test/resources/TestRemoveRecordField/output_schema/complex-person-no-building.avsc";
        String outPutFlowFile = "src/test/resources/TestRemoveRecordField/output/complex-person-no-building.json";
        String fieldToRemove = "//building";

        executeRemovalTest(inputSchema, inputFlowFile, outPutSchema, outPutFlowFile, fieldToRemove);
    }

    @Test
    public void testRemoveNonExistentField() throws InitializationException, IOException {
        // GIVEN
        String inputSchema = "src/test/resources/TestRemoveRecordField/input_schema/complex-person.avsc";
        String inputFlowFile = "src/test/resources/TestRemoveRecordField/input/complex-person.json";
        String outPutSchema = "src/test/resources/TestRemoveRecordField/output_schema/complex-person.avsc";
        String outPutFlowFile = "src/test/resources/TestRemoveRecordField/output/complex-person.json";
        String fieldToRemove = "/nonExistentField";

        executeRemovalTest(inputSchema, inputFlowFile, outPutSchema, outPutFlowFile, fieldToRemove);
    }

    @Test
    public void testRemoveNonExistentFieldFromDeepStructure() throws InitializationException, IOException {
        // GIVEN
        String inputSchema = "src/test/resources/TestRemoveRecordField/input_schema/complex-person.avsc";
        String inputFlowFile = "src/test/resources/TestRemoveRecordField/input/complex-person.json";
        String outPutSchema = "src/test/resources/TestRemoveRecordField/output_schema/complex-person.avsc";
        String outPutFlowFile = "src/test/resources/TestRemoveRecordField/output/complex-person.json";
        String fieldToRemove = "/workAddress/nonExistentField";

        executeRemovalTest(inputSchema, inputFlowFile, outPutSchema, outPutFlowFile, fieldToRemove);
    }

    @Test
    public void testRemoveMultipleFields() throws InitializationException, IOException {
        // GIVEN
        String inputSchema = "src/test/resources/TestRemoveRecordField/input_schema/complex-person.avsc";
        String inputFlowFile = "src/test/resources/TestRemoveRecordField/input/complex-person.json";
        String outPutSchema = "src/test/resources/TestRemoveRecordField/output_schema/complex-person-multiple-fields-removed.avsc";
        String outPutFlowFile = "src/test/resources/TestRemoveRecordField/output/complex-person-multiple-fields-removed.json";
        String fieldToRemove1 = "/name";
        String fieldToRemove2 = "/dateOfBirth";
        String fieldToRemove3 = "/workAddress/building";

        executeRemovalTest(inputSchema, inputFlowFile, outPutSchema, outPutFlowFile, fieldToRemove1, fieldToRemove2, fieldToRemove3);
    }

    @Test
    public void testRemoveEntireArrayField() throws InitializationException, IOException {
        // GIVEN
        String inputSchema = "src/test/resources/TestRemoveRecordField/input_schema/address-array.avsc";
        String inputFlowFile = "src/test/resources/TestRemoveRecordField/input/address-array.json";
        String outPutSchema = "src/test/resources/TestRemoveRecordField/output_schema/address-array-field-removed.avsc";
        String outPutFlowFile = "src/test/resources/TestRemoveRecordField/output/address-array-removed.json";
        String fieldToRemove = "/addresses";

        executeRemovalTest(inputSchema, inputFlowFile, outPutSchema, outPutFlowFile, fieldToRemove);
    }

    @Test
    public void testRemoveOneElementFromArray() throws InitializationException, IOException {
        // GIVEN
        String inputSchema = "src/test/resources/TestRemoveRecordField/input_schema/address-array.avsc";
        String inputFlowFile = "src/test/resources/TestRemoveRecordField/input/address-array.json";
        String outPutSchema = "src/test/resources/TestRemoveRecordField/output_schema/address-array.avsc";
        String outPutFlowFile = "src/test/resources/TestRemoveRecordField/output/address-array-one-element-removed.json";
        String fieldToRemove = "/addresses[1]";

        executeRemovalTest(inputSchema, inputFlowFile, outPutSchema, outPutFlowFile, fieldToRemove);
    }

    @Test
    public void testRemoveAllElementsFromArray1() throws InitializationException, IOException {
        // GIVEN
        String inputSchema = "src/test/resources/TestRemoveRecordField/input_schema/address-array.avsc";
        String inputFlowFile = "src/test/resources/TestRemoveRecordField/input/address-array.json";
        String outPutSchema = "src/test/resources/TestRemoveRecordField/output_schema/address-array.avsc";
        String outPutFlowFile = "src/test/resources/TestRemoveRecordField/output/address-array-empty.json";
        String fieldToRemove = "/addresses[*]";

        executeRemovalTest(inputSchema, inputFlowFile, outPutSchema, outPutFlowFile, fieldToRemove);
    }

    @Test
    public void testRemoveAllElementsFromArray2() throws InitializationException, IOException {
        // GIVEN
        String inputSchema = "src/test/resources/TestRemoveRecordField/input_schema/address-array.avsc";
        String inputFlowFile = "src/test/resources/TestRemoveRecordField/input/address-array.json";
        String outPutSchema = "src/test/resources/TestRemoveRecordField/output_schema/address-array.avsc";
        String outPutFlowFile = "src/test/resources/TestRemoveRecordField/output/address-array-empty.json";
        String fieldToRemove = "/addresses[0..-1]";

        executeRemovalTest(inputSchema, inputFlowFile, outPutSchema, outPutFlowFile, fieldToRemove);
    }

    @Test
    public void testRemoveFieldFromOneArrayElement() throws InitializationException, IOException {
        // The schema must not be modified.
        // GIVEN
        String inputSchema = "src/test/resources/TestRemoveRecordField/input_schema/address-array.avsc";
        String inputFlowFile = "src/test/resources/TestRemoveRecordField/input/address-array.json";
        String outPutSchema = "src/test/resources/TestRemoveRecordField/output_schema/address-array.avsc";
        String outPutFlowFile = "src/test/resources/TestRemoveRecordField/output/address-array-zip-removed-from-one-element.json";
        String fieldToRemove = "/addresses[1]/zip";

        executeRemovalTest(inputSchema, inputFlowFile, outPutSchema, outPutFlowFile, fieldToRemove);
    }

    @Test
    public void testRemoveFieldFromAllArrayElements() throws InitializationException, IOException {
        // The schema must be modified.
        // GIVEN
        String inputSchema = "src/test/resources/TestRemoveRecordField/input_schema/address-array.avsc";
        String inputFlowFile = "src/test/resources/TestRemoveRecordField/input/address-array.json";
        String outPutSchema = "src/test/resources/TestRemoveRecordField/output_schema/address-array-zip-removed.avsc";
        String outPutFlowFile = "src/test/resources/TestRemoveRecordField/output/address-array-zip-removed-from-all-elements.json";
        String fieldToRemove = "/addresses[*]/zip";

        executeRemovalTest(inputSchema, inputFlowFile, outPutSchema, outPutFlowFile, fieldToRemove);
    }

    @Test
    public void testArrayFieldInArrayFieldRemoveFieldFromOneElementInOneGroup() throws InitializationException, IOException {
        // The schema must not be modified.
        // GIVEN
        String inputSchema = "src/test/resources/TestRemoveRecordField/input_schema/machinegroups-array.avsc";
        String inputFlowFile = "src/test/resources/TestRemoveRecordField/input/machinegroups-array.json";
        String outPutSchema = "src/test/resources/TestRemoveRecordField/output_schema/machinegroups-array.avsc";
        String outPutFlowFile = "src/test/resources/TestRemoveRecordField/output/machinegroups-array-field-removed-from-one-element-in-one-group.json";
        String fieldToRemove = "/machineGroups[1]/machines[1]/units";

        executeRemovalTest(inputSchema, inputFlowFile, outPutSchema, outPutFlowFile, fieldToRemove);
    }

    @Test
    public void testArrayFieldInArrayFieldRemoveFieldFromAllElementsInOneGroup() throws InitializationException, IOException {
        // The schema must not be modified.
        // GIVEN
        String inputSchema = "src/test/resources/TestRemoveRecordField/input_schema/machinegroups-array.avsc";
        String inputFlowFile = "src/test/resources/TestRemoveRecordField/input/machinegroups-array.json";
        String outPutSchema = "src/test/resources/TestRemoveRecordField/output_schema/machinegroups-array.avsc";
        String outPutFlowFile = "src/test/resources/TestRemoveRecordField/output/machinegroups-array-field-removed-from-all-elements-in-one-group.json";
        String fieldToRemove = "/machineGroups[1]/machines[*]/units";

        executeRemovalTest(inputSchema, inputFlowFile, outPutSchema, outPutFlowFile, fieldToRemove);
    }

    @Test
    public void testArrayFieldInArrayFieldRemoveFieldOneElementInAllGroups() throws InitializationException, IOException {
        // The schema must not be modified.
        // GIVEN
        String inputSchema = "src/test/resources/TestRemoveRecordField/input_schema/machinegroups-array.avsc";
        String inputFlowFile = "src/test/resources/TestRemoveRecordField/input/machinegroups-array.json";
        String outPutSchema = "src/test/resources/TestRemoveRecordField/output_schema/machinegroups-array.avsc";
        String outPutFlowFile = "src/test/resources/TestRemoveRecordField/output/machinegroups-array-field-removed-from-one-element-in-all-groups.json";
        String fieldToRemove = "/machineGroups[*]/machines[1]/units";

        executeRemovalTest(inputSchema, inputFlowFile, outPutSchema, outPutFlowFile, fieldToRemove);
    }

    @Test
    public void testArrayFieldInArrayFieldRemoveFieldFromAllElementsInAllGroups() throws InitializationException, IOException {
        // The schema must be modified.
        // GIVEN
        String inputSchema = "src/test/resources/TestRemoveRecordField/input_schema/machinegroups-array.avsc";
        String inputFlowFile = "src/test/resources/TestRemoveRecordField/input/machinegroups-array.json";
        String outPutSchema = "src/test/resources/TestRemoveRecordField/output_schema/machinegroups-array-units-field-removed.avsc";
        String outPutFlowFile = "src/test/resources/TestRemoveRecordField/output/machinegroups-array-field-removed-from-all-elements-in-all-groups.json";
        String fieldToRemove = "/machineGroups[*]/machines[*]/units";

        executeRemovalTest(inputSchema, inputFlowFile, outPutSchema, outPutFlowFile, fieldToRemove);
    }

    @Test
    public void testMapRemoveOneItem() throws InitializationException, IOException {
        // The schema must not be modified.
        // GIVEN
        String inputSchema = "src/test/resources/TestRemoveRecordField/input_schema/bookshelf-map.avsc";
        String inputFlowFile = "src/test/resources/TestRemoveRecordField/input/bookshelf-map.json";
        String outPutSchema = "src/test/resources/TestRemoveRecordField/output_schema/bookshelf-map.avsc";
        String outPutFlowFile = "src/test/resources/TestRemoveRecordField/output/bookshelf-map-one-item-removed.json";
        String fieldToRemove = "/books['ISBN_2222222222222']";

        executeRemovalTest(inputSchema, inputFlowFile, outPutSchema, outPutFlowFile, fieldToRemove);
    }

    @Test
    public void testMapRemoveFieldFromOneItem() throws InitializationException, IOException {
        // The schema must not be modified.
        // GIVEN
        String inputSchema = "src/test/resources/TestRemoveRecordField/input_schema/bookshelf-map.avsc";
        String inputFlowFile = "src/test/resources/TestRemoveRecordField/input/bookshelf-map.json";
        String outPutSchema = "src/test/resources/TestRemoveRecordField/output_schema/bookshelf-map.avsc";
        String outPutFlowFile = "src/test/resources/TestRemoveRecordField/output/bookshelf-map-field-removed-from-one-element.json";
        String fieldToRemove = "/books['ISBN_2222222222222']/author";

        executeRemovalTest(inputSchema, inputFlowFile, outPutSchema, outPutFlowFile, fieldToRemove);
    }

    @Test
    public void testMapRemoveFieldFromAllItems() throws InitializationException, IOException {
        // The schema must be modified.
        // GIVEN
        String inputSchema = "src/test/resources/TestRemoveRecordField/input_schema/bookshelf-map.avsc";
        String inputFlowFile = "src/test/resources/TestRemoveRecordField/input/bookshelf-map.json";
        String outPutSchema = "src/test/resources/TestRemoveRecordField/output_schema/bookshelf-map-author-field-removed.avsc";
        String outPutFlowFile = "src/test/resources/TestRemoveRecordField/output/bookshelf-map-field-removed-from-all-elements.json";
        String fieldToRemove = "/books[*]/author";

        executeRemovalTest(inputSchema, inputFlowFile, outPutSchema, outPutFlowFile, fieldToRemove);
    }

    @Test
    public void testMapRemoveAllItems() throws InitializationException, IOException {
        // The schema must not be modified.
        // GIVEN
        String inputSchema = "src/test/resources/TestRemoveRecordField/input_schema/bookshelf-map.avsc";
        String inputFlowFile = "src/test/resources/TestRemoveRecordField/input/bookshelf-map.json";
        String outPutSchema = "src/test/resources/TestRemoveRecordField/output_schema/bookshelf-map.avsc";
        String outPutFlowFile = "src/test/resources/TestRemoveRecordField/output/bookshelf-map-all-items-removed.json";
        String fieldToRemove = "/books[*]";

        executeRemovalTest(inputSchema, inputFlowFile, outPutSchema, outPutFlowFile, fieldToRemove);
    }

    @Test
    public void testChoiceRemoveFieldFromPossibleSubtype() throws InitializationException, IOException {
        // The schema must be modified.
        // GIVEN
        String inputSchema = "src/test/resources/TestRemoveRecordField/input_schema/name-choice.avsc";
        String inputFlowFile = "src/test/resources/TestRemoveRecordField/input/name-choice.json";
        String outPutSchema = "src/test/resources/TestRemoveRecordField/output_schema/name-choice-firstName-removed.avsc";
        String outPutFlowFile = "src/test/resources/TestRemoveRecordField/output/name-choice-firstName-removed.json";
        String fieldToRemove = "/name/firstName";

        executeRemovalTest(inputSchema, inputFlowFile, outPutSchema, outPutFlowFile, fieldToRemove);
    }

    @Test
    public void testChoiceRemoveFieldFromChoiceInChoice() throws InitializationException, IOException {
        // The schema must be modified.
        // GIVEN
        String inputSchema = "src/test/resources/TestRemoveRecordField/input_schema/address-choice.avsc";
        String inputFlowFile = "src/test/resources/TestRemoveRecordField/input/address-choice.json";
        String outPutSchema = "src/test/resources/TestRemoveRecordField/output_schema/address-choice-letter-removed-from-building.avsc";
        String outPutFlowFile = "src/test/resources/TestRemoveRecordField/output/address-choice-letter-removed-from-building.json";
        String fieldToRemove = "/address/building/letter";

        executeRemovalTest(inputSchema, inputFlowFile, outPutSchema, outPutFlowFile, fieldToRemove);
    }

    private void executeRemovalTest(String inputSchema, String inputFlowFile, String outPutSchema, String outPutFlowFile, String... fieldsToRemove)
            throws IOException, InitializationException {
        // GIVEN
        executePreparation(inputSchema, inputFlowFile, fieldsToRemove);

        // WHEN
        runner.run();

        // THEN
        assertOutput(outPutSchema, outPutFlowFile);
    }

    private void executePreparation(String inputSchema, String inputFlowFile, String... fieldsToRemove) throws IOException, InitializationException {
        setUpJsonReader(inputSchema);
        setUpJsonWriter();

        Map<String, String> properties = new HashMap<>(1);
        String fieldName = "field-to-remove-";
        int counter = 1;
        for (String fieldToRemove : fieldsToRemove) {
            if (1 == counter) {
                // FIELD_TO_REMOVE_1 is mandatory propert, others are dynamic
                properties.put(RemoveRecordField.FIELD_TO_REMOVE_1.getName(), fieldToRemove);
            } else {
                properties.put(fieldName + counter, fieldToRemove);
            }
            ++counter;
        }
        setUpRunner(inputFlowFile, properties);
    }

    private void assertOutput(String outPutSchema, String outPutFlowFile) throws IOException {
        String expectedOutput = new String(Files.readAllBytes(Paths.get(outPutFlowFile)));
        String expectedSchema = new String(Files.readAllBytes(Paths.get(outPutSchema)));

        runner.assertAllFlowFilesTransferred(UpdateRecord.REL_SUCCESS, 1);
        MockFlowFile out = runner.getFlowFilesForRelationship(UpdateRecord.REL_SUCCESS).get(0);
        out.assertContentEquals(expectedOutput);
        out.assertAttributeEquals("avro.schema", expectedSchema);
    }

    private void setUpJsonReader(String schemaFilePath) throws IOException, InitializationException {
        final JsonTreeReader jsonReader = new JsonTreeReader();
        runner.addControllerService("reader", jsonReader);
        final String inputSchemaText = new String(Files.readAllBytes(Paths.get(schemaFilePath)));
        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.SCHEMA_TEXT_PROPERTY);
        runner.setProperty(jsonReader, SchemaAccessUtils.SCHEMA_TEXT, inputSchemaText);
        runner.enableControllerService(jsonReader);
        runner.setProperty(UpdateRecord.RECORD_READER, "reader");
    }

    private void setUpJsonWriter() throws InitializationException {
        final JsonRecordSetWriter jsonWriter = new JsonRecordSetWriter();
        runner.addControllerService("writer", jsonWriter);
        runner.setProperty(jsonWriter, SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY, SchemaAccessUtils.INHERIT_RECORD_SCHEMA);
        runner.setProperty(jsonWriter, "Pretty Print JSON", "true");
        runner.setProperty(jsonWriter, "Schema Write Strategy", "full-schema-attribute");
        runner.enableControllerService(jsonWriter);
        runner.setProperty(UpdateRecord.RECORD_WRITER, "writer");
    }

    private void setUpRunner(String flowFilePath, Map<String, String> properties) throws IOException {
        runner.enqueue(Paths.get(flowFilePath));
        properties.forEach((propertyName,propertyValue) -> runner.setProperty(propertyName, propertyValue));
    }
}
