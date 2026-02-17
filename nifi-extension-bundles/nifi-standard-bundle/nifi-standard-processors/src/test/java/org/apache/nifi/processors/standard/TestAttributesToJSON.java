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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.util.MockComponentLog;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.PropertyMigrationResult;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestAttributesToJSON {

    private static final String TEST_ATTRIBUTE_KEY = "TestAttribute";
    private static final String TEST_ATTRIBUTE_VALUE = "TestValue";
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private TestRunner runner;

    @BeforeEach
    void setUp() {
        runner = TestRunners.newTestRunner(AttributesToJSON.class);
    }

    @Test
    public void testInvalidUserSuppliedAttributeList() {
        //Attribute list CANNOT be empty
        runner.setProperty(AttributesToJSON.ATTRIBUTES_LIST, "");
        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        runner.enqueue(ff);
        assertThrows(AssertionError.class, runner::run);
    }

    @Test
    public void testInvalidIncludeCoreAttributesProperty() {
        runner.setProperty(AttributesToJSON.ATTRIBUTES_LIST, "val1,val2");
        runner.setProperty(AttributesToJSON.DESTINATION, AttributesToJSON.DESTINATION_ATTRIBUTE);
        runner.setProperty(AttributesToJSON.INCLUDE_CORE_ATTRIBUTES, "maybe");

        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        runner.enqueue(ff);
        assertThrows(AssertionError.class, runner::run);
    }

    @Test
    public void testNullValueForEmptyAttribute() throws Exception {
        runner.setProperty(AttributesToJSON.DESTINATION, AttributesToJSON.DESTINATION_ATTRIBUTE);
        final String NON_PRESENT_ATTRIBUTE_KEY = "NonExistingAttributeKey";
        runner.setProperty(AttributesToJSON.ATTRIBUTES_LIST, NON_PRESENT_ATTRIBUTE_KEY);
        runner.setProperty(AttributesToJSON.NULL_VALUE_FOR_EMPTY_STRING, "true");

        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        runner.enqueue(ff);
        runner.run();

        //Expecting success transition because Jackson is taking care of escaping the bad JSON characters
        runner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS).getFirst()
                .assertAttributeExists(AttributesToJSON.JSON_ATTRIBUTE_NAME);
        runner.assertTransferCount(AttributesToJSON.REL_SUCCESS, 1);
        runner.assertTransferCount(AttributesToJSON.REL_FAILURE, 0);

        //Make sure that the value is a true JSON null for the non-existing attribute
        String json = runner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS)
                .getFirst().getAttribute(AttributesToJSON.JSON_ATTRIBUTE_NAME);

        Map<String, String> val = MAPPER.readValue(json, new TypeReference<>() { });

        assertNull(val.get(NON_PRESENT_ATTRIBUTE_KEY));
    }

    @Test
    public void testEmptyStringValueForEmptyAttribute() throws Exception {
        runner.setProperty(AttributesToJSON.DESTINATION, AttributesToJSON.DESTINATION_ATTRIBUTE);
        final String NON_PRESENT_ATTRIBUTE_KEY = "NonExistingAttributeKey";
        runner.setProperty(AttributesToJSON.ATTRIBUTES_LIST, NON_PRESENT_ATTRIBUTE_KEY);
        runner.setProperty(AttributesToJSON.NULL_VALUE_FOR_EMPTY_STRING, "false");

        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        runner.enqueue(ff);
        runner.run();

        //Expecting success transition because Jackson is taking care of escaping the bad JSON characters
        runner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS).getFirst()
                .assertAttributeExists(AttributesToJSON.JSON_ATTRIBUTE_NAME);
        runner.assertTransferCount(AttributesToJSON.REL_SUCCESS, 1);
        runner.assertTransferCount(AttributesToJSON.REL_FAILURE, 0);

        //Make sure that the value is a true JSON null for the non-existing attribute
        String json = runner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS)
                .getFirst().getAttribute(AttributesToJSON.JSON_ATTRIBUTE_NAME);

        Map<String, String> val = MAPPER.readValue(json, new TypeReference<>() { });

        assertEquals(val.get(NON_PRESENT_ATTRIBUTE_KEY), "");
    }

    @Test
    public void testInvalidJSONValueInAttribute() {
        runner.setProperty(AttributesToJSON.DESTINATION, AttributesToJSON.DESTINATION_ATTRIBUTE);

        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        //Create attribute that contains an invalid JSON Character
        ff = session.putAttribute(ff, TEST_ATTRIBUTE_KEY, "'badjson'");

        runner.enqueue(ff);
        runner.run();

        //Expecting success transition because Jackson is taking care of escaping the bad JSON characters
        runner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS).getFirst()
                .assertAttributeExists(AttributesToJSON.JSON_ATTRIBUTE_NAME);
        runner.assertTransferCount(AttributesToJSON.REL_SUCCESS, 1);
        runner.assertTransferCount(AttributesToJSON.REL_FAILURE, 0);
    }


    @Test
    public void testAttributes_emptyListUserSpecifiedAttributes() throws Exception {
        runner.setProperty(AttributesToJSON.DESTINATION, AttributesToJSON.DESTINATION_ATTRIBUTE);

        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        ff = session.putAttribute(ff, TEST_ATTRIBUTE_KEY, TEST_ATTRIBUTE_VALUE);

        runner.enqueue(ff);
        runner.run();

        runner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS).getFirst()
                .assertAttributeExists(AttributesToJSON.JSON_ATTRIBUTE_NAME);
        runner.assertTransferCount(AttributesToJSON.REL_SUCCESS, 1);
        runner.assertTransferCount(AttributesToJSON.REL_FAILURE, 0);

        String json = runner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS)
                .getFirst().getAttribute(AttributesToJSON.JSON_ATTRIBUTE_NAME);

        Map<String, String> val = MAPPER.readValue(json, new TypeReference<>() { });
        assertEquals(TEST_ATTRIBUTE_VALUE, val.get(TEST_ATTRIBUTE_KEY));
    }


    @Test
    public void testContent_emptyListUserSpecifiedAttributes() {
        runner.setProperty(AttributesToJSON.DESTINATION, AttributesToJSON.DESTINATION_CONTENT);
        runner.setProperty(AttributesToJSON.INCLUDE_CORE_ATTRIBUTES, "false");

        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        runner.enqueue(ff);
        runner.run();

        runner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS).getFirst()
                .assertAttributeNotExists(AttributesToJSON.JSON_ATTRIBUTE_NAME);
        runner.assertTransferCount(AttributesToJSON.REL_SUCCESS, 1);
        runner.assertTransferCount(AttributesToJSON.REL_FAILURE, 0);
        runner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS).getFirst().assertContentEquals("{}");
    }

    @Test
    public void testAttribute_singleUserDefinedAttribute() throws Exception {
        runner.setProperty(AttributesToJSON.ATTRIBUTES_LIST, TEST_ATTRIBUTE_KEY);
        runner.setProperty(AttributesToJSON.DESTINATION, AttributesToJSON.DESTINATION_ATTRIBUTE);

        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();
        ff = session.putAttribute(ff, TEST_ATTRIBUTE_KEY, TEST_ATTRIBUTE_VALUE);

        runner.enqueue(ff);
        runner.run();

        runner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS).getFirst()
                .assertAttributeExists(AttributesToJSON.JSON_ATTRIBUTE_NAME);
        runner.assertTransferCount(AttributesToJSON.REL_SUCCESS, 1);
        runner.assertTransferCount(AttributesToJSON.REL_FAILURE, 0);

        String json = runner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS)
                .getFirst().getAttribute(AttributesToJSON.JSON_ATTRIBUTE_NAME);

        Map<String, String> val = MAPPER.readValue(json, new TypeReference<>() { });
        assertEquals(TEST_ATTRIBUTE_VALUE, val.get(TEST_ATTRIBUTE_KEY));
        assertEquals(1, val.size());
    }


    @Test
    public void testAttribute_singleUserDefinedAttributeWithWhiteSpace() throws Exception {
        runner.setProperty(AttributesToJSON.ATTRIBUTES_LIST, " " + TEST_ATTRIBUTE_KEY + " ");
        runner.setProperty(AttributesToJSON.DESTINATION, AttributesToJSON.DESTINATION_ATTRIBUTE);

        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();
        ff = session.putAttribute(ff, TEST_ATTRIBUTE_KEY, TEST_ATTRIBUTE_VALUE);

        runner.enqueue(ff);
        runner.run();

        runner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS).getFirst()
                .assertAttributeExists(AttributesToJSON.JSON_ATTRIBUTE_NAME);
        runner.assertTransferCount(AttributesToJSON.REL_SUCCESS, 1);
        runner.assertTransferCount(AttributesToJSON.REL_FAILURE, 0);

        String json = runner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS)
                .getFirst().getAttribute(AttributesToJSON.JSON_ATTRIBUTE_NAME);

        Map<String, String> val = MAPPER.readValue(json, new TypeReference<>() { });
        assertEquals(TEST_ATTRIBUTE_VALUE, val.get(TEST_ATTRIBUTE_KEY));
        assertEquals(1, val.size());
    }


    @Test
    public void testAttribute_singleNonExistingUserDefinedAttribute() throws Exception {
        runner.setProperty(AttributesToJSON.ATTRIBUTES_LIST, "NonExistingAttribute");
        runner.setProperty(AttributesToJSON.DESTINATION, AttributesToJSON.DESTINATION_ATTRIBUTE);

        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();
        ff = session.putAttribute(ff, TEST_ATTRIBUTE_KEY, TEST_ATTRIBUTE_VALUE);

        runner.enqueue(ff);
        runner.run();

        runner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS).getFirst()
                .assertAttributeExists(AttributesToJSON.JSON_ATTRIBUTE_NAME);
        runner.assertTransferCount(AttributesToJSON.REL_SUCCESS, 1);
        runner.assertTransferCount(AttributesToJSON.REL_FAILURE, 0);

        String json = runner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS)
                .getFirst().getAttribute(AttributesToJSON.JSON_ATTRIBUTE_NAME);

        Map<String, String> val = MAPPER.readValue(json, new TypeReference<>() { });

        //If an Attribute is requested but does not exist then it is placed in the JSON with an empty string
        assertEquals("", val.get("NonExistingAttribute"));
        assertEquals(1, val.size());
    }

    @Test
    public void testAttribute_noIncludeCoreAttributesUserDefined() throws IOException {
        runner.setProperty(AttributesToJSON.ATTRIBUTES_LIST, " " + TEST_ATTRIBUTE_KEY + " , " + CoreAttributes.PATH.key() + " ");
        runner.setProperty(AttributesToJSON.INCLUDE_CORE_ATTRIBUTES, "false");

        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();
        ff = session.putAttribute(ff, TEST_ATTRIBUTE_KEY, TEST_ATTRIBUTE_VALUE);
        ff = session.putAttribute(ff, CoreAttributes.PATH.key(), TEST_ATTRIBUTE_VALUE);

        runner.enqueue(ff);
        runner.run();

        runner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS).getFirst()
                .assertAttributeExists(AttributesToJSON.JSON_ATTRIBUTE_NAME);
        runner.assertTransferCount(AttributesToJSON.REL_SUCCESS, 1);
        runner.assertTransferCount(AttributesToJSON.REL_FAILURE, 0);

        String json = runner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS)
                .getFirst().getAttribute(AttributesToJSON.JSON_ATTRIBUTE_NAME);

        Map<String, String> val = MAPPER.readValue(json, new TypeReference<>() { });
        assertEquals(TEST_ATTRIBUTE_VALUE, val.get(TEST_ATTRIBUTE_KEY));
        assertEquals(TEST_ATTRIBUTE_VALUE, val.get(CoreAttributes.PATH.key()));
        assertEquals(2, val.size());
    }

    @Test
    public void testAttribute_noIncludeCoreAttributesRegex() throws IOException {
        runner.setProperty(AttributesToJSON.ATTRIBUTES_REGEX, CoreAttributes.PATH.key() + ".*");
        runner.setProperty(AttributesToJSON.INCLUDE_CORE_ATTRIBUTES, "false");

        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();
        ff = session.putAttribute(ff, TEST_ATTRIBUTE_KEY, TEST_ATTRIBUTE_VALUE);
        ff = session.putAttribute(ff, CoreAttributes.PATH.key(), TEST_ATTRIBUTE_VALUE);

        runner.enqueue(ff);
        runner.run();

        runner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS).getFirst()
                .assertAttributeExists(AttributesToJSON.JSON_ATTRIBUTE_NAME);
        runner.assertTransferCount(AttributesToJSON.REL_SUCCESS, 1);
        runner.assertTransferCount(AttributesToJSON.REL_FAILURE, 0);

        String json = runner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS)
                .getFirst().getAttribute(AttributesToJSON.JSON_ATTRIBUTE_NAME);

        Map<String, String> val = MAPPER.readValue(json, new TypeReference<>() { });
        assertEquals(TEST_ATTRIBUTE_VALUE, val.get(CoreAttributes.PATH.key()));
        assertEquals(1, val.size());
    }

    @Test
    public void testAttribute_noIncludeCoreAttributesContent() throws IOException {
        runner.setProperty(AttributesToJSON.INCLUDE_CORE_ATTRIBUTES, "false");
        runner.setProperty(AttributesToJSON.DESTINATION, AttributesToJSON.DESTINATION_CONTENT);

        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();
        ff = session.putAttribute(ff, TEST_ATTRIBUTE_KEY, TEST_ATTRIBUTE_VALUE);
        ff = session.putAttribute(ff, CoreAttributes.PATH.key(), TEST_ATTRIBUTE_VALUE);

        runner.enqueue(ff);
        runner.run();

        runner.assertTransferCount(AttributesToJSON.REL_SUCCESS, 1);
        runner.assertTransferCount(AttributesToJSON.REL_FAILURE, 0);

        Map<String, String> val = MAPPER.readValue(runner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS).getFirst().toByteArray(), new TypeReference<>() { });
        assertEquals(TEST_ATTRIBUTE_VALUE, val.get(TEST_ATTRIBUTE_KEY));
        assertEquals(1, val.size());
    }

    @Test
    public void testAttribute_includeCoreAttributesContent() throws IOException {
        runner.setProperty(AttributesToJSON.DESTINATION, AttributesToJSON.DESTINATION_CONTENT);
        runner.setProperty(AttributesToJSON.INCLUDE_CORE_ATTRIBUTES, "true");

        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        runner.enqueue(ff);
        runner.run();

        List<MockFlowFile> flowFilesForRelationship = runner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS);

        runner.assertTransferCount(AttributesToJSON.REL_FAILURE, 0);
        runner.assertTransferCount(AttributesToJSON.REL_SUCCESS, 1);

        MockFlowFile flowFile = flowFilesForRelationship.getFirst();

        assertEquals(AttributesToJSON.APPLICATION_JSON, flowFile.getAttribute(CoreAttributes.MIME_TYPE.key()));

        Map<String, String> val = MAPPER.readValue(flowFile.toByteArray(), new TypeReference<>() { });
        assertEquals(3, val.size());
        Set<String> coreAttributes = Arrays.stream(CoreAttributes.values()).map(CoreAttributes::key).collect(Collectors.toSet());
        val.keySet().forEach(k -> assertTrue(coreAttributes.contains(k)));
    }

    @Test
    public void testAttribute_includeCoreAttributesAttribute() throws IOException {
        runner.setProperty(AttributesToJSON.INCLUDE_CORE_ATTRIBUTES, "true");

        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();

        runner.enqueue(ff);
        runner.run();

        List<MockFlowFile> flowFilesForRelationship = runner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS);

        runner.assertTransferCount(AttributesToJSON.REL_FAILURE, 0);
        runner.assertTransferCount(AttributesToJSON.REL_SUCCESS, 1);

        MockFlowFile flowFile = flowFilesForRelationship.getFirst();

        assertNull(flowFile.getAttribute(CoreAttributes.MIME_TYPE.key()));

        Map<String, String> val = MAPPER.readValue(flowFile.getAttribute(AttributesToJSON.JSON_ATTRIBUTE_NAME), new TypeReference<>() { });
        assertEquals(3, val.size());
        Set<String> coreAttributes = Arrays.stream(CoreAttributes.values()).map(CoreAttributes::key).collect(Collectors.toSet());
        val.keySet().forEach(k -> assertTrue(coreAttributes.contains(k)));
    }

    @Test
    public void testAttributesRegex() throws IOException {
        runner.setEnvironmentVariableValue("regex", "delimited\\.header\\.column\\.[0-9]+");
        runner.setProperty(AttributesToJSON.ATTRIBUTES_REGEX, "${regex}");
        runner.setProperty(AttributesToJSON.ATTRIBUTES_LIST, "test, test1");

        Map<String, String> attributes = new HashMap<>();
        attributes.put("delimited.header.column.1", "Registry");
        attributes.put("delimited.header.column.2", "Assignment");
        attributes.put("delimited.header.column.3", "Organization Name");
        attributes.put("delimited.header.column.4", "Organization Address");
        attributes.put("delimited.footer.column.1", "not included");
        attributes.put("test", "test");
        attributes.put("test1", "test1");
        runner.enqueue("".getBytes(), attributes);

        runner.run();

        runner.assertTransferCount(AttributesToJSON.REL_FAILURE, 0);
        runner.assertTransferCount(AttributesToJSON.REL_SUCCESS, 1);

        MockFlowFile flowFile = runner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS).getFirst();

        Map<String, String> val = MAPPER.readValue(flowFile.getAttribute(AttributesToJSON.JSON_ATTRIBUTE_NAME), new TypeReference<>() { });
        assertTrue(val.containsKey("delimited.header.column.1"));
        assertTrue(val.containsKey("delimited.header.column.2"));
        assertTrue(val.containsKey("delimited.header.column.3"));
        assertTrue(val.containsKey("delimited.header.column.4"));
        assertFalse(val.containsKey("delimited.footer.column.1"));
        assertTrue(val.containsKey("test"));
        assertTrue(val.containsKey("test1"));
    }

    @ParameterizedTest
    @MethodSource("getNestedJson")
    public void testAttributeWithNestedJsonOutputAsJsonInContent(String nestedJson, Class<?> expectedClass) throws IOException {
        runner.setProperty(AttributesToJSON.DESTINATION, AttributesToJSON.DESTINATION_CONTENT);
        runner.setProperty(AttributesToJSON.JSON_HANDLING_STRATEGY, AttributesToJSON.JsonHandlingStrategy.NESTED);

        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();
        ff = session.putAttribute(ff, TEST_ATTRIBUTE_KEY, nestedJson);
        runner.enqueue(ff);
        runner.run();

        runner.assertTransferCount(AttributesToJSON.REL_FAILURE, 0);
        runner.assertTransferCount(AttributesToJSON.REL_SUCCESS, 1);

        List<MockFlowFile> flowFilesForRelationship = runner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS);
        MockFlowFile flowFile = flowFilesForRelationship.getFirst();
        assertEquals(AttributesToJSON.APPLICATION_JSON, flowFile.getAttribute(CoreAttributes.MIME_TYPE.key()));
        Map<String, Object> val = MAPPER.readValue(flowFile.toByteArray(), new TypeReference<>() { });
        assertInstanceOf(expectedClass, val.get(TEST_ATTRIBUTE_KEY));
    }

    private static Stream<Arguments> getNestedJson() throws IOException {
        return Stream.of(
                Arguments.of(Files.readString(Paths.get("src/test/resources/TestJson/json-sample.json")), List.class),
                Arguments.of(Files.readString(Paths.get("src/test/resources/TestAttributesToJSON/input.json")), Map.class));
    }

    @ParameterizedTest
    @MethodSource("getNestedJson")
    public void testAttributeWithNestedJsonOutputAsJsonInAttribute(String nestedJson, Class<?> expectedClass) throws IOException {
        runner.setProperty(AttributesToJSON.DESTINATION, AttributesToJSON.DESTINATION_ATTRIBUTE);
        runner.setProperty(AttributesToJSON.JSON_HANDLING_STRATEGY, AttributesToJSON.JsonHandlingStrategy.NESTED);

        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();
        ff = session.putAttribute(ff, TEST_ATTRIBUTE_KEY, nestedJson);
        runner.enqueue(ff);
        runner.run();

        runner.assertTransferCount(AttributesToJSON.REL_FAILURE, 0);
        runner.assertTransferCount(AttributesToJSON.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS).getFirst()
                .assertAttributeExists(AttributesToJSON.JSON_ATTRIBUTE_NAME);

        String json = runner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS)
                .getFirst().getAttribute(AttributesToJSON.JSON_ATTRIBUTE_NAME);
        Map<String, Object> val = MAPPER.readValue(json, new TypeReference<>() { });
        assertInstanceOf(expectedClass, val.get(TEST_ATTRIBUTE_KEY));
    }

    @ParameterizedTest
    @ValueSource(strings = {"[THIS IS NOT JSON]", "{THIS IS NOT JSON}"})
    public void testAttributesWithLookALikeJson(String lookAlikeJson) {
        runner.setProperty(AttributesToJSON.DESTINATION, AttributesToJSON.DESTINATION_CONTENT);
        runner.setProperty(AttributesToJSON.JSON_HANDLING_STRATEGY, AttributesToJSON.JsonHandlingStrategy.NESTED);

        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();
        ff = session.putAttribute(ff, TEST_ATTRIBUTE_KEY, lookAlikeJson);
        runner.enqueue(ff);
        runner.run();

        runner.assertTransferCount(AttributesToJSON.REL_FAILURE, 1);
        runner.assertTransferCount(AttributesToJSON.REL_SUCCESS, 0);
        MockComponentLog logger = runner.getLogger();
        assertTrue(logger.getErrorMessages().getFirst().getMsg().contains("expecting"));
    }

    @ParameterizedTest
    @MethodSource("getNestedJson")
    public void testAttributeWithNestedJsonOutputAsStringInAttribute(String nestedJson) throws IOException {
        runner.setProperty(AttributesToJSON.DESTINATION, AttributesToJSON.DESTINATION_ATTRIBUTE);

        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();
        ff = session.putAttribute(ff, TEST_ATTRIBUTE_KEY, nestedJson);
        runner.enqueue(ff);
        runner.run();

        runner.assertTransferCount(AttributesToJSON.REL_FAILURE, 0);
        runner.assertTransferCount(AttributesToJSON.REL_SUCCESS, 1);
        runner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS).getFirst()
                .assertAttributeExists(AttributesToJSON.JSON_ATTRIBUTE_NAME);

        String json = runner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS)
                .getFirst().getAttribute(AttributesToJSON.JSON_ATTRIBUTE_NAME);
        Map<String, Object> val = MAPPER.readValue(json, new TypeReference<>() { });
        assertInstanceOf(String.class, val.get(TEST_ATTRIBUTE_KEY));
    }

    @Test
    public void testAttributeWithJsonBetweenLeadingAndTrailingSpaces() throws JsonProcessingException {
        final String jsonBetweenLeadingAndTrailingSpaces = """

                {
                    "fruit": "Apple",
                    "size": "Large",
                    "color": "Red"
                }
                """;
        assertDoesNotThrow(() -> MAPPER.readValue(jsonBetweenLeadingAndTrailingSpaces, Map.class));

        runner.setProperty(AttributesToJSON.DESTINATION, AttributesToJSON.DESTINATION_CONTENT);
        runner.setProperty(AttributesToJSON.JSON_HANDLING_STRATEGY, AttributesToJSON.JsonHandlingStrategy.NESTED);

        ProcessSession session = runner.getProcessSessionFactory().createSession();
        FlowFile ff = session.create();
        ff = session.putAttribute(ff, TEST_ATTRIBUTE_KEY, jsonBetweenLeadingAndTrailingSpaces);
        runner.enqueue(ff);
        runner.run();

        runner.assertTransferCount(AttributesToJSON.REL_FAILURE, 0);
        runner.assertTransferCount(AttributesToJSON.REL_SUCCESS, 1);
        MockFlowFile result = runner.getFlowFilesForRelationship(AttributesToJSON.REL_SUCCESS).getFirst();
        Map<String, Object> attributes = MAPPER.readValue(result.getContent(), new TypeReference<>() { });
        assertInstanceOf(Map.class, attributes.get(TEST_ATTRIBUTE_KEY));
    }

    @Test
    void testMigrateProperties() {
        final Map<String, String> expectedRenamed =
                Map.of("attributes-to-json-regex", AttributesToJSON.ATTRIBUTES_REGEX.getName());

        final PropertyMigrationResult propertyMigrationResult = runner.migrateProperties();
        assertEquals(expectedRenamed, propertyMigrationResult.getPropertiesRenamed());
    }
}
