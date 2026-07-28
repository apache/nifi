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

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.json.schema.JsonSchema;
import org.apache.nifi.json.schema.SchemaVersion;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.JsonSchemaRegistryComponent;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schemaregistry.services.JsonSchemaRegistry;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opentest4j.AssertionFailedError;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestValidateJson {
    private static final Path RESOURCE_DIR = Paths.get("src/test/resources/TestValidateJson");
    private static final String JSON = getFileContent("simple-example.json");
    private static final String SIMPLE_SCHEMA = getFileContent("schema-simple-example.json");
    private static final String NON_JSON = "Not JSON";
    private static final String SCHEMA_VERSION = SchemaVersion.DRAFT_7.getValue();
    private TestRunner runner;

    @BeforeEach
    public void setUp() {
        runner = TestRunners.newTestRunner(ValidateJson.class);
    }

    @ParameterizedTest
    @MethodSource("customValidateArgs")
    void testCustomValidateMissingProperty(final ValidateJson.JsonSchemaStrategy strategy) {
        runner.setProperty(ValidateJson.SCHEMA_ACCESS_STRATEGY, strategy);
        runner.enqueue(JSON);

        assertThrows(AssertionFailedError.class, runner::run);
    }

    @Test
    void testPassSchema() {
        final String schemaPath = getFilePath("schema-simple-example.json");
        runner.setProperty(ValidateJson.SCHEMA_CONTENT, schemaPath);
        runner.setProperty(JsonSchemaRegistryComponent.SCHEMA_VERSION, SCHEMA_VERSION);

        runner.enqueue(JSON);

        runner.run();

        runner.assertAllFlowFilesTransferred(ValidateJson.REL_VALID);
        assertValidationErrors(ValidateJson.REL_VALID, false);
        assertEquals(1, runner.getProvenanceEvents().size());
        assertEquals(ProvenanceEventType.ROUTE, runner.getProvenanceEvents().getFirst().getEventType());
    }

    @Test
    void testNoSchemaVersionSpecified() {
        final String schemaPath = getFilePath("schema-simple-example.json");
        runner.setProperty(ValidateJson.SCHEMA_CONTENT, schemaPath);

        runner.enqueue(JSON);

        runner.run();

        runner.assertAllFlowFilesTransferred(ValidateJson.REL_VALID);
        assertValidationErrors(ValidateJson.REL_VALID, false);
        assertEquals(1, runner.getProvenanceEvents().size());
        assertEquals(ProvenanceEventType.ROUTE, runner.getProvenanceEvents().getFirst().getEventType());
    }

    @Test
    void testEmptySchema() {
        runner.setProperty(ValidateJson.SCHEMA_CONTENT, "{}");
        runner.setProperty(JsonSchemaRegistryComponent.SCHEMA_VERSION, SCHEMA_VERSION);

        runner.enqueue(JSON);
        runner.run();

        runner.assertAllFlowFilesTransferred(ValidateJson.REL_VALID);
        assertValidationErrors(ValidateJson.REL_VALID, false);
        assertEquals(1, runner.getProvenanceEvents().size());
        assertEquals(ProvenanceEventType.ROUTE, runner.getProvenanceEvents().getFirst().getEventType());
    }

    @Test
    void testAllUnknownKeywordsSchema() {
        runner.setProperty(ValidateJson.SCHEMA_CONTENT,
                "{\"fruit\": \"Apple\", \"size\": \"Large\", \"color\": \"Red\"}");
        runner.setProperty(JsonSchemaRegistryComponent.SCHEMA_VERSION, SCHEMA_VERSION);

        runner.enqueue(JSON);
        runner.run();

        runner.assertAllFlowFilesTransferred(ValidateJson.REL_VALID);
        assertValidationErrors(ValidateJson.REL_VALID, false);
        assertEquals(1, runner.getProvenanceEvents().size());
        assertEquals(ProvenanceEventType.ROUTE, runner.getProvenanceEvents().getFirst().getEventType());
    }

    @Test
    void testPatternSchemaCheck() {
        final String schemaPath = getFilePath("schema-simple-example-unmatched-pattern.json");
        runner.setProperty(ValidateJson.SCHEMA_CONTENT, schemaPath);
        runner.setProperty(JsonSchemaRegistryComponent.SCHEMA_VERSION, SCHEMA_VERSION);

        runner.enqueue(JSON);
        runner.run();

        runner.assertAllFlowFilesTransferred(ValidateJson.REL_INVALID);
        assertValidationErrors(ValidateJson.REL_INVALID, true);
        assertEquals(1, runner.getProvenanceEvents().size());
        assertEquals(ProvenanceEventType.ROUTE, runner.getProvenanceEvents().getFirst().getEventType());
    }

    @Test
    void testMissingRequiredValue() {
        final String schema = getFileContent("schema-simple-example-missing-required.json");
        runner.setProperty(ValidateJson.SCHEMA_CONTENT, schema);
        runner.setProperty(JsonSchemaRegistryComponent.SCHEMA_VERSION, SCHEMA_VERSION);

        runner.enqueue(JSON);
        runner.run();

        runner.assertAllFlowFilesTransferred(ValidateJson.REL_INVALID);
        assertValidationErrors(ValidateJson.REL_INVALID, true);
        assertEquals(1, runner.getProvenanceEvents().size());
        assertEquals(ProvenanceEventType.ROUTE, runner.getProvenanceEvents().getFirst().getEventType());
    }

    @Test
    void testInvalidJson() {
        runner.setProperty(ValidateJson.SCHEMA_CONTENT, SIMPLE_SCHEMA);
        runner.setProperty(JsonSchemaRegistryComponent.SCHEMA_VERSION, SCHEMA_VERSION);

        runner.enqueue(NON_JSON);
        runner.run();

        runner.assertAllFlowFilesTransferred(ValidateJson.REL_FAILURE);
        assertValidationErrors(ValidateJson.REL_FAILURE, false);
        assertEquals(1, runner.getProvenanceEvents().size());
        assertEquals(ProvenanceEventType.ROUTE, runner.getProvenanceEvents().getFirst().getEventType());
    }

    @Test
    void testNonExistingSchema() {
        runner.setProperty(ValidateJson.SCHEMA_CONTENT, "not-found.json");
        runner.setProperty(JsonSchemaRegistryComponent.SCHEMA_VERSION, SCHEMA_VERSION);

        runner.enqueue(JSON);
        assertThrows(AssertionFailedError.class, () -> runner.run());
    }

    @Test
    void testBadSchema() {
        runner.setProperty(ValidateJson.SCHEMA_CONTENT, NON_JSON);
        runner.setProperty(JsonSchemaRegistryComponent.SCHEMA_VERSION, SCHEMA_VERSION);

        runner.enqueue(JSON);
        assertThrows(AssertionFailedError.class, () -> runner.run());
    }

    @Test
    void testJsonWithComments() {
        final String schemaPath = getFilePath("schema-simple-example.json");
        runner.setProperty(ValidateJson.SCHEMA_CONTENT, schemaPath);
        runner.setProperty(JsonSchemaRegistryComponent.SCHEMA_VERSION, SCHEMA_VERSION);

        runner.enqueue(getFileContent("simple-example-with-comments.json"));

        runner.run();

        runner.assertAllFlowFilesTransferred(ValidateJson.REL_VALID);
        assertValidationErrors(ValidateJson.REL_VALID, false);
    }

    @Test
    void testSchemaRetrievalFromRegistry() throws InitializationException {
        final String registryIdentifier = "registry";
        final String schemaName = "someSchema";
        final JsonSchemaRegistry validJsonSchemaRegistry = new SampleJsonSchemaRegistry(registryIdentifier, schemaName);
        runner.addControllerService(registryIdentifier, validJsonSchemaRegistry);
        runner.enableControllerService(validJsonSchemaRegistry);
        runner.assertValid(validJsonSchemaRegistry);
        runner.setProperty(ValidateJson.SCHEMA_ACCESS_STRATEGY, ValidateJson.JsonSchemaStrategy.SCHEMA_NAME_PROPERTY);
        runner.setProperty(ValidateJson.SCHEMA_REGISTRY, registryIdentifier);

        Map<String, String> attributes = new HashMap<>();
        attributes.put("schema.name", schemaName);
        runner.enqueue(JSON, attributes);
        runner.run();

        runner.assertAllFlowFilesTransferred(ValidateJson.REL_VALID);
    }

    @ParameterizedTest
    @MethodSource("multilineJsonArgs")
     void testMultilineJsonWhereSecondLineInvalid(ValidateJson.InputFormat inputFormat, boolean expectedValid) {
        final String multilineJson = """
                {"FieldOne":"stringValue","FieldTwo":1234,"FieldThree":[{"arrayField":"arrayValue"}]}
                {"FieldOne":"stringValue","FieldTwo":"NAN","FieldThree":[{"arrayField":"arrayValue"}]}
                """;
        runner.setProperty(ValidateJson.SCHEMA_CONTENT, SIMPLE_SCHEMA);
        runner.setProperty(JsonSchemaRegistryComponent.SCHEMA_VERSION, SCHEMA_VERSION);
        runner.setProperty(ValidateJson.INPUT_FORMAT, inputFormat.getValue());
        runner.enqueue(multilineJson);

        runner.run();

        if (expectedValid) {
            runner.assertAllFlowFilesTransferred(ValidateJson.REL_VALID);
        } else {
            runner.assertAllFlowFilesTransferred(ValidateJson.REL_INVALID);

            assertTrue(runner.getLogger().getWarnMessages().stream()
                    .anyMatch(logMessage -> logMessage.getMsg().contains("JSON at line 2") && logMessage.getMsg().contains("is invalid")));
        }

        runner.clearTransferState();
    }

    @ParameterizedTest
    @MethodSource("schemasWithLeadingComments")
    void testSchemaContentWithLeadingComments(String schema) {
        runner.setProperty(ValidateJson.SCHEMA_CONTENT, schema);
        runner.setProperty(JsonSchemaRegistryComponent.SCHEMA_VERSION, SCHEMA_VERSION);
        runner.enqueue(getFileContent("simple-example-with-comments.json"));
        runner.assertValid();

        final AssertionFailedError assertionFailedError = assertThrows(AssertionFailedError.class, () -> runner.run());
        final String stackTrace = ExceptionUtils.getStackTrace(assertionFailedError);
        assertTrue(stackTrace.contains("JsonParseException") && !stackTrace.contains("FileNotFoundException"));
    }

    @ParameterizedTest
    @MethodSource("schemasWithEmbeddedComments")
    void testSchemaContentWithEmbeddedComments(String schema) {
        runner.setProperty(ValidateJson.SCHEMA_CONTENT, schema);
        runner.setProperty(JsonSchemaRegistryComponent.SCHEMA_VERSION, SCHEMA_VERSION);
        runner.enqueue(getFileContent("simple-example-with-comments.json"));
        runner.assertValid();

        final AssertionFailedError assertionFailedError = assertThrows(AssertionFailedError.class, () -> runner.run());
        final String stackTrace = ExceptionUtils.getStackTrace(assertionFailedError);
        assertTrue(stackTrace.contains("JsonParseException"));
    }

    @ParameterizedTest
    @MethodSource("schemasWithTrailingComments")
    void testSchemaContentWithTrailingCommentsIgnored(String schema) {
        runner.setProperty(ValidateJson.SCHEMA_CONTENT, schema);
        runner.setProperty(JsonSchemaRegistryComponent.SCHEMA_VERSION, SCHEMA_VERSION);
        runner.enqueue(getFileContent("simple-example-with-comments.json"));
        runner.assertValid();

        runner.run();

        runner.assertAllFlowFilesTransferred(ValidateJson.REL_VALID);
    }

    private static Stream<Arguments> schemasWithLeadingComments() {
        final String schemasWithSingleLineComment = """
                 // Single line Java comment
                {
                    "$schema": "http://json-schema.org/draft-07/schema#",
                    "type": "object",
                    "additionalProperties": true
                }
                """;
        final String schemaWithMultiLineComment = """
                /*
                    Multi-line Java comment
                */
                {
                   "$schema": "http://json-schema.org/draft-07/schema#",
                   "type": "object",
                   "additionalProperties": true
                }
                """;
        return Stream.of(
                Arguments.argumentSet("Leading Java single line comment", schemasWithSingleLineComment),
                Arguments.argumentSet("Leading Java multi-line comment", schemaWithMultiLineComment)
        );
    }

    private static Stream<Arguments> schemasWithEmbeddedComments() {
        final String schemasWithSingleLineComment = """
                {
                    // Single line Java comment
                    "$schema": "http://json-schema.org/draft-07/schema#",
                    "type": "object",
                    "additionalProperties": true
                }
                """;
        final String schemaWithMultiLineComment = """
                {
                    /*
                        Multi-line Java comment
                    */
                   "$schema": "http://json-schema.org/draft-07/schema#",
                   "type": "object",
                   "additionalProperties": true
                }
                """;
        return Stream.of(
                Arguments.argumentSet("Embedded Java single line comment", schemasWithSingleLineComment),
                Arguments.argumentSet("Embedded Java multi-line comment", schemaWithMultiLineComment)
        );
    }

    private static Stream<Arguments> schemasWithTrailingComments() {
        final String schemasWithSingleLineComment = """
                {
                    "$schema": "http://json-schema.org/draft-07/schema#",
                    "type": "object",
                    "additionalProperties": true
                }
                // Single line Java comment
                """;
        final String schemaWithMultiLineComment = """
                {
                   "$schema": "http://json-schema.org/draft-07/schema#",
                   "type": "object",
                   "additionalProperties": true
                }
                 /*
                        Multi-line Java comment
                  */
                """;
        return Stream.of(
                Arguments.argumentSet("Trailing Java single line comment", schemasWithSingleLineComment),
                Arguments.argumentSet("Trailing Java multi-line comment", schemaWithMultiLineComment)
        );
    }

    private void assertValidationErrors(Relationship relationship, boolean expected) {
        final Map<String, String> attributes = runner.getFlowFilesForRelationship(relationship).getFirst().getAttributes();

        if (expected) {
            // JSON library supports English and French validation output. Validate existence of message rather than value.
            assertFalse(attributes.get(ValidateJson.ERROR_ATTRIBUTE_KEY).isEmpty());
        } else {
            assertNull(attributes.get(ValidateJson.ERROR_ATTRIBUTE_KEY));
        }
    }

    private static Stream<Arguments> customValidateArgs() {
        return Stream.of(
                Arguments.argumentSet("Require JSON Schema Registry property to be set", ValidateJson.JsonSchemaStrategy.SCHEMA_NAME_PROPERTY),
                Arguments.argumentSet("Require JSON Schema property to be set", ValidateJson.JsonSchemaStrategy.SCHEMA_CONTENT_PROPERTY)
        );
    }

    private static Stream<Arguments> multilineJsonArgs() {
        return Stream.of(
                Arguments.argumentSet(ValidateJson.InputFormat.FLOW_FILE.getDisplayName(), ValidateJson.InputFormat.FLOW_FILE.getValue(), true),
                Arguments.argumentSet(ValidateJson.InputFormat.JSON_LINES.getDisplayName(), ValidateJson.InputFormat.JSON_LINES.getValue(), false)
        );
    }

    private static String getFilePath(final String filename) {
        final Path path = RESOURCE_DIR.resolve(filename);
        return path.toString();
    }

    private static String getFileContent(final String filename) {
        final Path path = RESOURCE_DIR.resolve(filename);
        try {
            return Files.readString(path);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static class SampleJsonSchemaRegistry extends AbstractControllerService implements JsonSchemaRegistry {
        private final String identifier;
        private final String schemaName;

        public SampleJsonSchemaRegistry(String identifier, String schemaName) {
            this.identifier = identifier;
            this.schemaName = schemaName;
        }

        @Override
        public String getIdentifier() {
            return identifier;
        }

        @Override
        public JsonSchema retrieveSchema(String schemaName) throws SchemaNotFoundException {
            if (this.schemaName.equals(schemaName)) {
                return new JsonSchema(SchemaVersion.DRAFT_2020_12, "{}");
            } else {
                throw new SchemaNotFoundException("");
            }
        }
    }
}
