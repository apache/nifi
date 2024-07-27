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

import org.apache.commons.io.IOUtils;
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
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestValidateJson {
    private static final String JSON = getFileContent("simple-example.json");
    private static final String SIMPLE_SCHEMA = getFileContent("schema-simple-example.json");
    private static final String NON_JSON = "Not JSON";
    private static final String SCHEMA_VERSION = SchemaVersion.DRAFT_7.getValue();
    private TestRunner runner;

    @BeforeEach
    public void setUp() {
        runner = TestRunners.newTestRunner(ValidateJson.class);
    }

    @ParameterizedTest(name = "{2}")
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

        runner.assertTransferCount(ValidateJson.REL_FAILURE, 0);
        runner.assertTransferCount(ValidateJson.REL_INVALID, 0);
        runner.assertTransferCount(ValidateJson.REL_VALID, 1);

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

        runner.assertTransferCount(ValidateJson.REL_FAILURE, 0);
        runner.assertTransferCount(ValidateJson.REL_INVALID, 0);
        runner.assertTransferCount(ValidateJson.REL_VALID, 1);

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

        runner.assertTransferCount(ValidateJson.REL_FAILURE, 0);
        runner.assertTransferCount(ValidateJson.REL_INVALID, 0);
        runner.assertTransferCount(ValidateJson.REL_VALID, 1);

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

        runner.assertTransferCount(ValidateJson.REL_FAILURE, 0);
        runner.assertTransferCount(ValidateJson.REL_INVALID, 0);
        runner.assertTransferCount(ValidateJson.REL_VALID, 1);

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

        runner.assertTransferCount(ValidateJson.REL_FAILURE, 0);
        runner.assertTransferCount(ValidateJson.REL_INVALID, 1);
        runner.assertTransferCount(ValidateJson.REL_VALID, 0);

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

        runner.assertTransferCount(ValidateJson.REL_FAILURE, 0);
        runner.assertTransferCount(ValidateJson.REL_INVALID, 1);
        runner.assertTransferCount(ValidateJson.REL_VALID, 0);

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

        runner.assertTransferCount(ValidateJson.REL_FAILURE, 1);
        runner.assertTransferCount(ValidateJson.REL_INVALID, 0);
        runner.assertTransferCount(ValidateJson.REL_VALID, 0);

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

        runner.assertTransferCount(ValidateJson.REL_FAILURE, 0);
        runner.assertTransferCount(ValidateJson.REL_INVALID, 0);
        runner.assertTransferCount(ValidateJson.REL_VALID, 1);

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

        runner.assertTransferCount(ValidateJson.REL_FAILURE, 0);
        runner.assertTransferCount(ValidateJson.REL_INVALID, 0);
        runner.assertTransferCount(ValidateJson.REL_VALID, 1);
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
                Arguments.of(ValidateJson.JsonSchemaStrategy.SCHEMA_NAME_PROPERTY, "requires that the JSON Schema Registry property be set"),
                Arguments.of(ValidateJson.JsonSchemaStrategy.SCHEMA_CONTENT_PROPERTY, "requires that the JSON Schema property be set")
        );
    }

    private static String getFilePath(final String filename) {
        final String path = getRelativeResourcePath(filename);
        final URL url = Objects.requireNonNull(TestValidateJson.class.getResource(path), "Resource not found");
        return url.getPath();
    }

    private static String getFileContent(final String filename) {
        final String path = getRelativeResourcePath(filename);
        try (final InputStream inputStream = TestValidateJson.class.getResourceAsStream(path)) {
            Objects.requireNonNull(inputStream, "Resource not found");
            return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String getRelativeResourcePath(final String filename) {
        return String.format("/%s/%s", TestValidateJson.class.getSimpleName(), filename);
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
