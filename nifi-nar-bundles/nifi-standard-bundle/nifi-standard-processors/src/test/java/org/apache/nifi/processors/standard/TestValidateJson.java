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
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestValidateJson {
    private static final String JSON = getFileContent("simple-example.json");
    private static final String NON_JSON = "Not JSON";
    private static final String SCHEMA_VERSION = ValidateJson.SchemaVersion.DRAFT_7.getValue();
    private TestRunner runner;

    @BeforeEach
    public void setUp() {
        runner = TestRunners.newTestRunner(ValidateJson.class);
    }

    @Test
    void testPassSchema() {
        final String schemaPath = getFilePath("schema-simple-example.json");
        runner.setProperty(ValidateJson.SCHEMA_CONTENT, schemaPath);
        runner.setProperty(ValidateJson.SCHEMA_VERSION, SCHEMA_VERSION);

        runner.enqueue(JSON);

        runner.run();

        runner.assertTransferCount(ValidateJson.REL_FAILURE, 0);
        runner.assertTransferCount(ValidateJson.REL_INVALID, 0);
        runner.assertTransferCount(ValidateJson.REL_VALID, 1);

        assertValidationErrors(ValidateJson.REL_VALID, false);
        assertEquals(1, runner.getProvenanceEvents().size());
        assertEquals(ProvenanceEventType.ROUTE, runner.getProvenanceEvents().get(0).getEventType());
    }

    @Test
    void testEmptySchema() {
        runner.setProperty(ValidateJson.SCHEMA_CONTENT, "{}");
        runner.setProperty(ValidateJson.SCHEMA_VERSION, SCHEMA_VERSION);

        runner.enqueue(JSON);
        runner.run();

        runner.assertTransferCount(ValidateJson.REL_FAILURE, 0);
        runner.assertTransferCount(ValidateJson.REL_INVALID, 0);
        runner.assertTransferCount(ValidateJson.REL_VALID, 1);

        assertValidationErrors(ValidateJson.REL_VALID, false);
        assertEquals(1, runner.getProvenanceEvents().size());
        assertEquals(ProvenanceEventType.ROUTE, runner.getProvenanceEvents().get(0).getEventType());
    }

    @Test
    void testAllUnknownKeywordsSchema() {
        runner.setProperty(ValidateJson.SCHEMA_CONTENT,
                "{\"fruit\": \"Apple\", \"size\": \"Large\", \"color\": \"Red\"}");
        runner.setProperty(ValidateJson.SCHEMA_VERSION, SCHEMA_VERSION);

        runner.enqueue(JSON);
        runner.run();

        runner.assertTransferCount(ValidateJson.REL_FAILURE, 0);
        runner.assertTransferCount(ValidateJson.REL_INVALID, 0);
        runner.assertTransferCount(ValidateJson.REL_VALID, 1);

        assertValidationErrors(ValidateJson.REL_VALID, false);
        assertEquals(1, runner.getProvenanceEvents().size());
        assertEquals(ProvenanceEventType.ROUTE, runner.getProvenanceEvents().get(0).getEventType());
    }

    @Test
    void testPatternSchemaCheck() {
        final String schemaPath = getFilePath("schema-simple-example-unmatched-pattern.json");
        runner.setProperty(ValidateJson.SCHEMA_CONTENT, schemaPath);
        runner.setProperty(ValidateJson.SCHEMA_VERSION, SCHEMA_VERSION);

        runner.enqueue(JSON);
        runner.run();

        runner.assertTransferCount(ValidateJson.REL_FAILURE, 0);
        runner.assertTransferCount(ValidateJson.REL_INVALID, 1);
        runner.assertTransferCount(ValidateJson.REL_VALID, 0);

        assertValidationErrors(ValidateJson.REL_INVALID, true);
        assertEquals(1, runner.getProvenanceEvents().size());
        assertEquals(ProvenanceEventType.ROUTE, runner.getProvenanceEvents().get(0).getEventType());
    }

    @Test
    void testMissingRequiredValue() {
        final String schema = getFileContent("schema-simple-example-missing-required.json");
        runner.setProperty(ValidateJson.SCHEMA_CONTENT, schema);
        runner.setProperty(ValidateJson.SCHEMA_VERSION, SCHEMA_VERSION);

        runner.enqueue(JSON);
        runner.run();

        runner.assertTransferCount(ValidateJson.REL_FAILURE, 0);
        runner.assertTransferCount(ValidateJson.REL_INVALID, 1);
        runner.assertTransferCount(ValidateJson.REL_VALID, 0);

        assertValidationErrors(ValidateJson.REL_INVALID, true);
        assertEquals(1, runner.getProvenanceEvents().size());
        assertEquals(ProvenanceEventType.ROUTE, runner.getProvenanceEvents().get(0).getEventType());
    }

    @Test
    void testInvalidJson() {
        final String schema = getFileContent("schema-simple-example.json");
        runner.setProperty(ValidateJson.SCHEMA_CONTENT, schema);
        runner.setProperty(ValidateJson.SCHEMA_VERSION, SCHEMA_VERSION);

        runner.enqueue(NON_JSON);
        runner.run();

        runner.assertTransferCount(ValidateJson.REL_FAILURE, 1);
        runner.assertTransferCount(ValidateJson.REL_INVALID, 0);
        runner.assertTransferCount(ValidateJson.REL_VALID, 0);

        assertValidationErrors(ValidateJson.REL_FAILURE, false);
        assertEquals(1, runner.getProvenanceEvents().size());
        assertEquals(ProvenanceEventType.ROUTE, runner.getProvenanceEvents().get(0).getEventType());
    }

    @Test
    void testNonExistingSchema() {
        runner.setProperty(ValidateJson.SCHEMA_CONTENT, "not-found.json");
        runner.setProperty(ValidateJson.SCHEMA_VERSION, SCHEMA_VERSION);

        runner.enqueue(JSON);
        assertThrows(AssertionFailedError.class, () -> runner.run());
    }

    @Test
    void testBadSchema() {
        runner.setProperty(ValidateJson.SCHEMA_CONTENT, NON_JSON);
        runner.setProperty(ValidateJson.SCHEMA_VERSION, SCHEMA_VERSION);

        runner.enqueue(JSON);
        assertThrows(AssertionFailedError.class, () -> runner.run());
    }

    @Test
    void testJsonWithComments() {
        final String schemaPath = getFilePath("schema-simple-example.json");
        runner.setProperty(ValidateJson.SCHEMA_CONTENT, schemaPath);
        runner.setProperty(ValidateJson.SCHEMA_VERSION, SCHEMA_VERSION);

        runner.enqueue(getFileContent("simple-example-with-comments.json"));

        runner.run();

        runner.assertTransferCount(ValidateJson.REL_FAILURE, 0);
        runner.assertTransferCount(ValidateJson.REL_INVALID, 0);
        runner.assertTransferCount(ValidateJson.REL_VALID, 1);

        assertValidationErrors(ValidateJson.REL_VALID, false);
    }
    private void assertValidationErrors(Relationship relationship, boolean expected) {
        final Map<String, String> attributes = runner.getFlowFilesForRelationship(relationship).get(0).getAttributes();

        if (expected) {
            // JSON library supports English and French validation output. Validate existence of message rather than value.
            assertFalse(attributes.get(ValidateJson.ERROR_ATTRIBUTE_KEY).isEmpty());
        } else {
            assertNull(attributes.get(ValidateJson.ERROR_ATTRIBUTE_KEY));
        }
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
}
