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

import org.apache.nifi.processor.Relationship;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;


class TestValidateJson {
    private static final Path JSON = Paths.get("src/test/resources/TestValidateJson/simple-example.json");
    private static final String NON_JSON = "This isn't JSON!";
    private TestRunner runner;

    @BeforeEach
    public void setUp() {
        runner = TestRunners.newTestRunner(ValidateJson.class);
    }

    @Test
    void testPassSchema() throws IOException {
        Path schema = Paths.get("src/test/resources/TestValidateJson/schema-simple-example.json");
        runner.setProperty(ValidateJson.SCHEMA_CONTENT, schema.toString());
        runner.setProperty(ValidateJson.SCHEMA_VERSION, ValidateJson.JsonSchemaVersion.V7.name());
        runner.enqueue(JSON);

        runner.run();

        runner.assertTransferCount(ValidateJson.REL_FAILURE, 0);
        runner.assertTransferCount(ValidateJson.REL_INVALID, 0);
        runner.assertTransferCount(ValidateJson.REL_VALID, 1);

        assertValidationErrors(ValidateJson.REL_VALID, false);
    }

    private void assertValidationErrors(Relationship relationship, boolean expected) {
        Map<String, String> attributes =
                runner.getFlowFilesForRelationship(relationship).get(0).getAttributes();

        if(expected) {
            // JSON library supports English and French validation output. Validate existence of message rather than value.
            Assertions.assertFalse(attributes.get(ValidateJson.ERROR_ATTRIBUTE_KEY).isEmpty());
        } else {
            Assertions.assertNull(attributes.get(ValidateJson.ERROR_ATTRIBUTE_KEY));
        }
    }

    @Test
    void testEmptySchema() throws IOException {
        runner.setProperty(ValidateJson.SCHEMA_CONTENT, "{}");
        runner.setProperty(ValidateJson.SCHEMA_VERSION, ValidateJson.JsonSchemaVersion.V7.name());

        runner.enqueue(JSON);
        runner.run();

        runner.assertTransferCount(ValidateJson.REL_FAILURE, 0);
        runner.assertTransferCount(ValidateJson.REL_INVALID, 0);
        runner.assertTransferCount(ValidateJson.REL_VALID, 1);

        assertValidationErrors(ValidateJson.REL_VALID, false);
    }

    @Test
    void testAllUnknownKeywordsSchema() throws IOException {
        runner.setProperty(ValidateJson.SCHEMA_CONTENT,
                "{\"fruit\": \"Apple\", \"size\": \"Large\", \"color\": \"Red\"}");
        runner.setProperty(ValidateJson.SCHEMA_VERSION, ValidateJson.JsonSchemaVersion.V7.name());

        runner.enqueue(JSON);
        runner.run();

        runner.assertTransferCount(ValidateJson.REL_FAILURE, 0);
        runner.assertTransferCount(ValidateJson.REL_INVALID, 0);
        runner.assertTransferCount(ValidateJson.REL_VALID, 1);

        assertValidationErrors(ValidateJson.REL_VALID, false);
    }

    @Test
    void testPatternSchemaCheck() throws IOException {
        Path schema =
                Paths.get("src/test/resources/TestValidateJson/schema-simple-example-unmatched-pattern.json");
        runner.setProperty(ValidateJson.SCHEMA_CONTENT, schema.toUri().toURL().toString());
        runner.setProperty(ValidateJson.SCHEMA_VERSION, ValidateJson.JsonSchemaVersion.V7.name());

        runner.enqueue(JSON);
        runner.run();

        runner.assertTransferCount(ValidateJson.REL_FAILURE, 0);
        runner.assertTransferCount(ValidateJson.REL_INVALID, 1);
        runner.assertTransferCount(ValidateJson.REL_VALID, 0);

        assertValidationErrors(ValidateJson.REL_INVALID, true);
    }

    @Test
    void testMissingRequiredValue() throws IOException {
        String schema =
                getContent(Paths.get("src/test/resources/TestValidateJson/schema-simple-example-missing-required.json"));
        runner.setProperty(ValidateJson.SCHEMA_CONTENT, schema);
        runner.setProperty(ValidateJson.SCHEMA_VERSION, ValidateJson.JsonSchemaVersion.V7.name());

        runner.enqueue(JSON);
        runner.run();

        runner.assertTransferCount(ValidateJson.REL_FAILURE, 0);
        runner.assertTransferCount(ValidateJson.REL_INVALID, 1);
        runner.assertTransferCount(ValidateJson.REL_VALID, 0);

        assertValidationErrors(ValidateJson.REL_INVALID, true);
    }

    private String getContent(Path path) throws IOException {
        return new String(Files.readAllBytes(path));
    }

    @Test
    void testInvalidJSON() throws IOException {
        String schema =
                getContent(Paths.get("src/test/resources/TestValidateJson/schema-simple-example.json"));
        runner.setProperty(ValidateJson.SCHEMA_CONTENT, schema);
        runner.setProperty(ValidateJson.SCHEMA_VERSION, ValidateJson.JsonSchemaVersion.V7.name());

        runner.enqueue(NON_JSON);
        runner.run();

        runner.assertTransferCount(ValidateJson.REL_FAILURE, 1);
        runner.assertTransferCount(ValidateJson.REL_INVALID, 0);
        runner.assertTransferCount(ValidateJson.REL_VALID, 0);

        assertValidationErrors(ValidateJson.REL_FAILURE, false);
    }

    @Test
    void testNonExistingSchema() throws IOException {
        Path schema = Paths.get("src/test/resources/TestValidateJson/does-not-exist.json");
        runner.setProperty(ValidateJson.SCHEMA_CONTENT, schema.toString());
        runner.setProperty(ValidateJson.SCHEMA_VERSION, ValidateJson.JsonSchemaVersion.V7.name());

        runner.enqueue(JSON);
        Assertions.assertThrows(AssertionFailedError.class, () -> runner.run());
    }

    @Test
    void testBadSchema() throws IOException {
        runner.setProperty(ValidateJson.SCHEMA_CONTENT, NON_JSON);
        runner.setProperty(ValidateJson.SCHEMA_VERSION, ValidateJson.JsonSchemaVersion.V7.name());

        runner.enqueue(JSON);
        Assertions.assertThrows(AssertionFailedError.class, () -> runner.run());
    }
}
