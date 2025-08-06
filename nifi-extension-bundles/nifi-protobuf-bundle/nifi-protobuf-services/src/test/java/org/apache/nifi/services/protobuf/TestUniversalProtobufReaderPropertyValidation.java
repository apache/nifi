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
package org.apache.nifi.services.protobuf;

import org.apache.nifi.components.ValidationResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_BRANCH_NAME;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_NAME;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_NAME_PROPERTY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_REFERENCE_READER;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_REFERENCE_READER_PROPERTY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_REGISTRY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_TEXT;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_TEXT_PROPERTY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_VERSION;
import static org.apache.nifi.services.protobuf.UniversalProtobufReader.MESSAGE_NAME;
import static org.apache.nifi.services.protobuf.UniversalProtobufReader.MESSAGE_NAME_RESOLVER_CONTROLLER_SERVICE;
import static org.apache.nifi.services.protobuf.UniversalProtobufReader.MESSAGE_NAME_RESOLVER_STRATEGY;
import static org.apache.nifi.services.protobuf.UniversalProtobufReader.MessageNameResolverStrategyName.MESSAGE_NAME_PROPERTY;
import static org.apache.nifi.services.protobuf.UniversalProtobufReader.MessageNameResolverStrategyName.MESSAGE_NAME_RESOLVER_SERVICE;
import static org.junit.jupiter.api.Assertions.assertEquals;

class TestUniversalProtobufReaderPropertyValidation extends UniversalProtobufReaderTestBase {

    @BeforeEach
    void beforeEach() {
        runner.setProperty(universalProtobufReader, SCHEMA_ACCESS_STRATEGY, SCHEMA_REFERENCE_READER_PROPERTY);
        runner.setProperty(universalProtobufReader, SCHEMA_REGISTRY, MOCK_SCHEMA_REGISTRY_ID);
        runner.setProperty(universalProtobufReader, SCHEMA_REFERENCE_READER, MOCK_SCHEMA_REFERENCE_READER_ID);
        runner.setProperty(universalProtobufReader, MESSAGE_NAME_RESOLVER_STRATEGY, MESSAGE_NAME_RESOLVER_SERVICE);
        runner.setProperty(universalProtobufReader, MESSAGE_NAME_RESOLVER_CONTROLLER_SERVICE, MOCK_MESSAGE_NAME_RESOLVER_ID);
        // Ensure configuration is valid before running tests
        runner.assertValid(universalProtobufReader);
    }

    @Test
    void testInvalidWithoutMessageNameProperty() {
        runner.setProperty(universalProtobufReader, MESSAGE_NAME_RESOLVER_STRATEGY, MESSAGE_NAME_PROPERTY);
        runner.removeProperty(universalProtobufReader, MESSAGE_NAME);
        final ValidationResult invalidResult = verifyExactlyOneValidationError();

        assertEquals("Message Name is required", invalidResult.getExplanation());
    }

    @Test
    void testInvalidWithoutMessageNameResolverService() {
        runner.setProperty(universalProtobufReader, MESSAGE_NAME_RESOLVER_STRATEGY, MESSAGE_NAME_RESOLVER_SERVICE);
        runner.removeProperty(universalProtobufReader, MESSAGE_NAME_RESOLVER_CONTROLLER_SERVICE);
        final ValidationResult invalidResult = verifyExactlyOneValidationError();

        assertEquals("Message Name Resolver Service is required", invalidResult.getExplanation());
    }

    @Nested
    class SchemaTextPropertyAccessStrategy {
        @BeforeEach
        void beforeEach() {
            runner.setProperty(universalProtobufReader, SCHEMA_ACCESS_STRATEGY, SCHEMA_TEXT_PROPERTY);
            // Ensure configuration is valid before running tests
            runner.assertValid(universalProtobufReader);
        }

        @Test
        void testValidWhenSchemaTextPropertyRemoved() { // default value of ${proto.schema}
            runner.removeProperty(universalProtobufReader, SCHEMA_TEXT);
            enableAllControllerServices();
            runner.assertValid(universalProtobufReader);
        }
    }

    @Nested
    class SchemaNamePropertyAccessStrategy {
        @BeforeEach
        void beforeEach() {
            runner.setProperty(universalProtobufReader, SCHEMA_ACCESS_STRATEGY, SCHEMA_NAME_PROPERTY);
            runner.setProperty(universalProtobufReader, SCHEMA_NAME, "Any schema name");
            runner.setProperty(universalProtobufReader, SCHEMA_BRANCH_NAME, "Any branch");
            // Ensure configuration is valid before running tests
            runner.assertValid(universalProtobufReader);
        }

        @Test
        void testValidWithMessageNameResolverService() { // default setting from beforeEach
            enableAllControllerServices();
            runner.assertValid(universalProtobufReader);

        }

        @Test
        void testValidWithMessageNameProperty() {
            runner.setProperty(universalProtobufReader, MESSAGE_NAME_RESOLVER_STRATEGY, MESSAGE_NAME_PROPERTY);
            runner.setProperty(universalProtobufReader, MESSAGE_NAME, "Any message name");
            enableAllControllerServices();
            runner.assertValid(universalProtobufReader);
        }

        @Test
        void testValidWithoutVersionAndBranch() {
            runner.removeProperty(universalProtobufReader, SCHEMA_VERSION);
            runner.removeProperty(universalProtobufReader, SCHEMA_BRANCH_NAME);
            enableAllControllerServices();
            runner.assertValid(universalProtobufReader);
        }

        @Test
        void testInvalidWhenBranchAndVersionSetTogether() {
            runner.setProperty(universalProtobufReader, SCHEMA_VERSION, "1");
            runner.setProperty(universalProtobufReader, SCHEMA_BRANCH_NAME, "Any branch");

            final ValidationResult invalidResult = verifyExactlyOneValidationError();

            assertEquals("Schema Branch and Schema Version cannot be specified together", invalidResult.getExplanation());
        }

        @Test
        void testInvalidWithoutSchemaRegistry() {
            runner.removeProperty(universalProtobufReader, SCHEMA_REGISTRY);
            final ValidationResult invalidResult = verifyExactlyOneValidationError();

            assertEquals("Schema Registry is required", invalidResult.getExplanation());
        }

        @Test
        void testValidWithoutSchemaName() { // default property value gets set automatically
            runner.removeProperty(universalProtobufReader, SCHEMA_NAME);
            runner.assertValid(universalProtobufReader);
        }
    }

    @Nested
    class SchemaReferenceReaderSchemaAccessStrategy {

        @BeforeEach
        void beforeEach() {
            runner.setProperty(universalProtobufReader, SCHEMA_ACCESS_STRATEGY, SCHEMA_REFERENCE_READER_PROPERTY);
            runner.setProperty(universalProtobufReader, SCHEMA_REGISTRY, MOCK_SCHEMA_REGISTRY_ID);
            runner.setProperty(universalProtobufReader, SCHEMA_REFERENCE_READER, MOCK_SCHEMA_REFERENCE_READER_ID);
            // Ensure configuration is valid before running tests
            runner.assertValid(universalProtobufReader);
        }

        @Test
        void testValidWithMessageNameResolverService() { // default setting from beforeEach
            enableAllControllerServices();
            runner.assertValid(universalProtobufReader);

        }

        @Test
        void testValidWithMessageNameProperty() {
            runner.setProperty(universalProtobufReader, MESSAGE_NAME_RESOLVER_STRATEGY, MESSAGE_NAME_PROPERTY);
            runner.setProperty(universalProtobufReader, MESSAGE_NAME, "Any message name");
            enableAllControllerServices();
            runner.assertValid(universalProtobufReader);
        }

        @Test
        void testInvalidWithoutSchemaRegistry() {
            runner.removeProperty(universalProtobufReader, SCHEMA_REGISTRY);
            final ValidationResult invalidResult = verifyExactlyOneValidationError();

            assertEquals("Schema Registry is required", invalidResult.getExplanation());
        }

        @Test
        void testInvalidWithoutSchemaReferenceReader() {
            runner.removeProperty(universalProtobufReader, SCHEMA_REFERENCE_READER);
            final ValidationResult invalidResult = verifyExactlyOneValidationError();

            assertEquals("Schema Reference Reader is required", invalidResult.getExplanation());
        }
    }
}