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
import static org.apache.nifi.services.protobuf.StandardProtobufReader.MESSAGE_NAME;
import static org.apache.nifi.services.protobuf.StandardProtobufReader.MESSAGE_NAME_RESOLUTION_STRATEGY;
import static org.apache.nifi.services.protobuf.StandardProtobufReader.MESSAGE_NAME_RESOLVER;
import static org.apache.nifi.services.protobuf.StandardProtobufReader.MessageNameResolverStrategy.MESSAGE_NAME_PROPERTY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestStandardProtobufReaderPropertyValidation extends StandardProtobufReaderTestBase {

    @BeforeEach
    void beforeEach() {
        runner.setProperty(standardProtobufReader, SCHEMA_ACCESS_STRATEGY, SCHEMA_REFERENCE_READER_PROPERTY);
        runner.setProperty(standardProtobufReader, SCHEMA_REGISTRY, MOCK_SCHEMA_REGISTRY_ID);
        runner.setProperty(standardProtobufReader, SCHEMA_REFERENCE_READER, MOCK_SCHEMA_REFERENCE_READER_ID);
        runner.setProperty(standardProtobufReader, MESSAGE_NAME_RESOLUTION_STRATEGY, StandardProtobufReader.MessageNameResolverStrategy.MESSAGE_NAME_RESOLVER);
        runner.setProperty(standardProtobufReader, MESSAGE_NAME_RESOLVER, MOCK_MESSAGE_NAME_RESOLVER_ID);
        // Ensure configuration is valid before running tests
        runner.assertValid(standardProtobufReader);
    }

    @Test
    void testInvalidWithoutMessageNameProperty() {
        runner.setProperty(standardProtobufReader, MESSAGE_NAME_RESOLUTION_STRATEGY, MESSAGE_NAME_PROPERTY);
        runner.removeProperty(standardProtobufReader, MESSAGE_NAME);
        final ValidationResult invalidResult = verifyExactlyOneValidationError();

        assertEquals("Message Name is required", invalidResult.getExplanation());
    }

    @Test
    void testInvalidWithoutMessageNameResolver() {
        runner.setProperty(standardProtobufReader, MESSAGE_NAME_RESOLUTION_STRATEGY, StandardProtobufReader.MessageNameResolverStrategy.MESSAGE_NAME_RESOLVER);
        runner.removeProperty(standardProtobufReader, MESSAGE_NAME_RESOLVER);
        final ValidationResult invalidResult = verifyExactlyOneValidationError();

        assertTrue(invalidResult.getExplanation().contains("Message Name Resolver"));
    }

    @Nested
    class SchemaTextPropertyAccessStrategy {
        @BeforeEach
        void beforeEach() {
            runner.setProperty(standardProtobufReader, SCHEMA_ACCESS_STRATEGY, SCHEMA_TEXT_PROPERTY);
            // Ensure configuration is valid before running tests
            runner.assertValid(standardProtobufReader);
        }

        @Test
        void testValidWhenSchemaTextPropertyRemoved() { // default value of ${proto.schema}
            runner.removeProperty(standardProtobufReader, SCHEMA_TEXT);
            enableAllControllerServices();
            runner.assertValid(standardProtobufReader);
        }
    }

    @Nested
    class SchemaNamePropertyAccessStrategy {
        @BeforeEach
        void beforeEach() {
            runner.setProperty(standardProtobufReader, SCHEMA_ACCESS_STRATEGY, SCHEMA_NAME_PROPERTY);
            runner.setProperty(standardProtobufReader, SCHEMA_NAME, "Any schema name");
            runner.setProperty(standardProtobufReader, SCHEMA_BRANCH_NAME, "Any branch");
            // Ensure configuration is valid before running tests
            runner.assertValid(standardProtobufReader);
        }

        @Test
        void testValidWithMessageNameResolver() { // default setting from beforeEach
            enableAllControllerServices();
            runner.assertValid(standardProtobufReader);

        }

        @Test
        void testValidWithMessageNameProperty() {
            runner.setProperty(standardProtobufReader, MESSAGE_NAME_RESOLUTION_STRATEGY, MESSAGE_NAME_PROPERTY);
            runner.setProperty(standardProtobufReader, MESSAGE_NAME, "Any message name");
            enableAllControllerServices();
            runner.assertValid(standardProtobufReader);
        }

        @Test
        void testValidWithoutVersionAndBranch() {
            runner.removeProperty(standardProtobufReader, SCHEMA_VERSION);
            runner.removeProperty(standardProtobufReader, SCHEMA_BRANCH_NAME);
            enableAllControllerServices();
            runner.assertValid(standardProtobufReader);
        }

        @Test
        void testInvalidWhenBranchAndVersionSetTogether() {
            runner.setProperty(standardProtobufReader, SCHEMA_VERSION, "1");
            runner.setProperty(standardProtobufReader, SCHEMA_BRANCH_NAME, "Any branch");

            final ValidationResult invalidResult = verifyExactlyOneValidationError();

            assertTrue(invalidResult.getExplanation().contains("Schema Branch"));
        }

        @Test
        void testInvalidWithoutSchemaRegistry() {
            runner.removeProperty(standardProtobufReader, SCHEMA_REGISTRY);
            final ValidationResult invalidResult = verifyExactlyOneValidationError();

            assertTrue(invalidResult.getExplanation().contains("Schema Name"));
        }

        @Test
        void testValidWithoutSchemaName() { // default property value gets set automatically
            runner.removeProperty(standardProtobufReader, SCHEMA_NAME);
            runner.assertValid(standardProtobufReader);
        }
    }

    @Nested
    class SchemaReferenceReaderSchemaAccessStrategy {

        @BeforeEach
        void beforeEach() {
            runner.setProperty(standardProtobufReader, SCHEMA_ACCESS_STRATEGY, SCHEMA_REFERENCE_READER_PROPERTY);
            runner.setProperty(standardProtobufReader, SCHEMA_REGISTRY, MOCK_SCHEMA_REGISTRY_ID);
            runner.setProperty(standardProtobufReader, SCHEMA_REFERENCE_READER, MOCK_SCHEMA_REFERENCE_READER_ID);
            // Ensure configuration is valid before running tests
            runner.assertValid(standardProtobufReader);
        }

        @Test
        void testValidWithMessageNameResolver() { // default setting from beforeEach
            enableAllControllerServices();
            runner.assertValid(standardProtobufReader);

        }

        @Test
        void testValidWithMessageNameProperty() {
            runner.setProperty(standardProtobufReader, MESSAGE_NAME_RESOLUTION_STRATEGY, MESSAGE_NAME_PROPERTY);
            runner.setProperty(standardProtobufReader, MESSAGE_NAME, "Any message name");
            enableAllControllerServices();
            runner.assertValid(standardProtobufReader);
        }

        @Test
        void testInvalidWithoutSchemaRegistry() {
            runner.removeProperty(standardProtobufReader, SCHEMA_REGISTRY);
            final ValidationResult invalidResult = verifyExactlyOneValidationError();

            assertTrue(invalidResult.getExplanation().contains("Schema Reference Reader"));
        }

        @Test
        void testInvalidWithoutSchemaReferenceReader() {
            runner.removeProperty(standardProtobufReader, SCHEMA_REFERENCE_READER);
            final ValidationResult invalidResult = verifyExactlyOneValidationError();

            assertTrue(invalidResult.getExplanation().contains("Schema Reference Reader"));
        }
    }
}