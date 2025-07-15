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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_BRANCH_NAME;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_NAME;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_REFERENCE_READER;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_REGISTRY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_TEXT;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_VERSION;
import static org.apache.nifi.services.protobuf.ProtobufReader.MESSAGE_NAME_RESOLVER_CONTROLLER_SERVICE;
import static org.apache.nifi.services.protobuf.ProtobufReader.MESSAGE_NAME_RESOLVER_STRATEGY;
import static org.apache.nifi.services.protobuf.ProtobufReader.MESSAGE_TYPE;
import static org.apache.nifi.services.protobuf.ProtobufReader.PROTOBUF_DIRECTORY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
public class ProtobufReaderTest {

    private static final String PROTOBUF_READER_SERVICE_ID = "protobuf-reader";
    private static final String PROTO3_MESSAGE_TYPE = "Proto3Message";
    private static final String REPEATED_PROTO3_MESSAGE_TYPE = "RootMessage";
    private static final String PROTO2_MESSAGE_TYPE = "Proto2Message";
    private static final String INVALID_MESSAGE_TYPE = "NonExistentMessage";

    private TestRunner runner;
    private ProtobufReader protobufReader;

    @BeforeEach
    void setUp() throws InitializationException {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        protobufReader = new ProtobufReader();
        runner.addControllerService(PROTOBUF_READER_SERVICE_ID, protobufReader);
    }


    @Test
    void testValidConfigurationWithProto3() throws IOException {
        Path testTempDir = createTempDirWithProtoFile("test_proto3.proto");
        runner.setProperty(protobufReader, PROTOBUF_DIRECTORY, testTempDir.toString());
        runner.setProperty(protobufReader, MESSAGE_TYPE, PROTO3_MESSAGE_TYPE);

        runner.enableControllerService(protobufReader);
        runner.assertValid(protobufReader);
    }

    @Test
    void testValidConfigurationWithRepeatedProto3() throws IOException {
        Path testTempDir = createTempDirWithProtoFile("test_repeated_proto3.proto");
        runner.setProperty(protobufReader, PROTOBUF_DIRECTORY, testTempDir.toString());
        runner.setProperty(protobufReader, MESSAGE_TYPE, REPEATED_PROTO3_MESSAGE_TYPE);

        runner.enableControllerService(protobufReader);
        runner.assertValid(protobufReader);
    }

    @Test
    void testValidConfigurationWithProto2() throws IOException {
        Path testTempDir = createTempDirWithProtoFile("test_proto2.proto");
        runner.setProperty(protobufReader, PROTOBUF_DIRECTORY, testTempDir.toString());
        runner.setProperty(protobufReader, MESSAGE_TYPE, PROTO2_MESSAGE_TYPE);

        runner.enableControllerService(protobufReader);
        runner.assertValid(protobufReader);
    }

    @Test
    void testInvalidConfigurationMissingDirectory() {
        runner.setProperty(protobufReader, MESSAGE_TYPE, PROTO3_MESSAGE_TYPE);

        Collection<ValidationResult> results = runner.validate(protobufReader);
        assertFalse(results.stream().allMatch(ValidationResult::isValid));

        ValidationResult invalidResult = findFirstInvalid(results);
        assertNotNull(invalidResult);
        assertTrue(invalidResult.getExplanation().contains("Proto Directory must be specified "));
    }

    @Test
    void testInvalidConfigurationMissingMessageType() throws IOException {
        Path testTempDir = createTempDirWithProtoFile("test_proto3.proto");
        runner.setProperty(protobufReader, PROTOBUF_DIRECTORY, testTempDir.toString());

        Collection<ValidationResult> results = runner.validate(protobufReader);
        assertFalse(results.stream().allMatch(ValidationResult::isValid));

        ValidationResult invalidResult = findFirstInvalid(results);
        assertNotNull(invalidResult);
        assertTrue(invalidResult.getExplanation().contains("Message Type must be specified"));
    }

    @Test
    void testInvalidConfigurationNonExistentDirectory() throws IOException {
        Path testTempDir = createTempDirWithProtoFile("test_proto3.proto");
        String nonExistentDir = testTempDir.resolve("non-existent").toString();
        runner.setProperty(protobufReader, PROTOBUF_DIRECTORY, nonExistentDir);
        runner.setProperty(protobufReader, MESSAGE_TYPE, PROTO3_MESSAGE_TYPE);

        Collection<ValidationResult> results = runner.validate(protobufReader);
        assertFalse(results.stream().allMatch(ValidationResult::isValid));

        ValidationResult invalidResult = findFirstInvalid(results);
        assertNotNull(invalidResult);
        assertTrue(invalidResult.getExplanation().contains("Directory does not exist"));
    }

    @Test
    void testCustomValidationWithValidMessageType() throws IOException {
        Path testTempDir = createTempDirWithProtoFile("test_proto3.proto");
        runner.setProperty(protobufReader, PROTOBUF_DIRECTORY, testTempDir.toString());
        runner.setProperty(protobufReader, MESSAGE_TYPE, PROTO3_MESSAGE_TYPE);

        Collection<ValidationResult> results = runner.validate(protobufReader);
        assertTrue(results.stream().allMatch(ValidationResult::isValid));
    }

    @Test
    void testCustomValidationWithInvalidMessageType() throws IOException {
        Path testTempDir = createTempDirWithProtoFile("test_proto3.proto");
        runner.setProperty(protobufReader, PROTOBUF_DIRECTORY, testTempDir.toString());
        runner.setProperty(protobufReader, MESSAGE_TYPE, INVALID_MESSAGE_TYPE);

        Collection<ValidationResult> results = runner.validate(protobufReader);
        assertFalse(results.stream().allMatch(ValidationResult::isValid));

        ValidationResult invalidResult = findFirstInvalid(results);

        assertNotNull(invalidResult);
        assertTrue(invalidResult.getExplanation().contains("message type cannot be found"));
    }

    @Test
    void testPropertyDescriptorsContainRequiredProperties() {
        Set<PropertyDescriptor> descriptors = Set.of(
            SCHEMA_ACCESS_STRATEGY,
            SCHEMA_REGISTRY,
            SCHEMA_NAME,
            SCHEMA_VERSION,
            SCHEMA_BRANCH_NAME,
            SCHEMA_TEXT,
            SCHEMA_REFERENCE_READER,
            PROTOBUF_DIRECTORY,
            MESSAGE_NAME_RESOLVER_STRATEGY,
            MESSAGE_TYPE,
            MESSAGE_NAME_RESOLVER_CONTROLLER_SERVICE
        );
        assertTrue(protobufReader.getSupportedPropertyDescriptors().containsAll(descriptors));
    }

    @Test
    void testProtobufDirectoryPropertyConfiguration() {
        assertNotNull(PROTOBUF_DIRECTORY);
        assertEquals("Proto Directory", PROTOBUF_DIRECTORY.getName());
        assertEquals("Proto Directory", PROTOBUF_DIRECTORY.getDisplayName());
        assertFalse(PROTOBUF_DIRECTORY.isRequired());
        assertTrue(PROTOBUF_DIRECTORY.isExpressionLanguageSupported());
    }

    @Test
    void testMessageTypePropertyConfiguration() {
        assertNotNull(MESSAGE_TYPE);
        assertEquals("Message Type", MESSAGE_TYPE.getName());
        assertEquals("Message Type", MESSAGE_TYPE.getDisplayName());
        assertFalse(MESSAGE_TYPE.isRequired());
        assertTrue(MESSAGE_TYPE.isExpressionLanguageSupported());
    }

    @Test
    void testCreateRecordReaderWithValidConfiguration() throws Exception {
        Path testTempDir = createTempDirWithProtoFile("test_proto3.proto");
        runner.setProperty(protobufReader, PROTOBUF_DIRECTORY, testTempDir.toString());
        runner.setProperty(protobufReader, MESSAGE_TYPE, PROTO3_MESSAGE_TYPE);
        runner.enableControllerService(protobufReader);

        try (InputStream inputStream = ProtoTestUtil.generateInputDataForProto3()) {
            RecordReader recordReader = protobufReader.createRecordReader(Collections.emptyMap(), inputStream, 100L, runner.getLogger());

            assertNotNull(recordReader);
            assertInstanceOf(ProtobufRecordReader.class, recordReader);

            Record record = recordReader.nextRecord();
            assertNotNull(record);
            assertEquals(true, record.getValue("booleanField"));
            assertEquals("Test text", record.getValue("stringField"));
        }
    }

    @Test
    void testCreateRecordReaderWithRepeatedFields() throws Exception {
        Path testTempDir = createTempDirWithProtoFile("test_repeated_proto3.proto");
        runner.setProperty(protobufReader, PROTOBUF_DIRECTORY, testTempDir.toString());
        runner.setProperty(protobufReader, MESSAGE_TYPE, REPEATED_PROTO3_MESSAGE_TYPE);
        runner.enableControllerService(protobufReader);

        try (InputStream inputStream = ProtoTestUtil.generateInputDataForRepeatedProto3()) {
            RecordReader recordReader = protobufReader.createRecordReader(Collections.emptyMap(), inputStream, 100L, runner.getLogger());

            assertNotNull(recordReader);
            Record record = recordReader.nextRecord();
            assertNotNull(record);
            assertNotNull(record.getValue("repeatedMessage"));
        }
    }

    @Test
    void testSchemaAccessStrategyDefaultValue() {
        assertEquals("generate-from-proto-file", protobufReader.getDefaultSchemaAccessStrategy().getValue());
    }


    @Test
    void testOnEnabledInitializesSchema() throws Exception {
        Path testTempDir = createTempDirWithProtoFile("test_proto3.proto");
        runner.setProperty(protobufReader, PROTOBUF_DIRECTORY, testTempDir.toString());
        runner.setProperty(protobufReader, MESSAGE_TYPE, PROTO3_MESSAGE_TYPE);

        // Before enabling, internal state should not be initialized
        assertNull(protobufReader.messageType);
        assertNull(protobufReader.protoSchema);

        runner.enableControllerService(protobufReader);

        // After enabling, internal state should be initialized
        assertEquals(PROTO3_MESSAGE_TYPE, protobufReader.messageType);
        assertNotNull(protobufReader.protoSchema);
    }

    @Test
    void testValidationResourceCaching() throws Exception {
        Path testTempDir = createTempDirWithProtoFile("test_proto3.proto");
        runner.setProperty(protobufReader, PROTOBUF_DIRECTORY, testTempDir.toString());
        runner.setProperty(protobufReader, MESSAGE_TYPE, PROTO3_MESSAGE_TYPE);

        // First validation should create and cache the validation resource
        Collection<ValidationResult> results1 = runner.validate(protobufReader);
        assertTrue(results1.stream().allMatch(ValidationResult::isValid));

        // Get the cached validation resource after first validation
        Object validationResource1 = protobufReader.validationResourceHolder.get();
        assertNotNull(validationResource1, "Validation resource should be cached after first validation");

        // Second validation should reuse the cached resource
        Collection<ValidationResult> results2 = runner.validate(protobufReader);
        assertTrue(results2.stream().allMatch(ValidationResult::isValid));

        // Get the cached validation resource after second validation
        Object validationResource2 = protobufReader.validationResourceHolder.get();
        assertNotNull(validationResource2, "Validation resource should still be cached after second validation");

        // Verify that the same cached resource instance is reused
        assertSame(validationResource1, validationResource2, "Same validation resource should be reused between validations");
    }

    @Test
    void testValidationResourceCacheInvalidation() throws Exception {
        Path testTempDir1 = createTempDirWithProtoFile("test_proto3.proto");
        Path testTempDir2 = createTempDirWithProtoFile("test_proto2.proto");

        // First validation with first directory
        runner.setProperty(protobufReader, PROTOBUF_DIRECTORY, testTempDir1.toString());
        runner.setProperty(protobufReader, MESSAGE_TYPE, PROTO3_MESSAGE_TYPE);
        Collection<ValidationResult> results1 = runner.validate(protobufReader);
        assertTrue(results1.stream().allMatch(ValidationResult::isValid));

        Object validationResource1 = protobufReader.validationResourceHolder.get();
        assertNotNull(validationResource1, "Validation resource should be cached after first validation");

        // Change to second directory - this should invalidate the cache
        runner.setProperty(protobufReader, PROTOBUF_DIRECTORY, testTempDir2.toString());
        runner.setProperty(protobufReader, MESSAGE_TYPE, PROTO2_MESSAGE_TYPE);
        Collection<ValidationResult> results2 = runner.validate(protobufReader);
        assertTrue(results2.stream().allMatch(ValidationResult::isValid));

        Object validationResource2 = protobufReader.validationResourceHolder.get();
        assertNotNull(validationResource2, "Validation resource should be cached after validation with new directory");

        // Verify that cache was invalidated - different resource instance should be used
        assertNotSame(validationResource1, validationResource2, "Validation resource should be different when directory changes");
    }

    @Test
    void testExpressionLanguageSupport() {
        // Test that the properties support expression language
        assertTrue(PROTOBUF_DIRECTORY.isExpressionLanguageSupported());
        assertTrue(MESSAGE_TYPE.isExpressionLanguageSupported());

        // Verify the expression language scope
        assertEquals(org.apache.nifi.expression.ExpressionLanguageScope.ENVIRONMENT, PROTOBUF_DIRECTORY.getExpressionLanguageScope());
        assertEquals(org.apache.nifi.expression.ExpressionLanguageScope.ENVIRONMENT, MESSAGE_TYPE.getExpressionLanguageScope());
    }

    @Test
    void testValidationWithCircularReferenceProto() throws IOException {
        Path testTempDir = createTempDirWithProtoFile("test_circular_reference.proto");
        runner.setProperty(protobufReader, PROTOBUF_DIRECTORY, testTempDir.toString());
        runner.setProperty(protobufReader, MESSAGE_TYPE, "A");

        Collection<ValidationResult> results = runner.validate(protobufReader);
        assertTrue(results.stream().allMatch(ValidationResult::isValid));
    }

    @Test
    void testSchemaReferenceReaderStrategyRequiresSchemaReferenceReader() {
        runner.setProperty(protobufReader, SCHEMA_ACCESS_STRATEGY, "schema-reference-reader");
        runner.setProperty(protobufReader, MESSAGE_NAME_RESOLVER_STRATEGY, "message-name-resolver-service");

        Collection<ValidationResult> results = runner.validate(protobufReader);
        assertFalse(results.stream().allMatch(ValidationResult::isValid));

        boolean hasSchemaRegistryError = results.stream()
            .anyMatch(result -> !result.isValid() && 
                (result.getExplanation().contains("Schema Reference Reader")));
        assertTrue(hasSchemaRegistryError, "Should have validation error for missing schema reference reader configuration");
    }

    @Test
    void testSchemaReferenceReaderValidationRequiresMessageNameResolver() {
        runner.setProperty(protobufReader, SCHEMA_ACCESS_STRATEGY, "schema-reference-reader");
        runner.setProperty(protobufReader, MESSAGE_NAME_RESOLVER_STRATEGY, "message-name-resolver-service");

        Collection<ValidationResult> results = runner.validate(protobufReader);
        assertFalse(results.stream().allMatch(ValidationResult::isValid));

        boolean hasMessageNameResolverError = results.stream()
            .anyMatch(result -> !result.isValid() && 
                (result.getExplanation().contains("Message Name Resolver") ));
        assertTrue(hasMessageNameResolverError, "Should have validation error for missing message name resolver configuration");
    }

    private Path createTempDirWithProtoFile(String protoFileName) throws IOException {
        Path testTempDir = Files.createTempDirectory("proto-test-");
        try (InputStream resourceStream = getClass().getClassLoader().getResourceAsStream(protoFileName)) {
            if (resourceStream != null) {
                Files.copy(resourceStream, testTempDir.resolve(protoFileName), StandardCopyOption.REPLACE_EXISTING);
            }
        }
        return testTempDir;
    }

    private @Nullable ValidationResult findFirstInvalid(final Collection<ValidationResult> results) {
        return results.stream().filter(result -> !result.isValid()).findFirst().orElse(null);
    }

}