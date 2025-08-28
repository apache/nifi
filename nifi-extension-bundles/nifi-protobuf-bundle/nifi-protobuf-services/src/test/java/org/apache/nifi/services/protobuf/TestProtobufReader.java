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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_BRANCH_NAME;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_NAME;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_REFERENCE_READER;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_REGISTRY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_TEXT;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_VERSION;
import static org.apache.nifi.services.protobuf.ProtobufReader.MESSAGE_TYPE;
import static org.apache.nifi.services.protobuf.ProtobufReader.PROTOBUF_DIRECTORY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestProtobufReader {

    private static final String PROTOBUF_READER_SERVICE_ID = "protobuf-reader";
    private static final String PROTO3_MESSAGE_TYPE = "Proto3Message";
    private static final String REPEATED_PROTO3_MESSAGE_TYPE = "RootMessage";
    private static final String PROTO2_MESSAGE_TYPE = "Proto2Message";

    private TestRunner runner;
    private ProtobufReader protobufReader;

    @BeforeEach
    void setUp() throws InitializationException {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        protobufReader = new ProtobufReader();
        runner.addControllerService(PROTOBUF_READER_SERVICE_ID, protobufReader);
    }


    private static Stream<Arguments> validConfigurationProvider() {
        return Stream.of(
            Arguments.of("test_proto3.proto", PROTO3_MESSAGE_TYPE),
            Arguments.of("test_repeated_proto3.proto", REPEATED_PROTO3_MESSAGE_TYPE),
            Arguments.of("test_proto2.proto", PROTO2_MESSAGE_TYPE)
        );
    }

    @ParameterizedTest
    @MethodSource("validConfigurationProvider")
    void testValidConfigurations(final String protoFileName, final String messageType) throws IOException {
        final Path testTempDir = createTempDirWithProtoFile(protoFileName);
        runner.setProperty(protobufReader, PROTOBUF_DIRECTORY, testTempDir.toString());
        runner.setProperty(protobufReader, MESSAGE_TYPE, messageType);
        runner.enableControllerService(protobufReader);

        runner.assertValid(protobufReader);
    }

    @Test
    void testInvalidConfigurationMissingDirectory() {
        runner.setProperty(protobufReader, MESSAGE_TYPE, PROTO3_MESSAGE_TYPE);

        final Collection<ValidationResult> results = runner.validate(protobufReader);
        assertFalse(results.stream().allMatch(ValidationResult::isValid));

        final ValidationResult invalidResult = findFirstInvalid(results);
        assertNotNull(invalidResult);
        assertEquals("Proto Directory is required", invalidResult.getExplanation());
    }

    @Test
    void testInvalidConfigurationMissingMessageType() throws IOException {
        final Path testTempDir = createTempDirWithProtoFile("test_proto3.proto");
        runner.setProperty(protobufReader, PROTOBUF_DIRECTORY, testTempDir.toString());

        final Collection<ValidationResult> results = runner.validate(protobufReader);
        assertFalse(results.stream().allMatch(ValidationResult::isValid));

        final ValidationResult invalidResult = findFirstInvalid(results);
        assertNotNull(invalidResult);
        assertEquals("Message Type is required", invalidResult.getExplanation());
    }

    @Test
    void testInvalidConfigurationNonExistentDirectory() throws IOException {
        final Path testTempDir = createTempDirWithProtoFile("test_proto3.proto");
        final String nonExistentDir = testTempDir.resolve("non-existent").toString();
        runner.setProperty(protobufReader, PROTOBUF_DIRECTORY, nonExistentDir);
        runner.setProperty(protobufReader, MESSAGE_TYPE, PROTO3_MESSAGE_TYPE);

        final Collection<ValidationResult> results = runner.validate(protobufReader);
        assertFalse(results.stream().allMatch(ValidationResult::isValid));

        final ValidationResult invalidResult = findFirstInvalid(results);
        assertNotNull(invalidResult);
        assertTrue(invalidResult.getExplanation().contains("Directory does not exist"));
    }

    @Test
    void testCustomValidationWithInvalidMessageType() throws IOException {
        final Path testTempDir = createTempDirWithProtoFile("test_proto3.proto");
        runner.setProperty(protobufReader, PROTOBUF_DIRECTORY, testTempDir.toString());
        runner.setProperty(protobufReader, MESSAGE_TYPE, "NonExistentMessage");

        final Collection<ValidationResult> results = runner.validate(protobufReader);
        assertFalse(results.stream().allMatch(ValidationResult::isValid));

        final ValidationResult invalidResult = findFirstInvalid(results);

        assertNotNull(invalidResult);
        assertTrue(invalidResult.getExplanation().contains("message type cannot be found"));
    }

    @Test
    void testPropertyDescriptorsContainRequiredProperties() {
        final List<PropertyDescriptor> descriptors = List.of(
            SCHEMA_ACCESS_STRATEGY,
            SCHEMA_REGISTRY,
            SCHEMA_NAME,
            SCHEMA_VERSION,
            SCHEMA_BRANCH_NAME,
            SCHEMA_TEXT,
            SCHEMA_REFERENCE_READER,
            PROTOBUF_DIRECTORY,
            MESSAGE_TYPE
        );
        assertEquals(descriptors, protobufReader.getSupportedPropertyDescriptors());
    }

    @Test
    void testCreateRecordReaderWithValidConfiguration() throws Exception {
        final Path testTempDir = createTempDirWithProtoFile("test_proto3.proto");
        runner.setProperty(protobufReader, PROTOBUF_DIRECTORY, testTempDir.toString());
        runner.setProperty(protobufReader, MESSAGE_TYPE, PROTO3_MESSAGE_TYPE);
        runner.enableControllerService(protobufReader);

        try (final InputStream inputStream = ProtoTestUtil.generateInputDataForProto3()) {
            final RecordReader recordReader = protobufReader.createRecordReader(Collections.emptyMap(), inputStream, 100L, runner.getLogger());

            assertNotNull(recordReader);

            final Record record = recordReader.nextRecord();
            assertNotNull(record);
            assertEquals(true, record.getValue("booleanField"));
            assertEquals("Test text", record.getValue("stringField"));
        }
    }

    @Test
    void testCreateRecordReaderWithRepeatedFields() throws Exception {
        final Path testTempDir = createTempDirWithProtoFile("test_repeated_proto3.proto");
        runner.setProperty(protobufReader, PROTOBUF_DIRECTORY, testTempDir.toString());
        runner.setProperty(protobufReader, MESSAGE_TYPE, REPEATED_PROTO3_MESSAGE_TYPE);
        runner.enableControllerService(protobufReader);

        try (final InputStream inputStream = ProtoTestUtil.generateInputDataForRepeatedProto3()) {
            final RecordReader recordReader = protobufReader.createRecordReader(Collections.emptyMap(), inputStream, 100L, runner.getLogger());
            assertNotNull(recordReader);
            final Record record = recordReader.nextRecord();
            assertNotNull(record);
            assertNotNull(record.getValue("repeatedMessage"));
        }
    }


    @Test
    void testValidationWithCircularReferenceProto() throws IOException {
        final Path testTempDir = createTempDirWithProtoFile("test_circular_reference.proto");
        runner.setProperty(protobufReader, PROTOBUF_DIRECTORY, testTempDir.toString());
        runner.setProperty(protobufReader, MESSAGE_TYPE, "A");

        final Collection<ValidationResult> results = runner.validate(protobufReader);
        assertTrue(results.stream().allMatch(ValidationResult::isValid));
    }


    private Path createTempDirWithProtoFile(final String protoFileName) throws IOException {
        final Path testTempDir = Files.createTempDirectory("proto-test-");
        try (final InputStream resourceStream = getClass().getClassLoader().getResourceAsStream(protoFileName)) {
            if (resourceStream != null) {
                Files.copy(resourceStream, testTempDir.resolve(protoFileName), StandardCopyOption.REPLACE_EXISTING);
            }
        }
        return testTempDir;
    }

    private ValidationResult findFirstInvalid(final Collection<ValidationResult> results) {
        return results.stream().filter(result -> !result.isValid()).findFirst().orElse(null);
    }
}