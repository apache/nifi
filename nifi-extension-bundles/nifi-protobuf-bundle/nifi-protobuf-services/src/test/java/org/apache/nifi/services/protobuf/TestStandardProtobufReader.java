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

import com.google.protobuf.DynamicMessage;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schemaregistry.services.SchemaDefinition;
import org.apache.nifi.schemaregistry.services.StandardSchemaDefinition;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

import static com.google.protobuf.Descriptors.Descriptor;
import static com.google.protobuf.Descriptors.DescriptorValidationException;
import static java.util.Collections.emptyMap;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_NAME;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_NAME_PROPERTY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_REFERENCE_READER;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_REFERENCE_READER_PROPERTY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_REGISTRY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_TEXT;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_TEXT_PROPERTY;
import static org.apache.nifi.services.protobuf.ProtoTestUtil.generateInputDataForProto3;
import static org.apache.nifi.services.protobuf.ProtobufSchemaValidator.validateSchemaDefinitionIdentifiers;
import static org.apache.nifi.services.protobuf.StandardProtobufReader.MESSAGE_NAME;
import static org.apache.nifi.services.protobuf.StandardProtobufReader.MESSAGE_NAME_RESOLUTION_STRATEGY;
import static org.apache.nifi.services.protobuf.StandardProtobufReader.MESSAGE_NAME_RESOLVER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestStandardProtobufReader extends StandardProtobufReaderTestBase {

    @BeforeEach
    void beforeEach() {
        runner.setProperty(standardProtobufReader, SCHEMA_ACCESS_STRATEGY, SCHEMA_REFERENCE_READER_PROPERTY);
        runner.setProperty(standardProtobufReader, SCHEMA_REGISTRY, MOCK_SCHEMA_REGISTRY_ID);
        runner.setProperty(standardProtobufReader, SCHEMA_REFERENCE_READER, MOCK_SCHEMA_REFERENCE_READER_ID);
        runner.setProperty(standardProtobufReader, MESSAGE_NAME_RESOLUTION_STRATEGY, StandardProtobufReader.MessageNameResolverStrategy.MESSAGE_NAME_RESOLVER);
        runner.setProperty(standardProtobufReader, MESSAGE_NAME_RESOLVER, MOCK_MESSAGE_NAME_RESOLVER_ID);
    }

    @Test
    void testStandardProtobufReaderWithTestProto3Schema() throws Exception {
        final String testSchema = getTestProto3File();
        mockSchemaRegistry.returnSchemaText(testSchema);
        mockSchemaReferenceReader.returnSchemaIdentifierWithName(PROTO_3_SCHEMA);
        mockMessageNameResolver.returnMessageName(PROTO_3_MESSAGE);
        enableAllControllerServices();

        final InputStream testDataStream = generateInputDataForProto3();
        final RecordReader recordReader = standardProtobufReader.createRecordReader(emptyMap(), testDataStream, 0L, runner.getLogger());

        assertNotNull(recordReader);
        runAssertionsOnTestProto3Message(recordReader);
    }


    @Test
    void testSchemaNotFoundRethrown() {
        mockSchemaRegistry.throwSchemaNotFoundWhenCalled(true);
        enableAllControllerServices();

        assertThrows(SchemaNotFoundException.class, this::createRecordReader);
    }

    @Test
    void testSchemaReferenceReaderException() {
        mockSchemaRegistry.returnSchemaText(getTestProto3File());
        mockSchemaReferenceReader.throwExceptionWhenCalled(true);

        enableAllControllerServices();

        assertThrows(IOException.class, this::createRecordReader);
    }

    @Test
    void testCreateRecordReaderWithSchemaReferences() throws Exception {
        // Setup schema registry to return schema with references using existing mock
        final String userProfileSchema = loadUserProfileSchema();
        final String userSettingsSchema = loadUserSettingsSchema();

        // Create schema definition with references
        final SchemaDefinition userProfileDef = createSchemaDefinition("user_profile.proto", userProfileSchema, new HashMap<>());
        // root message here is user_settings.proto, it has a reference to user_profile.proto
        final SchemaDefinition userSettingsDef = createSchemaDefinition("user_settings.proto", userSettingsSchema, Map.of("user_profile.proto", userProfileDef));

        mockSchemaRegistry.returnSchemaDefinition(userSettingsDef);
        mockSchemaReferenceReader.returnSchemaIdentifierWithName("user_settings");
        mockMessageNameResolver.returnMessageName("org.apache.nifi.protobuf.test.UserSettings");
        enableAllControllerServices();

        // Create test data using DynamicMessage
        final InputStream inputStream = generateInputDataForUserSettings();
        final RecordReader recordReader = standardProtobufReader.createRecordReader(emptyMap(), inputStream, 0L, runner.getLogger());

        assertNotNull(recordReader);

        final Record userSettings = recordReader.nextRecord();
        assertNotNull(userSettings);
        assertEquals("en-US", userSettings.getAsString("language"));
        assertTrue(userSettings.getAsBoolean("two_factor_enabled"));

        final Record profileRecord = (Record) userSettings.getValue("profile");
        assertNotNull(profileRecord);
        assertEquals("user123", profileRecord.getAsString("user_id"));
        assertEquals(30, profileRecord.getAsInt("age"));
    }

    @Test
    void testValidSchemaDefinitionWithProtoExtension() {
        final SchemaDefinition validReferencedSchema = createSchemaDefinition("user_profile.proto");
        final SchemaDefinition validMainSchema = createSchemaDefinition("user_settings.proto", Map.of("user_profile.proto", validReferencedSchema));

        enableAllControllerServices();

        validateSchemaDefinitionIdentifiers(validMainSchema, true);
    }

    @Test
    void testRootSchemaIsAllowedToHaveInvalidName() {
        final SchemaDefinition validReferencedSchema = createSchemaDefinition("user_profile.proto");
        final SchemaDefinition validSchema = createSchemaDefinition("user_settings.noproto.extension", Map.of("user_profile.proto", validReferencedSchema));

        enableAllControllerServices();

        validateSchemaDefinitionIdentifiers(validSchema, true);
    }

    @Test
    void testValidMainSchemaWithInvalidReferencedSchema() {
        final SchemaDefinition invalidReferencedSchema = createSchemaDefinition("user_profile.invalid");
        final SchemaDefinition mixedSchema = createSchemaDefinition("user_settings.proto",
            Map.of("user_profile.invalid", invalidReferencedSchema));

        enableAllControllerServices();

        final IllegalArgumentException referencedException = assertThrows(IllegalArgumentException.class, () -> {
            validateSchemaDefinitionIdentifiers(mixedSchema, true);
        });

        assertTrue(referencedException.getMessage().contains("ends with .proto extension"));
    }

    @Test
    void testSchemaDefinitionWithMissingName() {
        final SchemaDefinition schemaWithoutName = createSchemaDefinitionWithoutName();
        enableAllControllerServices();
        validateSchemaDefinitionIdentifiers(schemaWithoutName, true);
    }

    @Nested
    class WithSchemaNamePropertyAccessStrategy {

        @BeforeEach
        void beforeEach() {
            runner.setProperty(standardProtobufReader, SCHEMA_ACCESS_STRATEGY, SCHEMA_NAME_PROPERTY);
            runner.setProperty(standardProtobufReader, MESSAGE_NAME, PROTO_3_MESSAGE);

        }

        @Test
        void testStandardProtobufReaderWithTestProto3Schema() throws Exception {
            runner.setProperty(standardProtobufReader, SCHEMA_NAME, PROTO_3_SCHEMA);
            TestStandardProtobufReader.this.testStandardProtobufReaderWithTestProto3Schema();
        }

        @Test
        void testSchemaNotFoundRethrown() {
            TestStandardProtobufReader.this.testSchemaNotFoundRethrown();
        }

        @Test
        void testCreateRecordReaderWithSchemaReferences() throws Exception {
            runner.setProperty(standardProtobufReader, SCHEMA_NAME, "user_settings");
            TestStandardProtobufReader.this.testCreateRecordReaderWithSchemaReferences();
        }
    }

    @Nested
    class WithSchemaTextAccessStrategy {

        @BeforeEach
        void beforeEach() {
            runner.setProperty(standardProtobufReader, SCHEMA_ACCESS_STRATEGY, SCHEMA_TEXT_PROPERTY);
            runner.setProperty(standardProtobufReader, SCHEMA_TEXT, getTestProto3File());

        }

        @Test
        void testStandardProtobufReaderWithTestProto3Schema() throws Exception {
            TestStandardProtobufReader.this.testStandardProtobufReaderWithTestProto3Schema();
        }
    }

    private SchemaDefinition createSchemaDefinition(final String name) {
        return createSchemaDefinition(name, "test schema content", new HashMap<>());
    }

    private SchemaDefinition createSchemaDefinition(final String name, final Map<String, SchemaDefinition> references) {
        return createSchemaDefinition(name, "test schema content", references);
    }

    private SchemaDefinition createSchemaDefinition(final String name, final String schemaText, final Map<String, SchemaDefinition> references) {
        return new StandardSchemaDefinition(
            SchemaIdentifier.builder()
                .name(name)
                .id(Long.MAX_VALUE)
                .build(),
            schemaText,
            SchemaDefinition.SchemaType.PROTOBUF,
            references
        );
    }

    private SchemaDefinition createSchemaDefinitionWithoutName() {
        return new StandardSchemaDefinition(
            SchemaIdentifier.builder()
                .id(1L)
                .build(),
            "test schema content",
            SchemaDefinition.SchemaType.PROTOBUF,
            new HashMap<>()
        );
    }

    private InputStream generateInputDataForUserSettings() throws IOException, DescriptorValidationException {
        // Load descriptor container from .desc file containing both UserProfile and UserSettings
        final ProtoTestUtil.DescriptorContainer descriptorContainer = ProtoTestUtil.loadDescriptorContainer("org/apache/nifi/protobuf/test/user_settings.desc");

        final Descriptor userProfileDescriptor = descriptorContainer.findMessageTypeByName("UserProfile");
        final Descriptor userSettingsDescriptor = descriptorContainer.findMessageTypeByName("UserSettings");

        // Create UserProfile message
        final DynamicMessage userProfile = DynamicMessage.newBuilder(userProfileDescriptor)
            .setField(userProfileDescriptor.findFieldByName("user_id"), "user123")
            .setField(userProfileDescriptor.findFieldByName("age"), 30)
            .build();

        // Create UserSettings message with embedded UserProfile
        final DynamicMessage userSettings = DynamicMessage.newBuilder(userSettingsDescriptor)
            .setField(userSettingsDescriptor.findFieldByName("profile"), userProfile)
            .setField(userSettingsDescriptor.findFieldByName("language"), "en-US")
            .setField(userSettingsDescriptor.findFieldByName("two_factor_enabled"), true)
            .build();

        return userSettings.toByteString().newInput();
    }

    private String loadProtoSchema(final String resourcePath) throws IOException {
        try (final InputStream inputStream = getClass().getClassLoader().getResourceAsStream(resourcePath)) {
            if (inputStream == null) {
                throw new IOException("Could not find " + resourcePath + " in classpath");
            }
            return new String(inputStream.readAllBytes());
        }
    }

    private String loadUserProfileSchema() throws IOException {
        return loadProtoSchema("org/apache/nifi/protobuf/test/user_profile.proto");
    }

    private String loadUserSettingsSchema() throws IOException {
        return loadProtoSchema("org/apache/nifi/protobuf/test/user_settings.proto");
    }
}