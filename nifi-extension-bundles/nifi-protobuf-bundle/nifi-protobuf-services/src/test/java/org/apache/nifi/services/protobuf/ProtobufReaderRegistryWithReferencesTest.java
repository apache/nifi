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
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schemaregistry.services.MessageName;
import org.apache.nifi.schemaregistry.services.MessageNameResolver;
import org.apache.nifi.schemaregistry.services.SchemaDefinition;
import org.apache.nifi.schemaregistry.services.SchemaDefinition.SchemaType;
import org.apache.nifi.schemaregistry.services.SchemaReferenceReader;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.schemaregistry.services.StandardMessageName;
import org.apache.nifi.schemaregistry.services.StandardSchemaDefinition;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.protobuf.DescriptorProtos.DescriptorProto;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.newBuilder;
import static com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import static com.google.protobuf.Descriptors.Descriptor;
import static com.google.protobuf.Descriptors.DescriptorValidationException;
import static com.google.protobuf.Descriptors.FileDescriptor;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_REFERENCE_READER;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_REGISTRY;
import static org.apache.nifi.services.protobuf.ProtobufReader.MESSAGE_NAME_RESOLVER_CONTROLLER_SERVICE;
import static org.apache.nifi.services.protobuf.ProtobufReader.MESSAGE_NAME_RESOLVER_STRATEGY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ProtobufReaderRegistryWithReferencesTest {

    private static final String PROTOBUF_READER_SERVICE_ID = "protobuf-reader";
    private static final String SCHEMA_REGISTRY_SERVICE_ID = "schema-registry";
    private static final String SCHEMA_REFERENCE_READER_SERVICE_ID = "schema-reference-reader";
    private static final String MESSAGE_NAME_RESOLVER_SERVICE_ID = "message-name-resolver";
    private static final String BASE_TEST_PATH = "src/test/resources/";

    private TestRunner runner;
    private ProtobufReader protobufReader;
    private MockSchemaRegistryWithReferences schemaRegistry;
    private MockSchemaReferenceReader schemaReferenceReader;
    private MockMessageNameResolver messageNameResolver;

    private static String loadUserProfileSchema() throws IOException {
        Path path = Paths.get(BASE_TEST_PATH + "org/apache/nifi/protobuf/test/user_profile.proto");
        return Files.readString(path);
    }

    private static String loadUserSettingsSchema() throws IOException {
        Path path = Paths.get(BASE_TEST_PATH + "org/apache/nifi/protobuf/test/user_settings.proto");
        return Files.readString(path);
    }

    @BeforeEach
    void setUp() throws InitializationException {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);

        protobufReader = new ProtobufReader();
        schemaRegistry = new MockSchemaRegistryWithReferences();
        schemaReferenceReader = new MockSchemaReferenceReader();
        messageNameResolver = new MockMessageNameResolver();

        runner.addControllerService(PROTOBUF_READER_SERVICE_ID, protobufReader);
        runner.addControllerService(SCHEMA_REGISTRY_SERVICE_ID, schemaRegistry);
        runner.addControllerService(SCHEMA_REFERENCE_READER_SERVICE_ID, schemaReferenceReader);
        runner.addControllerService(MESSAGE_NAME_RESOLVER_SERVICE_ID, messageNameResolver);

        // Configure ProtobufReader to use schema reference reader strategy
        runner.setProperty(protobufReader, SCHEMA_ACCESS_STRATEGY, "schema-reference-reader");
        runner.setProperty(protobufReader, SCHEMA_REGISTRY, SCHEMA_REGISTRY_SERVICE_ID);
        runner.setProperty(protobufReader, SCHEMA_REFERENCE_READER, SCHEMA_REFERENCE_READER_SERVICE_ID);
        runner.setProperty(protobufReader, MESSAGE_NAME_RESOLVER_STRATEGY, "message-name-resolver-service");
        runner.setProperty(protobufReader, MESSAGE_NAME_RESOLVER_CONTROLLER_SERVICE, MESSAGE_NAME_RESOLVER_SERVICE_ID);
    }

    @Test
    void testCreateRecordReaderWithSchemaReferences() throws Exception {

        enableAllServices();

        InputStream inputStream = generateInputDataForUserSettings();
        RecordReader recordReader = protobufReader.createRecordReader(Collections.emptyMap(), inputStream, 100L, runner.getLogger());

        assertNotNull(recordReader);
        assertInstanceOf(ProtobufRecordReader.class, recordReader);

        Record record = recordReader.nextRecord();
        assertNotNull(record);

        // Verify record contains expected fields
        Record profileRecord = (Record) record.getValue("profile");
        assertNotNull(profileRecord);
        assertEquals("user123", profileRecord.getValue("user_id"));
        assertEquals("en-US", record.getValue("language"));
        assertEquals(true, record.getValue("two_factor_enabled"));
    }

    private void enableAllServices() {
        runner.enableControllerService(schemaRegistry);
        runner.enableControllerService(schemaReferenceReader);
        runner.enableControllerService(messageNameResolver);
        runner.enableControllerService(protobufReader);
        runner.assertValid(protobufReader);
    }



    @Test
    void testSchemaDefinitionValidationDirect() throws Exception {
        // Test valid schema definitions with .proto extension
        SchemaDefinition validReferencedSchema = createSchemaDefinition("user_profile.proto");
        SchemaDefinition validMainSchema = createSchemaDefinition("user_settings.proto", 
            Map.of("user_profile.proto", validReferencedSchema));
        
        // This should not throw an exception
        ProtobufReader reader = new ProtobufReader();
        reader.validateSchemaDefinitionIdentifiers(validMainSchema);
        
        // Test invalid schema definitions without .proto extension
        SchemaDefinition invalidReferencedSchema = createSchemaDefinition("user_profile.invalid");
        SchemaDefinition invalidMainSchema = createSchemaDefinition("user_settings.wrong", 
            Map.of("user_profile.invalid", invalidReferencedSchema));
        
        // This should throw an IllegalArgumentException for the main schema
        IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
            reader.validateSchemaDefinitionIdentifiers(invalidMainSchema);
        });
        
        assertTrue(exception.getMessage().contains("must end with .proto extension"));
        
        // Test with valid main schema but invalid referenced schema
        SchemaDefinition mixedSchema = createSchemaDefinition("user_settings.proto", 
            Map.of("user_profile.invalid", invalidReferencedSchema));
        
        // This should throw an IllegalArgumentException for the referenced schema
        IllegalArgumentException referencedException = assertThrows(IllegalArgumentException.class, () -> {
            reader.validateSchemaDefinitionIdentifiers(mixedSchema);
        });
        
        assertTrue(referencedException.getMessage().contains("must end with .proto extension"));
        
        // Test with missing name (empty Optional)
        SchemaDefinition schemaWithoutName = createSchemaDefinitionWithoutName();
        
        // This should throw an IllegalArgumentException for missing name
        IllegalArgumentException missingNameException = assertThrows(IllegalArgumentException.class, () -> {
            reader.validateSchemaDefinitionIdentifiers(schemaWithoutName);
        });
        
        assertTrue(missingNameException.getMessage().contains("Schema identifier must have a name"));
    }

    /**
     * Helper method to create a StandardSchemaDefinition with the given name and no references.
     */
    private static SchemaDefinition createSchemaDefinition(String name) {
        return createSchemaDefinition(name, "test schema content", new HashMap<>());
    }

    /**
     * Helper method to create a StandardSchemaDefinition with the given name and references.
     */
    private static SchemaDefinition createSchemaDefinition(String name, Map<String, SchemaDefinition> references) {
        return createSchemaDefinition(name, "test schema content", references);
    }

    /**
     * Helper method to create a StandardSchemaDefinition with the given name, schema text, and references.
     */
    private static SchemaDefinition createSchemaDefinition(String name, String schemaText, Map<String, SchemaDefinition> references) {
        return new StandardSchemaDefinition(
            SchemaIdentifier.builder()
                .name(name)
                .id((long) (Math.random() * 1000))
                .version((int) (Math.random() * 10) + 1)
                .build(),
            schemaText,
            SchemaType.PROTOBUF,
            references
        );
    }

    /**
     * Helper method to create a StandardSchemaDefinition without a name for testing validation.
     */
    private SchemaDefinition createSchemaDefinitionWithoutName() {
        return new StandardSchemaDefinition(
            SchemaIdentifier.builder()
                .id((long) (Math.random() * 1000))
                .version((int) (Math.random() * 10) + 1)
                .build(),
            "test schema content",
            SchemaType.PROTOBUF,
            new HashMap<>()
        );
    }

    private InputStream generateInputDataForUserSettings() throws DescriptorValidationException {
        // Create UserProfile message first
        FileDescriptorProto userProfileProto = FileDescriptorProto.newBuilder()
            .setName("user_profile.proto")
            .setPackage("org.apache.nifi.protobuf.test")
            .addMessageType(DescriptorProto.newBuilder()
                .setName("UserProfile")
                .addField(newBuilder()
                    .setName("user_id")
                    .setNumber(1)
                    .setType(Type.TYPE_STRING)
                    .build())
                .addField(newBuilder()
                    .setName("age")
                    .setNumber(4)
                    .setType(Type.TYPE_INT32)
                    .build())
                .build())
            .build();

        // Create UserSettings message that references UserProfile
        FileDescriptorProto userSettingsProto = FileDescriptorProto.newBuilder()
            .setName("user_settings.proto")
            .setPackage("org.apache.nifi.protobuf.test")
            .addDependency("user_profile.proto")
            .addMessageType(DescriptorProto.newBuilder()
                .setName("UserSettings")
                .addField(newBuilder()
                    .setName("profile")
                    .setNumber(1)
                    .setType(Type.TYPE_MESSAGE)
                    .setTypeName(".org.apache.nifi.protobuf.test.UserProfile")
                    .build())
                .addField(newBuilder()
                    .setName("language")
                    .setNumber(5)
                    .setType(Type.TYPE_STRING)
                    .build())
                .addField(newBuilder()
                    .setName("two_factor_enabled")
                    .setNumber(7)
                    .setType(Type.TYPE_BOOL)
                    .build())
                .build())
            .build();

        FileDescriptor userProfileFileDescriptor = FileDescriptor.buildFrom(userProfileProto, new FileDescriptor[0]);
        FileDescriptor userSettingsFileDescriptor = FileDescriptor.buildFrom(userSettingsProto, new FileDescriptor[] {userProfileFileDescriptor});

        Descriptor userProfileDescriptor = userProfileFileDescriptor.findMessageTypeByName("UserProfile");
        Descriptor userSettingsDescriptor = userSettingsFileDescriptor.findMessageTypeByName("UserSettings");

        // Create UserProfile message
        DynamicMessage userProfile = DynamicMessage.newBuilder(userProfileDescriptor)
            .setField(userProfileDescriptor.findFieldByName("user_id"), "user123")
            .setField(userProfileDescriptor.findFieldByName("age"), 30)
            .build();

        // Create UserSettings message with embedded UserProfile
        DynamicMessage userSettings = DynamicMessage.newBuilder(userSettingsDescriptor)
            .setField(userSettingsDescriptor.findFieldByName("profile"), userProfile)
            .setField(userSettingsDescriptor.findFieldByName("language"), "en-US")
            .setField(userSettingsDescriptor.findFieldByName("two_factor_enabled"), true)
            .build();

        return userSettings.toByteString().newInput();
    }

    private static class MockSchemaRegistryWithReferences extends AbstractControllerService implements SchemaRegistry {

        @Override
        public RecordSchema retrieveSchema(SchemaIdentifier schemaIdentifier) {
            throw new UnsupportedOperationException("Not implemented for this test");
        }

        // Schema registry always returns schema definition with one reference
        @Override
        public SchemaDefinition retrieveSchemaRaw(SchemaIdentifier schemaIdentifier) throws IOException {
            SchemaDefinition userProfile = createSchemaDefinition("user_profile.proto", loadUserProfileSchema(), new HashMap<>());
            SchemaDefinition userSettings = createSchemaDefinition("user_settings.proto", loadUserSettingsSchema(), 
                Map.of("user_profile.proto", userProfile));
            return userSettings;
        }

        @Override
        public Set<SchemaField> getSuppliedSchemaFields() {
            return Collections.emptySet();
        }
    }



    private static class MockSchemaReferenceReader extends AbstractControllerService implements SchemaReferenceReader {


        @Override
        public SchemaIdentifier getSchemaIdentifier(Map<String, String> variables, InputStream contentStream) {
            return SchemaIdentifier.builder()
                .name("user_settings")
                .version(1)
                .build();
        }

        @Override
        public Set<SchemaField> getSuppliedSchemaFields() {
            return Collections.emptySet();
        }
    }

    private static class MockMessageNameResolver extends AbstractControllerService implements MessageNameResolver {
        @Override
        public MessageName getMessageName(SchemaDefinition schemaDefinition, InputStream contentStream) {
            return new StandardMessageName(Optional.of("org.apache.nifi.protobuf.test"), "UserSettings");
        }
    }


}