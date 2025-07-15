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

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schemaregistry.services.MessageName;
import org.apache.nifi.schemaregistry.services.MessageNameResolver;
import org.apache.nifi.schemaregistry.services.SchemaDefinition;
import org.apache.nifi.schemaregistry.services.SchemaReferenceReader;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
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
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_ACCESS_STRATEGY;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_REFERENCE_READER;
import static org.apache.nifi.schema.access.SchemaAccessUtils.SCHEMA_REGISTRY;
import static org.apache.nifi.services.protobuf.ProtoTestUtil.generateInputDataForProto3;
import static org.apache.nifi.services.protobuf.ProtobufReader.MESSAGE_NAME_RESOLVER_CONTROLLER_SERVICE;
import static org.apache.nifi.services.protobuf.ProtobufReader.MESSAGE_NAME_RESOLVER_STRATEGY;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ProtobufReaderWithSchemaRegistryTest {

    private static final String PROTOBUF_READER_SERVICE_ID = "protobuf-reader";
    private static final String SCHEMA_REGISTRY_SERVICE_ID = "schema-registry";
    private static final String SCHEMA_REFERENCE_READER_SERVICE_ID = "schema-reference-reader";
    private static final String MESSAGE_NAME_RESOLVER_SERVICE_ID = "message-name-resolver";

    private TestRunner runner;
    private ProtobufReader protobufReader;
    private MockSchemaRegistry schemaRegistry;
    private MockSchemaReferenceReader schemaReferenceReader;
    private MockMessageNameResolver messageNameResolver;

    @BeforeEach
    void setUp() throws InitializationException {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);

        protobufReader = new ProtobufReader();
        schemaRegistry = new MockSchemaRegistry();
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
    void testCreateRecordReaderWithValidSchemaRegistry() throws Exception {
        // Configure mock services
        schemaRegistry.setSchemaText(getTestProtoSchema());
        messageNameResolver.setMessageName("Proto3Message");

        enableAllServices();

        final InputStream inputStream = generateInputDataForProto3();
        final RecordReader recordReader = protobufReader.createRecordReader(Collections.emptyMap(), inputStream, 100L, runner.getLogger());

        assertNotNull(recordReader);
        assertInstanceOf(ProtobufRecordReader.class, recordReader);

        final Record record = recordReader.nextRecord();
        assertNotNull(record);
        assertEquals(true, record.getValue("booleanField"));
        assertEquals("Test text", record.getValue("stringField"));
    }

    @Test
    void testSchemaDefinitionCaching() throws Exception {
        // Configure mock services
        schemaRegistry.setSchemaText(getTestProtoSchema());
        messageNameResolver.setMessageName("Proto3Message");

        enableAllServices();

        // First call should populate cache
        final int initialRetrieveCount = schemaRegistry.getRetrieveCount();
        final InputStream inputStream1 = generateInputDataForProto3();
        final RecordReader recordReader1 = protobufReader.createRecordReader(Collections.emptyMap(), inputStream1, 100L, runner.getLogger());
        assertNotNull(recordReader1);

        // Second call should use cache
        final InputStream inputStream2 = generateInputDataForProto3();
        final RecordReader recordReader2 = protobufReader.createRecordReader(Collections.emptyMap(), inputStream2, 200L, runner.getLogger());
        assertNotNull(recordReader2);

        // Verify schema was only retrieved once due to caching
        assertEquals(initialRetrieveCount + 1, schemaRegistry.getRetrieveCount());
    }

    @Test
    void testSchemaCompilationError() throws Exception {
        // Configure mock services with invalid schema
        schemaRegistry.setSchemaText("invalid proto syntax that cannot be compiled");
        messageNameResolver.setMessageName("Proto3Message");

        enableAllServices();

        final InputStream inputStream = generateInputDataForProto3();
        final Exception exception = assertThrows(RuntimeException.class, () -> protobufReader.createRecordReader(Collections.emptyMap(), inputStream, 100L, runner.getLogger()));
        assertTrue(exception.getMessage().contains("Failed to compile Protobuf schema for identifier"), "Expected: Failed to compile Protobuf schema for identifier");
    }

    @Test
    void testSchemaNotFoundError() throws Exception {
        // Configure mock services to throw exception
        schemaRegistry.setThrowOnRetrieve(true);
        messageNameResolver.setMessageName("Proto3Message");

        enableAllServices();

        // Test that schema not found error is handled properly
        final InputStream inputStream = generateInputDataForProto3();
        final Exception exception = assertThrows(RuntimeException.class, () -> protobufReader.createRecordReader(Collections.emptyMap(), inputStream, 100L, runner.getLogger()));
        assertTrue(exception.getMessage().contains("Could not retrieve schema for identifier"), "Expected: Could not retrieve schema for identifier");
    }

    @Test
    void testPackagedMessageHandling() throws Exception {
        // Configure mock services with packaged message
        schemaRegistry.setSchemaText(getPackagedProtoSchema());
        messageNameResolver.setMessageName("com.example.TestMessage");

        enableAllServices();

        final InputStream inputStream = generateInputDataForProto3();
        final RecordReader recordReader = protobufReader.createRecordReader(Collections.emptyMap(), inputStream, 100L, runner.getLogger());
        assertNotNull(recordReader);

        final Record record = recordReader.nextRecord();
        assertNotNull(record);
        assertEquals(true, record.getValue("booleanField"));
        assertEquals("Test text", record.getValue("stringField"));

    }

    @Test
    void testMultipleDifferentSchemas() throws Exception {
        schemaRegistry.setSchemaText(getTestProtoSchema());
        messageNameResolver.setMessageName("Proto3Message");

        enableAllServices();

        final InputStream inputStream1 = generateInputDataForProto3();
        final RecordReader recordReader1 = protobufReader.createRecordReader(Collections.emptyMap(), inputStream1, 100L, runner.getLogger());
        assertNotNull(recordReader1);

        // Reconfigure mocks for a different schema to test it's going to be cached separately
        schemaReferenceReader.setSchemaIdentifier(
            SchemaIdentifier.builder()
                .name("different-schema.proto")
                .version(2)
                .build());
        schemaRegistry.setSchemaText(getPackagedProtoSchema());
        messageNameResolver.setMessageName("com.example.TestMessage");

        // Second schema should be retrieved and cached separately
        final int retrieveCountBefore = schemaRegistry.getRetrieveCount();
        final InputStream inputStream2 = generateInputDataForProto3();
        final RecordReader recordReader2 = protobufReader.createRecordReader(Collections.emptyMap(), inputStream2, 100L, runner.getLogger());
        assertNotNull(recordReader2);


        // Verify that a new schema was retrieved for the different identifier
        assertEquals(retrieveCountBefore + 1, schemaRegistry.getRetrieveCount());
    }

    @Test
    void testOnDisabledClearsCache() throws Exception {
        schemaRegistry.setSchemaText(getTestProtoSchema());
        messageNameResolver.setMessageName("Proto3Message");

        enableAllServices();


        final InputStream inputStream = generateInputDataForProto3();
        final RecordReader recordReader = protobufReader.createRecordReader(Collections.emptyMap(), inputStream, 100L, runner.getLogger());
        assertNotNull(recordReader);


        // Verify caches are initialized
        assertNotNull(protobufReader.schemaDefinitionCache);
        assertNotNull(protobufReader.compiledSchemaCache);
        assertEquals(1, protobufReader.schemaDefinitionCache.estimatedSize());
        assertEquals(1, protobufReader.compiledSchemaCache.estimatedSize());

        // Disable the service - this should clear the caches
        runner.disableControllerService(protobufReader);

        // Verify caches are cleared (they should be empty after disable)
        assertEquals(0, protobufReader.schemaDefinitionCache.estimatedSize());
        assertEquals(0, protobufReader.compiledSchemaCache.estimatedSize());
    }

    private void enableAllServices() throws InitializationException {
        runner.enableControllerService(schemaRegistry);
        runner.enableControllerService(schemaReferenceReader);
        runner.enableControllerService(messageNameResolver);
        runner.enableControllerService(protobufReader);
        runner.assertValid(protobufReader);
    }

    private String getTestProtoSchema() {
        return """
            syntax = "proto3";
            message Proto3Message {
                bool booleanField = 1;
                string stringField = 2;
                int32 int32Field = 3;
                uint32 uint32Field = 4;
                sint32 sint32Field = 5;
                fixed32 fixed32Field = 6;
                sfixed32 sfixed32Field = 7;
                double doubleField = 8;
                float floatField = 9;
                bytes bytesField = 10;
                int64 int64Field = 11;
                uint64 uint64Field = 12;
                sint64 sint64Field = 13;
                fixed64 fixed64Field = 14;
                sfixed64 sfixed64Field = 15;
                NestedMessage nestedMessage = 16;
            }
                        
            message NestedMessage {
                TestEnum testEnum = 1;
                map<string, int32> testMap = 2;
            }
                        
            enum TestEnum {
                ENUM_VALUE_1 = 0;
                ENUM_VALUE_2 = 1;
                ENUM_VALUE_3 = 2;
            }
            """;
    }

    private String getPackagedProtoSchema() {
        return """
            syntax = "proto3";
            package com.example;
            message TestMessage {
                bool booleanField = 1;
                string stringField = 2;
            }
            """;
    }


    private static class MockSchemaRegistry extends AbstractControllerService implements SchemaRegistry {
        private String schemaText;
        private boolean throwOnRetrieve = false;
        private int retrieveCount = 0;

        public void setSchemaText(final String schemaText) {
            this.schemaText = schemaText;
        }

        public void setThrowOnRetrieve(final boolean throwOnRetrieve) {
            this.throwOnRetrieve = throwOnRetrieve;
        }

        public int getRetrieveCount() {
            return retrieveCount;
        }

        @Override
        public RecordSchema retrieveSchema(final SchemaIdentifier schemaIdentifier) throws IOException, SchemaNotFoundException {
            throw new UnsupportedOperationException("Not implemented for this test");
        }

        @Override
        public SchemaDefinition retrieveSchemaRaw(final SchemaIdentifier schemaIdentifier) throws IOException, SchemaNotFoundException {
            retrieveCount++;
            if (throwOnRetrieve) {
                throw new IOException("Test exception - schema not found");
            }
            return new MockSchemaDefinition(schemaIdentifier, schemaText);
        }

        @Override
        public Set<SchemaField> getSuppliedSchemaFields() {
            return Collections.emptySet();
        }
    }

    private static class MockSchemaReferenceReader extends AbstractControllerService implements SchemaReferenceReader {
        private SchemaIdentifier schemaIdentifier = SchemaIdentifier.builder()
            .name("test-schema.proto")
            .version(1)
            .build();

        public void setSchemaIdentifier(final SchemaIdentifier schemaIdentifier) {
            this.schemaIdentifier = schemaIdentifier;
        }

        @Override
        public SchemaIdentifier getSchemaIdentifier(final Map<String, String> variables, final InputStream contentStream) throws IOException {
            return schemaIdentifier;
        }

        @Override
        public Set<SchemaField> getSuppliedSchemaFields() {
            return Collections.emptySet();
        }
    }

    private static class MockMessageNameResolver extends AbstractControllerService implements MessageNameResolver {
        private String messageName = "Proto3Message";

        public void setMessageName(final String messageName) {
            this.messageName = messageName;
        }

        @Override
        public MessageName getMessageName(final SchemaDefinition schemaDefinition, final InputStream contentStream) throws IOException {
            return new MockMessageName(messageName);
        }
    }

    private record MockSchemaDefinition(SchemaIdentifier identifier, String text) implements SchemaDefinition {

        @Override
        public Map<String, SchemaDefinition> getReferences() {
            return Collections.emptyMap();
        }

        @Override
        public SchemaType getSchemaType() {
            return SchemaType.PROTOBUF;
        }
    }

    private record MockMessageName(String messageName) implements MessageName {

        @Override
        public String getName() {
            final int lastDot = messageName.lastIndexOf('.');
            if (lastDot > 0) {
                return messageName.substring(lastDot + 1);
            }
            return messageName;
        }

        @Override
        public Optional<String> getNamespace() {
            final int lastDot = messageName.lastIndexOf('.');
            if (lastDot > 0) {
                return Optional.of(messageName.substring(0, lastDot));
            }
            return Optional.empty();
        }
    }
} 