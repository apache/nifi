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

import org.apache.commons.lang3.ArrayUtils;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaField;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.schemaregistry.services.MessageName;
import org.apache.nifi.schemaregistry.services.MessageNameResolver;
import org.apache.nifi.schemaregistry.services.SchemaDefinition;
import org.apache.nifi.schemaregistry.services.SchemaReferenceReader;
import org.apache.nifi.schemaregistry.services.SchemaRegistry;
import org.apache.nifi.schemaregistry.services.StandardMessageNameFactory;
import org.apache.nifi.schemaregistry.services.StandardSchemaDefinition;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNullElseGet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Base class for StandardProtobufReader tests containing common functionality
 * and mock services that can be shared across different schema access strategy tests.
 */
public abstract class StandardProtobufReaderTestBase {

    protected static final String STANDARD_PROTOBUF_READER_SERVICE_ID = "standard-protobuf-reader";
    protected static final String MOCK_SCHEMA_REGISTRY_ID = "mock-schema-registry";
    protected static final String MOCK_SCHEMA_REFERENCE_READER_ID = "mock-schema-reference-reader";
    protected static final String MOCK_MESSAGE_NAME_RESOLVER_ID = "mock-message-name-resolver";

    protected static final String PROTO_3_MESSAGE = "Proto3Message";
    protected static final String PROTO_3_SCHEMA = "test_proto3.proto";

    protected TestRunner runner;
    protected StandardProtobufReader standardProtobufReader;
    protected MockSchemaRegistry mockSchemaRegistry;
    protected MockSchemaReferenceReader mockSchemaReferenceReader;
    protected MockMessageNameResolver mockMessageNameResolver;


    @BeforeEach
    void setUp() throws InitializationException {
        runner = TestRunners.newTestRunner(NoOpProcessor.class);
        standardProtobufReader = new StandardProtobufReader();
        runner.addControllerService(STANDARD_PROTOBUF_READER_SERVICE_ID, standardProtobufReader);
        setupMockServices();
    }

    protected void setupMockServices() throws InitializationException {
        mockSchemaRegistry = new MockSchemaRegistry();
        mockSchemaReferenceReader = new MockSchemaReferenceReader();
        mockMessageNameResolver = new MockMessageNameResolver();
        runner.addControllerService(MOCK_SCHEMA_REGISTRY_ID, mockSchemaRegistry);
        runner.addControllerService(MOCK_SCHEMA_REFERENCE_READER_ID, mockSchemaReferenceReader);
        runner.addControllerService(MOCK_MESSAGE_NAME_RESOLVER_ID, mockMessageNameResolver);
    }

    protected void enableAllControllerServices() {
        runner.enableControllerService(mockSchemaRegistry);
        runner.enableControllerService(mockSchemaReferenceReader);
        runner.enableControllerService(mockMessageNameResolver);
        runner.enableControllerService(standardProtobufReader);
    }

    protected RecordReader createRecordReader(final byte[] testData) throws IOException, SchemaNotFoundException {
        return standardProtobufReader.createRecordReader(
            emptyMap(),
            new ByteArrayInputStream(testData),
            testData.length,
            runner.getLogger()
        );
    }

    protected RecordReader createRecordReader() throws IOException, SchemaNotFoundException {
        final byte[] testData = new byte[0];
        return createRecordReader(testData);
    }

    /**
     * Reads the test_proto3.proto file from resources.
     */
    protected String getTestProto3File() {
        try {
            return new String(
                getClass().getClassLoader().getResourceAsStream("test_proto3.proto").readAllBytes()
            );
        } catch (final Exception e) {
            throw new RuntimeException("Failed to read test_proto3.proto from resources", e);
        }
    }

    protected ValidationResult verifyExactlyOneValidationError() {
        final Collection<ValidationResult> results = runner.validate(standardProtobufReader);
        final List<ValidationResult> invalids = results.stream().filter(result -> !result.isValid()).toList();
        if (invalids.size() != 1) {
            fail("Expected exactly one invalid result, but found: " + invalids.size());
        }
        return invalids.getFirst();
    }

    // assertions made on the test_proto3.proto message generated by the ProtoTestUtil.generateInputDataForProto3() method
    protected void runAssertionsOnTestProto3Message(final RecordReader recordReader) throws MalformedRecordException, IOException {
        final RecordSchema schema = recordReader.getSchema();
        assertNotNull(schema);

        // Iterate over record reader and validate actual data
        final Record record = recordReader.nextRecord();
        assertNotNull(record);

        assertTrue(record.getAsBoolean("booleanField"));
        assertEquals("Test text", record.getAsString("stringField"));
        assertEquals(Integer.MAX_VALUE, record.getAsInt("int32Field"));
        assertEquals(Integer.MIN_VALUE, record.getAsInt("sint32Field"));
        final Byte[] bytesValue = (Byte[]) record.getValue("bytesField");
        final String bytesString = new String(ArrayUtils.toPrimitive(bytesValue), UTF_8);

        assertEquals("Test bytes", bytesString);
        assertEquals(Long.MAX_VALUE, record.getAsLong("int64Field"));
        assertEquals(Long.MIN_VALUE, record.getAsLong("sint64Field"));

        // Validate nested message
        final Record nestedMessage = (Record) record.getValue("nestedMessage");
        assertNotNull(nestedMessage, "nestedMessage should not be null");
        assertEquals("ENUM_VALUE_3", nestedMessage.getAsString("testEnum"));

        // Validate nested message2 array
        final Object[] nestedMessage2Value = nestedMessage.getAsArray("nestedMessage2");
        assertNotNull(nestedMessage2Value);


        assertEquals(1, nestedMessage2Value.length);
        final Record nestedMessage2 = (Record) nestedMessage2Value[0];
        assertNotNull(nestedMessage2);

        // Validate map field
        final Map<String, Object> testMap = (Map<String, Object>) nestedMessage2.getValue("testMap");
        assertNotNull(testMap);

        assertEquals(2, testMap.size());
        assertEquals(101, ((Number) testMap.get("test_key_entry1")).intValue());
        assertEquals(202, ((Number) testMap.get("test_key_entry2")).intValue());

        assertNull(recordReader.nextRecord());
    }


    /**
     * Mock implementation of SchemaRegistry for testing purposes.
     */
    protected static class MockSchemaRegistry extends AbstractControllerService implements SchemaRegistry {
        private String schemaText = "";
        private SchemaDefinition schemaDefinition = null;
        private boolean shouldThrowSchemaNotFound = false;

        public void returnSchemaText(final String schemaText) {
            this.schemaText = schemaText;
        }

        public void returnSchemaDefinition(final SchemaDefinition schemaDefinition) {
            this.schemaDefinition = schemaDefinition;
        }

        public void throwSchemaNotFoundWhenCalled(final boolean shouldThrow) {
            this.shouldThrowSchemaNotFound = shouldThrow;
        }

        @Override
        public RecordSchema retrieveSchema(final SchemaIdentifier schemaIdentifier) throws IOException, SchemaNotFoundException {
            throw new UnsupportedOperationException("retrieveSchema is not implemented in MockSchemaRegistry");
        }

        @Override
        public Set<SchemaField> getSuppliedSchemaFields() {
            return emptySet();
        }

        @Override
        public SchemaDefinition retrieveSchemaDefinition(final SchemaIdentifier schemaIdentifier) throws SchemaNotFoundException {
            if (shouldThrowSchemaNotFound) {
                throw new SchemaNotFoundException("Schema not found: " + schemaIdentifier);
            }
            return requireNonNullElseGet(schemaDefinition, () -> new StandardSchemaDefinition(schemaIdentifier, schemaText, SchemaDefinition.SchemaType.PROTOBUF));
        }
    }

    /**
     * Mock implementation of SchemaReferenceReader for testing purposes.
     */
    protected static class MockSchemaReferenceReader extends AbstractControllerService implements SchemaReferenceReader {
        private SchemaIdentifier schemaIdentifier;
        private boolean shouldThrowException = false;

        public void returnSchemaIdentifierWithName(final String schemaName) {
            schemaIdentifier = SchemaIdentifier.builder()
                .name(schemaName)
                .build();
        }

        public void throwExceptionWhenCalled(final boolean shouldThrowException) {
            this.shouldThrowException = shouldThrowException;
        }

        @Override
        public SchemaIdentifier getSchemaIdentifier(final Map<String, String> variables, final InputStream contentStream) throws IOException {
            if (shouldThrowException) {
                throw new IOException("Mock schema reference reader configured to throw exception");
            }
            return schemaIdentifier;
        }

        @Override
        public Set<SchemaField> getSuppliedSchemaFields() {
            return emptySet();
        }
    }

    protected static class MockMessageNameResolver extends AbstractControllerService implements MessageNameResolver {
        private String messageName;

        public void returnMessageName(final String messageName) {
            this.messageName = messageName;
        }

        @Override
        public MessageName getMessageName(final Map<String, String> variables, final SchemaDefinition schemaDefinition, final InputStream contentStream) {
            return StandardMessageNameFactory.fromName(messageName);
        }
    }
}