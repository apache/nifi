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
package org.apache.nifi.confluent.schemaregistry;

import org.apache.nifi.schemaregistry.services.MessageName;
import org.apache.nifi.schemaregistry.services.SchemaDefinition;
import org.apache.nifi.schemaregistry.services.StandardSchemaDefinition;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.stream.Stream;

import static org.apache.nifi.confluent.schemaregistry.VarintUtils.writeZigZagVarint;
import static org.apache.nifi.schemaregistry.services.SchemaDefinition.SchemaType.PROTOBUF;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(MockitoExtension.class)
class ConfluentProtobufMessageNameResolverTest {


    private static final int MAGIC_BYTE_LENGTH = 1;
    private static final int SCHEMA_ID_LENGTH = 4;
    // Schema without package (default package)
    private static final String DEFAULT_PACKAGE_SCHEMA = """
        syntax = "proto3";
        message User {
          int32 id = 1;
          string name = 2;
          Address address = 3;
          message Profile {
            string bio = 1;
            Settings settings = 2;
            message Settings {
              bool notifications = 1;
              string theme = 2;
            }
          }
        }
        message Company {
          string name = 1;
          Address address = 2;
        }
        message Address {
          string street = 1;
          string city = 2;
        }""";

    // Schema with explicit package
    private static final String EXPLICIT_PACKAGE_SCHEMA = """
        syntax = "proto3";
        package com.example.proto;
        message User {
          int32 id = 1;
          string name = 2;
          Address address = 3;
          message Profile {
            string bio = 1;
            Settings settings = 2;
            message Settings {
              bool notifications = 1;
              string theme = 2;
            }
          }
        }
        message Company {
          string name = 1;
          Address address = 2;
        }
        message Address {
          string street = 1;
          string city = 2;
        }""";

    private static final SchemaIdentifier ID = SchemaIdentifier.builder()
        .id(1L)
        .build();

    private static final SchemaDefinition SCHEMA_WITH_DEFAULT_PACKAGE = new StandardSchemaDefinition(
        ID,
        DEFAULT_PACKAGE_SCHEMA,
        PROTOBUF,
        Map.of());
    private static final SchemaDefinition SCHEMA_WITH_EXPLICIT_PACKAGE = new StandardSchemaDefinition(
        ID,
        EXPLICIT_PACKAGE_SCHEMA,
        PROTOBUF,
        Map.of()
    );

    private ConfluentProtobufMessageNameResolver resolver;

    private static Stream<Arguments> provideMessageNameTestCases() {
        return Stream.of(
            // Default package schema tests
            // the format is [input schema], [message indexes as decoded from protobuf header], [expected FQN message name]
            Arguments.of(SCHEMA_WITH_DEFAULT_PACKAGE, new int[] {0}, "User"),
            Arguments.of(SCHEMA_WITH_DEFAULT_PACKAGE, new int[] {1}, "Company"),
            Arguments.of(SCHEMA_WITH_DEFAULT_PACKAGE, new int[] {2}, "Address"),
            Arguments.of(SCHEMA_WITH_DEFAULT_PACKAGE, new int[] {0, 0}, "User.Profile"),
            Arguments.of(SCHEMA_WITH_DEFAULT_PACKAGE, new int[] {0, 0, 0}, "User.Profile.Settings"),

            // Packaged schema tests
            Arguments.of(SCHEMA_WITH_EXPLICIT_PACKAGE, new int[] {0}, "com.example.proto.User"),
            Arguments.of(SCHEMA_WITH_EXPLICIT_PACKAGE, new int[] {1}, "com.example.proto.Company"),
            Arguments.of(SCHEMA_WITH_EXPLICIT_PACKAGE, new int[] {2}, "com.example.proto.Address"),
            Arguments.of(SCHEMA_WITH_EXPLICIT_PACKAGE, new int[] {0, 0}, "com.example.proto.User.Profile"),
            Arguments.of(SCHEMA_WITH_EXPLICIT_PACKAGE, new int[] {0, 0, 0}, "com.example.proto.User.Profile.Settings")
        );
    }

    @BeforeEach
    void setUp() throws Exception {
        resolver = new ConfluentProtobufMessageNameResolver();
        TestRunner testRunner = TestRunners.newTestRunner(NoOpProcessor.class);
        testRunner.addControllerService("messageNameResolver", resolver);
        testRunner.enableControllerService(resolver);

    }

    @ParameterizedTest
    @MethodSource("provideMessageNameTestCases")
    void testGetMessageName(final SchemaDefinition schemaDefinition, final int[] messageIndexes, final String expectedMessageName) throws IOException {
        final InputStream inputStream = createWireFormatData(schemaDefinition, messageIndexes);

        final MessageName messageName = resolver.getMessageName(Map.of(), schemaDefinition, inputStream);

        assertNotNull(messageName);
        assertEquals(expectedMessageName, messageName.getFullyQualifiedName());
    }

    @Test
    void testGetMessageNameWithSpecialCaseZero() throws IOException {
        // Test special case: single 0 byte means first message type for default package
        InputStream inputStream = createWireFormatDataSpecialCase(SCHEMA_WITH_DEFAULT_PACKAGE);
        MessageName messageName = resolver.getMessageName(Map.of(), SCHEMA_WITH_DEFAULT_PACKAGE, inputStream);
        assertNotNull(messageName);
        assertEquals("User", messageName.getFullyQualifiedName());
        assertTrue(messageName.getNamespace().isEmpty());

        inputStream = createWireFormatDataSpecialCase(SCHEMA_WITH_EXPLICIT_PACKAGE);
        messageName = resolver.getMessageName(Map.of(), SCHEMA_WITH_EXPLICIT_PACKAGE, inputStream);
        assertNotNull(messageName);
        assertEquals("com.example.proto.User", messageName.getFullyQualifiedName());
        assertEquals("com.example.proto", messageName.getNamespace().get());
        assertEquals("User", messageName.getName());
    }

    @Test
    void testGetMessageNameEmptyInputStream() {
        final ByteArrayInputStream inputStream = new ByteArrayInputStream(new byte[0]);
        assertThrows(IOException.class, () -> resolver.getMessageName(Map.of(), SCHEMA_WITH_EXPLICIT_PACKAGE, inputStream));
    }

    @Test
    void testGetMessageNameTooShortInputStream() {
        final ByteArrayInputStream inputStream = new ByteArrayInputStream(new byte[] {1}); // Only 4 bytes
        assertThrows(IllegalStateException.class, () -> resolver.getMessageName(Map.of(), SCHEMA_WITH_EXPLICIT_PACKAGE, inputStream));
    }

    @Test
    void testGetMessageNameIndexOutOfBounds() throws IOException {
        // Test with index [99] which should be out of bounds for both schemas

        // Test with default package schema
        final InputStream inputStream1 = createWireFormatData(SCHEMA_WITH_DEFAULT_PACKAGE, new int[] {99});
        assertThrows(IllegalStateException.class, () -> resolver.getMessageName(Map.of(), SCHEMA_WITH_DEFAULT_PACKAGE, inputStream1));

        // Test with explicit package schema
        final InputStream inputStream2 = createWireFormatData(SCHEMA_WITH_EXPLICIT_PACKAGE, new int[] {99});
        assertThrows(IllegalStateException.class, () -> resolver.getMessageName(Map.of(), SCHEMA_WITH_EXPLICIT_PACKAGE, inputStream2));
    }


    /**
     * Creates wire format data according to Confluent specification:
     * [magic_byte:1][schema_id:4][message_indexes:variable][protobuf_payload:rest]
     * <br>
     * <a href="https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format">Confluent specification</a>
     */
    private InputStream createWireFormatData(final SchemaDefinition schemaDefinition, final int[] messageIndexes) throws IOException {
        final ByteArrayOutputStream output = new ByteArrayOutputStream();

        // Magic byte (0x0)
        output.write(0x00);

        // Schema ID (4 bytes, big-endian)
        final ByteBuffer schemaIdBuffer = ByteBuffer.allocate(4);
        schemaIdBuffer.putInt((int) schemaDefinition.getIdentifier().getIdentifier().getAsLong());
        output.write(schemaIdBuffer.array());

        // Message indexes (varint zigzag encoded)
        final byte[] encodedIndexes = encodeMessageIndexes(messageIndexes);
        output.write(encodedIndexes);

        // Optional: Add some dummy protobuf payload (not needed for message name resolution)
        output.write(new byte[] {0x5F, 0x5F, 0x5F, 0x5F, 0x48, 0x45, 0x4C, 0x50, 0x20, 0x4D, 0x45, 0x5F, 0x5F, 0x5F});
        final ByteArrayInputStream payloadStream = new ByteArrayInputStream(output.toByteArray());
        // Skip magic byte, schema ID,
        // Message name resolver is designed to work on a stream positioned exactly
        // at the start of the message index wireformat section (6 th byte)
        final int expectedSkipLength = MAGIC_BYTE_LENGTH + SCHEMA_ID_LENGTH;
        final long actualSkipped = payloadStream.skip(expectedSkipLength);
        if (actualSkipped != expectedSkipLength) {
            throw new IOException("Failed to skip expected bytes: expected " + expectedSkipLength + ", actual " + actualSkipped);
        }
        return payloadStream;
    }

    /**
     * Creates wire format data for the special case (single 0 byte for first message type)
     */
    private InputStream createWireFormatDataSpecialCase(final SchemaDefinition schemaDefinition) throws IOException {
        final ByteArrayOutputStream output = new ByteArrayOutputStream();

        output.write(0x00);

        final ByteBuffer schemaIdBuffer = ByteBuffer.allocate(4);
        schemaIdBuffer.putInt((int) schemaDefinition.getIdentifier().getIdentifier().getAsLong());
        output.write(schemaIdBuffer.array());

        // Special case: single 0 byte
        output.write(0x00);
        final ByteArrayInputStream payloadStream = new ByteArrayInputStream(output.toByteArray());
        final int expectedSkipLength = MAGIC_BYTE_LENGTH + SCHEMA_ID_LENGTH;
        final long actualSkipped = payloadStream.skip(expectedSkipLength);
        if (actualSkipped != expectedSkipLength) {
            throw new IOException("Failed to skip expected bytes: expected " + expectedSkipLength + ", actual " + actualSkipped);
        }
        return payloadStream;
    }

    /**
     * Encodes message indexes using zigzag varint encoding as per Confluent specification
     */
    private byte[] encodeMessageIndexes(final int[] indexes) throws IOException {
        final ByteArrayOutputStream output = new ByteArrayOutputStream();

        // Encode array length as zigzag varint
        output.write(writeZigZagVarint(indexes.length));

        // Encode each index as zigzag varint
        for (final int index : indexes) {
            output.write(writeZigZagVarint(index));
        }

        return output.toByteArray();
    }
}
