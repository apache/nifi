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

import org.apache.nifi.confluent.schema.AntlrProtobufMessageSchemaParser;
import org.apache.nifi.confluent.schema.ProtobufMessageSchema;
import org.apache.nifi.confluent.schema.VarintUtils;
import org.apache.nifi.schemaregistry.services.MessageName;
import org.apache.nifi.schemaregistry.services.StandardMessageName;
import org.apache.nifi.schemaregistry.services.StandardMessageNameFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * This class contains no NiFi framework dependencies: it exercises {@link ProtobufMessageIndexEncoder}
 * directly, without a TestRunner or any controller-service context.
 */
class ProtobufMessageIndexEncoderTest {

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

    private static Stream<Arguments> provideMessageIndexTestCases() {
        return Stream.of(
            // the format is [input schema], [target message name], [expected message indexes]
            Arguments.of(DEFAULT_PACKAGE_SCHEMA, new StandardMessageName(Optional.empty(), "User"), new int[] {0}),
            Arguments.of(DEFAULT_PACKAGE_SCHEMA, new StandardMessageName(Optional.empty(), "Company"), new int[] {1}),
            Arguments.of(DEFAULT_PACKAGE_SCHEMA, new StandardMessageName(Optional.empty(), "Address"), new int[] {2}),
            Arguments.of(DEFAULT_PACKAGE_SCHEMA, new StandardMessageName(Optional.empty(), "User.Profile"), new int[] {0, 0}),
            Arguments.of(DEFAULT_PACKAGE_SCHEMA, new StandardMessageName(Optional.empty(), "User.Profile.Settings"), new int[] {0, 0, 0}),

            Arguments.of(EXPLICIT_PACKAGE_SCHEMA, new StandardMessageName(Optional.of("com.example.proto"), "User"), new int[] {0}),
            Arguments.of(EXPLICIT_PACKAGE_SCHEMA, new StandardMessageName(Optional.of("com.example.proto"), "Company"), new int[] {1}),
            Arguments.of(EXPLICIT_PACKAGE_SCHEMA, new StandardMessageName(Optional.of("com.example.proto"), "Address"), new int[] {2}),
            Arguments.of(EXPLICIT_PACKAGE_SCHEMA, new StandardMessageName(Optional.of("com.example.proto"), "User.Profile"), new int[] {0, 0}),
            Arguments.of(EXPLICIT_PACKAGE_SCHEMA, new StandardMessageName(Optional.of("com.example.proto"), "User.Profile.Settings"), new int[] {0, 0, 0})
        );
    }

    @ParameterizedTest
    @MethodSource("provideMessageIndexTestCases")
    void testEncode(final String schemaText, final MessageName messageName, final int[] expectedIndexes) throws IOException {
        final List<ProtobufMessageSchema> rootMessages = new AntlrProtobufMessageSchemaParser().parse(schemaText);

        final byte[] encoded = ProtobufMessageIndexEncoder.encode(rootMessages, messageName);

        assertEquals(IntStream.of(expectedIndexes).boxed().toList(), decodeIndexes(encoded));
    }

    @Test
    void testEncodeFirstRootMessageUsesSingleByteOptimization() {
        final List<ProtobufMessageSchema> rootMessages = new AntlrProtobufMessageSchemaParser().parse(DEFAULT_PACKAGE_SCHEMA);

        final byte[] encoded = ProtobufMessageIndexEncoder.encode(rootMessages, new StandardMessageName(Optional.empty(), "User"));

        assertArrayEquals(new byte[] {0x00}, encoded);
    }

    @Test
    void testEncodeMessageNotFoundThrows() {
        final List<ProtobufMessageSchema> rootMessages = new AntlrProtobufMessageSchemaParser().parse(DEFAULT_PACKAGE_SCHEMA);

        assertThrows(IllegalStateException.class,
            () -> ProtobufMessageIndexEncoder.encode(rootMessages, new StandardMessageName(Optional.empty(), "DoesNotExist")));
    }

    @Test
    void testEncodeNamespaceMismatchThrows() {
        final List<ProtobufMessageSchema> rootMessages = new AntlrProtobufMessageSchemaParser().parse(EXPLICIT_PACKAGE_SCHEMA);

        assertThrows(IllegalStateException.class,
            () -> ProtobufMessageIndexEncoder.encode(rootMessages, new StandardMessageName(Optional.empty(), "User")));
    }

    @Test
    void testEncodeNestedMessageFromFactorySplitName() throws IOException {
        // StandardMessageNameFactory.fromName is what the writer's static Message Name property uses. For a nested
        // message it splits "User.Profile" into namespace="User"/name="Profile" - the encoder must still resolve it
        // to the nested index [0, 0] by matching on the fully qualified name.
        final List<ProtobufMessageSchema> rootMessages = new AntlrProtobufMessageSchemaParser().parse(DEFAULT_PACKAGE_SCHEMA);

        final byte[] encoded = ProtobufMessageIndexEncoder.encode(rootMessages, StandardMessageNameFactory.fromName("User.Profile"));

        assertEquals(List.of(0, 0), decodeIndexes(encoded));
    }

    @Test
    void testEncodeNestedMessageWithPackageFromFactorySplitName() throws IOException {
        // With a package, fromName splits "com.example.proto.User.Profile" into namespace="com.example.proto.User"/
        // name="Profile"; the fully qualified name still resolves to the nested index [0, 0].
        final List<ProtobufMessageSchema> rootMessages = new AntlrProtobufMessageSchemaParser().parse(EXPLICIT_PACKAGE_SCHEMA);

        final byte[] encoded = ProtobufMessageIndexEncoder.encode(rootMessages, StandardMessageNameFactory.fromName("com.example.proto.User.Profile"));

        assertEquals(List.of(0, 0), decodeIndexes(encoded));
    }

    /**
     * Decodes message indexes according to Confluent wire format, mirroring the read side, to
     * verify what {@link ProtobufMessageIndexEncoder} produced can be decoded back correctly.
     */
    private List<Integer> decodeIndexes(final byte[] encoded) throws IOException {
        final ByteArrayInputStream inputStream = new ByteArrayInputStream(encoded);
        final int firstByte = inputStream.read();
        if (firstByte == 0) {
            return List.of(0);
        }

        int arrayLength = VarintUtils.readVarintFromStreamAfterFirstByteConsumed(inputStream, firstByte);
        arrayLength = VarintUtils.decodeZigZag(arrayLength);

        final List<Integer> indexes = new ArrayList<>();
        for (int i = 0; i < arrayLength; i++) {
            final int rawIndex = VarintUtils.readVarintFromStream(inputStream);
            indexes.add(VarintUtils.decodeZigZag(rawIndex));
        }
        return indexes;
    }
}
