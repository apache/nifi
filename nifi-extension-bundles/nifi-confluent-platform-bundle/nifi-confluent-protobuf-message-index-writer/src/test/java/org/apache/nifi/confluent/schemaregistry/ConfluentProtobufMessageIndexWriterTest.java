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
import org.apache.nifi.schemaregistry.services.StandardMessageName;
import org.apache.nifi.schemaregistry.services.StandardSchemaDefinition;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import static org.apache.nifi.schemaregistry.services.SchemaDefinition.SchemaType.PROTOBUF;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ConfluentProtobufMessageIndexWriterTest {

    // Schema without package (default package)
    private static final String DEFAULT_PACKAGE_SCHEMA = """
        syntax = "proto3";
        message User {
          int32 id = 1;
          string name = 2;
          Address address = 3;
          message Profile {
            string bio = 1;
          }
        }
        message Company {
          string name = 1;
        }
        message Address {
          string street = 1;
        }""";

    private static final SchemaIdentifier ID = SchemaIdentifier.builder()
        .id(1L)
        .build();

    private static final SchemaDefinition SCHEMA_WITH_DEFAULT_PACKAGE = new StandardSchemaDefinition(
        ID,
        DEFAULT_PACKAGE_SCHEMA,
        PROTOBUF,
        Map.of());

    private ConfluentProtobufMessageIndexWriter writer;

    private static Stream<Arguments> provideMessageIndexTestCases() {
        return Stream.of(
            // the format is [target message name], [expected wire-format bytes for the message index]
            Arguments.of(new StandardMessageName(Optional.empty(), "User"), new byte[] {0x00}),
            Arguments.of(new StandardMessageName(Optional.empty(), "Company"), new byte[] {0x02, 0x02}),
            Arguments.of(new StandardMessageName(Optional.empty(), "Address"), new byte[] {0x02, 0x04}),
            Arguments.of(new StandardMessageName(Optional.empty(), "User.Profile"), new byte[] {0x04, 0x00, 0x00})
        );
    }

    @BeforeEach
    void setUp() throws Exception {
        writer = new ConfluentProtobufMessageIndexWriter();
        final TestRunner testRunner = TestRunners.newTestRunner(NoOpProcessor.class);
        testRunner.addControllerService("messageIndexWriter", writer);
        testRunner.enableControllerService(writer);
    }

    @ParameterizedTest
    @MethodSource("provideMessageIndexTestCases")
    void testWriteMessageIndex(final MessageName messageName, final byte[] expectedBytes) throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        writer.writeMessageIndex(Map.of(), SCHEMA_WITH_DEFAULT_PACKAGE, messageName, outputStream);

        assertArrayEquals(expectedBytes, outputStream.toByteArray());
    }

    @Test
    void testWriteMessageIndexUnknownMessageThrows() {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final MessageName unknownMessageName = new StandardMessageName(Optional.empty(), "DoesNotExist");

        assertThrows(IllegalStateException.class,
            () -> writer.writeMessageIndex(Map.of(), SCHEMA_WITH_DEFAULT_PACKAGE, unknownMessageName, outputStream));
    }
}
