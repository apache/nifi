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

import com.squareup.wire.schema.Schema;
import org.apache.nifi.schemaregistry.services.SchemaDefinition;
import org.apache.nifi.schemaregistry.services.StandardSchemaDefinition;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.util.MockComponentLog;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.apache.nifi.schemaregistry.services.SchemaDefinition.SchemaType.PROTOBUF;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Verifies schema compilation when a schema imports another file. Confluent Schema Registry keys each
 * reference by the import path used inside the {@code .proto} (for example {@code airlines/ph/cdm/shared.proto}),
 * while the referenced schema's identifier carries the registry subject, which is a different value and is not
 * required to look like a file path.
 */
class TestProtobufSchemaCompiler {

    private static final String ROOT_SUBJECT = "airlines.ph.cdm.reservation.AirlineReservation";
    private static final String IMPORT_PATH = "airlines/ph/cdm/shared.proto";
    private static final String REFERENCE_SUBJECT = "airlines.ph.cdm.shared";

    private static final String ROOT_SCHEMA = """
        syntax = "proto3";
        package airlines.ph.cdm.reservation;
        import "airlines/ph/cdm/shared.proto";
        message AirlineReservation {
          string reservation_id = 1;
          airlines.ph.cdm.Status status = 2;
        }""";

    private static final String REFERENCED_SCHEMA = """
        syntax = "proto3";
        package airlines.ph.cdm;
        message Status {
          string code = 1;
        }""";

    @Test
    void testCompileSchemaWithCrossFileImportFromRegistry() {
        final SchemaDefinition referencedSchema = new StandardSchemaDefinition(
            SchemaIdentifier.builder().name(REFERENCE_SUBJECT).id(4L).version(1).build(),
            REFERENCED_SCHEMA,
            PROTOBUF);

        final SchemaDefinition rootSchema = new StandardSchemaDefinition(
            SchemaIdentifier.builder().name(ROOT_SUBJECT).id(3L).version(2).build(),
            ROOT_SCHEMA,
            PROTOBUF,
            Map.of(IMPORT_PATH, referencedSchema));

        final ProtobufSchemaCompiler compiler = new ProtobufSchemaCompiler("test", new MockComponentLog("test", new Object()));
        final Schema compiled = compiler.compileOrGetFromCache(rootSchema);

        assertNotNull(compiled.getType("airlines.ph.cdm.reservation.AirlineReservation"));
        assertNotNull(compiled.getType("airlines.ph.cdm.Status"));
    }

    @Test
    void testCompileSchemaWithNestedCrossFileImports() {
        final SchemaDefinition leafSchema = new StandardSchemaDefinition(
            SchemaIdentifier.builder().name("airlines.ph.cdm.common").id(5L).version(1).build(),
            """
                syntax = "proto3";
                package airlines.ph.cdm.common;
                message Audit {
                  string created_by = 1;
                }""",
            PROTOBUF);

        final SchemaDefinition referencedSchema = new StandardSchemaDefinition(
            SchemaIdentifier.builder().name(REFERENCE_SUBJECT).id(4L).version(1).build(),
            """
                syntax = "proto3";
                package airlines.ph.cdm;
                import "airlines/ph/cdm/common/audit.proto";
                message Status {
                  string code = 1;
                  airlines.ph.cdm.common.Audit audit = 2;
                }""",
            PROTOBUF,
            Map.of("airlines/ph/cdm/common/audit.proto", leafSchema));

        final SchemaDefinition rootSchema = new StandardSchemaDefinition(
            SchemaIdentifier.builder().name(ROOT_SUBJECT).id(3L).version(2).build(),
            ROOT_SCHEMA,
            PROTOBUF,
            Map.of(IMPORT_PATH, referencedSchema));

        final ProtobufSchemaCompiler compiler = new ProtobufSchemaCompiler("test", new MockComponentLog("test", new Object()));
        final Schema compiled = compiler.compileOrGetFromCache(rootSchema);

        assertNotNull(compiled.getType("airlines.ph.cdm.reservation.AirlineReservation"));
        assertNotNull(compiled.getType("airlines.ph.cdm.Status"));
        assertNotNull(compiled.getType("airlines.ph.cdm.common.Audit"));
    }
}
