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

import com.squareup.wire.schema.MessageType;
import com.squareup.wire.schema.ProtoFile;
import com.squareup.wire.schema.Schema;
import com.squareup.wire.schema.SchemaException;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schemaregistry.services.SchemaDefinition;
import org.apache.nifi.schemaregistry.services.StandardSchemaDefinition;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.util.MockComponentLog;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class TestProtobufSchemaCompiler {

    private static final ComponentLog COMPONENT_LOG = new MockComponentLog("test", TestProtobufSchemaCompiler.class);

    private static final String PACKAGE = "example";
    private static final String SAMPLE_TYPE = PACKAGE + ".Sample";

    private static final String SAMPLE_PROTO = "sample.proto";

    private static final String FIELD_VALUE = "value";
    private static final String FIELD_EXTRA = "extra";

    // Confluent Schema Registry keys each reference by the import path used inside the .proto file, while the
    // identifier of the referenced schema carries the registry subject, which is not required to look like a file path
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

    private final ProtobufSchemaCompiler compiler = new ProtobufSchemaCompiler("test", COMPONENT_LOG);

    @Test
    void testCompileSchemaWithoutCustomOptions() {
        final String schemaText = """
            syntax = "proto3";
            package example;

            message Sample {
              int32 value = 1;
            }
            """;
        final SchemaDefinition schemaDefinition = schemaDefinition(SAMPLE_PROTO, schemaText);

        final Schema schema = compiler.compileOrGetFromCache(schemaDefinition);

        assertNotNull(schema.getField(SAMPLE_TYPE, FIELD_VALUE));
    }

    @Test
    void testCompileFailsWhenMessageTypeIsUnresolved() {
        final String schemaText = """
            syntax = "proto3";
            package example;

            message Root {
              Common item = 1;
            }
            """;
        final SchemaDefinition schemaDefinition = schemaDefinition(SAMPLE_PROTO, schemaText);

        final RuntimeException exception = assertThrows(RuntimeException.class, () -> compiler.compileOrGetFromCache(schemaDefinition));

        assertInstanceOf(SchemaException.class, exception.getCause());
    }

    @Test
    void testCompileFailsWhenTypeImportIsMissing() {
        final String schemaText = """
            syntax = "proto3";
            package example;

            import "common.proto";

            message Root {
              Common item = 1;
            }
            """;
        final SchemaDefinition schemaDefinition = schemaDefinition(SAMPLE_PROTO, schemaText);

        final RuntimeException exception = assertThrows(RuntimeException.class, () -> compiler.compileOrGetFromCache(schemaDefinition));

        assertInstanceOf(SchemaException.class, exception.getCause());
    }

    @Test
    void testPayloadExtendDeclarationsArePreserved() {
        final String schemaText = """
            syntax = "proto2";
            package example;

            message Base {
              extensions 100 to 199;
              optional string name = 1;
            }

            extend Base {
              optional int32 extra = 100;
            }
            """;
        final SchemaDefinition schemaDefinition = schemaDefinition(SAMPLE_PROTO, schemaText);

        final Schema schema = compiler.compileOrGetFromCache(schemaDefinition);
        final MessageType base = assertInstanceOf(MessageType.class, schema.getType(PACKAGE + ".Base"));
        final ProtoFile protoFile = schema.protoFile(SAMPLE_PROTO);

        assertNotNull(base.field("name"));
        assertNotNull(protoFile);
        assertEquals(1, protoFile.getExtendList().size());
        assertEquals(FIELD_EXTRA, protoFile.getExtendList().get(0).getFields().get(0).getName());
    }

    @Test
    void testCompileSchemaWithCrossFileImportFromRegistry() {
        final SchemaDefinition referencedSchema = new StandardSchemaDefinition(
            SchemaIdentifier.builder().name(REFERENCE_SUBJECT).id(4L).version(1).build(),
            REFERENCED_SCHEMA,
            SchemaDefinition.SchemaType.PROTOBUF);

        final SchemaDefinition rootSchema = new StandardSchemaDefinition(
            SchemaIdentifier.builder().name(ROOT_SUBJECT).id(3L).version(2).build(),
            ROOT_SCHEMA,
            SchemaDefinition.SchemaType.PROTOBUF,
            Map.of(IMPORT_PATH, referencedSchema));

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
            SchemaDefinition.SchemaType.PROTOBUF);

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
            SchemaDefinition.SchemaType.PROTOBUF,
            Map.of("airlines/ph/cdm/common/audit.proto", leafSchema));

        final SchemaDefinition rootSchema = new StandardSchemaDefinition(
            SchemaIdentifier.builder().name(ROOT_SUBJECT).id(3L).version(2).build(),
            ROOT_SCHEMA,
            SchemaDefinition.SchemaType.PROTOBUF,
            Map.of(IMPORT_PATH, referencedSchema));

        final Schema compiled = compiler.compileOrGetFromCache(rootSchema);

        assertNotNull(compiled.getType("airlines.ph.cdm.reservation.AirlineReservation"));
        assertNotNull(compiled.getType("airlines.ph.cdm.Status"));
        assertNotNull(compiled.getType("airlines.ph.cdm.common.Audit"));
    }

    private SchemaDefinition schemaDefinition(final String name, final String schemaText) {
        return schemaDefinition(name, schemaText, Map.of());
    }

    private SchemaDefinition schemaDefinition(final String name, final String schemaText, final Map<String, SchemaDefinition> references) {
        return new StandardSchemaDefinition(
            SchemaIdentifier.builder().name(name).build(),
            schemaText,
            SchemaDefinition.SchemaType.PROTOBUF,
            references);
    }
}
