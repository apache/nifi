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

import com.squareup.wire.schema.EnumConstant;
import com.squareup.wire.schema.EnumType;
import com.squareup.wire.schema.Field;
import com.squareup.wire.schema.MessageType;
import com.squareup.wire.schema.OneOf;
import com.squareup.wire.schema.Options;
import com.squareup.wire.schema.ProtoFile;
import com.squareup.wire.schema.Rpc;
import com.squareup.wire.schema.Schema;
import com.squareup.wire.schema.SchemaException;
import com.squareup.wire.schema.Service;
import com.squareup.wire.schema.internal.parser.OptionElement;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.schemaregistry.services.SchemaDefinition;
import org.apache.nifi.schemaregistry.services.StandardSchemaDefinition;
import org.apache.nifi.serialization.record.SchemaIdentifier;
import org.apache.nifi.util.MockComponentLog;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestProtobufSchemaCompiler {

    private static final ComponentLog COMPONENT_LOG = new MockComponentLog("test", TestProtobufSchemaCompiler.class);

    private static final String PACKAGE = "example";
    private static final String SAMPLE_TYPE = PACKAGE + ".Sample";
    private static final String WRAPPER_TYPE = PACKAGE + ".Wrapper";
    private static final String WRAPPER_PAYLOAD_TYPE = WRAPPER_TYPE + ".Payload";

    private static final String SAMPLE_PROTO = "sample.proto";
    private static final String COMMON_PROTO = "common.proto";

    private static final String FIELD_VALUE = "value";
    private static final String FIELD_EXTRA = "extra";
    private static final String ENUM_RED = "RED";

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
    void testCompileSchemaWithCustomOptionWithoutImport() {
        final String schemaText = """
            syntax = "proto3";
            package example;

            message Sample {
              optional int32 enabled = 1 [(confluent.field_meta) = {
                params: [
                  {
                    key: "connect.type",
                    value: "int16"
                  }
                ]
              }];
            }
            """;
        final SchemaDefinition schemaDefinition = schemaDefinition(SAMPLE_PROTO, schemaText);

        final Schema schema = compiler.compileOrGetFromCache(schemaDefinition);

        final Field enabled = schema.getField(SAMPLE_TYPE, "enabled");

        assertNotNull(enabled);
        assertNoParenthesizedOptions(enabled.getOptions());
    }

    @Test
    void testCompileNestedSchemaWithRepeatedFieldMetaOptions() throws IOException {
        final String schemaText = readTestResource("/org/apache/nifi/protobuf/test/nested_wrapper_with_field_meta.proto");
        final SchemaDefinition schemaDefinition = schemaDefinition(SAMPLE_PROTO, schemaText);

        final Schema schema = compiler.compileOrGetFromCache(schemaDefinition);

        final Field flagA = schema.getField(WRAPPER_PAYLOAD_TYPE, "flag_a");
        final Field flagB = schema.getField(WRAPPER_PAYLOAD_TYPE, "flag_b");
        final Field flagC = schema.getField(WRAPPER_PAYLOAD_TYPE, "flag_c");

        assertNotNull(schema.getType(WRAPPER_TYPE));
        assertNotNull(schema.getType(WRAPPER_TYPE + ".Origin"));
        assertNotNull(schema.getType(WRAPPER_TYPE + ".chunk"));
        assertNotNull(flagA);
        assertNotNull(flagB);
        assertNotNull(flagC);
        assertNoParenthesizedOptions(flagA.getOptions());
        assertNoParenthesizedOptions(flagB.getOptions());
        assertNoParenthesizedOptions(flagC.getOptions());
    }

    @Test
    void testCompileKeepsPackedWhenCustomFieldOptionPresent() {
        final String schemaText = """
            syntax = "proto2";
            package example;

            message Sample {
              repeated int32 values = 1 [packed = true, (confluent.field_meta) = {
                tags: "x"
              }];
            }
            """;
        final SchemaDefinition schemaDefinition = schemaDefinition(SAMPLE_PROTO, schemaText);

        final Schema schema = compiler.compileOrGetFromCache(schemaDefinition);
        final Field values = schema.getField(SAMPLE_TYPE, "values");

        assertNotNull(values);
        assertTrue(values.isRepeated());
        assertTrue(values.isPacked());
        assertNoParenthesizedOptions(values.getOptions());
    }

    @Test
    void testCompileKeepsJavaPackageWhenCustomFileOptionPresent() {
        final String schemaText = """
            syntax = "proto3";
            package example;

            option java_package = "com.example";
            option (custom.file_meta) = "docs";

            message Sample {
              int32 value = 1;
            }
            """;
        final SchemaDefinition schemaDefinition = schemaDefinition(SAMPLE_PROTO, schemaText);

        final Schema schema = compiler.compileOrGetFromCache(schemaDefinition);
        final ProtoFile protoFile = schema.protoFile(SAMPLE_PROTO);

        assertNotNull(schema.getType(SAMPLE_TYPE));
        assertNotNull(protoFile);
        assertEquals("com.example", protoFile.javaPackage());
        assertNoParenthesizedOptions(protoFile.getOptions());
    }

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
    void testCompileReferencedSchemaWithCustomOptions() {
        final String referencedSchemaText = """
            syntax = "proto3";
            package example;

            message Common {
              int32 id = 1 [(custom.field_meta) = "c"];
            }
            """;
        final String rootSchemaText = """
            syntax = "proto3";
            package example;

            import "common.proto";

            message Root {
              Common item = 1 [(custom.field_meta) = "r"];
            }
            """;
        final SchemaDefinition referencedSchema = schemaDefinition(COMMON_PROTO, referencedSchemaText);
        final SchemaDefinition rootSchema = schemaDefinition(SAMPLE_PROTO, rootSchemaText, Map.of(COMMON_PROTO, referencedSchema));

        final Schema schema = compiler.compileOrGetFromCache(rootSchema);

        final Field id = schema.getField(PACKAGE + ".Common", "id");
        final Field item = schema.getField(PACKAGE + ".Root", "item");

        assertNotNull(id);
        assertNoParenthesizedOptions(id.getOptions());
        assertNotNull(item);
        assertNoParenthesizedOptions(item.getOptions());
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
    void testCompileNestedMessageAndEnumWithCustomOptions() {
        final String schemaText = """
            syntax = "proto3";
            package example;

            message Outer {
              option (custom.message_meta) = true;

              message Inner {
                int32 value = 1 [(custom.field_meta) = "y"];
              }

              Inner inner = 1;
            }

            enum Color {
              option (custom.enum_meta) = true;
              RED = 0 [(custom.enum_value_meta) = "r"];
              GREEN = 1;
            }
            """;
        final SchemaDefinition schemaDefinition = schemaDefinition(SAMPLE_PROTO, schemaText);

        final Schema schema = compiler.compileOrGetFromCache(schemaDefinition);
        final MessageType outer = assertInstanceOf(MessageType.class, schema.getType(PACKAGE + ".Outer"));
        final EnumType color = assertInstanceOf(EnumType.class, schema.getType(PACKAGE + ".Color"));
        final EnumConstant red = color.constant(ENUM_RED);

        assertNotNull(outer.field("inner"));
        assertNotNull(schema.getField(PACKAGE + ".Outer.Inner", FIELD_VALUE));
        assertNotNull(red);
        assertNotNull(color.constant("GREEN"));
        assertNoParenthesizedOptions(color.getOptions());
        assertNoParenthesizedOptions(red.getOptions());
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
    void testCompileRemovesCustomOptionsFromAllSyntaxElements() throws IOException {
        final String schemaText = readTestResource("/org/apache/nifi/protobuf/test/custom_options_on_all_elements.proto");
        final SchemaDefinition schemaDefinition = schemaDefinition(SAMPLE_PROTO, schemaText);

        final Schema schema = compiler.compileOrGetFromCache(schemaDefinition);

        final ProtoFile protoFile = schema.protoFile(SAMPLE_PROTO);
        assertNotNull(protoFile);
        assertEquals("com.example.comprehensive", protoFile.javaPackage());
        assertNoParenthesizedOptions(protoFile.getOptions());

        final MessageType wrapper = assertInstanceOf(MessageType.class, schema.getType(WRAPPER_TYPE));
        assertNoParenthesizedOptions(wrapper.getOptions());

        final Field flag = schema.getField(WRAPPER_TYPE, "flag");
        assertNotNull(flag);
        assertNoParenthesizedOptions(flag.getOptions());

        final OneOf kind = wrapper.oneOf("kind");
        assertNotNull(kind);
        assertNoParenthesizedOptions(kind.getOptions());

        final Field text = schema.getField(WRAPPER_TYPE, "text");
        assertNotNull(text);
        assertNoParenthesizedOptions(text.getOptions());

        assertEquals(1, protoFile.getExtendList().size());
        final Field extra = protoFile.getExtendList().get(0).getFields().get(0);
        assertEquals(FIELD_EXTRA, extra.getName());
        assertNoParenthesizedOptions(extra.getOptions());

        assertNotNull(schema.getType(WRAPPER_TYPE + ".Nested"));
        assertEquals(1, wrapper.getNestedExtendList().size());
        final Field nestedExtra = wrapper.getNestedExtendList().get(0).getFields().get(0);
        assertEquals("nested_extra", nestedExtra.getName());
        assertNoParenthesizedOptions(nestedExtra.getOptions());

        final EnumType color = assertInstanceOf(EnumType.class, schema.getType(WRAPPER_TYPE + ".Color"));
        assertNoParenthesizedOptions(color.getOptions());

        final EnumConstant red = color.constant(ENUM_RED);
        assertNotNull(red);
        assertNoParenthesizedOptions(red.getOptions());

        final Service catalog = schema.getService(PACKAGE + ".Catalog");
        assertNotNull(catalog);
        assertNoParenthesizedOptions(catalog.options());

        final Rpc lookup = catalog.rpc("Lookup");
        assertNotNull(lookup);
        assertEquals(PACKAGE + ".Request", lookup.getRequestType().toString());
        assertEquals(PACKAGE + ".Response", lookup.getResponseType().toString());
        assertNoParenthesizedOptions(lookup.getOptions());
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

    private void assertNoParenthesizedOptions(final Options options) {
        final List<OptionElement> elements = options.getElements();
        final List<Executable> assertions = new ArrayList<>(elements.size());
        for (final OptionElement option : elements) {
            assertions.add(() -> assertFalse(option.isParenthesized()));
        }

        assertAll(assertions);
    }

    private String readTestResource(final String resourcePath) throws IOException {
        try (InputStream inputStream = getClass().getResourceAsStream(resourcePath)) {
            assertNotNull(inputStream, "Test resource not found: " + resourcePath);
            return new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
        }
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
