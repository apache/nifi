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
        final SchemaDefinition schemaDefinition = schemaDefinition("sample.proto", schemaText);

        final Schema schema = compiler.compileOrGetFromCache(schemaDefinition);

        final Field enabled = schema.getField("example.Sample", "enabled");

        assertNotNull(enabled);
        assertNoParenthesizedOptions(enabled.getOptions());
    }

    @Test
    void testCompileNestedSchemaWithRepeatedFieldMetaOptions() throws IOException {
        final String schemaText = readTestResource("/org/apache/nifi/protobuf/test/nested_wrapper_with_field_meta.proto");
        final SchemaDefinition schemaDefinition = schemaDefinition("nested_wrapper_with_field_meta.proto", schemaText);

        final Schema schema = compiler.compileOrGetFromCache(schemaDefinition);
        final String payloadMessageName = "example.sample.Wrapper.Payload";

        final Field flagA = schema.getField(payloadMessageName, "flag_a");
        final Field flagB = schema.getField(payloadMessageName, "flag_b");
        final Field flagC = schema.getField(payloadMessageName, "flag_c");

        assertNotNull(schema.getType("example.sample.Wrapper"));
        assertNotNull(schema.getType("example.sample.Wrapper.Origin"));
        assertNotNull(schema.getType("example.sample.Wrapper.chunk"));
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
        final SchemaDefinition schemaDefinition = schemaDefinition("packed.proto", schemaText);

        final Schema schema = compiler.compileOrGetFromCache(schemaDefinition);
        final Field values = schema.getField("example.Sample", "values");

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
        final SchemaDefinition schemaDefinition = schemaDefinition("java-package.proto", schemaText);

        final Schema schema = compiler.compileOrGetFromCache(schemaDefinition);
        final ProtoFile protoFile = schema.protoFile("java-package.proto");

        assertNotNull(schema.getType("example.Sample"));
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
        final SchemaDefinition schemaDefinition = schemaDefinition("plain.proto", schemaText);

        final Schema schema = compiler.compileOrGetFromCache(schemaDefinition);

        assertNotNull(schema.getField("example.Sample", "value"));
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
        final SchemaDefinition referencedSchema = schemaDefinition("common.proto", referencedSchemaText);
        final SchemaDefinition rootSchema = schemaDefinition("root.proto", rootSchemaText, Map.of("common.proto", referencedSchema));

        final Schema schema = compiler.compileOrGetFromCache(rootSchema);

        final Field id = schema.getField("example.Common", "id");
        final Field item = schema.getField("example.Root", "item");

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
        final SchemaDefinition schemaDefinition = schemaDefinition("root.proto", schemaText);

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
        final SchemaDefinition schemaDefinition = schemaDefinition("root.proto", schemaText);

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
        final SchemaDefinition schemaDefinition = schemaDefinition("nested.proto", schemaText);

        final Schema schema = compiler.compileOrGetFromCache(schemaDefinition);
        final MessageType outer = assertInstanceOf(MessageType.class, schema.getType("example.Outer"));
        final EnumType color = assertInstanceOf(EnumType.class, schema.getType("example.Color"));
        final EnumConstant red = color.constant("RED");

        assertNotNull(outer.field("inner"));
        assertNotNull(schema.getField("example.Outer.Inner", "value"));
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
        final SchemaDefinition schemaDefinition = schemaDefinition("extend.proto", schemaText);

        final Schema schema = compiler.compileOrGetFromCache(schemaDefinition);
        final MessageType base = assertInstanceOf(MessageType.class, schema.getType("example.Base"));
        final ProtoFile protoFile = schema.protoFile("extend.proto");

        assertNotNull(base.field("name"));
        assertNotNull(protoFile);
        assertEquals(1, protoFile.getExtendList().size());
        assertEquals("extra", protoFile.getExtendList().get(0).getFields().get(0).getName());
    }

    @Test
    void testCompileRemovesCustomOptionsFromAllSyntaxElements() throws IOException {
        final String schemaText = readTestResource("/org/apache/nifi/protobuf/test/custom_options_on_all_elements.proto");
        final SchemaDefinition schemaDefinition = schemaDefinition("custom_options_on_all_elements.proto", schemaText);

        final Schema schema = compiler.compileOrGetFromCache(schemaDefinition);
        final String wrapperTypeName = "example.comprehensive.Wrapper";

        final ProtoFile protoFile = schema.protoFile("custom_options_on_all_elements.proto");
        assertNotNull(protoFile);
        assertEquals("com.example.comprehensive", protoFile.javaPackage());
        assertNoParenthesizedOptions(protoFile.getOptions());

        final MessageType wrapper = assertInstanceOf(MessageType.class, schema.getType(wrapperTypeName));
        assertNoParenthesizedOptions(wrapper.getOptions());

        final Field flag = schema.getField(wrapperTypeName, "flag");
        assertNotNull(flag);
        assertNoParenthesizedOptions(flag.getOptions());

        final OneOf kind = wrapper.oneOf("kind");
        assertNotNull(kind);
        assertNoParenthesizedOptions(kind.getOptions());

        final Field text = schema.getField(wrapperTypeName, "text");
        assertNotNull(text);
        assertNoParenthesizedOptions(text.getOptions());

        assertEquals(1, protoFile.getExtendList().size());
        final Field extra = protoFile.getExtendList().get(0).getFields().get(0);
        assertEquals("extra", extra.getName());
        assertNoParenthesizedOptions(extra.getOptions());

        assertNotNull(schema.getType(wrapperTypeName + ".Nested"));
        assertEquals(1, wrapper.getNestedExtendList().size());
        final Field nestedExtra = wrapper.getNestedExtendList().get(0).getFields().get(0);
        assertEquals("nested_extra", nestedExtra.getName());
        assertNoParenthesizedOptions(nestedExtra.getOptions());

        final EnumType color = assertInstanceOf(EnumType.class, schema.getType(wrapperTypeName + ".Color"));
        assertNoParenthesizedOptions(color.getOptions());

        final EnumConstant red = color.constant("RED");
        assertNotNull(red);
        assertNoParenthesizedOptions(red.getOptions());

        final Service catalog = schema.getService("example.comprehensive.Catalog");
        assertNotNull(catalog);
        assertNoParenthesizedOptions(catalog.options());

        final Rpc lookup = catalog.rpc("Lookup");
        assertNotNull(lookup);
        assertEquals("example.comprehensive.Request", lookup.getRequestType().toString());
        assertEquals("example.comprehensive.Response", lookup.getResponseType().toString());
        assertNoParenthesizedOptions(lookup.getOptions());
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
