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
import com.squareup.wire.schema.Service;
import com.squareup.wire.schema.Type;
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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestProtobufSchemaDefinitionSanitizer {

    private static final ComponentLog COMPONENT_LOG = new MockComponentLog("test", TestProtobufSchemaDefinitionSanitizer.class);

    private static final String PACKAGE = "example";
    private static final String SAMPLE_TYPE = PACKAGE + ".Sample";
    private static final String WRAPPER_TYPE = PACKAGE + ".Wrapper";
    private static final String WRAPPER_PAYLOAD_TYPE = WRAPPER_TYPE + ".Payload";

    private static final String SAMPLE_PROTO = "sample.proto";
    private static final String COMMON_PROTO = "common.proto";

    private static final String ENUM_RED = "RED";

    private final ProtobufSchemaDefinitionSanitizer sanitizer = new ProtobufSchemaDefinitionSanitizer(COMPONENT_LOG);
    private final ProtobufSchemaCompiler compiler = new ProtobufSchemaCompiler("test", COMPONENT_LOG);

    @Test
    void testSanitizeRemovesCustomOptionsFromSchemaAndReferences() {
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
              Common item = 1 [(confluent.field_meta) = { params: [ { key: "k", value: "v" } ] }];
            }
            """;
        final SchemaDefinition referencedSchema = schemaDefinition(COMMON_PROTO, referencedSchemaText);
        final SchemaDefinition rootSchema = schemaDefinition(SAMPLE_PROTO, rootSchemaText, Map.of(COMMON_PROTO, referencedSchema));

        final SchemaDefinition sanitized = sanitizer.sanitizeOrGetFromCache(rootSchema);

        assertEquals(rootSchema.getIdentifier(), sanitized.getIdentifier());
        assertEquals(rootSchema.getSchemaType(), sanitized.getSchemaType());
        final SchemaDefinition sanitizedReference = sanitized.getReferences().get(COMMON_PROTO);
        assertNotNull(sanitizedReference);
        assertEquals(referencedSchema.getIdentifier(), sanitizedReference.getIdentifier());
        assertEquals(referencedSchema.getSchemaType(), sanitizedReference.getSchemaType());

        final Schema schema = compiler.compileOrGetFromCache(sanitized);
        final Field id = schema.getField(PACKAGE + ".Common", "id");
        final Field item = schema.getField(PACKAGE + ".Root", "item");

        assertNotNull(id);
        assertNoParenthesizedOptions(id.getOptions());
        assertNotNull(item);
        assertNoParenthesizedOptions(item.getOptions());
    }

    @Test
    void testSanitizeKeepsSchemaTextWithoutCustomOptions() {
        final String schemaText = """
            syntax = "proto3";
            package example;

            message Sample {
              int32 value = 1;
            }
            """;

        final SchemaDefinition sanitized = sanitizer.sanitizeOrGetFromCache(schemaDefinition(SAMPLE_PROTO, schemaText));

        assertEquals(schemaText, sanitized.getText());
    }

    @Test
    void testSanitizeReturnsCachedDefinitionForSameIdentifier() {
        final String schemaText = """
            syntax = "proto3";
            package example;

            message Sample {
              int32 value = 1 [(custom.field_meta) = "v"];
            }
            """;

        final SchemaDefinition first = sanitizer.sanitizeOrGetFromCache(schemaDefinition(SAMPLE_PROTO, schemaText));
        final SchemaDefinition second = sanitizer.sanitizeOrGetFromCache(schemaDefinition(SAMPLE_PROTO, schemaText));

        assertSame(first, second);
    }

    @Test
    void testSanitizeSchemaWithCustomOptionWithoutImport() {
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

        final Schema schema = sanitizeAndCompile(schemaText);
        final Field enabled = schema.getField(SAMPLE_TYPE, "enabled");

        assertNotNull(enabled);
        assertEquals(1, enabled.getTag());
        assertNoParenthesizedOptions(enabled.getOptions());
    }

    @Test
    void testSanitizeNestedSchemaWithRepeatedFieldMetaOptions() throws IOException {
        final String schemaText = readTestResource("/org/apache/nifi/protobuf/test/nested_wrapper_with_field_meta.proto");

        final Schema schema = sanitizeAndCompile(schemaText);
        final Field flagA = schema.getField(WRAPPER_PAYLOAD_TYPE, "flag_a");
        final Field flagB = schema.getField(WRAPPER_PAYLOAD_TYPE, "flag_b");
        final Field flagC = schema.getField(WRAPPER_PAYLOAD_TYPE, "flag_c");

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
    void testSanitizeKeepsPackedWhenCustomFieldOptionPresent() {
        final String schemaText = """
            syntax = "proto2";
            package example;

            message Sample {
              repeated int32 values = 1 [packed = true, (confluent.field_meta) = {
                tags: "x"
              }];
            }
            """;

        final Schema schema = sanitizeAndCompile(schemaText);
        final Field values = schema.getField(SAMPLE_TYPE, "values");

        assertNotNull(values);
        assertTrue(values.isRepeated());
        assertTrue(values.isPacked());
        assertNoParenthesizedOptions(values.getOptions());
    }

    @Test
    void testSanitizeRemovesCustomOptionsFromAllSyntaxElements() throws IOException {
        final String schemaText = readTestResource("/org/apache/nifi/protobuf/test/custom_options_on_all_elements.proto");

        final Schema schema = sanitizeAndCompile(schemaText);

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
        assertEquals("extra", extra.getName());
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
    void testSanitizePreservesTopLevelMessageOrder() {
        final String schemaText = """
            syntax = "proto3";
            package example;

            message A {
              int32 value = 1 [(custom.field_meta) = "a"];
            }

            message B {
              int32 value = 1 [(custom.field_meta) = "b"];
            }

            message C {
              int32 value = 1 [(custom.field_meta) = "c"];
            }
            """;

        final Schema schema = sanitizeAndCompile(schemaText);
        final ProtoFile protoFile = schema.protoFile(SAMPLE_PROTO);

        assertNotNull(protoFile);
        assertEquals(List.of("A", "B", "C"), protoFile.getTypes().stream().map(Type::getName).toList());
    }

    private Schema sanitizeAndCompile(final String schemaText) {
        return compiler.compileOrGetFromCache(sanitizer.sanitizeOrGetFromCache(schemaDefinition(SAMPLE_PROTO, schemaText)));
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
        return new StandardSchemaDefinition(SchemaIdentifier.builder().name(name).build(), schemaText, SchemaDefinition.SchemaType.PROTOBUF, references);
    }
}
