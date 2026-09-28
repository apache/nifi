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
package org.apache.nifi.services.protobuf.converter;

import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.squareup.wire.schema.Location;
import com.squareup.wire.schema.Schema;
import com.squareup.wire.schema.SchemaLoader;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.services.protobuf.ProtoTestUtil;
import org.apache.nifi.services.protobuf.schema.ProtoSchemaParser;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.services.protobuf.ProtoTestUtil.generateInputDataForRootMessage;
import static org.apache.nifi.services.protobuf.ProtoTestUtil.loadProto2TestSchema;
import static org.apache.nifi.services.protobuf.ProtoTestUtil.loadProto3TestSchema;
import static org.apache.nifi.services.protobuf.ProtoTestUtil.loadRepeatedProto3TestSchema;
import static org.apache.nifi.services.protobuf.ProtoTestUtil.loadRootMessageSchema;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Validates {@link ProtobufDataSerializer} by round-tripping through the reader-side
 * {@link ProtobufDataConverter}: a Record is produced from known protobuf bytes, re-serialized by
 * the serializer, and parsed back, asserting the values survive the round trip losslessly. Runs
 * without the NiFi framework (no TestRunner).
 */
public class TestProtobufDataSerializer {

    @Test
    public void testSerializeProto3RoundTrip() throws DescriptorValidationException, IOException {
        final Schema schema = loadProto3TestSchema();
        final RecordSchema recordSchema = new ProtoSchemaParser(schema).createSchema("Proto3Message");

        final MapRecord originalRecord = new ProtobufDataConverter(schema, "Proto3Message", recordSchema, false, false)
            .createRecord(ProtoTestUtil.generateInputDataForProto3());

        final byte[] serialized = new ProtobufDataSerializer(schema, "Proto3Message").serialize(originalRecord);

        final MapRecord record = new ProtobufDataConverter(schema, "Proto3Message", recordSchema, false, false)
            .createRecord(new ByteArrayInputStream(serialized));

        assertEquals(true, record.getValue("booleanField"));
        assertEquals("Test text", record.getValue("stringField"));
        assertEquals(Integer.MAX_VALUE, record.getValue("int32Field"));
        assertEquals(4294967295L, record.getValue("uint32Field"));
        assertEquals(Integer.MIN_VALUE, record.getValue("sint32Field"));
        assertEquals(4294967294L, record.getValue("fixed32Field"));
        assertEquals(Integer.MAX_VALUE, record.getValue("sfixed32Field"));
        assertEquals(Double.MAX_VALUE, record.getValue("doubleField"));
        assertEquals(Float.MAX_VALUE, record.getValue("floatField"));
        assertArrayEquals("Test bytes".getBytes(), (byte[]) record.getValue("bytesField"));
        assertEquals(Long.MAX_VALUE, record.getValue("int64Field"));
        assertEquals(new BigInteger("18446744073709551615"), record.getValue("uint64Field"));
        assertEquals(Long.MIN_VALUE, record.getValue("sint64Field"));
        assertEquals(new BigInteger("18446744073709551614"), record.getValue("fixed64Field"));
        assertEquals(Long.MAX_VALUE, record.getValue("sfixed64Field"));

        final MapRecord nestedRecord = (MapRecord) record.getValue("nestedMessage");
        assertEquals("ENUM_VALUE_3", nestedRecord.getValue("testEnum"));

        final Object[] recordList = (Object[]) nestedRecord.getValue("nestedMessage2");
        assertEquals(1, recordList.length);

        final MapRecord nestedRecord2 = (MapRecord) recordList[0];
        assertEquals(Map.of("test_key_entry1", 101, "test_key_entry2", 202), nestedRecord2.getValue("testMap"));

        // Only one field is set in the OneOf field
        assertNull(nestedRecord2.getValue("stringOption"));
        assertNull(nestedRecord2.getValue("booleanOption"));
        assertEquals(3, nestedRecord2.getValue("int32Option"));
    }

    @Test
    public void testSerializeNestedMessageRoundTrip() throws DescriptorValidationException, IOException {
        final Schema schema = loadRootMessageSchema();
        final String messageName = "org.apache.nifi.protobuf.test.RootMessage";
        final RecordSchema recordSchema = new ProtoSchemaParser(schema).createSchema(messageName);

        final MapRecord originalRecord = new ProtobufDataConverter(schema, messageName, recordSchema, false, false)
            .createRecord(generateInputDataForRootMessage());

        final byte[] serialized = new ProtobufDataSerializer(schema, messageName).serialize(originalRecord);

        final MapRecord record = new ProtobufDataConverter(schema, messageName, recordSchema, false, false)
            .createRecord(new ByteArrayInputStream(serialized));

        final MapRecord nestedRecord = (MapRecord) record.getValue("nestedMessage");
        assertEquals("ENUM_VALUE_3", nestedRecord.getValue("testEnum"));
    }

    @Test
    public void testSerializeRepeatedProto3RoundTrip() throws DescriptorValidationException, IOException {
        final Schema schema = loadRepeatedProto3TestSchema();
        final RecordSchema recordSchema = new ProtoSchemaParser(schema).createSchema("RootMessage");

        final MapRecord originalRecord = new ProtobufDataConverter(schema, "RootMessage", recordSchema, false, false)
            .createRecord(ProtoTestUtil.generateInputDataForRepeatedProto3());

        final byte[] serialized = new ProtobufDataSerializer(schema, "RootMessage").serialize(originalRecord);

        final MapRecord record = new ProtobufDataConverter(schema, "RootMessage", recordSchema, false, false)
            .createRecord(new ByteArrayInputStream(serialized));

        final Object[] repeatedMessage = (Object[]) record.getValue("repeatedMessage");
        final MapRecord record1 = (MapRecord) repeatedMessage[0];

        assertArrayEquals(new Object[]{true, false}, (Object[]) record1.getValue("booleanField"));
        assertArrayEquals(new Object[]{"Test text1", "Test text2"}, (Object[]) record1.getValue("stringField"));
        assertArrayEquals(new Object[]{Integer.MAX_VALUE, Integer.MAX_VALUE - 1}, (Object[]) record1.getValue("int32Field"));
        assertArrayEquals(new Object[]{4294967295L, 4294967294L}, (Object[]) record1.getValue("uint32Field"));
        assertArrayEquals(new Object[]{Integer.MIN_VALUE, Integer.MIN_VALUE + 1}, (Object[]) record1.getValue("sint32Field"));
        assertArrayEquals(new Object[]{4294967294L, 4294967293L}, (Object[]) record1.getValue("fixed32Field"));
        assertArrayEquals(new Object[]{Integer.MAX_VALUE, Integer.MAX_VALUE - 1}, (Object[]) record1.getValue("sfixed32Field"));
        assertArrayEquals(new Object[]{Double.MAX_VALUE, Double.MAX_VALUE - 1}, (Object[]) record1.getValue("doubleField"));
        assertArrayEquals(new Object[]{Float.MAX_VALUE, Float.MAX_VALUE - 1}, (Object[]) record1.getValue("floatField"));
        assertArrayEquals(new Object[]{Long.MAX_VALUE, Long.MAX_VALUE - 1}, (Object[]) record1.getValue("int64Field"));
        assertArrayEquals(new Object[]{Long.MIN_VALUE, Long.MIN_VALUE + 1}, (Object[]) record1.getValue("sint64Field"));
        assertArrayEquals(new Object[]{Long.MAX_VALUE, Long.MAX_VALUE - 1}, (Object[]) record1.getValue("sfixed64Field"));
        assertArrayEquals(new Object[]{"ENUM_VALUE_2", "ENUM_VALUE_3"}, (Object[]) record1.getValue("testEnum"));

        final Object[] uint64FieldValues = (Object[]) record1.getValue("uint64Field");
        assertEquals(new BigInteger("18446744073709551615"), uint64FieldValues[0]);
        assertEquals(new BigInteger("18446744073709551614"), uint64FieldValues[1]);

        final Object[] bytesFieldValues = (Object[]) record1.getValue("bytesField");
        assertArrayEquals("Test bytes1".getBytes(), (byte[]) bytesFieldValues[0]);
        assertArrayEquals("Test bytes2".getBytes(), (byte[]) bytesFieldValues[1]);

        final MapRecord record2 = (MapRecord) repeatedMessage[1];
        assertArrayEquals(new Object[]{true}, (Object[]) record2.getValue("booleanField"));
    }

    @Test
    public void testSerializeProto2RequiredFieldPresent() throws IOException {
        final Schema schema = loadProto2TestSchema();
        final RecordSchema recordSchema = new ProtoSchemaParser(schema).createSchema("Proto2Message");
        final MapRecord originalRecord = new MapRecord(recordSchema, Map.of("booleanField", true));

        final byte[] serialized = new ProtobufDataSerializer(schema, "Proto2Message").serialize(originalRecord);

        final MapRecord record = new ProtobufDataConverter(schema, "Proto2Message", recordSchema, false, false)
            .createRecord(new ByteArrayInputStream(serialized));
        assertEquals(true, record.getValue("booleanField"));
    }

    @Test
    public void testSerializeProto2MissingRequiredFieldFails() {
        final Schema schema = loadProto2TestSchema();
        final RecordSchema recordSchema = new ProtoSchemaParser(schema).createSchema("Proto2Message");
        final MapRecord record = new MapRecord(recordSchema, Map.of("stringField", "Test text"));

        final IOException exception = assertThrows(IOException.class,
            () -> new ProtobufDataSerializer(schema, "Proto2Message").serialize(record));
        assertTrue(exception.getMessage().contains("booleanField"));
    }

    @Test
    public void testSerializeNullMapKeyFails() throws DescriptorValidationException, IOException {
        final Schema schema = loadProto3TestSchema();
        final RecordSchema recordSchema = new ProtoSchemaParser(schema).createSchema("Proto3Message");
        final MapRecord record = new ProtobufDataConverter(schema, "Proto3Message", recordSchema, false, false)
            .createRecord(ProtoTestUtil.generateInputDataForProto3());

        final Map<String, Object> mapWithNullKey = new HashMap<>();
        mapWithNullKey.put(null, 1);
        final MapRecord nestedRecord = (MapRecord) record.getValue("nestedMessage");
        final MapRecord nestedRecord2 = (MapRecord) ((Object[]) nestedRecord.getValue("nestedMessage2"))[0];
        nestedRecord2.setValue("testMap", mapWithNullKey);

        final IOException exception = assertThrows(IOException.class,
            () -> new ProtobufDataSerializer(schema, "Proto3Message").serialize(record));
        assertTrue(exception.getMessage().contains("null key"));
    }

    // Tag bytes: (field number << 3) | wire type, where wire type 0 is varint and 2 is length-delimited
    private static final byte FIELD_1_VARINT_TAG = 0x08;
    private static final byte FIELD_1_LENGTH_DELIMITED_TAG = 0x0A;

    @TempDir
    private Path schemaDirectory;

    @Test
    public void testSerializeOneofWithMultipleValuesFails() throws IOException {
        final Schema schema = compileSchema("""
            syntax = "proto3";
            message Choice {
              oneof value {
                string text = 1;
                int32 number = 2;
              }
            }""");
        final MapRecord record = new MapRecord(new ProtoSchemaParser(schema).createSchema("Choice"), Map.of("text", "a", "number", 1));

        final IOException exception = assertThrows(IOException.class, () -> new ProtobufDataSerializer(schema, "Choice").serialize(record));
        assertTrue(exception.getMessage().contains("value"));
    }

    @Test
    public void testSerializeOneofWithSingleValue() throws IOException {
        final Schema schema = compileSchema("""
            syntax = "proto3";
            message Choice {
              oneof value {
                string text = 1;
                int32 number = 2;
              }
            }""");
        final RecordSchema recordSchema = new ProtoSchemaParser(schema).createSchema("Choice");
        final MapRecord originalRecord = new MapRecord(recordSchema, Map.of("number", 7));

        final byte[] serialized = new ProtobufDataSerializer(schema, "Choice").serialize(originalRecord);

        final MapRecord record = new ProtobufDataConverter(schema, "Choice", recordSchema, false, false).createRecord(new ByteArrayInputStream(serialized));
        assertEquals(7, record.getValue("number"));
        assertNull(record.getValue("text"));
    }

    @Test
    public void testSerializeProto2RepeatedUnpackedByDefault() throws IOException {
        assertArrayEquals(new byte[] {FIELD_1_VARINT_TAG, 1, FIELD_1_VARINT_TAG, 2}, serializeRepeated("proto2", "int32", ""));
    }

    @Test
    public void testSerializeProto2RepeatedPackedOption() throws IOException {
        assertArrayEquals(new byte[] {FIELD_1_LENGTH_DELIMITED_TAG, 2, 1, 2}, serializeRepeated("proto2", "int32", " [packed = true]"));
    }

    @Test
    public void testSerializeProto3RepeatedPackedByDefault() throws IOException {
        assertArrayEquals(new byte[] {FIELD_1_LENGTH_DELIMITED_TAG, 2, 1, 2}, serializeRepeated("proto3", "int32", ""));
    }

    @Test
    public void testSerializeProto3RepeatedUnpackedOption() throws IOException {
        assertArrayEquals(new byte[] {FIELD_1_VARINT_TAG, 1, FIELD_1_VARINT_TAG, 2}, serializeRepeated("proto3", "int32", " [packed = false]"));
    }

    @Test
    public void testSerializeProto3RepeatedEnumPackedByDefault() throws IOException {
        assertArrayEquals(new byte[] {FIELD_1_LENGTH_DELIMITED_TAG, 2, 1, 2}, serializeRepeated("proto3", "Color", ""));
    }

    @Test
    public void testSerializeProto2RepeatedEnumUnpackedByDefault() throws IOException {
        assertArrayEquals(new byte[] {FIELD_1_VARINT_TAG, 1, FIELD_1_VARINT_TAG, 2}, serializeRepeated("proto2", "Color", ""));
    }

    @Test
    public void testSerializeBytesFromByteBuffer() throws IOException {
        final byte[] expected = "bytes".getBytes(StandardCharsets.UTF_8);
        assertArrayEquals(expected, serializeAndReadBytes(ByteBuffer.wrap(expected)));
    }

    @Test
    public void testSerializeBytesFromUnsupportedValueFails() {
        assertThrows(IOException.class, () -> serializeAndReadBytes("text"));
    }

    @Test
    public void testSerializeBytesWithNonNumericElementFails() {
        assertThrows(IOException.class, () -> serializeAndReadBytes(new Object[] {1, "two"}));
    }

    private byte[] serializeRepeated(final String syntax, final String elementType, final String fieldOptions) throws IOException {
        final Schema schema = compileSchema("""
            syntax = "%s";
            enum Color {
              RED = 0;
              GREEN = 1;
              BLUE = 2;
            }
            message Values {
              repeated %s values = 1%s;
            }""".formatted(syntax, elementType, fieldOptions));
        final Object[] values = "Color".equals(elementType) ? new Object[] {"GREEN", "BLUE"} : new Object[] {1, 2};
        final MapRecord record = new MapRecord(new ProtoSchemaParser(schema).createSchema("Values"), Map.of("values", values));

        return new ProtobufDataSerializer(schema, "Values").serialize(record);
    }

    private byte[] serializeAndReadBytes(final Object value) throws IOException {
        final Schema schema = compileSchema("""
            syntax = "proto3";
            message Binary {
              bytes data = 1;
            }""");
        final RecordSchema recordSchema = new ProtoSchemaParser(schema).createSchema("Binary");
        final MapRecord originalRecord = new MapRecord(recordSchema, Map.of("data", value));

        final byte[] serialized = new ProtobufDataSerializer(schema, "Binary").serialize(originalRecord);

        final MapRecord record = new ProtobufDataConverter(schema, "Binary", recordSchema, false, false).createRecord(new ByteArrayInputStream(serialized));
        return (byte[]) record.getValue("data");
    }

    private Schema compileSchema(final String schemaText) throws IOException {
        Files.writeString(schemaDirectory.resolve("test.proto"), schemaText);
        final SchemaLoader schemaLoader = new SchemaLoader(FileSystems.getDefault());
        schemaLoader.initRoots(List.of(Location.get(schemaDirectory.toString())), List.of());
        return schemaLoader.loadSchema();
    }
}
