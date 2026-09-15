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

import com.google.protobuf.CodedOutputStream;
import com.squareup.wire.schema.EnumConstant;
import com.squareup.wire.schema.EnumType;
import com.squareup.wire.schema.Field;
import com.squareup.wire.schema.MessageType;
import com.squareup.wire.schema.OneOf;
import com.squareup.wire.schema.ProtoType;
import com.squareup.wire.schema.Schema;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.services.protobuf.FieldType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Serializes a NiFi {@link Record} into Protocol Buffers binary payload using a Square Wire
 * {@link Schema}. This is the write-side inverse of {@code ProtobufDataConverter}: it walks the
 * declared fields of the target message type and encodes each present Record value to a
 * {@link CodedOutputStream} following the Protocol Buffers wire format.
 * <p>
 * This class has no dependency on the NiFi framework and can be exercised directly with plain
 * unit tests.
 */
public class ProtobufDataSerializer {

    private static final int WIRETYPE_VARINT = 0;
    private static final int WIRETYPE_FIXED64 = 1;
    private static final int WIRETYPE_LENGTH_DELIMITED = 2;
    private static final int WIRETYPE_FIXED32 = 5;

    private static final int MAP_KEY_TAG = 1;
    private static final int MAP_VALUE_TAG = 2;

    private final Schema schema;
    private final String rootMessageType;

    public ProtobufDataSerializer(final Schema schema, final String rootMessageType) {
        this.schema = schema;
        this.rootMessageType = rootMessageType;
    }

    /**
     * Serializes the provided Record into Protocol Buffers binary format for the configured root
     * message type.
     *
     * @param record the record to serialize
     * @return the serialized protobuf payload
     * @throws IOException if the record cannot be encoded
     */
    public byte[] serialize(final Record record) throws IOException {
        final MessageType messageType = (MessageType) schema.getType(rootMessageType);
        Objects.requireNonNull(messageType, String.format("Message with name [%s] not found in the provided proto files", rootMessageType));

        return serializeMessage(messageType, record);
    }

    private byte[] serializeMessage(final MessageType messageType, final Record record) throws IOException {
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        final CodedOutputStream codedOutput = CodedOutputStream.newInstance(output);

        for (final Field field : messageType.getDeclaredFields()) {
            writeField(codedOutput, field, record.getValue(field.getName()));
        }
        for (final Field field : messageType.getExtensionFields()) {
            writeField(codedOutput, field, record.getValue(field.getName()));
        }
        for (final OneOf oneOf : messageType.getOneOfs()) {
            for (final Field field : oneOf.getFields()) {
                writeField(codedOutput, field, record.getValue(field.getName()));
            }
        }

        codedOutput.flush();
        return output.toByteArray();
    }

    private void writeField(final CodedOutputStream output, final Field field, final Object value) throws IOException {
        if (value == null) {
            return;
        }

        final int tag = field.getTag();
        final ProtoType protoType = field.getType();

        if (protoType.isMap()) {
            writeMap(output, tag, protoType, value);
        } else if (field.isRepeated()) {
            writeRepeated(output, tag, protoType, value);
        } else {
            writeSingleValue(output, tag, protoType, value);
        }
    }

    private void writeRepeated(final CodedOutputStream output, final int tag, final ProtoType protoType, final Object value) throws IOException {
        final Object[] values = toArray(value);

        if (isPackable(protoType)) {
            // proto3 packs repeated scalar and enum fields into a single length-delimited entry
            final ByteArrayOutputStream packedBytes = new ByteArrayOutputStream();
            final CodedOutputStream packedOutput = CodedOutputStream.newInstance(packedBytes);
            for (final Object element : values) {
                writeScalarOrEnumValueNoTag(packedOutput, protoType, element);
            }
            packedOutput.flush();

            output.writeTag(tag, WIRETYPE_LENGTH_DELIMITED);
            final byte[] packed = packedBytes.toByteArray();
            output.writeUInt32NoTag(packed.length);
            output.writeRawBytes(packed);
        } else {
            // repeated messages, strings and bytes are written as separate length-delimited entries
            for (final Object element : values) {
                writeSingleValue(output, tag, protoType, element);
            }
        }
    }

    private void writeSingleValue(final CodedOutputStream output, final int tag, final ProtoType protoType, final Object value) throws IOException {
        if (protoType.isScalar()) {
            final FieldType fieldType = FieldType.findValue(protoType.getSimpleName());
            output.writeTag(tag, wireTypeFor(fieldType));
            writeScalarValueNoTag(output, fieldType, value);
            return;
        }

        if (schema.getType(protoType) instanceof EnumType) {
            output.writeTag(tag, WIRETYPE_VARINT);
            output.writeEnumNoTag(enumTag(protoType, value));
            return;
        }

        // nested message
        final MessageType messageType = (MessageType) schema.getType(protoType);
        Objects.requireNonNull(messageType, String.format("Message type with name [%s] not found in the provided proto files", protoType));
        final byte[] nested = serializeMessage(messageType, toRecord(value, protoType));
        output.writeTag(tag, WIRETYPE_LENGTH_DELIMITED);
        output.writeUInt32NoTag(nested.length);
        output.writeRawBytes(nested);
    }

    private void writeMap(final CodedOutputStream output, final int tag, final ProtoType protoType, final Object value) throws IOException {
        if (!(value instanceof final Map<?, ?> map)) {
            throw new IOException(String.format("Expected a Map value for map field but received [%s]", value.getClass()));
        }

        final ProtoType keyType = protoType.getKeyType();
        final ProtoType valueType = protoType.getValueType();

        for (final Map.Entry<?, ?> entry : map.entrySet()) {
            final ByteArrayOutputStream entryBytes = new ByteArrayOutputStream();
            final CodedOutputStream entryOutput = CodedOutputStream.newInstance(entryBytes);

            writeSingleValue(entryOutput, MAP_KEY_TAG, keyType, entry.getKey());
            if (entry.getValue() != null) {
                writeSingleValue(entryOutput, MAP_VALUE_TAG, valueType, entry.getValue());
            }
            entryOutput.flush();

            output.writeTag(tag, WIRETYPE_LENGTH_DELIMITED);
            final byte[] entryEncoded = entryBytes.toByteArray();
            output.writeUInt32NoTag(entryEncoded.length);
            output.writeRawBytes(entryEncoded);
        }
    }

    private void writeScalarOrEnumValueNoTag(final CodedOutputStream output, final ProtoType protoType, final Object value) throws IOException {
        if (protoType.isScalar()) {
            writeScalarValueNoTag(output, FieldType.findValue(protoType.getSimpleName()), value);
        } else {
            output.writeEnumNoTag(enumTag(protoType, value));
        }
    }

    private void writeScalarValueNoTag(final CodedOutputStream output, final FieldType fieldType, final Object value) throws IOException {
        // 32-bit varint types (uint32, sint32, fixed32) map to a Record LONG in ProtoSchemaParser, so they are
        // coerced through long and narrowed to int; the unsigned/zigzag encoding is handled by CodedOutputStream.
        switch (fieldType) {
            case BOOL -> output.writeBoolNoTag(DataTypeUtils.toBoolean(value, null));
            case INT32 -> output.writeInt32NoTag((int) DataTypeUtils.toLong(value, null).longValue());
            case SFIXED32 -> output.writeSFixed32NoTag((int) DataTypeUtils.toLong(value, null).longValue());
            case UINT32 -> output.writeUInt32NoTag((int) DataTypeUtils.toLong(value, null).longValue());
            case SINT32 -> output.writeSInt32NoTag((int) DataTypeUtils.toLong(value, null).longValue());
            case FIXED32 -> output.writeFixed32NoTag((int) DataTypeUtils.toLong(value, null).longValue());
            case INT64 -> output.writeInt64NoTag(DataTypeUtils.toLong(value, null));
            case SFIXED64 -> output.writeSFixed64NoTag(DataTypeUtils.toLong(value, null));
            case SINT64 -> output.writeSInt64NoTag(DataTypeUtils.toLong(value, null));
            case UINT64 -> output.writeUInt64NoTag(toBigInteger(value).longValue());
            case FIXED64 -> output.writeFixed64NoTag(toBigInteger(value).longValue());
            case FLOAT -> output.writeFloatNoTag(DataTypeUtils.toFloat(value, null));
            case DOUBLE -> output.writeDoubleNoTag(DataTypeUtils.toDouble(value, null));
            case STRING -> output.writeStringNoTag(DataTypeUtils.toString(value, (String) null));
            case BYTES -> output.writeByteArrayNoTag(toByteArray(value));
        }
    }

    private int wireTypeFor(final FieldType fieldType) {
        return switch (fieldType) {
            case BOOL, INT32, INT64, UINT32, UINT64, SINT32, SINT64 -> WIRETYPE_VARINT;
            case FIXED32, SFIXED32, FLOAT -> WIRETYPE_FIXED32;
            case FIXED64, SFIXED64, DOUBLE -> WIRETYPE_FIXED64;
            case STRING, BYTES -> WIRETYPE_LENGTH_DELIMITED;
        };
    }

    private boolean isPackable(final ProtoType protoType) {
        if (protoType.isScalar()) {
            final FieldType fieldType = FieldType.findValue(protoType.getSimpleName());
            return fieldType != FieldType.STRING && fieldType != FieldType.BYTES;
        }
        // enums are packable; messages are not
        return schema.getType(protoType) instanceof EnumType;
    }

    private int enumTag(final ProtoType protoType, final Object value) {
        final EnumType enumType = (EnumType) schema.getType(protoType);
        Objects.requireNonNull(enumType, String.format("Enum with name [%s] not found in the provided proto files", protoType));

        final String constantName = String.valueOf(value);
        final EnumConstant constant = enumType.constant(constantName);
        if (constant == null) {
            throw new IllegalStateException(String.format("Enum constant [%s] not found in enum [%s]", constantName, protoType));
        }
        return constant.getTag();
    }

    private Record toRecord(final Object value, final ProtoType protoType) throws IOException {
        if (value instanceof final Record record) {
            return record;
        }
        throw new IOException(String.format("Expected a Record value for message field [%s] but received [%s]", protoType, value.getClass()));
    }

    private Object[] toArray(final Object value) {
        if (value instanceof final Object[] array) {
            return array;
        }
        if (value instanceof final List<?> list) {
            return list.toArray();
        }
        return new Object[] {value};
    }

    private BigInteger toBigInteger(final Object value) {
        if (value instanceof final BigInteger bigInteger) {
            return bigInteger;
        }
        if (value instanceof final Number number) {
            return BigInteger.valueOf(number.longValue());
        }
        return new BigInteger(String.valueOf(value));
    }

    private byte[] toByteArray(final Object value) {
        if (value instanceof final byte[] bytes) {
            return bytes;
        }
        if (value instanceof final Object[] array) {
            final byte[] bytes = new byte[array.length];
            for (int i = 0; i < array.length; i++) {
                bytes[i] = ((Number) array[i]).byteValue();
            }
            return bytes;
        }
        final List<Byte> collected = new ArrayList<>();
        if (value instanceof final List<?> list) {
            for (final Object element : list) {
                collected.add(((Number) element).byteValue());
            }
        }
        final byte[] bytes = new byte[collected.size()];
        for (int i = 0; i < collected.size(); i++) {
            bytes[i] = collected.get(i);
        }
        return bytes;
    }
}
