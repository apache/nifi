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

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.UnknownFieldSet;
import com.squareup.wire.schema.EnumType;
import com.squareup.wire.schema.Field;
import com.squareup.wire.schema.MessageType;
import com.squareup.wire.schema.OneOf;
import com.squareup.wire.schema.ProtoType;
import com.squareup.wire.schema.Schema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.services.protobuf.FieldType;
import org.apache.nifi.services.protobuf.schema.ProtoSchemaParser;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import static com.google.protobuf.CodedInputStream.decodeZigZag32;
import static com.google.protobuf.TextFormat.unsignedToString;

/**
 * The class is responsible for creating Record by mapping the provided proto schema fields with the list of Unknown fields parsed from encoded proto data.
 */
public class ProtobufDataConverter {

    public static final String MAP_KEY_FIELD_NAME = "key";
    public static final String MAP_VALUE_FIELD_NAME = "value";
    public static final String ANY_TYPE_URL_FIELD_NAME = "type_url";
    public static final String ANY_VALUE_FIELD_NAME = "value";
    public static final String ANY_MESSAGE_TYPE = "google.protobuf.Any";

    private final Schema schema;
    private final String message;
    private final RecordSchema rootRecordSchema;
    private final boolean coerceTypes;
    private final boolean dropUnknownFields;

    private boolean containsAnyField = false;

    public ProtobufDataConverter(Schema schema, String message, RecordSchema recordSchema, boolean coerceTypes, boolean dropUnknownFields) {
        this.schema = schema;
        this.message = message;
        this.rootRecordSchema = recordSchema;
        this.coerceTypes = coerceTypes;
        this.dropUnknownFields = dropUnknownFields;
    }

    /**
     * Creates a record from the root message.
     *
     * @return created record
     * @throws IOException failed to read input stream
     */
    public MapRecord createRecord(InputStream data) throws IOException {
        final MessageType rootType = (MessageType) schema.getType(message);
        Objects.requireNonNull(rootType, String.format("Message with name [%s] not found in the provided proto files", message));

        MapRecord record = createRecord(rootType, ByteString.readFrom(data), rootRecordSchema);
        if (containsAnyField) {
            record.regenerateSchema();
        }

        return record;
    }

    /**
     * Creates a record for the provided message.
     *
     * @param messageType  message to create a record from
     * @param data         proto message data
     * @param recordSchema record schema for the created record
     * @return created record
     * @throws InvalidProtocolBufferException failed to parse input data
     */
    private MapRecord createRecord(MessageType messageType, ByteString data, RecordSchema recordSchema) throws InvalidProtocolBufferException {
        final UnknownFieldSet unknownFieldSet = UnknownFieldSet.parseFrom(data);

        if ((ANY_MESSAGE_TYPE).equals(messageType.getType().toString())) {
            containsAnyField = true;
            return handleAnyField(unknownFieldSet);
        }

        return new MapRecord(recordSchema, processMessageFields(messageType, unknownFieldSet), false, dropUnknownFields);
    }

    /**
     * Process declared, extension and oneOf fields in the provided message.
     *
     * @param messageType     message with fields to be processed
     * @param unknownFieldSet received proto data fields
     * @return Map of processed fields
     */
    private Map<String, Object> processMessageFields(MessageType messageType, UnknownFieldSet unknownFieldSet) throws InvalidProtocolBufferException {
        Map<String, Object> recordValues = new HashMap<>();

        for (final Field field : messageType.getDeclaredFields()) {
            getField(new ProtoField(field), unknownFieldSet.getField(field.getTag()), recordValues);
        }

        for (final Field field : messageType.getExtensionFields()) {
            getField(new ProtoField(field), unknownFieldSet.getField(field.getTag()), recordValues);
        }

        for (final OneOf oneOf : messageType.getOneOfs()) {
            for (Field field : oneOf.getFields()) {
                getField(new ProtoField(field), unknownFieldSet.getField(field.getTag()), recordValues);
            }
        }
        return recordValues;
    }

    /**
     * Checks the field value's presence and sets it into the result Map.
     *
     * @param protoField   proto field's properties
     * @param unknownField field's value
     * @param values       Map of values
     */
    private void getField(ProtoField protoField, UnknownFieldSet.Field unknownField, Map<String, Object> values) throws InvalidProtocolBufferException {
        Optional<Object> fieldValue = convertFieldValues(protoField, unknownField);
        fieldValue.ifPresent(o -> values.put(protoField.getFieldName(), o));
    }

    private Optional<Object> convertFieldValues(ProtoField protoField, UnknownFieldSet.Field unknownField) throws InvalidProtocolBufferException {
        if (!unknownField.getLengthDelimitedList().isEmpty()) {
            return Optional.of(convertLengthDelimitedFields(protoField, unknownField.getLengthDelimitedList()));
        }
        if (!unknownField.getFixed32List().isEmpty()) {
            return Optional.of(convertFixed32Fields(protoField, unknownField.getFixed32List()));
        }
        if (!unknownField.getFixed64List().isEmpty()) {
            return Optional.of(convertFixed64Fields(protoField, unknownField.getFixed64List()));
        }
        if (!unknownField.getVarintList().isEmpty()) {
            return Optional.of(convertVarintFields(protoField, unknownField.getVarintList()));
        }

        return Optional.empty();
    }

    /**
     * Converts a Length-Delimited field value into it's suitable data type.
     *
     * @param protoField proto field's properties
     * @param values     field's unprocessed values
     * @return converted field values
     * @throws InvalidProtocolBufferException failed to parse input data
     */
    private Object convertLengthDelimitedFields(ProtoField protoField, List<ByteString> values) throws InvalidProtocolBufferException {
        final ProtoType protoType = protoField.getProtoType();
        if (protoType.isScalar()) {
            switch (FieldType.findValue(protoType.getSimpleName())) {
                case STRING:
                    return resolveFieldValue(protoField, values, ByteString::toStringUtf8);
                case BYTES:
                    return resolveFieldValue(protoField, values, ByteString::toByteArray);
                default:
                    throw new IllegalStateException(String.format("Incompatible value was received for field [%s]," +
                            " [%s] is not LengthDelimited field type", protoField.getFieldName(), protoType.getSimpleName()));
            }
        } else if (protoType.isMap()) {
            return createMap(protoType, values);
        } else {
            final MessageType messageType = (MessageType) schema.getType(protoType);
            Objects.requireNonNull(messageType, String.format("Message with name [%s] not found in the provided proto files", protoType));

            final Function<ByteString, Object> getRecord = v -> {
                try {
                    Optional<DataType> recordDataType = rootRecordSchema.getDataType(protoField.getFieldName());
                    RecordSchema recordSchema = recordDataType.map(dataType ->
                            ((RecordDataType) dataType).getChildSchema()).orElse(generateRecordSchema(messageType.getType().toString()));
                    return createRecord(messageType, v, recordSchema);
                } catch (InvalidProtocolBufferException e) {
                    throw new IllegalStateException("Failed to create record from the provided input data for field " + protoField.getFieldName(), e);
                }
            };

            return resolveFieldValue(protoField, values, getRecord);
        }
    }

    /**
     * Converts a Fixed32 field value into it's suitable data type.
     *
     * @param protoField proto field's properties
     * @param values     field's unprocessed values
     * @return converted field values
     */
    private Object convertFixed32Fields(ProtoField protoField, List<Integer> values) {
        final String typeName = protoField.getProtoType().getSimpleName();
        switch (FieldType.findValue(typeName)) {
            case FIXED32:
                return resolveFieldValue(protoField, values, v -> Long.parseLong(unsignedToString(v)));
            case SFIXED32:
                return resolveFieldValue(protoField, values, v -> v);
            case FLOAT:
                return resolveFieldValue(protoField, values, Float::intBitsToFloat);
            default:
                throw new IllegalStateException(String.format("Incompatible value was received for field [%s]," +
                        " [%s] is not Fixed32 field type", protoField.getFieldName(), typeName));
        }
    }

    /**
     * Converts a Fixed64 field value into it's suitable data type.
     *
     * @param protoField proto field's properties
     * @param values     field's unprocessed values
     * @return converted field values
     */
    private Object convertFixed64Fields(ProtoField protoField, List<Long> values) {
        final String typeName = protoField.getProtoType().getSimpleName();
        switch (FieldType.findValue(typeName)) {
            case FIXED64:
                return resolveFieldValue(protoField, values, v -> new BigInteger(unsignedToString(v)));
            case SFIXED64:
                return resolveFieldValue(protoField, values, v -> v);
            case DOUBLE:
                return resolveFieldValue(protoField, values, Double::longBitsToDouble);
            default:
                throw new IllegalStateException(String.format("Incompatible value was received for field [%s]," +
                        " [%s] is not Fixed64 field type", protoField.getFieldName(), typeName));

        }
    }

    /**
     * Converts a Varint field value into it's suitable data type.
     *
     * @param protoField proto field's properties
     * @param values     field's unprocessed values
     * @return converted field values
     */
    private Object convertVarintFields(ProtoField protoField, List<Long> values) {
        final ProtoType protoType = protoField.getProtoType();
        if (protoField.getProtoType().isScalar()) {
            switch (FieldType.findValue(protoType.getSimpleName())) {
                case BOOL:
                    return resolveFieldValue(protoField, values, v -> v.equals(1L));
                case INT32:
                case SFIXED32:
                    return resolveFieldValue(protoField, values, Long::intValue);
                case UINT32:
                case INT64:
                case SFIXED64:
                    return resolveFieldValue(protoField, values, v -> v);
                case UINT64:
                    return resolveFieldValue(protoField, values, v -> new BigInteger(unsignedToString(v)));
                case SINT32:
                    return resolveFieldValue(protoField, values, v -> decodeZigZag32(v.intValue()));
                case SINT64:
                    return resolveFieldValue(protoField, values, CodedInputStream::decodeZigZag64);
                default:
                    throw new IllegalStateException(String.format("Incompatible value was received for field [%s]," +
                            " [%s] is not Varint field type", protoField.getFieldName(), protoType.getSimpleName()));
            }
        } else {
            final Function<Long, Object> enumFunction = v -> {
                final EnumType enumType = (EnumType) schema.getType(protoType);
                Objects.requireNonNull(enumType, String.format("Enum with name [%s] not found in the provided proto files", protoType));
                return enumType.constant(Integer.parseInt(v.toString())).getName();
            };

            return resolveFieldValue(protoField, values, enumFunction);
        }
    }

    private <T> Object resolveFieldValue(ProtoField protoField, List<T> values, Function<T, Object> getValue) {
        List<Object> resultValues = values.stream().map(getValue).toList();

        if (coerceTypes) {
            Optional<RecordField> recordField = rootRecordSchema.getField(protoField.getFieldName());
            if (recordField.isPresent()) {
                resultValues = resultValues.stream().map(o -> DataTypeUtils.convertType(o, recordField.get().getDataType(), recordField.get().getFieldName())).toList();
            }
        }

        if (!protoField.isRepeatable()) {
            return resultValues.get(0);
        } else {
            return resultValues.toArray();
        }
    }

    /**
     * Handles Map type creation in the record.
     *
     * @param protoType field's proto type
     * @param data      data to be processed
     * @return created Map
     * @throws InvalidProtocolBufferException failed to parse input data
     */
    private Map<String, Object> createMap(ProtoType protoType, List<ByteString> data) throws InvalidProtocolBufferException {
        Map<String, Object> mapResult = new HashMap<>();

        for (final ByteString entry : data) {
            final UnknownFieldSet unknownFieldSet = UnknownFieldSet.parseFrom(entry);
            Map<String, Object> mapEntry = new HashMap<>();

            getField(new ProtoField(MAP_KEY_FIELD_NAME, protoType.getKeyType(), false), unknownFieldSet.getField(1), mapEntry);
            getField(new ProtoField(MAP_VALUE_FIELD_NAME, protoType.getValueType(), false), unknownFieldSet.getField(2), mapEntry);

            mapResult.put(String.valueOf(mapEntry.get(MAP_KEY_FIELD_NAME)), mapEntry.get(MAP_VALUE_FIELD_NAME));
        }

        return mapResult;
    }

    /**
     * Process a 'google.protobuf.Any' typed field. The method gets the schema for the message provided in the 'type_url' property
     * and parse the serialized message from the 'value' field. The result record will contain only the parsed message's fields.
     *
     * @param unknownFieldSet 'google.protobuf.Any' typed message's field list
     * @return created record from the parsed message
     * @throws InvalidProtocolBufferException failed to parse input data
     */
    private MapRecord handleAnyField(UnknownFieldSet unknownFieldSet) throws InvalidProtocolBufferException {
        Map<String, Object> recordValues = new HashMap<>();
        getField(new ProtoField(ANY_TYPE_URL_FIELD_NAME, ProtoType.STRING, false), unknownFieldSet.getField(1), recordValues);
        getField(new ProtoField(ANY_VALUE_FIELD_NAME, ProtoType.BYTES, false), unknownFieldSet.getField(2), recordValues);

        final String typeName = String.valueOf(recordValues.get(ANY_TYPE_URL_FIELD_NAME));
        final UnknownFieldSet anyFieldSet = UnknownFieldSet.parseFrom((byte[]) recordValues.get(ANY_VALUE_FIELD_NAME));
        final MessageType messageType = (MessageType) schema.getType(normalizeTypeName(typeName));
        Objects.requireNonNull(messageType, String.format("Message with name [%s] not found in the provided proto files", typeName));

        return new MapRecord(generateRecordSchema(typeName), processMessageFields(messageType, anyFieldSet), false, dropUnknownFields);
    }

    /**
     * Generates a schema for the provided message type
     *
     * @param typeName name of the message
     * @return generated schema
     */
    private RecordSchema generateRecordSchema(String typeName) {
        final ProtoSchemaParser schemaParser = new ProtoSchemaParser(schema);
        return schemaParser.createSchema(normalizeTypeName(typeName));
    }

    /**
     * Gets the fully qualified name of the message type.
     *
     * @param typeName name of the message
     * @return fully qualified name of the message type
     */
    private String normalizeTypeName(String typeName) {
        return typeName.substring(typeName.lastIndexOf('/') + 1);
    }
}
