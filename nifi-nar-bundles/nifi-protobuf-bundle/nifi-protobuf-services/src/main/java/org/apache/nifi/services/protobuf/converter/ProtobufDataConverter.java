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
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.services.protobuf.FieldType;
import org.apache.nifi.services.protobuf.schema.ProtoSchemaParser;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.google.protobuf.CodedInputStream.decodeZigZag32;
import static com.google.protobuf.TextFormat.unsignedToString;
import static org.apache.nifi.services.protobuf.FieldType.STRING;
import static org.apache.nifi.services.protobuf.FieldType.BYTES;

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
    private final String rootMessageType;
    private final RecordSchema rootRecordSchema;
    private final boolean coerceTypes;
    private final boolean dropUnknownFields;

    private boolean containsAnyField = false;

    public ProtobufDataConverter(Schema schema, String messageType, RecordSchema recordSchema, boolean coerceTypes, boolean dropUnknownFields) {
        this.schema = schema;
        this.rootMessageType = messageType;
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
        final MessageType rootMessageType = (MessageType) schema.getType(this.rootMessageType);
        Objects.requireNonNull(rootMessageType, String.format("Message with name [%s] not found in the provided proto files", this.rootMessageType));

        MapRecord record = createRecord(rootMessageType, ByteString.readFrom(data), rootRecordSchema);
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

        final Map<String, Object> fieldValues = processMessageFields(messageType, unknownFieldSet);
        return new MapRecord(recordSchema, fieldValues, false, dropUnknownFields);
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
            collectFieldValue(recordValues, new ProtoField(field), unknownFieldSet.getField(field.getTag()));
        }

        for (final Field field : messageType.getExtensionFields()) {
            collectFieldValue(recordValues, new ProtoField(field), unknownFieldSet.getField(field.getTag()));
        }

        for (final OneOf oneOf : messageType.getOneOfs()) {
            for (Field field : oneOf.getFields()) {
                collectFieldValue(recordValues, new ProtoField(field), unknownFieldSet.getField(field.getTag()));
            }
        }
        return recordValues;
    }

    /**
     * Checks the field value's presence and sets it into the result Map.
     *
     * @param fieldNameToConvertedValue Map of converter values
     * @param protoField                proto field's properties
     * @param unknownField              field's value
     */
    private void collectFieldValue(Map<String, Object> fieldNameToConvertedValue, ProtoField protoField, UnknownFieldSet.Field unknownField) throws InvalidProtocolBufferException {
        final Optional<Object> fieldValue = convertFieldValues(protoField, unknownField);
        fieldValue.ifPresent(value -> fieldNameToConvertedValue.put(protoField.getFieldName(), value));
    }

    private Optional<Object> convertFieldValues(ProtoField protoField, UnknownFieldSet.Field unknownField) throws InvalidProtocolBufferException {
        if (!unknownField.getLengthDelimitedList().isEmpty()) {
            if (protoField.isRepeatable() && !isLengthDelimitedType(protoField)) {
                return Optional.of(convertRepeatedFields(protoField, unknownField.getLengthDelimitedList()));
            } else {
                return Optional.of(convertLengthDelimitedFields(protoField, unknownField.getLengthDelimitedList()));
            }
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

    private Object convertRepeatedFields(ProtoField protoField, List<ByteString> fieldValues) {
        final CodedInputStream inputStream = fieldValues.get(0).newCodedInput();
        final ProtoType protoType = protoField.getProtoType();
        if (protoType.isScalar()) {
            final ValueReader<CodedInputStream, Object> valueReader;
            switch (FieldType.findValue(protoType.getSimpleName())) {
                case BOOL:
                    valueReader = CodedInputStream::readBool;
                    break;
                case INT32:
                    valueReader = CodedInputStream::readInt32;
                    break;
                case UINT32:
                    valueReader = value -> Integer.toUnsignedLong(value.readUInt32());
                    break;
                case SINT32:
                    valueReader = CodedInputStream::readSInt32;
                    break;
                case INT64:
                    valueReader = CodedInputStream::readInt64;
                    break;
                case UINT64:
                    valueReader = value -> new BigInteger(unsignedToString(value.readUInt64()));
                    break;
                case SINT64:
                    valueReader = CodedInputStream::readSInt64;
                    break;
                case FIXED32:
                    valueReader = value -> Integer.toUnsignedLong(value.readFixed32());
                    break;
                case SFIXED32:
                    valueReader = CodedInputStream::readSFixed32;
                    break;
                case FIXED64:
                    valueReader = value -> new BigInteger(unsignedToString(value.readFixed64()));
                    break;
                case SFIXED64:
                    valueReader = CodedInputStream::readSFixed64;
                    break;
                case FLOAT:
                    valueReader = CodedInputStream::readFloat;
                    break;
                case DOUBLE:
                    valueReader = CodedInputStream::readDouble;
                    break;
                default:
                    throw new IllegalStateException(String.format("Unexpected type [%s] was received for field [%s]",
                            protoType.getSimpleName(), protoField.getFieldName()));
            }
            return resolveFieldValue(protoField, processRepeatedValues(inputStream, valueReader), value -> value);
        } else {
            List<Integer> values = processRepeatedValues(inputStream, CodedInputStream::readEnum);
            return resolveFieldValue(protoField, values, value -> convertEnum(value, protoType));
        }
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
        final Function<ByteString, Object> valueConverter;
        if (protoType.isScalar()) {
            switch (FieldType.findValue(protoType.getSimpleName())) {
                case STRING:
                    valueConverter = ByteString::toStringUtf8;
                    break;
                case BYTES:
                    valueConverter = ByteString::toByteArray;
                    break;
                default:
                    throw new IllegalStateException(String.format("Incompatible value was received for field [%s]," +
                            " [%s] is not LengthDelimited field type", protoField.getFieldName(), protoType.getSimpleName()));
            }
        } else if (protoType.isMap()) {
            return createMap(protoType, values);
        } else {
            final MessageType messageType = (MessageType) schema.getType(protoType);
            Objects.requireNonNull(messageType, String.format("Message type with name [%s] not found in the provided proto files", protoType));

            valueConverter = value -> {
                try {
                    Optional<DataType> recordDataType = rootRecordSchema.getDataType(protoField.getFieldName());
                    if (protoField.isRepeatable()) {
                        final ArrayDataType arrayDataType = (ArrayDataType) recordDataType.get();
                        recordDataType = Optional.ofNullable(arrayDataType.getElementType());
                    }
                    RecordSchema recordSchema = recordDataType.map(dataType ->
                            ((RecordDataType) dataType).getChildSchema()).orElse(generateRecordSchema(messageType.getType().toString()));
                    return createRecord(messageType, value, recordSchema);
                } catch (InvalidProtocolBufferException e) {
                    throw new IllegalStateException("Failed to create record from the provided input data for field " + protoField.getFieldName(), e);
                }
            };
        }

        return resolveFieldValue(protoField, values, valueConverter);
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
        Function<Integer, Object> valueConverter;
        switch (FieldType.findValue(typeName)) {
            case FIXED32:
                valueConverter = Integer::toUnsignedLong;
                break;
            case SFIXED32:
                valueConverter = value -> value;
                break;
            case FLOAT:
                valueConverter = Float::intBitsToFloat;
                break;
            default:
                throw new IllegalStateException(String.format("Incompatible value was received for field [%s]," +
                        " [%s] is not Fixed32 field type", protoField.getFieldName(), typeName));
        }

        return resolveFieldValue(protoField, values, valueConverter);
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
        Function<Long, Object> valueConverter;
        switch (FieldType.findValue(typeName)) {
            case FIXED64:
                valueConverter = value -> new BigInteger(unsignedToString(value));
                break;
            case SFIXED64:
                valueConverter = value -> value;
                break;
            case DOUBLE:
                valueConverter = Double::longBitsToDouble;
                break;
            default:
                throw new IllegalStateException(String.format("Incompatible value was received for field [%s]," +
                        " [%s] is not Fixed64 field type", protoField.getFieldName(), typeName));
        }

        return resolveFieldValue(protoField, values, valueConverter);
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
        final Function<Long, Object> valueConverter;
        if (protoType.isScalar()) {
            switch (FieldType.findValue(protoType.getSimpleName())) {
                case BOOL:
                    valueConverter = value -> value.equals(1L);
                    break;
                case INT32:
                case SFIXED32:
                    valueConverter = Long::intValue;
                    break;
                case UINT32:
                case INT64:
                case SFIXED64:
                    valueConverter = value -> value;
                    break;
                case UINT64:
                    valueConverter = value -> new BigInteger(unsignedToString(value));
                    break;
                case SINT32:
                    valueConverter = value -> decodeZigZag32(value.intValue());
                    break;
                case SINT64:
                    valueConverter = CodedInputStream::decodeZigZag64;
                    break;
                default:
                    throw new IllegalStateException(String.format("Incompatible value was received for field [%s]," +
                            " [%s] is not Varint field type", protoField.getFieldName(), protoType.getSimpleName()));
            }
        } else {
            valueConverter = value -> convertEnum(value.intValue(), protoType);
        }

        return resolveFieldValue(protoField, values, valueConverter);
    }

    private <T> Object resolveFieldValue(ProtoField protoField, List<T> values, Function<T, Object> valueConverter) {
        List<Object> resultValues = values.stream().map(valueConverter).collect(Collectors.toList());

        if (coerceTypes) {
            final Optional<RecordField> recordField = rootRecordSchema.getField(protoField.getFieldName());
            if (recordField.isPresent()) {
                final DataType dataType;
                if (protoField.isRepeatable()) {
                    final ArrayDataType arrayDataType = (ArrayDataType) recordField.get().getDataType();
                    dataType = arrayDataType.getElementType();
                } else {
                    dataType = recordField.get().getDataType();
                }
                resultValues = resultValues.stream().map(value -> DataTypeUtils.convertType(value, dataType, recordField.get().getFieldName())).collect(Collectors.toList());
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

            collectFieldValue(mapEntry, new ProtoField(MAP_KEY_FIELD_NAME, protoType.getKeyType()), unknownFieldSet.getField(1));
            collectFieldValue(mapEntry, new ProtoField(MAP_VALUE_FIELD_NAME, protoType.getValueType()), unknownFieldSet.getField(2));

            mapResult.put(String.valueOf(mapEntry.get(MAP_KEY_FIELD_NAME)), mapEntry.get(MAP_VALUE_FIELD_NAME));
        }

        return mapResult;
    }

    private String convertEnum(Integer value, ProtoType protoType) {
        final EnumType enumType = (EnumType) schema.getType(protoType);
        Objects.requireNonNull(enumType, String.format("Enum with name [%s] not found in the provided proto files", protoType));
        return enumType.constant(value).getName();
    }

    /**
     * Process a 'google.protobuf.Any' typed field. The method gets the schema for the message type provided in the 'type_url' property
     * and parse the serialized message from the 'value' field. The result record will contain only the parsed message's fields.
     *
     * @param unknownFieldSet 'google.protobuf.Any' typed message's field list
     * @return created record from the parsed message
     * @throws InvalidProtocolBufferException failed to parse input data
     */
    private MapRecord handleAnyField(UnknownFieldSet unknownFieldSet) throws InvalidProtocolBufferException {
        Map<String, Object> recordValues = new HashMap<>();
        collectFieldValue(recordValues, new ProtoField(ANY_TYPE_URL_FIELD_NAME, ProtoType.STRING), unknownFieldSet.getField(1));
        collectFieldValue(recordValues, new ProtoField(ANY_VALUE_FIELD_NAME, ProtoType.BYTES), unknownFieldSet.getField(2));

        final String typeName = String.valueOf(recordValues.get(ANY_TYPE_URL_FIELD_NAME));
        final UnknownFieldSet anyFieldSet = UnknownFieldSet.parseFrom((byte[]) recordValues.get(ANY_VALUE_FIELD_NAME));
        final MessageType messageType = (MessageType) schema.getType(getQualifiedTypeName(typeName));
        Objects.requireNonNull(messageType, String.format("Message type with name [%s] not found in the provided proto files", typeName));

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
        return schemaParser.createSchema(getQualifiedTypeName(typeName));
    }

    /**
     * Gets the fully qualified name of the message type.
     *
     * @param typeName name of the message
     * @return fully qualified name of the message type
     */
    private String getQualifiedTypeName(String typeName) {
        return typeName.substring(typeName.lastIndexOf('/') + 1);
    }

    private <T> List<T> processRepeatedValues(CodedInputStream input, ValueReader<CodedInputStream, T> valueReader) {
        List<T> result = new ArrayList<>();
        try {
            while (input.getBytesUntilLimit() > 0) {
                result.add(valueReader.apply(input));
            }
        } catch (Exception e) {
            throw new IllegalStateException("Unable to parse repeated field", e);
        }
        return result;
    }

    private boolean isLengthDelimitedType(ProtoField protoField) {
        boolean lengthDelimitedScalarType = false;
        final ProtoType protoType = protoField.getProtoType();

        if (protoType.isScalar()) {
            final FieldType fieldType = FieldType.findValue(protoType.getSimpleName());
            lengthDelimitedScalarType = fieldType.equals(STRING) || fieldType.equals(BYTES);
        }

        return lengthDelimitedScalarType || schema.getType(protoType) instanceof MessageType;
    }
}
