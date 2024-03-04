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
package org.apache.nifi.services.protobuf.schema;

import com.squareup.wire.schema.EnumConstant;
import com.squareup.wire.schema.EnumType;
import com.squareup.wire.schema.Field;
import com.squareup.wire.schema.MessageType;
import com.squareup.wire.schema.OneOf;
import com.squareup.wire.schema.ProtoType;
import com.squareup.wire.schema.Schema;
import com.squareup.wire.schema.Type;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.EnumDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.services.protobuf.FieldType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Creates a {@link RecordSchema} for the provided proto schema.
 */
public class ProtoSchemaParser {

    private final Schema schema;

    public ProtoSchemaParser(Schema schema) {
        this.schema = schema;
    }

    /**
     * Creates a {@link RecordSchema} for the provided message type.
     * @param messageTypeName proto message type
     * @return record schema
     */
    public RecordSchema createSchema(String messageTypeName) {
        final MessageType messageType = (MessageType) schema.getType(messageTypeName);
        Objects.requireNonNull(messageType, String.format("Message type with name [%s] not found in the provided proto files", messageTypeName));
        List<RecordField> recordFields = new ArrayList<>();

        recordFields.addAll(processFields(messageType.getDeclaredFields()));
        recordFields.addAll(processFields(messageType.getExtensionFields()));
        recordFields.addAll(processOneOfFields(messageType));

        return new SimpleRecordSchema(recordFields);
    }

    /**
     * Iterates through and process OneOf fields in the given message type.
     * @param messageType message type
     * @return generated {@link RecordSchema} list from the OneOf fields
     */
    private List<RecordField> processOneOfFields(MessageType messageType) {
        List<RecordField> recordFields = new ArrayList<>();
        for (final OneOf oneOf : messageType.getOneOfs()) {

            for (Field field : oneOf.getFields()) {
                final DataType dataType = getDataTypeForField(field.getType());
                recordFields.add(new RecordField(field.getName(), dataType, field.getDefault(), true));
            }
        }

        return recordFields;
    }

    /**
     * Iterates through and process fields in the given message type.
     * @return generated {@link RecordSchema} list from the provided fields
     */
    private List<RecordField> processFields(List<Field> fields) {
        List<RecordField> recordFields = new ArrayList<>();
        for (final Field field : fields) {
            DataType dataType = getDataTypeForField(field.getType());

            if (field.isRepeated()) {
                dataType = RecordFieldType.ARRAY.getArrayDataType(dataType);
            }

            recordFields.add(new RecordField(field.getName(), dataType, field.getDefault(), !field.isRequired()));
        }

        return recordFields;
    }

    /**
     * Checks the provided field's type and calls the proper {@link DataType} processing function.
     * @param protoType field's type
     * @return data type
     */
    private DataType getDataTypeForField(ProtoType protoType) {
        if (protoType.isScalar()) {
            return getDataTypeForScalarField(protoType);
        } else {
            return getDataTypeForCompositeField(protoType);
        }
    }

    /**
     * Gets the suitable {@link DataType} for the provided composite field.
     * @param protoType field's type
     * @return data type
     */
    private DataType getDataTypeForCompositeField(ProtoType protoType) {
        if (protoType.isMap()) {
            final DataType valueType = getDataTypeForField(protoType.getValueType());
            return new MapDataType(valueType);
        }

        final Type fieldType = schema.getType(protoType);

        if (fieldType instanceof MessageType) {
            final RecordSchema recordSchema = createSchema(protoType.toString());
            return new RecordDataType(recordSchema);
        } else if (fieldType instanceof EnumType) {
            return new EnumDataType(((EnumType) fieldType).getConstants().stream().map(EnumConstant::getName).toList());
        } else {
            throw new IllegalStateException("Unknown proto type: " + fieldType);
        }
    }

    /**
     * Gets the suitable {@link DataType} for the provided scalar field.
     * @param protoType field's type
     * @return data type
     */
    private DataType getDataTypeForScalarField(ProtoType protoType) {
        return switch (FieldType.findValue(protoType.getSimpleName())) {
            case DOUBLE -> RecordFieldType.DOUBLE.getDataType();
            case FLOAT -> RecordFieldType.FLOAT.getDataType();
            case INT32, SFIXED32 -> RecordFieldType.INT.getDataType();
            case UINT32, SINT32, FIXED32, INT64, SINT64, SFIXED64 -> RecordFieldType.LONG.getDataType();
            case UINT64, FIXED64 -> RecordFieldType.BIGINT.getDataType();
            case BOOL -> RecordFieldType.BOOLEAN.getDataType();
            case STRING -> RecordFieldType.STRING.getDataType();
            case BYTES -> RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType());
        };
    }
}
