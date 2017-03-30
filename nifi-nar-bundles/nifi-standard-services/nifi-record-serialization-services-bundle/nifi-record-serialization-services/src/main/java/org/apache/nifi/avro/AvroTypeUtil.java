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

package org.apache.nifi.avro;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.util.IllegalTypeConversionException;

public class AvroTypeUtil {

    public static DataType determineDataType(final Schema avroSchema) {
        final Type avroType = avroSchema.getType();

        switch (avroType) {
            case BYTES:
            case FIXED:
                return RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.BYTE.getDataType());
            case ARRAY:
                final DataType elementType = determineDataType(avroSchema.getElementType());
                return RecordFieldType.ARRAY.getArrayDataType(elementType);
            case BOOLEAN:
                return RecordFieldType.BOOLEAN.getDataType();
            case DOUBLE:
                return RecordFieldType.DOUBLE.getDataType();
            case ENUM:
            case STRING:
                return RecordFieldType.STRING.getDataType();
            case FLOAT:
                return RecordFieldType.FLOAT.getDataType();
            case INT: {
                final LogicalType logicalType = avroSchema.getLogicalType();
                if (logicalType == null) {
                    return RecordFieldType.INT.getDataType();
                }

                if (LogicalTypes.date().getName().equals(logicalType.getName())) {
                    return RecordFieldType.DATE.getDataType();
                } else if (LogicalTypes.timeMillis().getName().equals(logicalType.getName())) {
                    return RecordFieldType.TIME.getDataType();
                }

                return RecordFieldType.INT.getDataType();
            }
            case LONG: {
                final LogicalType logicalType = avroSchema.getLogicalType();
                if (logicalType == null) {
                    return RecordFieldType.LONG.getDataType();
                }

                if (LogicalTypes.timestampMillis().getName().equals(logicalType.getName())) {
                    return RecordFieldType.TIMESTAMP.getDataType();
                } else if (LogicalTypes.timestampMicros().getName().equals(logicalType.getName())) {
                    return RecordFieldType.TIMESTAMP.getDataType();
                } else if (LogicalTypes.timeMicros().getName().equals(logicalType.getName())) {
                    return RecordFieldType.TIME.getDataType();
                }

                return RecordFieldType.LONG.getDataType();
            }
            case RECORD: {
                final List<Field> avroFields = avroSchema.getFields();
                final List<RecordField> recordFields = new ArrayList<>(avroFields.size());

                for (final Field field : avroFields) {
                    final String fieldName = field.name();
                    final Schema fieldSchema = field.schema();
                    final DataType fieldType = determineDataType(fieldSchema);
                    recordFields.add(new RecordField(fieldName, fieldType));
                }

                final RecordSchema recordSchema = new SimpleRecordSchema(recordFields);
                return RecordFieldType.RECORD.getRecordDataType(recordSchema);
            }
            case NULL:
            case MAP:
                return RecordFieldType.RECORD.getDataType();
            case UNION: {
                final List<Schema> nonNullSubSchemas = avroSchema.getTypes().stream()
                    .filter(s -> s.getType() != Type.NULL)
                    .collect(Collectors.toList());

                if (nonNullSubSchemas.size() == 1) {
                    return determineDataType(nonNullSubSchemas.get(0));
                }

                final List<DataType> possibleChildTypes = new ArrayList<>(nonNullSubSchemas.size());
                for (final Schema subSchema : nonNullSubSchemas) {
                    final DataType childDataType = determineDataType(subSchema);
                    possibleChildTypes.add(childDataType);
                }

                return RecordFieldType.CHOICE.getChoiceDataType(possibleChildTypes);
            }
        }

        return null;
    }

    public static RecordSchema createSchema(final Schema avroSchema) {
        final List<RecordField> recordFields = new ArrayList<>(avroSchema.getFields().size());
        for (final Field field : avroSchema.getFields()) {
            final String fieldName = field.name();
            final DataType dataType = AvroTypeUtil.determineDataType(field.schema());
            recordFields.add(new RecordField(fieldName, dataType));
        }

        final RecordSchema recordSchema = new SimpleRecordSchema(recordFields);
        return recordSchema;
    }

    public static Object[] convertByteArray(final byte[] bytes) {
        final Object[] array = new Object[bytes.length];
        for (int i = 0; i < bytes.length; i++) {
            array[i] = Byte.valueOf(bytes[i]);
        }
        return array;
    }

    public static ByteBuffer convertByteArray(final Object[] bytes) {
        final ByteBuffer bb = ByteBuffer.allocate(bytes.length);
        for (final Object o : bytes) {
            if (o instanceof Byte) {
                bb.put(((Byte) o).byteValue());
            } else {
                throw new IllegalTypeConversionException("Cannot convert value " + bytes + " of type " + bytes.getClass() + " to ByteBuffer");
            }
        }
        bb.flip();
        return bb;
    }
}
