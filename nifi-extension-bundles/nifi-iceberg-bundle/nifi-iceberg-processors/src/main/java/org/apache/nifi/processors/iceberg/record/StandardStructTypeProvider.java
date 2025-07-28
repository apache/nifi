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
package org.apache.nifi.processors.iceberg.record;

import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.DecimalDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Objects;

/**
 * Standard implementation of Struct Type Provider
 */
public class StandardStructTypeProvider implements StructTypeProvider {
    /**
     * Get Iceberg Struct Type for NiFi Record Schema
     *
     * @param recordSchema NiFi Record Schema
     * @return Iceberg Struct Type
     */
    @Override
    public Types.StructType getStructType(final RecordSchema recordSchema) {
        Objects.requireNonNull(recordSchema, "Record Schema required");

        final List<Types.NestedField> nestedFields = new ArrayList<>();

        final ListIterator<RecordField> recordFields = recordSchema.getFields().listIterator();
        while (recordFields.hasNext()) {
            final RecordField recordField = recordFields.next();
            final int id = recordFields.nextIndex();
            final boolean optional = recordField.isNullable();
            final String fieldName = recordField.getFieldName();
            final DataType dataType = recordField.getDataType();
            final Type fieldType = getType(dataType);
            final Types.NestedField nestedField;
            if (optional) {
                nestedField = Types.NestedField.optional(id, fieldName, fieldType);
            } else {
                nestedField = Types.NestedField.required(id, fieldName, fieldType);
            }
            nestedFields.add(nestedField);
        }

        return Types.StructType.of(nestedFields);
    }

    private Type getType(final DataType dataType) {
        final RecordFieldType recordFieldType = dataType.getFieldType();

        return switch (recordFieldType) {
            case RecordFieldType.BOOLEAN -> Types.BooleanType.get();
            case RecordFieldType.BYTE, RecordFieldType.SHORT, RecordFieldType.INT -> Types.IntegerType.get();
            case RecordFieldType.LONG -> Types.LongType.get();
            case RecordFieldType.BIGINT, RecordFieldType.CHAR, RecordFieldType.STRING, RecordFieldType.ENUM -> Types.StringType.get();
            case RecordFieldType.FLOAT -> Types.FloatType.get();
            case RecordFieldType.DOUBLE -> Types.DoubleType.get();
            case RecordFieldType.TIMESTAMP -> Types.TimestampType.withoutZone();
            case RecordFieldType.DATE -> Types.DateType.get();
            case RecordFieldType.TIME -> Types.TimeType.get();
            case RecordFieldType.UUID -> Types.UUIDType.get();
            case RecordFieldType.DECIMAL -> getDecimalType((DecimalDataType) dataType);
            case RecordFieldType.ARRAY -> getListType((ArrayDataType) dataType);
            case RecordFieldType.MAP -> getMapType((MapDataType) dataType);
            case RecordFieldType.RECORD -> getStructType((RecordDataType) dataType);
            case RecordFieldType.CHOICE -> Types.VariantType.get();
        };
    }

    private Type getDecimalType(final DecimalDataType dataType) {
        return Types.DecimalType.of(dataType.getPrecision(), dataType.getScale());
    }

    private Type getListType(final ArrayDataType dataType) {
        final DataType elementType = dataType.getElementType();
        return Types.ListType.ofOptional(0, getType(elementType));
    }

    private Type getMapType(final MapDataType dataType) {
        return Types.MapType.ofOptional(0, 0, Types.StringType.get(), getType(dataType.getValueType()));
    }

    private Type getStructType(final RecordDataType dataType) {
        return getStructType(dataType.getChildSchema());
    }
}
