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
package org.apache.nifi.processors.iceberg.appender.orc;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;

import java.util.List;
import java.util.Optional;

/**
 * This class contains Orc specific visitor methods to traverse schema and build value writer list for data types.
 */
abstract class IcebergOrcSchemaVisitor<T> {

    static <T> T visit(RecordSchema record, Schema schema, IcebergOrcSchemaVisitor<T> visitor) {
        return visit(new RecordDataType(record), schema.asStruct(), visitor);
    }

    private static <T> T visit(DataType dataType, Type iType, IcebergOrcSchemaVisitor<T> visitor) {
        switch (iType.typeId()) {
            case STRUCT:
                return visitRecord(dataType, iType.asStructType(), visitor);

            case MAP:
                MapDataType mapType = (MapDataType) dataType;
                Types.MapType iMapType = iType.asMapType();

                T key;
                T value;

                Types.NestedField keyField = iMapType.field(iMapType.keyId());
                visitor.beforeField(keyField);
                try {
                    key = visit(RecordFieldType.STRING.getDataType(), iMapType.keyType(), visitor);
                } finally {
                    visitor.afterField(keyField);
                }

                Types.NestedField valueField = iMapType.field(iMapType.valueId());
                visitor.beforeField(valueField);
                try {
                    value = visit(mapType.getValueType(), iMapType.valueType(), visitor);
                } finally {
                    visitor.afterField(valueField);
                }

                return visitor.map(key, value, RecordFieldType.STRING.getDataType(), mapType.getValueType());

            case LIST:
                ArrayDataType listType = (ArrayDataType) dataType;
                Types.ListType iListType = iType.asListType();

                T element;

                Types.NestedField elementField = iListType.field(iListType.elementId());
                visitor.beforeField(elementField);
                try {
                    element = visit(listType.getElementType(), iListType.elementType(), visitor);
                } finally {
                    visitor.afterField(elementField);
                }

                return visitor.list(element, listType.getElementType());

            default:
                return visitor.primitive(iType.asPrimitiveType(), dataType);
        }
    }

    private static <T> T visitRecord(DataType dataType, Types.StructType struct, IcebergOrcSchemaVisitor<T> visitor) {
        Preconditions.checkArgument(dataType instanceof RecordDataType, "%s is not a RecordDataType.", dataType);
        RecordDataType recordType = (RecordDataType) dataType;

        int fieldSize = struct.fields().size();
        List<T> results = Lists.newArrayListWithExpectedSize(fieldSize);
        List<Types.NestedField> nestedFields = struct.fields();

        for (int i = 0; i < fieldSize; i++) {
            Types.NestedField iField = nestedFields.get(i);
            Optional<RecordField> recordField = recordType.getChildSchema().getField(iField.name());
            Preconditions.checkArgument(recordField.isPresent(), "NestedField: %s is not found in DataType: %s", iField, recordType);

            visitor.beforeField(iField);
            try {
                results.add(visit(recordField.get().getDataType(), iField.type(), visitor));
            } finally {
                visitor.afterField(iField);
            }
        }

        return visitor.record(results, recordType.getChildSchema().getFields());
    }

    abstract public void beforeField(Types.NestedField field);

    abstract public void afterField(Types.NestedField field);

    abstract public T record(List<T> results, List<RecordField> recordFields);

    abstract public T list(T element, DataType elementType);

    abstract public T map(T key, T value, DataType keyType, DataType valueType);

    abstract public T primitive(Type.PrimitiveType iPrimitive, DataType dataType);
}
