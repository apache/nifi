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
package org.apache.nifi.processors.iceberg.appender.parquet;

import com.google.common.collect.Lists;
import org.apache.commons.lang.Validate;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;

import java.util.Deque;
import java.util.List;

/**
 * This class contains Parquet specific visitor methods to traverse schema and build value writer list for data types.
 */
public abstract class ParquetWithNiFiSchemaVisitor<T> {

    private final Deque<String> fieldNames = Lists.newLinkedList();

    public static <T> T visit(DataType dataType, Type type, ParquetWithNiFiSchemaVisitor<T> visitor) {
        Validate.notNull(dataType, "Invalid DataType: null");
        if (type instanceof MessageType) {
            Validate.isTrue(dataType instanceof RecordDataType, String.format("Invalid recordDataType: %s is not a RecordDataType", dataType));
            RecordDataType recordDataType = (RecordDataType) dataType;
            return visitor.message(recordDataType, (MessageType) type, visitFields(recordDataType.getChildSchema(), type.asGroupType(), visitor));
        } else if (type.isPrimitive()) {
            return visitor.primitive(dataType, type.asPrimitiveType());
        } else {
            GroupType group = type.asGroupType();
            OriginalType annotation = group.getOriginalType();
            if (annotation != null) {
                switch (annotation) {
                    case LIST:
                        Validate.isTrue(!group.isRepetition(Repetition.REPEATED), String.format("Invalid list: top-level group is repeated: %s", group));
                        Validate.isTrue(group.getFieldCount() == 1, String.format("Invalid list: does not contain single repeated field: %s", group));
                        GroupType repeatedElement = group.getFields().get(0).asGroupType();
                        Validate.isTrue(repeatedElement.isRepetition(Repetition.REPEATED), "Invalid list: inner group is not repeated");
                        Validate.isTrue(repeatedElement.getFieldCount() <= 1, String.format("Invalid list: repeated group is not a single field: %s", group));
                        Validate.isTrue(dataType instanceof ArrayDataType, String.format("Invalid list: %s is not an ArrayDataType", dataType));
                        ArrayDataType arrayType = (ArrayDataType) dataType;
                        RecordField element = new RecordField("element", arrayType.getElementType());
                        visitor.fieldNames.push(repeatedElement.getName());

                        try {
                            T elementResult = null;
                            if (repeatedElement.getFieldCount() > 0) {
                                elementResult = visitField(element, repeatedElement.getType(0), visitor);
                            }

                            return visitor.list(arrayType, group, elementResult);

                        } finally {
                            visitor.fieldNames.pop();
                        }

                    case MAP:
                        Validate.isTrue(!group.isRepetition(Repetition.REPEATED), String.format("Invalid map: top-level group is repeated: %s", group));
                        Validate.isTrue(group.getFieldCount() == 1, String.format("Invalid map: does not contain single repeated field: %s", group));
                        GroupType repeatedKeyValue = group.getType(0).asGroupType();
                        Validate.isTrue(repeatedKeyValue.isRepetition(Repetition.REPEATED), "Invalid map: inner group is not repeated");
                        Validate.isTrue(repeatedKeyValue.getFieldCount() <= 2, "Invalid map: repeated group does not have 2 fields");
                        Validate.isTrue(dataType instanceof MapDataType, String.format("Invalid map: %s is not a MapDataType", dataType));
                        MapDataType mapType = (MapDataType) dataType;
                        RecordField keyField = new RecordField("key", RecordFieldType.STRING.getDataType());
                        RecordField valueField = new RecordField("value", mapType.getValueType());

                        visitor.fieldNames.push(repeatedKeyValue.getName());
                        try {
                            T keyResult = null;
                            T valueResult = null;
                            switch (repeatedKeyValue.getFieldCount()) {
                                case 2:
                                    // if there are 2 fields, both key and value are projected
                                    keyResult = visitField(keyField, repeatedKeyValue.getType(0), visitor);
                                    valueResult = visitField(valueField, repeatedKeyValue.getType(1), visitor);
                                    break;
                                case 1:
                                    // if there is just one, use the name to determine what it is
                                    Type keyOrValue = repeatedKeyValue.getType(0);
                                    if (keyOrValue.getName().equalsIgnoreCase("key")) {
                                        keyResult = visitField(keyField, keyOrValue, visitor);
                                        // value result remains null
                                    } else {
                                        valueResult = visitField(valueField, keyOrValue, visitor);
                                        // key result remains null
                                    }
                                    break;
                                default:
                                    // both results will remain null
                            }

                            return visitor.map(mapType, group, keyResult, valueResult);

                        } finally {
                            visitor.fieldNames.pop();
                        }

                    default:
                }
            }

            Validate.isTrue(dataType instanceof RecordDataType, String.format("Invalid recordDataType: %s is not a recordDataType", dataType));
            RecordDataType recordType = (RecordDataType) dataType;
            return visitor.struct(recordType, group, visitFields(recordType.getChildSchema(), group, visitor));
        }
    }

    private static <T> T visitField(RecordField recordField, Type field, ParquetWithNiFiSchemaVisitor<T> visitor) {
        visitor.fieldNames.push(field.getName());
        try {
            return visit(recordField.getDataType(), field, visitor);
        } finally {
            visitor.fieldNames.pop();
        }
    }

    private static <T> List<T> visitFields(RecordSchema recordSchema, GroupType group, ParquetWithNiFiSchemaVisitor<T> visitor) {
        List<RecordField> recordFields = recordSchema.getFields();
        Validate.isTrue(recordFields.size() == group.getFieldCount(), String.format("Schemas do not match: %s and %s", recordSchema, group));
        List<T> results = Lists.newArrayListWithExpectedSize(group.getFieldCount());

        for (int i = 0; i < recordFields.size(); ++i) {
            Type field = group.getFields().get(i);
            RecordField recordField = recordFields.get(i);
            Validate.isTrue(field.getName().equals(AvroSchemaUtil.makeCompatibleName(recordField.getFieldName())),
                    String.format("Schemas do not match: field %s != %s", field.getName(), recordField.getFieldName()));
            results.add(visitField(recordField, field, visitor));
        }

        return results;
    }

    abstract public T message(RecordDataType recordType, MessageType message, List<T> fields);

    abstract public T struct(RecordDataType recordType, GroupType struct, List<T> fields);

    abstract public T list(ArrayDataType listType, GroupType array, T element);

    abstract public T map(MapDataType mapType, GroupType map, T key, T value);

    abstract public T primitive(DataType dataType, PrimitiveType primitive);

    protected String[] currentPath() {
        return Lists.newArrayList(fieldNames.descendingIterator()).toArray(new String[0]);
    }

    protected String[] path(String name) {
        List<String> list = Lists.newArrayList(fieldNames.descendingIterator());
        list.add(name);
        return list.toArray(new String[0]);
    }
}
