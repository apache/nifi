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
package org.apache.nifi.processors.iceberg.appender.avro;

import com.google.common.base.Preconditions;
import org.apache.iceberg.avro.AvroWithPartnerByStructureVisitor;
import org.apache.iceberg.util.Pair;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;

/**
 * This class contains Avro specific visitor methods to traverse schema and build value writer list for data types.
 */
public class AvroWithNifiSchemaVisitor<T> extends AvroWithPartnerByStructureVisitor<DataType, T> {

    @Override
    protected boolean isStringType(DataType dataType) {
        return dataType.getFieldType().equals(RecordFieldType.STRING);
    }

    @Override
    protected boolean isMapType(DataType dataType) {
        return dataType instanceof MapDataType;
    }

    @Override
    protected DataType arrayElementType(DataType arrayType) {
        Preconditions.checkArgument(arrayType instanceof ArrayDataType, "Invalid array: %s is not an array", arrayType);
        return ((ArrayDataType) arrayType).getElementType();
    }

    @Override
    protected DataType mapKeyType(DataType mapType) {
        Preconditions.checkArgument(isMapType(mapType), "Invalid map: %s is not a map", mapType);
        return RecordFieldType.STRING.getDataType();
    }

    @Override
    protected DataType mapValueType(DataType mapType) {
        Preconditions.checkArgument(isMapType(mapType), "Invalid map: %s is not a map", mapType);
        return ((MapDataType) mapType).getValueType();
    }

    @Override
    protected Pair<String, DataType> fieldNameAndType(DataType structType, int pos) {
        Preconditions.checkArgument(structType instanceof RecordDataType, "Invalid struct: %s is not a struct", structType);
        RecordField field = ((RecordDataType) structType).getChildSchema().getField(pos);
        return Pair.of(field.getFieldName(), field.getDataType());
    }

    @Override
    protected DataType nullType() {
        return RecordFieldType.STRING.getDataType();
    }
}
