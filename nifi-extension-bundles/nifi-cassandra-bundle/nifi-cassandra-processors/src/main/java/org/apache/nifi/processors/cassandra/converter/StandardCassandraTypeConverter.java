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
package org.apache.nifi.processors.cassandra.converter;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;

public class StandardCassandraTypeConverter implements CassandraTypeConverter {

    @Override
    public Object getCassandraObject(final Row row, final int index) {
        final DataType dataType = row.getColumnDefinitions().get(index).getType();

        try {
            if (dataType.equals(DataTypes.BLOB)) {
                return row.getByteBuffer(index);

            } else if (dataType.equals(DataTypes.VARINT) || dataType.equals(DataTypes.DECIMAL)) {
                Object obj = row.getObject(index);
                return obj != null ? obj.toString() : null;

            } else if (dataType.equals(DataTypes.BOOLEAN)) {
                return row.getBoolean(index);

            } else if (dataType.equals(DataTypes.INT)) {
                return row.getInt(index);

            } else if (dataType.equals(DataTypes.BIGINT) || dataType.equals(DataTypes.COUNTER)) {
                return row.getLong(index);

            } else if (dataType.equals(DataTypes.ASCII) || dataType.equals(DataTypes.TEXT)) {
                return row.getString(index);

            } else if (dataType.equals(DataTypes.FLOAT)) {
                return row.getFloat(index);

            } else if (dataType.equals(DataTypes.DOUBLE)) {
                return row.getDouble(index);

            } else if (dataType.equals(DataTypes.TIMESTAMP)) {
                Instant instant = row.getInstant(index);
                return instant != null ? instant.toString() : null;
            } else if (dataType.equals(DataTypes.DATE)) {
                LocalDate localDate = row.getLocalDate(index);
                return localDate != null ? localDate.toString() : null;
            } else if (dataType.equals(DataTypes.TIME)) {
                LocalTime time = row.getLocalTime(index);
                return time != null ? time.toString() : null;
            } else if (dataType instanceof ListType) {
                ListType listType = (ListType) dataType;
                Class<?> elementClass = getJavaClassForCassandraType(listType.getElementType());
                return row.getList(index, elementClass);

            } else if (dataType instanceof SetType) {
                SetType setType = (SetType) dataType;
                Class<?> elementClass = getJavaClassForCassandraType(setType.getElementType());
                return row.getSet(index, elementClass);

            } else if (dataType instanceof MapType) {
                MapType mapType = (MapType) dataType;
                Class<?> keyClass = getJavaClassForCassandraType(mapType.getKeyType());
                Class<?> valueClass = getJavaClassForCassandraType(mapType.getValueType());
                return row.getMap(index, keyClass, valueClass);
            }

            // Fallback for any other types
            Object value = row.getObject(index);
            return value != null ? value.toString() : null;

        } catch (Exception e) {
            return row.getObject(index) != null ? row.getObject(index).toString() : null;
        }
    }

    @Override
    public Class<?> getJavaClassForCassandraType(final DataType type) {
        if (type.equals(DataTypes.ASCII) || type.equals(DataTypes.TEXT)) {
            return String.class;
        } else if (type.equals(DataTypes.INT)) {
            return Integer.class;
        } else if (type.equals(DataTypes.BIGINT) || type.equals(DataTypes.COUNTER)) {
            return Long.class;
        } else if (type.equals(DataTypes.BOOLEAN)) {
            return Boolean.class;
        } else if (type.equals(DataTypes.FLOAT)) {
            return Float.class;
        } else if (type.equals(DataTypes.DOUBLE)) {
            return Double.class;
        } else if (type.equals(DataTypes.VARINT) || type.equals(DataTypes.DECIMAL)) {
            return String.class;
        } else if (type.equals(DataTypes.UUID) || type.equals(DataTypes.TIMEUUID)) {
            return java.util.UUID.class;
        } else if (type.equals(DataTypes.TIMESTAMP)) {
            return java.time.Instant.class;
        } else if (type.equals(DataTypes.DATE)) {
            return java.time.LocalDate.class;
        } else if (type.equals(DataTypes.TIME)) {
            return Long.class;
        } else if (type.equals(DataTypes.BLOB)) {
            return java.nio.ByteBuffer.class;
        } else {
            return Object.class;
        }
    }

    @Override
    public String getPrimitiveAvroTypeFromCassandraType(final DataType dataType) {

        if (dataType.equals(DataTypes.ASCII)
                || dataType.equals(DataTypes.TEXT)
                || dataType.equals(DataTypes.TIMESTAMP)
                || dataType.equals(DataTypes.DATE)
                || dataType.equals(DataTypes.TIME)
                || dataType.equals(DataTypes.TIMEUUID)
                || dataType.equals(DataTypes.UUID)
                || dataType.equals(DataTypes.INET)
                || dataType.equals(DataTypes.VARINT)
                || dataType.equals(DataTypes.DECIMAL)
                || dataType.equals(DataTypes.DURATION)) {

            return "string";

        } else if (dataType.equals(DataTypes.BOOLEAN)) {
            return "boolean";

        } else if (dataType.equals(DataTypes.INT)
                || dataType.equals(DataTypes.SMALLINT)
                || dataType.equals(DataTypes.TINYINT)) {
            return "int";

        } else if (dataType.equals(DataTypes.BIGINT)
                || dataType.equals(DataTypes.COUNTER)) {
            return "long";

        } else if (dataType.equals(DataTypes.FLOAT)) {
            return "float";

        } else if (dataType.equals(DataTypes.DOUBLE)) {
            return "double";

        } else if (dataType.equals(DataTypes.BLOB)) {
            return "bytes";

        } else {
            throw new IllegalArgumentException(
                    String.format("createSchema: Unknown Cassandra data type %s cannot be converted to Avro type", dataType)
            );
        }
    }

    @Override
    public DataType getPrimitiveDataTypeFromString(final String dataTypeName) {
        List<DataType> primitiveTypes = List.of(
                DataTypes.ASCII,
                DataTypes.BIGINT,
                DataTypes.BLOB,
                DataTypes.BOOLEAN,
                DataTypes.COUNTER,
                DataTypes.DATE,
                DataTypes.DECIMAL,
                DataTypes.DOUBLE,
                DataTypes.FLOAT,
                DataTypes.INET,
                DataTypes.INT,
                DataTypes.SMALLINT,
                DataTypes.TEXT,
                DataTypes.TIME,
                DataTypes.TIMESTAMP,
                DataTypes.TIMEUUID,
                DataTypes.TINYINT,
                DataTypes.UUID,
                DataTypes.VARINT
        );

        for (DataType primitiveType : primitiveTypes) {
            if (primitiveType.asCql(false, false).equalsIgnoreCase(dataTypeName)) {
                return primitiveType;
            }
        }
        return null;
    }

    @Override
    public Schema getSchemaForType(final String dataType) {
        SchemaBuilder.TypeBuilder<Schema> typeBuilder = SchemaBuilder.builder();
        Schema returnSchema;
        switch (dataType) {
            case "string":
                returnSchema = typeBuilder.stringType();
                break;
            case "boolean":
                returnSchema = typeBuilder.booleanType();
                break;
            case "int":
                returnSchema = typeBuilder.intType();
                break;
            case "long":
                returnSchema = typeBuilder.longType();
                break;
            case "float":
                returnSchema = typeBuilder.floatType();
                break;
            case "double":
                returnSchema = typeBuilder.doubleType();
                break;
            case "bytes":
                returnSchema = typeBuilder.bytesType();
                break;
            default: throw new IllegalArgumentException(String.format("Unknown Avro primitive type: %s", dataType));
        }
        return returnSchema;
    }

    @Override
    public Schema getUnionFieldType(final String dataType) {
        return SchemaBuilder.builder().unionOf().nullBuilder().endNull().and().type(getSchemaForType(dataType)).endUnion();
    }
}
