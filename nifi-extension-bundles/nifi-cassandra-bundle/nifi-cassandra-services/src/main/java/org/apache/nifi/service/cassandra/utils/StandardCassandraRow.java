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
package org.apache.nifi.service.cassandra.utils;

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import org.apache.nifi.cassandra.models.CassandraRow;
import org.apache.nifi.cassandra.models.CassandraType;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Base64;
import java.util.List;

final class StandardCassandraRow implements CassandraRow {
    private final Row row;
    private final DataStaxTypeMapper typeMapper;

    StandardCassandraRow(final Row row, final DataStaxTypeMapper typeMapper) {
        this.row = row;
        this.typeMapper = typeMapper;
    }

    @Override
    public boolean isNull(final int index) {
        return row.isNull(index);
    }

    @Override
    public Object getValue(final int index) {
        final DataType dataType = row.getColumnDefinitions().get(index).getType();

        try {
            if (dataType.equals(DataTypes.BLOB)) {
                final ByteBuffer buffer = row.getByteBuffer(index);
                if (buffer == null) {
                    return null;
                }
                final ByteBuffer duplicate = buffer.duplicate();
                final byte[] bytes = new byte[duplicate.remaining()];
                duplicate.get(bytes);
                return Base64.getEncoder().encodeToString(bytes);
            } else if (dataType.equals(DataTypes.VARINT) || dataType.equals(DataTypes.DECIMAL)) {
                final Object object = row.getObject(index);
                return object != null ? object.toString() : null;
            } else if (dataType.equals(DataTypes.BOOLEAN)) {
                return row.getBoolean(index);
            } else if (dataType.equals(DataTypes.INT) || dataType.equals(DataTypes.SMALLINT)
                    || dataType.equals(DataTypes.TINYINT)) {
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
                final Instant instant = row.getInstant(index);
                return instant != null ? instant.toString() : null;
            } else if (dataType.equals(DataTypes.DATE)) {
                final LocalDate localDate = row.getLocalDate(index);
                return localDate != null ? localDate.toString() : null;
            } else if (dataType.equals(DataTypes.TIME)) {
                final LocalTime localTime = row.getLocalTime(index);
                return localTime != null ? localTime.toString() : null;
            } else if (dataType instanceof ListType listType) {
                final Class<?> elementClass = getJavaClass(listType.getElementType());
                final List<?> list = row.getList(index, elementClass);
                return list != null ? list.toArray() : null;
            } else if (dataType instanceof SetType setType) {
                final Class<?> elementClass = getJavaClass(setType.getElementType());
                final java.util.Set<?> set = row.getSet(index, elementClass);
                return set != null ? set.toArray() : null;
            } else if (dataType instanceof MapType mapType) {
                return row.getMap(index, getJavaClass(mapType.getKeyType()), getJavaClass(mapType.getValueType()));
            }

            final Object fallback = row.getObject(index);
            return fallback != null ? fallback.toString() : null;
        } catch (Exception e) {
            final Object fallback = row.getObject(index);
            return fallback != null ? fallback.toString() : null;
        }
    }

    @Override
    public CassandraType getType(final int index) {
        return typeMapper.map(row.getColumnDefinitions().get(index).getType());
    }

    private Class<?> getJavaClass(final DataType dataType) {
        if (dataType.equals(DataTypes.ASCII) || dataType.equals(DataTypes.TEXT)) {
            return String.class;
        } else if (dataType.equals(DataTypes.INT) || dataType.equals(DataTypes.SMALLINT)
                || dataType.equals(DataTypes.TINYINT)) {
            return Integer.class;
        } else if (dataType.equals(DataTypes.BIGINT) || dataType.equals(DataTypes.COUNTER)) {
            return Long.class;
        } else if (dataType.equals(DataTypes.BOOLEAN)) {
            return Boolean.class;
        } else if (dataType.equals(DataTypes.FLOAT)) {
            return Float.class;
        } else if (dataType.equals(DataTypes.DOUBLE)) {
            return Double.class;
        } else if (dataType.equals(DataTypes.TIMESTAMP)) {
            return Instant.class;
        } else if (dataType.equals(DataTypes.DATE)) {
            return LocalDate.class;
        } else if (dataType.equals(DataTypes.TIME)) {
            return LocalTime.class;
        } else if (dataType.equals(DataTypes.BLOB)) {
            return ByteBuffer.class;
        }
        return Object.class;
    }
}
