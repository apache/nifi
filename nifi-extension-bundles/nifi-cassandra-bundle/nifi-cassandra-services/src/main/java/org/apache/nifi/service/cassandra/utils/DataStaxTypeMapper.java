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

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import org.apache.nifi.cassandra.models.CassandraType;
import org.apache.nifi.cassandra.models.CassandraTypeName;

import java.util.Map;

public final class DataStaxTypeMapper {

    private static final Map<DataType, CassandraTypeName> PRIMITIVE_TYPES = Map.ofEntries(
            Map.entry(DataTypes.ASCII, CassandraTypeName.ASCII),
            Map.entry(DataTypes.BIGINT, CassandraTypeName.BIGINT),
            Map.entry(DataTypes.BLOB, CassandraTypeName.BLOB),
            Map.entry(DataTypes.BOOLEAN, CassandraTypeName.BOOLEAN),
            Map.entry(DataTypes.COUNTER, CassandraTypeName.COUNTER),
            Map.entry(DataTypes.DATE, CassandraTypeName.DATE),
            Map.entry(DataTypes.DECIMAL, CassandraTypeName.DECIMAL),
            Map.entry(DataTypes.DOUBLE, CassandraTypeName.DOUBLE),
            Map.entry(DataTypes.DURATION, CassandraTypeName.DURATION),
            Map.entry(DataTypes.FLOAT, CassandraTypeName.FLOAT),
            Map.entry(DataTypes.INET, CassandraTypeName.INET),
            Map.entry(DataTypes.INT, CassandraTypeName.INT),
            Map.entry(DataTypes.SMALLINT, CassandraTypeName.SMALLINT),
            Map.entry(DataTypes.TEXT, CassandraTypeName.TEXT),
            Map.entry(DataTypes.TIME, CassandraTypeName.TIME),
            Map.entry(DataTypes.TIMESTAMP, CassandraTypeName.TIMESTAMP),
            Map.entry(DataTypes.TIMEUUID, CassandraTypeName.TIMEUUID),
            Map.entry(DataTypes.TINYINT, CassandraTypeName.TINYINT),
            Map.entry(DataTypes.UUID, CassandraTypeName.UUID),
            Map.entry(DataTypes.VARINT, CassandraTypeName.VARINT)
    );

    public CassandraType map(final DataType dataType) {
        return switch (dataType) {
            case ListType listType -> CassandraType.list(map(listType.getElementType()));
            case SetType  setType  -> CassandraType.set(map(setType.getElementType()));
            case MapType  mapType  -> CassandraType.map(map(mapType.getValueType()));
            default -> {
                CassandraTypeName typeName = PRIMITIVE_TYPES.getOrDefault(dataType, CassandraTypeName.UNKNOWN);
                yield CassandraType.primitive(typeName);
            }
        };
    }
}
