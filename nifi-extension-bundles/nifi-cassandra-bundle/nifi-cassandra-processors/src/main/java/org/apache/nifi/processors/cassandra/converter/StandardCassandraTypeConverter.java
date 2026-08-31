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

import org.apache.nifi.cassandra.models.CassandraRow;
import org.apache.nifi.cassandra.models.CassandraType;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordFieldType;

public class StandardCassandraTypeConverter implements CassandraTypeConverter {

    @Override
    public Object getCassandraObject(final CassandraRow row, final int index) {
        return row.getValue(index);
    }

    @Override
    public DataType getDataType(final CassandraType dataType) {
        return switch (dataType.name()) {
            case BOOLEAN -> RecordFieldType.BOOLEAN.getDataType();
            case INT, SMALLINT, TINYINT -> RecordFieldType.INT.getDataType();
            case BIGINT, COUNTER -> RecordFieldType.LONG.getDataType();
            case FLOAT -> RecordFieldType.FLOAT.getDataType();
            case DOUBLE -> RecordFieldType.DOUBLE.getDataType();
            case LIST, SET -> RecordFieldType.ARRAY.getArrayDataType(getDataType(dataType.elementType()));
            case MAP -> RecordFieldType.MAP.getMapDataType(getDataType(dataType.valueType()));
            default -> RecordFieldType.STRING.getDataType();
        };
    }
}
