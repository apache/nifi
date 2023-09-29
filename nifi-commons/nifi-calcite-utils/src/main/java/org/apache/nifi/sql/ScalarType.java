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

package org.apache.nifi.sql;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

public record ScalarType(Class<?> scalarType) implements ColumnType {

    public static final ScalarType STRING = new ScalarType(String.class);
    public static final ScalarType INTEGER = new ScalarType(Integer.class);
    public static final ScalarType LONG = new ScalarType(Long.class);
    public static final ScalarType BOOLEAN = new ScalarType(Boolean.class);
    public static final ScalarType SHORT = new ScalarType(Short.class);
    public static final ScalarType BYTE = new ScalarType(Byte.class);
    public static final ScalarType CHARACTER = new ScalarType(Character.class);
    public static final ScalarType DOUBLE = new ScalarType(Double.class);
    public static final ScalarType FLOAT = new ScalarType(Float.class);
    public static final ScalarType DECIMAL = new ScalarType(BigDecimal.class);
    public static final ScalarType BIGINT = new ScalarType(BigInteger.class);
    public static final ScalarType DATE = new ScalarType(Date.class);
    public static final ScalarType TIME = new ScalarType(Time.class);
    public static final ScalarType TIMESTAMP = new ScalarType(Timestamp.class);
    public static final ScalarType UUID = new ScalarType(java.util.UUID.class);
    public static final ScalarType OBJECT = new ScalarType(Object.class);

    @Override
    public RelDataType getRelationalDataType(final JavaTypeFactory typeFactory) {
        return typeFactory.createJavaType(scalarType());
    }
}
