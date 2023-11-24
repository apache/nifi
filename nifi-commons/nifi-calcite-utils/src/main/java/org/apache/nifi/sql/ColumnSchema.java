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

import java.util.Objects;

/**
 * A schema that describes a single column in a {@link NiFiTable}
 */
public class ColumnSchema {
    private final String name;
    private final ColumnType columnType;
    private final boolean nullable;

    /**
     * <p>
     * Convenience method to create a schema for a column with a scalar value.
     * </p>
     *
     * <p>
     * This is equivalent to calling <code>new ColumnSchema(name, new ScalarType(type), nullable);</code>
     * </p>
     *
     * @param name     the name of the column
     * @param type     the type of the column
     * @param nullable whether or not the column supports null values
     */
    public ColumnSchema(final String name, final Class<?> type, final boolean nullable) {
        this(name, new ScalarType(type), nullable);
    }

    /**
     * Creates a column schema with the given name and type that is optionally nullable
     *
     * @param name     the name of the column
     * @param type     the type of the column
     * @param nullable whether or not the column supports null values
     */
    public ColumnSchema(final String name, final ColumnType type, final boolean nullable) {
        this.name = Objects.requireNonNull(name);
        this.columnType = type;
        this.nullable = nullable;
    }

    public String getName() {
        return name;
    }

    public boolean isNullable() {
        return nullable;
    }

    RelDataType toRelationalDataType(final JavaTypeFactory typeFactory) {
        final RelDataType javaType = columnType.getRelationalDataType(typeFactory);
        return typeFactory.createTypeWithNullability(javaType, nullable);
    }

    @Override
    public boolean equals(final Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        final ColumnSchema that = (ColumnSchema) other;
        return nullable == that.nullable
            && Objects.equals(name, that.name)
            && Objects.equals(columnType, that.columnType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, columnType, nullable);
    }

    @Override
    public String toString() {
        return "ColumnSchema[name='" + name + "', columnType=" + columnType + ", nullable=" + nullable + "]";
    }
}
