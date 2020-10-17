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

package org.apache.nifi.serialization.record.type;

import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordFieldType;

import java.util.Objects;

public class MapDataType extends DataType {

    public static final boolean DEFAULT_NULLABLE = false;

    private final DataType valueType;
    private final boolean valuesNullable;

    public MapDataType(final DataType elementType) {
        this(elementType, DEFAULT_NULLABLE);
    }

    public MapDataType(final DataType elementType, boolean valuesNullable) {
        super(RecordFieldType.MAP, null);
        this.valueType = elementType;
        this.valuesNullable = valuesNullable;
    }

    public DataType getValueType() {
        return valueType;
    }

    public boolean isValuesNullable() {
        return valuesNullable;
    }

    @Override
    public RecordFieldType getFieldType() {
        return RecordFieldType.MAP;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof MapDataType)) return false;
        if (!super.equals(o)) return false;
        MapDataType that = (MapDataType) o;
        return valuesNullable == that.valuesNullable
                && Objects.equals(getValueType(), that.getValueType());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), getValueType(), valuesNullable);
    }

    @Override
    public String toString() {
        return "MAP<" + valueType + ">";
    }
}
