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
    private final DataType valueType;

    public MapDataType(final DataType elementType) {
        super(RecordFieldType.MAP, null);
        this.valueType = elementType;
    }

    public DataType getValueType() {
        return valueType;
    }

    @Override
    public RecordFieldType getFieldType() {
        return RecordFieldType.MAP;
    }

    @Override
    public int hashCode() {
        return 31 + 41 * getFieldType().hashCode() + 41 * (valueType == null ? 0 : valueType.hashCode());
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof MapDataType)) {
            return false;
        }

        final MapDataType other = (MapDataType) obj;
        return getValueType().equals(other.getValueType()) && Objects.equals(valueType, other.valueType);
    }

    @Override
    public String toString() {
        return "MAP[" + valueType + "]";
    }
}
