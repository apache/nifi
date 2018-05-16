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
import org.apache.nifi.serialization.record.RecordSchema;

import java.util.Objects;

public class RecordDataType extends DataType {
    private final RecordSchema childSchema;

    public RecordDataType(final RecordSchema childSchema) {
        super(RecordFieldType.RECORD, null);
        this.childSchema = childSchema;
    }

    @Override
    public RecordFieldType getFieldType() {
        return RecordFieldType.RECORD;
    }

    public RecordSchema getChildSchema() {
        return childSchema;
    }

    @Override
    public int hashCode() {
        return 31 + 41 * getFieldType().hashCode() + 41 * (childSchema == null ? 0 : childSchema.hashCode());
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof RecordDataType)) {
            return false;
        }

        final RecordDataType other = (RecordDataType) obj;
        return getFieldType().equals(other.getFieldType()) && Objects.equals(childSchema, other.childSchema);
    }

    @Override
    public String toString() {
        return RecordFieldType.RECORD.toString();
    }
}
