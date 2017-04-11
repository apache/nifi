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

package org.apache.nifi.serialization.record;

public class DataType {
    private final RecordFieldType fieldType;
    private final String format;

    protected DataType(final RecordFieldType fieldType, final String format) {
        this.fieldType = fieldType;
        this.format = format;
    }

    public String getFormat() {
        return format;
    }

    public RecordFieldType getFieldType() {
        return fieldType;
    }

    @Override
    public int hashCode() {
        return 31 + 41 * fieldType.hashCode() + 41 * (format == null ? 0 : format.hashCode());
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof DataType)) {
            return false;
        }

        final DataType other = (DataType) obj;
        return fieldType.equals(other.fieldType) && ((format == null && other.format == null) || (format != null && format.equals(other.format)));
    }

    @Override
    public String toString() {
        if (format == null) {
            return fieldType.toString();
        } else {
            return fieldType.toString() + ":" + format;
        }
    }
}
