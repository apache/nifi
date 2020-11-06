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

import java.util.List;
import java.util.Objects;

public class EnumDataType extends DataType {

    private final List<String> enums;

    public EnumDataType(final List<String> enums) {
        super(RecordFieldType.ENUM, null);
        this.enums = enums;
    }

    public List<String> getEnums() {
        return enums;
    }

    @Override
    public RecordFieldType getFieldType() {
        return RecordFieldType.ENUM;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof EnumDataType)) return false;
        if (!super.equals(o)) return false;
        EnumDataType that = (EnumDataType) o;
        return Objects.equals(getEnums(), that.getEnums());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), getEnums());
    }

    @Override
    public String toString() {
        return "ENUM" + getEnums();
    }
}
