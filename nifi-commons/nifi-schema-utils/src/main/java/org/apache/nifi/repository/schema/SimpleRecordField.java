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

package org.apache.nifi.repository.schema;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class SimpleRecordField implements RecordField {
    private final String fieldName;
    private final FieldType fieldType;
    private final Repetition repetition;

    public SimpleRecordField(final String fieldName, final FieldType fieldType, final Repetition repetition) {
        Objects.requireNonNull(fieldName);
        Objects.requireNonNull(fieldType);
        Objects.requireNonNull(repetition);

        this.fieldName = fieldName;
        this.fieldType = fieldType;
        this.repetition = repetition;
    }

    @Override
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public FieldType getFieldType() {
        return fieldType;
    }

    @Override
    public Repetition getRepetition() {
        return repetition;
    }

    @Override
    public List<RecordField> getSubFields() {
        return Collections.emptyList();
    }

    @Override
    public int hashCode() {
        return 31 + fieldName.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof RecordField)) {
            return false;
        }

        final RecordField other = (RecordField) obj;
        return fieldName.equals(other.getFieldName());
    }

    @Override
    public String toString() {
        return "SimpleRecordField[fieldName=" + fieldName + ", type=" + fieldType.name() + "]";
    }
}
