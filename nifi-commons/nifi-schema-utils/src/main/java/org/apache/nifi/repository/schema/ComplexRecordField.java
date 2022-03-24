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

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ComplexRecordField implements RecordField {
    private static final FieldType fieldType = FieldType.COMPLEX;

    private final String fieldName;
    private final Repetition repetition;
    private final List<RecordField> subFields;

    public ComplexRecordField(final String fieldName, final Repetition repetition, final RecordField... subFields) {
        this(fieldName, repetition, Stream.of(subFields).collect(Collectors.toList()));
    }

    public ComplexRecordField(final String fieldName, final Repetition repetition, final List<RecordField> subFields) {
        Objects.requireNonNull(fieldName);
        Objects.requireNonNull(repetition);
        Objects.requireNonNull(subFields);

        if (subFields.isEmpty()) {
            throw new IllegalArgumentException("Cannot have a RecordField of type " + fieldType.name() + " without any sub-fields");
        }

        this.fieldName = fieldName;
        this.repetition = repetition;
        this.subFields = subFields;
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
        return subFields;
    }

    @Override
    public String toString() {
        return "ComplexRecordField[" + fieldName + "]";
    }

    @Override
    public int hashCode() {
        return 81 + fieldName.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
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
}
