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

package org.apache.nifi.record.path;

import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;

import org.apache.nifi.record.path.util.Filters;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;

public class StandardFieldValue implements FieldValue {
    private final Object value;
    private final RecordField field;
    private final Optional<FieldValue> parent;

    public StandardFieldValue(final Object value, final RecordField field, final FieldValue parent) {
        this.value = value;
        this.field = field;
        this.parent = Optional.ofNullable(parent);
    }

    @Override
    public Object getValue() {
        return value;
    }

    @Override
    public RecordField getField() {
        return field;
    }

    @Override
    public Optional<FieldValue> getParent() {
        return parent;
    }

    @Override
    public int hashCode() {
        return Objects.hash(value, field, parent);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof StandardFieldValue)) {
            return false;
        }

        final StandardFieldValue other = (StandardFieldValue) obj;
        return Objects.equals(getValue(), other.getValue()) && Objects.equals(getField(), other.getField()) && Objects.equals(getParent(), other.getParent());
    }

    @Override
    public String toString() {
        if (value instanceof Object[]) {
            return Arrays.toString((Object[]) value);
        }

        return value.toString();
    }

    protected static FieldValue validateParentRecord(final FieldValue parent) {
        Objects.requireNonNull(parent, "Cannot create an ArrayIndexFieldValue without a parent");
        if (!Filters.isRecord(parent)) {
            if (!parent.getParentRecord().isPresent()) {
                throw new IllegalArgumentException("Field must have a Parent Record");
            }
        }

        final Object parentRecord = parent.getValue();
        if (parentRecord == null) {
            throw new IllegalArgumentException("Parent Record cannot be null");
        }

        return parent;
    }

    private Optional<Record> getParentRecord(final Optional<FieldValue> fieldValueOption) {
        if (!fieldValueOption.isPresent()) {
            return Optional.empty();
        }

        final FieldValue fieldValue = fieldValueOption.get();
        if (Filters.isRecord(fieldValue)) {
            return Optional.ofNullable((Record) fieldValue.getValue());
        }

        return getParentRecord(fieldValue.getParent());
    }

    @Override
    public Optional<Record> getParentRecord() {
        return getParentRecord(parent);
    }

    @Override
    public void updateValue(final Object newValue) {
        final Optional<Record> parentRecord = getParentRecord();
        if (!parentRecord.isPresent()) {
            if (value instanceof Record) {
                ((Record) value).setValue(getField().getFieldName(), newValue);
                return;
            } else {
                throw new UnsupportedOperationException("Cannot update the field value because the value is not associated with any record");
            }
        }

        parentRecord.get().setValue(getField().getFieldName(), newValue);
    }
}
