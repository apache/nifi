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

import java.util.Objects;

import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;

public class ArrayIndexFieldValue extends StandardFieldValue {
    private final int index;

    public ArrayIndexFieldValue(final Object value, final RecordField field, final FieldValue parent, final int index) {
        super(value, field, validateParent(parent));
        this.index = index;
    }

    private static FieldValue validateParent(final FieldValue parent) {
        Objects.requireNonNull(parent, "Cannot create an ArrayIndexFieldValue without a parent");
        if (RecordFieldType.ARRAY != parent.getField().getDataType().getFieldType()) {
            throw new IllegalArgumentException("Cannot create an ArrayIndexFieldValue with a parent of type " + parent.getField().getDataType().getFieldType());
        }

        final Object parentRecord = parent.getValue();
        if (parentRecord == null) {
            throw new IllegalArgumentException("Cannot create an ArrayIndexFieldValue without a parent Record");
        }

        return parent;
    }

    public int getArrayIndex() {
        return index;
    }

    @Override
    public void updateValue(final Object newValue) {
        getParentRecord().get().setArrayValue(getField().getFieldName(), getArrayIndex(), newValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getValue(), getField(), getParent(), index);
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof ArrayIndexFieldValue)) {
            return false;
        }

        final ArrayIndexFieldValue other = (ArrayIndexFieldValue) obj;
        return Objects.equals(getValue(), other.getValue()) && Objects.equals(getField(), other.getField())
            && Objects.equals(getParent(), other.getParent()) && getArrayIndex() == other.getArrayIndex();
    }
}
