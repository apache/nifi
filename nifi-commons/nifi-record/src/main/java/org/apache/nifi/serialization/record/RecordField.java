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

import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.nifi.serialization.record.validation.FieldValidator;

public class RecordField {
    public static final boolean DEFAULT_NULLABLE = true;

    private final String fieldName;
    private final DataType dataType;
    private final Set<String> aliases;
    private final Object defaultValue;
    private final boolean nullable;
    private final List<FieldValidator> fieldValidators;

    public RecordField(final String fieldName, final DataType dataType) {
        this(fieldName, dataType, null, Collections.emptySet(), DEFAULT_NULLABLE);
    }

    public RecordField(final String fieldName, final DataType dataType, final boolean nullable) {
        this(fieldName, dataType, null, Collections.emptySet(), nullable);
    }

    public RecordField(final String fieldName, final DataType dataType, final Object defaultValue) {
        this(fieldName, dataType, defaultValue, Collections.emptySet(), DEFAULT_NULLABLE);
    }

    public RecordField(final String fieldName, final DataType dataType, final Object defaultValue, final boolean nullable) {
        this(fieldName, dataType, defaultValue, Collections.emptySet(), nullable);
    }

    public RecordField(final String fieldName, final DataType dataType, final Set<String> aliases) {
        this(fieldName, dataType, null, aliases, DEFAULT_NULLABLE);
    }

    public RecordField(final String fieldName, final DataType dataType, final Set<String> aliases, final boolean nullable) {
        this(fieldName, dataType, null, aliases, nullable);
    }

    public RecordField(final String fieldName, final DataType dataType, final Object defaultValue, final Set<String> aliases) {
        this(fieldName, dataType, defaultValue, aliases, DEFAULT_NULLABLE);
    }

    public RecordField(final String fieldName, final DataType dataType, final Object defaultValue, final Set<String> aliases, final boolean nullable) {
        this(fieldName, dataType, defaultValue, aliases, nullable, List.of());
    }

    public RecordField(final String fieldName, final DataType dataType, final Object defaultValue, final Set<String> aliases, final boolean nullable,
            final List<FieldValidator> fieldValidators) {
        if (defaultValue != null && !DataTypeUtils.isCompatibleDataType(defaultValue, dataType)) {
            throw new IllegalArgumentException("Cannot set the default value for field [" + fieldName + "] to [" + defaultValue
                + "] because that is not a valid value for Data Type [" + dataType + "]");
        }

        this.fieldName = Objects.requireNonNull(fieldName);
        this.dataType = Objects.requireNonNull(dataType);

        // If aliases is the empty set, don't bother with the expense of wrapping in an unmodifiableSet.
        Objects.requireNonNull(aliases);
        if (aliases == Collections.EMPTY_SET) {
            this.aliases = aliases;
        } else {
            this.aliases = Collections.unmodifiableSet(aliases);
        }

        this.defaultValue = defaultValue;
        this.nullable = nullable;
        Objects.requireNonNull(fieldValidators, "Field validators cannot be null");
        this.fieldValidators = fieldValidators.isEmpty() ? List.of() : List.copyOf(fieldValidators);
    }

    public String getFieldName() {
        return fieldName;
    }

    public Set<String> getAliases() {
        return aliases;
    }

    public DataType getDataType() {
        return dataType;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

    public boolean isNullable() {
        return nullable;
    }

    public List<FieldValidator> getFieldValidators() {
        return fieldValidators;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + dataType.hashCode();
        result = prime * result + fieldName.hashCode();
        result = prime * result + aliases.hashCode();
        result = prime * result + ((defaultValue == null) ? 0 : defaultValue.hashCode());
        result = prime * result + Boolean.hashCode(nullable);
        result = prime * result + fieldValidators.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }

        if (getClass() != obj.getClass()) {
            return false;
        }

        RecordField other = (RecordField) obj;
        return dataType.equals(other.getDataType()) && fieldName.equals(other.getFieldName()) && aliases.equals(other.getAliases()) && Objects.equals(defaultValue, other.defaultValue)
            && nullable == other.nullable
            && fieldValidators.equals(other.fieldValidators);
    }

    @Override
    public String toString() {
        return "RecordField[name=" + fieldName + ", dataType=" + dataType + (aliases.isEmpty() ? "" : ", aliases=" + aliases)
            + ", nullable=" + nullable + (fieldValidators.isEmpty() ? "" : ", validators=" + fieldValidators.size()) + "]";
    }
}
