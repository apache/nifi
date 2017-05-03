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

import java.util.Date;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class MapRecord implements Record {
    private final RecordSchema schema;
    private final Map<String, Object> values;
    private final Optional<SerializedForm> serializedForm;

    public MapRecord(final RecordSchema schema, final Map<String, Object> values) {
        this.schema = Objects.requireNonNull(schema);
        this.values = Objects.requireNonNull(values);
        this.serializedForm = Optional.empty();
    }

    public MapRecord(final RecordSchema schema, final Map<String, Object> values, final SerializedForm serializedForm) {
        this.schema = Objects.requireNonNull(schema);
        this.values = Objects.requireNonNull(values);
        this.serializedForm = Optional.ofNullable(serializedForm);
    }

    @Override
    public RecordSchema getSchema() {
        return schema;
    }

    @Override
    public Object[] getValues() {
        final Object[] values = new Object[schema.getFieldCount()];
        int i = 0;
        for (final RecordField recordField : schema.getFields()) {
            values[i++] = getValue(recordField);
        }
        return values;
    }

    @Override
    public Object getValue(final String fieldName) {
        final Optional<RecordField> fieldOption = schema.getField(fieldName);
        if (fieldOption.isPresent()) {
            return getValue(fieldOption.get());
        }

        return null;
    }

    @Override
    public Object getValue(final RecordField field) {
        Object explicitValue = getExplicitValue(field);
        if (explicitValue != null) {
            return explicitValue;
        }

        final Optional<RecordField> resolvedField = resolveField(field);
        final boolean resolvedFieldDifferent = resolvedField.isPresent() && !resolvedField.get().equals(field);
        if (resolvedFieldDifferent) {
            explicitValue = getExplicitValue(resolvedField.get());
            if (explicitValue != null) {
                return explicitValue;
            }
        }

        Object defaultValue = field.getDefaultValue();
        if (defaultValue != null) {
            return defaultValue;
        }

        if (resolvedFieldDifferent) {
            return resolvedField.get().getDefaultValue();
        }

        return null;
    }

    private Optional<RecordField> resolveField(final RecordField field) {
        Optional<RecordField> resolved = schema.getField(field.getFieldName());
        if (resolved.isPresent()) {
            return resolved;
        }

        for (final String alias : field.getAliases()) {
            resolved = schema.getField(alias);
            if (resolved.isPresent()) {
                return resolved;
            }
        }

        return Optional.empty();
    }

    private Object getExplicitValue(final RecordField field) {
        final String canonicalFieldName = field.getFieldName();

        // We use containsKey here instead of just calling get() and checking for a null value
        // because if the true field name is set to null, we want to return null, rather than
        // what the alias points to. Likewise for a specific alias, since aliases are defined
        // in a List with a specific ordering.
        Object value = values.get(canonicalFieldName);
        if (value != null) {
            return value;
        }

        for (final String alias : field.getAliases()) {
            value = values.get(alias);
            if (value != null) {
                return value;
            }
        }

        return null;
    }

    @Override
    public String getAsString(final String fieldName) {
        final Optional<DataType> dataTypeOption = schema.getDataType(fieldName);
        if (!dataTypeOption.isPresent()) {
            return null;
        }

        return convertToString(getValue(fieldName), dataTypeOption.get().getFormat());
    }

    @Override
    public String getAsString(final String fieldName, final String format) {
        return convertToString(getValue(fieldName), format);
    }

    @Override
    public String getAsString(final RecordField field, final String format) {
        return convertToString(getValue(field), format);
    }

    private String convertToString(final Object value, final String format) {
        if (value == null) {
            return null;
        }

        return DataTypeUtils.toString(value, format);
    }

    @Override
    public Long getAsLong(final String fieldName) {
        return DataTypeUtils.toLong(getValue(fieldName), fieldName);
    }

    @Override
    public Integer getAsInt(final String fieldName) {
        return DataTypeUtils.toInteger(getValue(fieldName), fieldName);
    }

    @Override
    public Double getAsDouble(final String fieldName) {
        return DataTypeUtils.toDouble(getValue(fieldName), fieldName);
    }

    @Override
    public Float getAsFloat(final String fieldName) {
        return DataTypeUtils.toFloat(getValue(fieldName), fieldName);
    }

    @Override
    public Record getAsRecord(String fieldName, final RecordSchema schema) {
        return DataTypeUtils.toRecord(getValue(fieldName), schema, fieldName);
    }

    @Override
    public Boolean getAsBoolean(final String fieldName) {
        return DataTypeUtils.toBoolean(getValue(fieldName), fieldName);
    }

    @Override
    public Date getAsDate(final String fieldName, final String format) {
        return DataTypeUtils.toDate(getValue(fieldName), format == null ? null : DataTypeUtils.getDateFormat(format), fieldName);
    }

    @Override
    public Object[] getAsArray(final String fieldName) {
        return DataTypeUtils.toArray(getValue(fieldName), fieldName);
    }


    @Override
    public int hashCode() {
        return 31 + 41 * values.hashCode() + 7 * schema.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof MapRecord)) {
            return false;
        }
        final MapRecord other = (MapRecord) obj;
        return schema.equals(other.schema) && values.equals(other.values);
    }

    @Override
    public String toString() {
        return "MapRecord[values=" + values + "]";
    }

    @Override
    public Optional<SerializedForm> getSerializedForm() {
        return serializedForm;
    }
}
