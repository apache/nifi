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

import java.text.DateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.nifi.serialization.SchemaValidationException;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.serialization.record.util.IllegalTypeConversionException;

public class MapRecord implements Record {
    private RecordSchema schema;
    private final Map<String, Object> values;
    private Optional<SerializedForm> serializedForm;
    private final boolean checkTypes;
    private final boolean dropUnknownFields;


    public MapRecord(final RecordSchema schema, final Map<String, Object> values) {
        this(schema, values, false, false);
    }

    public MapRecord(final RecordSchema schema, final Map<String, Object> values, final boolean checkTypes, final boolean dropUnknownFields) {
        Objects.requireNonNull(values);

        this.schema = Objects.requireNonNull(schema);
        this.values = checkTypes ? checkTypes(values, schema) : values;
        this.serializedForm = Optional.empty();
        this.checkTypes = checkTypes;
        this.dropUnknownFields = dropUnknownFields;
    }

    public MapRecord(final RecordSchema schema, final Map<String, Object> values, final SerializedForm serializedForm) {
        this(schema, values, serializedForm, false, false);
    }

    public MapRecord(final RecordSchema schema, final Map<String, Object> values, final SerializedForm serializedForm, final boolean checkTypes, final boolean dropUnknownFields) {
        Objects.requireNonNull(values);

        this.schema = Objects.requireNonNull(schema);
        this.values = checkTypes ? checkTypes(values, schema) : values;
        this.serializedForm = Optional.ofNullable(serializedForm);
        this.checkTypes = checkTypes;
        this.dropUnknownFields = dropUnknownFields;
    }

    private Map<String, Object> checkTypes(final Map<String, Object> values, final RecordSchema schema) {
        for (final RecordField field : schema.getFields()) {
            final Object value = getExplicitValue(field, values);

            if (value == null) {
                if (field.isNullable()) {
                    continue;
                }

                throw new SchemaValidationException("Field " + field.getFieldName() + " cannot be null");
            }

            if (!DataTypeUtils.isCompatibleDataType(value, field.getDataType())) {
                throw new SchemaValidationException("Field " + field.getFieldName() + " has a value of " + value
                    + ", which cannot be coerced into the appropriate data type of " + field.getDataType());
            }
        }

        return values;
    }

    @Override
    public boolean isDropUnknownFields() {
        return dropUnknownFields;
    }

    @Override
    public boolean isTypeChecked() {
        return checkTypes;
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

        if (dropUnknownFields) {
            return null;
        }

        return this.values.get(fieldName);
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
        return getExplicitValue(field, this.values);
    }

    private Object getExplicitValue(final RecordField field, final Map<String, Object> values) {
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
        if (dataTypeOption.isPresent()) {
            return convertToString(getValue(fieldName), dataTypeOption.get().getFormat());
        }

        return DataTypeUtils.toString(getValue(fieldName), (Supplier<DateFormat>) null);
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
        return DataTypeUtils.toDate(getValue(fieldName), () -> DataTypeUtils.getDateFormat(format), fieldName);
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
        return "MapRecord[" + values + "]";
    }

    @Override
    public Optional<SerializedForm> getSerializedForm() {
        return serializedForm;
    }

    @Override
    public void setValue(final String fieldName, final Object value) {
        final Optional<RecordField> field = getSchema().getField(fieldName);
        if (!field.isPresent()) {
            if (dropUnknownFields) {
                return;
            }

            final Object previousValue = values.put(fieldName, value);
            if (!Objects.equals(value, previousValue)) {
                serializedForm = Optional.empty();
            }

            return;
        }

        final RecordField recordField = field.get();
        final Object coerced = isTypeChecked() ? DataTypeUtils.convertType(value, recordField.getDataType(), fieldName) : value;
        final Object previousValue = values.put(recordField.getFieldName(), coerced);
        if (!Objects.equals(coerced, previousValue)) {
            serializedForm = Optional.empty();
        }
    }

    @Override
    public void setArrayValue(final String fieldName, final int arrayIndex, final Object value) {
        final Optional<RecordField> field = getSchema().getField(fieldName);
        if (!field.isPresent()) {
            return;
        }

        final RecordField recordField = field.get();
        final DataType dataType = recordField.getDataType();
        if (dataType.getFieldType() != RecordFieldType.ARRAY) {
            throw new IllegalTypeConversionException("Cannot set the value of an array index on Record because the field '" + fieldName
                + "' is of type '" + dataType + "' and cannot be coerced into an ARRAY type");
        }

        final Object arrayObject = values.get(recordField.getFieldName());
        if (arrayObject == null) {
            return;
        }
        if (!(arrayObject instanceof Object[])) {
            return;
        }

        final Object[] array = (Object[]) arrayObject;
        if (arrayIndex >= array.length) {
            return;
        }

        final ArrayDataType arrayDataType = (ArrayDataType) dataType;
        final DataType elementType = arrayDataType.getElementType();
        final Object coerced = DataTypeUtils.convertType(value, elementType, fieldName);

        final boolean update = !Objects.equals(coerced, array[arrayIndex]);
        if (update) {
            array[arrayIndex] = coerced;
            serializedForm = Optional.empty();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void setMapValue(final String fieldName, final String mapKey, final Object value) {
        final Optional<RecordField> field = getSchema().getField(fieldName);
        if (!field.isPresent()) {
            return;
        }

        final RecordField recordField = field.get();
        final DataType dataType = recordField.getDataType();
        if (dataType.getFieldType() != RecordFieldType.MAP) {
            throw new IllegalTypeConversionException("Cannot set the value of map entry on Record because the field '" + fieldName
                + "' is of type '" + dataType + "' and cannot be coerced into an MAP type");
        }

        Object mapObject = values.get(recordField.getFieldName());
        if (mapObject == null) {
            mapObject = new HashMap<String, Object>();
        }
        if (!(mapObject instanceof Map)) {
            return;
        }

        final Map<String, Object> map = (Map<String, Object>) mapObject;

        final MapDataType mapDataType = (MapDataType) dataType;
        final DataType valueDataType = mapDataType.getValueType();
        final Object coerced = DataTypeUtils.convertType(value, valueDataType, fieldName);

        final Object replaced = map.put(mapKey, coerced);
        if (replaced == null || !replaced.equals(coerced)) {
            serializedForm = Optional.empty();
        }
    }

    @Override
    public void incorporateSchema(RecordSchema other) {
        this.schema = DataTypeUtils.merge(this.schema, other);
    }

    @Override
    public Set<String> getRawFieldNames() {
        return values.keySet();
    }
}
