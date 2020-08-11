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

import org.apache.nifi.serialization.SchemaValidationException;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.serialization.record.util.IllegalTypeConversionException;

import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

public class MapRecord implements Record {
    private RecordSchema schema;
    private final Map<String, Object> values;
    private Optional<SerializedForm> serializedForm;
    private final boolean checkTypes;
    private final boolean dropUnknownFields;
    private Set<RecordField> inactiveFields = null;

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
            Object value = getExplicitValue(field, values);

            if (value == null) {
                if (field.isNullable() || field.getDefaultValue() != null) {
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
            Object value = getExplicitValue(recordField);
            if (value == null) {
                value = recordField.getDefaultValue();
            }

            values[i++] = value;
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
        return DataTypeUtils.toArray(getValue(fieldName), fieldName, null, StandardCharsets.UTF_8);
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
    public Map<String, Object> toMap() {
        return Collections.unmodifiableMap(values);
    }

    @Override
    public void setValue(final RecordField field, final Object value) {
        final Optional<RecordField> existingField = setValueAndGetField(field.getFieldName(), value);

        if (!existingField.isPresent()) {
            if (inactiveFields == null) {
                inactiveFields = new LinkedHashSet<>();
            }

            inactiveFields.add(field);
        }
    }

    @Override
    public void setValue(final String fieldName, final Object value) {
        final Optional<RecordField> existingField = setValueAndGetField(fieldName, value);

        if (!existingField.isPresent()) {
            if (inactiveFields == null) {
                inactiveFields = new LinkedHashSet<>();
            }

            final DataType inferredDataType = DataTypeUtils.inferDataType(value, RecordFieldType.STRING.getDataType());
            final RecordField field = new RecordField(fieldName, inferredDataType);
            inactiveFields.add(field);
        }
    }

    private Optional<RecordField> setValueAndGetField(final String fieldName, final Object value) {
        final Optional<RecordField> field = getSchema().getField(fieldName);
        if (!field.isPresent()) {
            if (dropUnknownFields) {
                return field;
            }

            final Object previousValue = values.put(fieldName, value);
            if (!Objects.equals(value, previousValue)) {
                serializedForm = Optional.empty();
            }

            return field;
        }

        final RecordField recordField = field.get();
        final Object coerced = isTypeChecked() ? DataTypeUtils.convertType(value, recordField.getDataType(), fieldName) : value;
        final Object previousValue = values.put(recordField.getFieldName(), coerced);
        if (!Objects.equals(coerced, previousValue)) {
            serializedForm = Optional.empty();
        }

        return field;
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
    public void incorporateInactiveFields() {
        final List<RecordField> updatedFields = new ArrayList<>();

        boolean fieldUpdated = false;
        for (final RecordField field : schema.getFields()) {
            final RecordField updated = getUpdatedRecordField(field);
            if (!updated.equals(field)) {
                fieldUpdated = true;
            }

            updatedFields.add(updated);
        }

        if (!fieldUpdated && (inactiveFields == null || inactiveFields.isEmpty())) {
            return;
        }

        if (inactiveFields != null) {
            for (final RecordField field : inactiveFields) {
                if (!updatedFields.contains(field)) {
                    updatedFields.add(field);
                }
            }
        }

        this.schema = new SimpleRecordSchema(updatedFields);
    }

    private RecordField getUpdatedRecordField(final RecordField field) {
        final DataType dataType = field.getDataType();
        final RecordFieldType fieldType = dataType.getFieldType();

        if (isSimpleType(fieldType)) {
            return field;
        }

        final Object value = getValue(field);
        if (value == null) {
            return field;
        }

        if (fieldType == RecordFieldType.RECORD && value instanceof Record) {
            final Record childRecord = (Record) value;
            childRecord.incorporateInactiveFields();

            final RecordSchema definedChildSchema = ((RecordDataType) dataType).getChildSchema();
            final RecordSchema actualChildSchema = childRecord.getSchema();
            final RecordSchema combinedChildSchema = DataTypeUtils.merge(definedChildSchema, actualChildSchema);
            final DataType combinedDataType = RecordFieldType.RECORD.getRecordDataType(combinedChildSchema);

            final RecordField updatedField = new RecordField(field.getFieldName(), combinedDataType, field.getDefaultValue(), field.getAliases(), field.isNullable());
            return updatedField;
        }

        if (fieldType == RecordFieldType.ARRAY && value instanceof Object[]) {
            final DataType elementType = ((ArrayDataType) dataType).getElementType();
            final RecordFieldType elementFieldType = elementType.getFieldType();

            if (elementFieldType == RecordFieldType.RECORD) {
                final Object[] array = (Object[]) value;
                RecordSchema mergedSchema = ((RecordDataType) elementType).getChildSchema();

                for (final Object element : array) {
                    if (element == null) {
                        continue;
                    }

                    final Record record = (Record) element;
                    record.incorporateInactiveFields();
                    mergedSchema = DataTypeUtils.merge(mergedSchema, record.getSchema());
                }

                final DataType mergedRecordType = RecordFieldType.RECORD.getRecordDataType(mergedSchema);
                final DataType mergedDataType = RecordFieldType.ARRAY.getArrayDataType(mergedRecordType);
                final RecordField updatedField = new RecordField(field.getFieldName(), mergedDataType, field.getDefaultValue(), field.getAliases(), field.isNullable());
                return updatedField;
            }

            return field;
        }

        if (fieldType == RecordFieldType.CHOICE) {
            final ChoiceDataType choiceDataType = (ChoiceDataType) dataType;
            final List<DataType> possibleTypes = choiceDataType.getPossibleSubTypes();

            final DataType chosenDataType = DataTypeUtils.chooseDataType(value, choiceDataType);
            if (chosenDataType.getFieldType() != RecordFieldType.RECORD || !(value instanceof Record)) {
                return field;
            }

            final RecordDataType recordDataType = (RecordDataType) chosenDataType;
            final Record childRecord = (Record) value;
            childRecord.incorporateInactiveFields();

            final RecordSchema definedChildSchema = recordDataType.getChildSchema();
            final RecordSchema actualChildSchema = childRecord.getSchema();
            final RecordSchema combinedChildSchema = DataTypeUtils.merge(definedChildSchema, actualChildSchema);
            final DataType combinedDataType = RecordFieldType.RECORD.getRecordDataType(combinedChildSchema);

            final List<DataType> updatedPossibleTypes = new ArrayList<>(possibleTypes.size());
            for (final DataType possibleType : possibleTypes) {
                if (possibleType.equals(chosenDataType)) {
                    updatedPossibleTypes.add(combinedDataType);
                } else {
                    updatedPossibleTypes.add(possibleType);
                }
            }

            final DataType mergedDataType = RecordFieldType.CHOICE.getChoiceDataType(updatedPossibleTypes);
            return new RecordField(field.getFieldName(), mergedDataType, field.getDefaultValue(), field.getAliases(), field.isNullable());
        }

        return field;
    }

    private boolean isSimpleType(final RecordFieldType fieldType) {
        switch (fieldType) {
            case ARRAY:
            case RECORD:
            case MAP:
            case CHOICE:
                return false;
        }

        return true;
    }

    @Override
    public Set<String> getRawFieldNames() {
        return values.keySet();
    }
}
