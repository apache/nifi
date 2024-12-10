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
import org.apache.nifi.serialization.record.field.FieldConverter;
import org.apache.nifi.serialization.record.field.StandardFieldConverterRegistry;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;
import org.apache.nifi.serialization.record.util.IllegalTypeConversionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class MapRecord implements Record {
    private static final Logger logger = LoggerFactory.getLogger(MapRecord.class);

    private RecordSchema schema;
    private final Map<String, Object> values;
    private Optional<SerializedForm> serializedForm;
    private final boolean checkTypes;
    private final boolean dropUnknownFields;
    private Set<RecordField> inactiveFields = null;
    private Map<String, RecordField> updatedFields = null;

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

        final FieldConverter<Object, String> converter = StandardFieldConverterRegistry.getRegistry().getFieldConverter(String.class);
        return converter.convertField(getValue(fieldName), Optional.empty(), fieldName);
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
    public LocalDate getAsLocalDate(final String fieldName, final String format) {
        return convertFieldToDateTime(LocalDate.class, fieldName, format);
    }

    @Override
    public LocalDateTime getAsLocalDateTime(String fieldName, String format) {
        return convertFieldToDateTime(LocalDateTime.class, fieldName, format);
    }

    @Override
    public OffsetDateTime getAsOffsetDateTime(final String fieldName, final String format) {
        return convertFieldToDateTime(OffsetDateTime.class, fieldName, format);
    }

    private <T> T convertFieldToDateTime(Class<T> clazz, String fieldName, String format) {
        final FieldConverter<Object, T> converter = StandardFieldConverterRegistry.getRegistry().getFieldConverter(clazz);
        return converter.convertField(getValue(fieldName), Optional.ofNullable(format), fieldName);
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
        if (!(obj instanceof final MapRecord other)) {
            return false;
        }
        return schema.equals(other.schema) && valuesEqual(values, other.values);
    }

    private boolean valuesEqual(final Map<String, Object> thisValues, final Map<String, Object> otherValues) {
        if (thisValues == null || otherValues == null) {
            return false;
        }

        if (thisValues.size() != otherValues.size()) {
            return false;
        }

        for (final Map.Entry<String, Object> entry : thisValues.entrySet()) {
            final Object thisValue = entry.getValue();
            final Object otherValue = otherValues.get(entry.getKey());
            if (Objects.equals(thisValue, otherValue)) {
                continue;
            }

            if (thisValue instanceof Object[] && otherValue instanceof Object[]) {
                if (!Arrays.equals((Object[]) thisValue, (Object[]) otherValue)) {
                    return false;
                }
            } else {
                return false;
            }
        }

        return true;
    }

    @Override
    public String toString() {
        final Optional<SerializedForm> serializedForm = getSerializedForm();
        if (serializedForm.isEmpty()) {
            return "MapRecord[" + values + "]";
        }

        final Object serialized = serializedForm.get().getSerialized();
        return serialized == null ? "MapRecord[" + values + "]" : serialized.toString();
    }

    @Override
    public Optional<SerializedForm> getSerializedForm() {
        if (serializedForm.isEmpty()) {
            return Optional.empty();
        }

        if (isSerializedFormReset()) {
            return Optional.empty();
        }

        return serializedForm;
    }

    private boolean isSerializedFormReset() {
        if (serializedForm.isEmpty()) {
            return true;
        }

        for (final Object value : values.values()) {
            if (isSerializedFormReset(value)) {
                return true;
            }
        }

        return false;
    }

    private boolean isSerializedFormReset(final Object value) {
        if (value == null) {
            return true;
        }

        if (value instanceof final MapRecord childRecord) {
            if (childRecord.isSerializedFormReset()) {
                return true;
            }
        } else if (value instanceof final Collection<?> collection) {
            for (final Object collectionValue : collection) {
                if (isSerializedFormReset(collectionValue)) {
                    return true;
                }
            }
        } else if (value instanceof final Object[] array) {
            for (final Object arrayValue : array) {
                if (isSerializedFormReset(arrayValue)) {
                    return true;
                }
            }
        }

        return false;
    }

    @Override
    public Map<String, Object> toMap() {
        return toMap(false);
    }

    public Map<String, Object> toMap(boolean convertSubRecords) {
        if (convertSubRecords) {
            Map<String, Object> newMap = new LinkedHashMap<>();
            values.forEach((key, value) -> {
                Object valueToAdd;

                if (value instanceof MapRecord) {
                    valueToAdd = ((MapRecord) value).toMap(true);
                } else if (value != null
                        && value.getClass().isArray()
                        && ((Object[]) value)[0] instanceof MapRecord) {
                    Object[] records = (Object[]) value;
                    Map<String, Object>[] maps = new Map[records.length];
                    for (int index = 0; index < records.length; index++) {
                        maps[index] = ((MapRecord) records[index]).toMap(true);
                    }
                    valueToAdd = maps;
                } else if (value instanceof final List<?> valueList) {
                    if (!valueList.isEmpty() && valueList.get(0) instanceof MapRecord) {
                        List<Map<String, Object>> newRecords = new ArrayList<>();
                        for (Object o : valueList) {
                            MapRecord rec = (MapRecord) o;
                            newRecords.add(rec.toMap(true));
                        }

                        valueToAdd = newRecords;
                    } else {
                        valueToAdd = value;
                    }
                } else {
                    valueToAdd = value;
                }

                newMap.put(key, valueToAdd);
            });

            return newMap;
        } else {
            return Collections.unmodifiableMap(values);
        }
    }

    @Override
    public void setValue(final RecordField field, final Object value) {
        final Optional<RecordField> existingField = setValueAndGetField(field.getFieldName(), value);

        // Keep track of any fields whose definition has been added or changed so that it can be taken into account when
        // calling #incorporateInactiveFields
        if (existingField.isPresent()) {
            final RecordField existingRecordField = existingField.get();
            final RecordField merged = DataTypeUtils.merge(existingRecordField, field);
            if (!Objects.equals(existingRecordField, merged)) {
                if (updatedFields == null) {
                    updatedFields = new LinkedHashMap<>();
                }
                updatedFields.put(field.getFieldName(), merged);
            }
        } else {
            if (inactiveFields == null) {
                inactiveFields = new LinkedHashSet<>();
            }

            inactiveFields.add(field);
        }
    }

    @Override
    public void setValue(final String fieldName, final Object value) {
        final Optional<RecordField> existingField = getSchema().getField(fieldName);
        RecordField recordField = null;
        if (existingField.isPresent()) {
            final DataType existingDataType = existingField.get().getDataType();
            final boolean compatible = DataTypeUtils.isCompatibleDataType(value, existingDataType);
            if (compatible) {
                recordField = existingField.get();
            }
        }
        if (recordField == null) {
            final DataType inferredDataType = DataTypeUtils.inferDataType(value, RecordFieldType.STRING.getDataType());
            recordField = new RecordField(fieldName, inferredDataType);
        }

        setValue(recordField, value);
    }

    @Override
    public void remove(final RecordField field) {
        final Optional<RecordField> existingField = resolveField(field);
        existingField.ifPresent(this::removeValue);
    }

    @Override
    public boolean rename(final RecordField field, final String newName) {
        final Optional<RecordField> resolvedField = resolveField(field);
        if (resolvedField.isEmpty()) {
            logger.debug("Could not rename {} to {} because the field could not be resolved to any field in the schema", field, newName);
            return false;
        }

        // If the new name already exists in the schema, and there's already a value, do not rename.
        if (schema.getField(newName).isPresent()) {
            throw new IllegalArgumentException("Could not rename [" + field + "] to [" + newName + "] because a field already exists with the name [" + newName + "]");
        }

        final String currentName = resolvedField.get().getFieldName();
        final boolean renamed = schema.renameField(currentName, newName);
        if (!renamed) {
            return false;
        }

        final Object currentValue = removeValue(currentName);
        updateValue(newName, currentValue);
        return true;
    }

    @Override
    public void regenerateSchema() {
        final List<RecordField> schemaFields = new ArrayList<>(schema.getFieldCount());

        for (final RecordField schemaField : schema.getFields()) {
            final Object fieldValue = getValue(schemaField);
            if (schemaField.getDataType().getFieldType() == RecordFieldType.CHOICE) {
                schemaFields.add(schemaField);
            } else if (fieldValue instanceof final Record childRecord) {
                schemaFields.add(new RecordField(schemaField.getFieldName(), RecordFieldType.RECORD.getRecordDataType(childRecord.getSchema()), schemaField.isNullable()));
            } else {
                schemaFields.add(schemaField);
            }
        }

        schema = new SimpleRecordSchema(schemaFields);
    }


    private Optional<RecordField> setValueAndGetField(final String fieldName, final Object value) {
        final Optional<RecordField> field = getSchema().getField(fieldName);
        if (field.isEmpty()) {
            updateValue(fieldName, value);
            return field;
        }

        final RecordField recordField = field.get();
        final Object coerced = isTypeChecked() ? DataTypeUtils.convertType(value, recordField.getDataType(), fieldName) : value;
        updateValue(recordField.getFieldName(), coerced);

        return field;
    }

    private void updateValue(final String fieldName, final Object value) {
        final Object previousValue = values.put(fieldName, value);
        if (!Objects.equals(value, previousValue)) {
            serializedForm = Optional.empty();
        }
    }

    private Object removeValue(final RecordField field) {
        if (field == null) {
            return null;
        }

        return removeValue(field.getFieldName());
    }

    private Object removeValue(final String fieldName) {
        final Object previousValue = values.remove(fieldName);
        if (previousValue != null) {
            serializedForm = Optional.empty();
        }

        return previousValue;
    }

    @Override
    public void setArrayValue(final String fieldName, final int arrayIndex, final Object value) {
        final Optional<RecordField> field = getSchema().getField(fieldName);
        if (field.isEmpty()) {
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
        if (field.isEmpty()) {
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
            mapObject = new LinkedHashMap<>();
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
        final Map<String, RecordField> fieldsByName = new LinkedHashMap<>();

        boolean fieldUpdated = false;
        for (final RecordField field : schema.getFields()) {
            final RecordField updated = getUpdatedRecordField(field);
            if (!updated.equals(field)) {
                fieldUpdated = true;
            }

            fieldsByName.put(updated.getFieldName(), updated);
        }

        if (!fieldUpdated && (inactiveFields == null || inactiveFields.isEmpty())) {
            return;
        }

        if (inactiveFields != null) {
            for (final RecordField field : inactiveFields) {
                final RecordField existingField = fieldsByName.get(field.getFieldName());
                if (existingField == null) {
                    fieldsByName.put(field.getFieldName(), field);
                } else {
                    if (Objects.equals(existingField, field)) {
                        continue;
                    }

                    final RecordField merged = DataTypeUtils.merge(existingField, field);
                    fieldsByName.put(field.getFieldName(), merged);
                }
            }
        }

        this.schema = new SimpleRecordSchema(new ArrayList<>(fieldsByName.values()));
    }

    private RecordField getUpdatedRecordField(final RecordField field) {
        final String fieldName = field.getFieldName();
        final RecordField specField;
        if (updatedFields == null) {
            specField = field;
        } else {
            specField = updatedFields.getOrDefault(fieldName, field);
        }

        final DataType dataType = specField.getDataType();
        final RecordFieldType fieldType = dataType.getFieldType();

        if (isSimpleType(fieldType)) {
            return specField;
        }

        final Object value = getValue(specField);
        if (value == null) {
            return specField;
        }

        if (fieldType == RecordFieldType.RECORD && value instanceof Record) {
            final Record childRecord = (Record) value;
            childRecord.incorporateInactiveFields();

            final RecordSchema definedChildSchema = ((RecordDataType) dataType).getChildSchema();
            final RecordSchema actualChildSchema = childRecord.getSchema();
            final RecordSchema combinedChildSchema = DataTypeUtils.merge(definedChildSchema, actualChildSchema);
            final DataType combinedDataType = RecordFieldType.RECORD.getRecordDataType(combinedChildSchema);

            return new RecordField(specField.getFieldName(), combinedDataType, specField.getDefaultValue(), specField.getAliases(), specField.isNullable());
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
                return new RecordField(specField.getFieldName(), mergedDataType, specField.getDefaultValue(), specField.getAliases(), specField.isNullable());
            }

            return specField;
        }

        if (fieldType == RecordFieldType.CHOICE) {
            final ChoiceDataType choiceDataType = (ChoiceDataType) dataType;
            final List<DataType> possibleTypes = choiceDataType.getPossibleSubTypes();

            final DataType chosenDataType = DataTypeUtils.chooseDataType(value, choiceDataType);
            if (chosenDataType.getFieldType() != RecordFieldType.RECORD || !(value instanceof Record)) {
                return specField;
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
            return new RecordField(specField.getFieldName(), mergedDataType, specField.getDefaultValue(), specField.getAliases(), specField.isNullable());
        }

        return specField;
    }

    private boolean isSimpleType(final RecordFieldType fieldType) {
        return switch (fieldType) {
            case ARRAY, RECORD, MAP, CHOICE -> false;
            default -> true;
        };

    }

    @Override
    public Set<String> getRawFieldNames() {
        return values.keySet();
    }
}
