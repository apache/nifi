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
package org.apache.nifi.schema.inference;

import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public abstract class HierarchicalSchemaInference<T> implements SchemaInferenceEngine<T> {

    public RecordSchema inferSchema(final RecordSource<T> recordSource) throws IOException {
        final Map<String, FieldTypeInference> typeMap = new LinkedHashMap<>();
        String rootElementName = null;

        while (true) {
            final T rawRecord = recordSource.next();
            if (rawRecord == null) {
                break;
            }

            inferSchema(rawRecord, typeMap);

            final String name = getRootName(rawRecord);
            if (rootElementName == null) {
                rootElementName = name;
            } else if (!rootElementName.equals(name)) {
                rootElementName = null;
            }
        }

        RecordSchema inferredSchema = createSchema(typeMap, rootElementName);
        // Replace array<null> with array<string> in the typeMap. We use array<null> internally for empty arrays because for example if we encounter an empty array in the first record,
        // we have no way of knowing the type of elements. If we just decide to use STRING as the type like was previously done, this can cause problems because anything can be coerced
        // into a STRING, and if we later encounter an array of Records there, we end up inferring that as a STRING so we end up converting the Record objects into STRINGs.
        // Instead, we create an Array where the element type is null, then consider ARRAY[x] wider than ARRAY[null] for any x (other than null). But to cover all cases we have to wait
        // until the very end, after making inferences based on all data. At that point if the type is still inferred to be null we can just change it to a STRING.
        return defaultArrayTypes(inferredSchema);
    }

    protected void inferSchema(final T rawRecord, final Map<String, FieldTypeInference> inferences) {
        if (isObject(rawRecord)) {
            final BiConsumer<String, T> inferType = (fieldName, value) -> inferType(fieldName, value, inferences);
            forEachFieldInRecord(rawRecord, inferType);
        } else if (isArray(rawRecord)) {
            forEachRawRecordInArray(rawRecord, arrayElement -> inferSchema(arrayElement, inferences));
        } else {
            throw new IllegalArgumentException("Cannot derive a Record Schema : expected an Array or Complex Object but got " + rawRecord);
        }
    }

    private void inferType(final String fieldName, final T value, final Map<String, FieldTypeInference> inferences) {
        if (value == null) {
            return;
        }

        final FieldTypeInference typeInference = inferences.computeIfAbsent(fieldName, key -> new FieldTypeInference());

        if (isObject(value)) {
            final RecordSchema schema = createSchema(value);
            final DataType fieldDataType = RecordFieldType.RECORD.getRecordDataType(schema);
            typeInference.addPossibleDataType(fieldDataType);
        } else if (isArray(value)) {
            if (isEmptyArray(value)) {
                // At this point we don't know the type of array elements as the array is empty, and it is too early to assume an array of strings. Use null as the
                // element type for now, and call defaultArrayTypes() when all inferences are complete, to ensure that if there are any arrays with inferred element type
                // of null, they default to string for the final schema.
                final DataType arrayDataType = RecordFieldType.ARRAY.getArrayDataType(null);
                typeInference.addPossibleDataType(arrayDataType);
            } else {
                final FieldTypeInference arrayElementTypeInference = new FieldTypeInference();
                forEachRawRecordInArray(value, arrayElement -> inferType(arrayElement, arrayElementTypeInference));

                DataType elementDataType = arrayElementTypeInference.toDataType();
                final DataType arrayDataType = RecordFieldType.ARRAY.getArrayDataType(elementDataType);
                typeInference.addPossibleDataType(arrayDataType);
            }
        } else {
            typeInference.addPossibleDataType(getDataType(value));
        }
    }

    /*
     * This method checks a RecordSchema's child fields for array<null> datatypes recursively and replaces them with the default array<string>. This should be called
     * after all inferences have been completed.
     */
    private RecordSchema defaultArrayTypes(final RecordSchema recordSchema) {
        List<RecordField> newRecordFields = new ArrayList<>(recordSchema.getFieldCount());
        for (RecordField childRecordField : recordSchema.getFields()) {
            newRecordFields.add(defaultArrayTypes(childRecordField));
        }
        return new SimpleRecordSchema(newRecordFields, recordSchema.getIdentifier());
    }

    /*
     * This method checks a RecordField for array<null> datatypes recursively and replaces them with the default array<string>
     */
    private RecordField defaultArrayTypes(final RecordField recordField) {
        final DataType dataType = recordField.getDataType();
        final RecordFieldType fieldType = dataType.getFieldType();
        if (fieldType == RecordFieldType.ARRAY) {
            final ArrayDataType arrayDataType = (ArrayDataType) dataType;

            if (arrayDataType.getElementType() == null) {
                return new RecordField(recordField.getFieldName(), RecordFieldType.ARRAY.getArrayDataType(RecordFieldType.STRING.getDataType()),
                        recordField.getDefaultValue(), recordField.getAliases(), recordField.isNullable());
            } else {
                // Iterate over the array element type (using a synthesized temporary RecordField), defaulting any arrays as well
                final RecordField elementRecordField = new RecordField(recordField.getFieldName() + "_element", arrayDataType.getElementType(), recordField.isNullable());
                final RecordField adjustedElementRecordField = defaultArrayTypes(elementRecordField);

                return new RecordField(recordField.getFieldName(), RecordFieldType.ARRAY.getArrayDataType(adjustedElementRecordField.getDataType()),
                        recordField.getDefaultValue(), recordField.getAliases(), recordField.isNullable());
            }
        } else if (fieldType == RecordFieldType.RECORD) {
            final RecordDataType recordDataType = (RecordDataType) dataType;
            final RecordSchema childSchema = recordDataType.getChildSchema();
            final RecordSchema adjustedRecordSchema = defaultArrayTypes(childSchema);

            return new RecordField(recordField.getFieldName(), RecordFieldType.RECORD.getRecordDataType(adjustedRecordSchema), recordField.getDefaultValue(),
                    recordField.getAliases(), recordField.isNullable());
        } else if (fieldType == RecordFieldType.CHOICE) {
            final ChoiceDataType choiceDataType = (ChoiceDataType) dataType;
            final List<DataType> choices = choiceDataType.getPossibleSubTypes();

            // Use a LinkedHashSet to preserve ordering while at the same time ensuring that we don't add duplicates,
            // as resolving null values could cause a duplicate (e.g., if there's a STRING and a NULL, that may become a choice of two STRINGs).
            final Set<DataType> defaulted = new LinkedHashSet<>(choices.size());

            for (final DataType choice : choices) {
                final RecordField choiceRecordField = new RecordField(recordField.getFieldName() + "_choice", choice, recordField.isNullable());
                final RecordField defaultedRecordField = defaultArrayTypes(choiceRecordField);
                defaulted.add(defaultedRecordField.getDataType());
            }

            // If there's only 1 possible sub-type, don't use a CHOICE. Instead, just use that type.
            if (defaulted.size() == 1) {
                return new RecordField(recordField.getFieldName(), defaulted.iterator().next(), recordField.getDefaultValue(), recordField.getAliases(),
                        recordField.isNullable());
            }

            // Create a CHOICE for all of the possible types
            final List<DataType> defaultedTypeList = new ArrayList<>(defaulted);
            return new RecordField(recordField.getFieldName(), RecordFieldType.CHOICE.getChoiceDataType(defaultedTypeList), recordField.getDefaultValue(),
                    recordField.getAliases(), recordField.isNullable());
        }

        return recordField;
    }

    private void inferType(final T value, final FieldTypeInference typeInference) {
        if (isObject(value)) {
            final RecordSchema schema = createSchema(value);
            final DataType fieldDataType = RecordFieldType.RECORD.getRecordDataType(schema);
            typeInference.addPossibleDataType(fieldDataType);
        } else if (isArray(value)) {
            if (isEmptyArray(value)) {
                // At this point we don't know the type of array elements as the array is empty, and it is too early to assume an array of strings. Use null as the
                // element type for now, and call defaultArrayTypes() when all inferences are complete, to ensure that if there are any arrays with inferred element type
                // of null, they default to string for the final schema.
                final DataType arrayDataType = RecordFieldType.ARRAY.getArrayDataType(null);
                typeInference.addPossibleDataType(arrayDataType);
            } else {
                final FieldTypeInference arrayElementTypeInference = new FieldTypeInference();
                forEachRawRecordInArray(value, arrayElement -> inferType(arrayElement, arrayElementTypeInference));

                DataType elementDataType = arrayElementTypeInference.toDataType();
                final DataType arrayDataType = RecordFieldType.ARRAY.getArrayDataType(elementDataType);
                typeInference.addPossibleDataType(arrayDataType);
            }
        } else {
            typeInference.addPossibleDataType(getDataType(value));
        }
    }

    private RecordSchema createSchema(final Map<String, FieldTypeInference> inferences, final String rootElementName) {
        final List<RecordField> recordFields = new ArrayList<>(inferences.size());
        inferences.forEach((fieldName, type) -> recordFields.add(new RecordField(fieldName, type.toDataType())));
        final SimpleRecordSchema schema = new SimpleRecordSchema(recordFields);
        schema.setSchemaName(rootElementName);
        return schema;
    }

    protected RecordSchema createSchema(final T rawRecord) {
        final Map<String, FieldTypeInference> typeMap = new LinkedHashMap<>();
        inferSchema(rawRecord, typeMap);

        final RecordSchema schema = createSchema(typeMap, getRootName(rawRecord));
        return schema;
    }


    protected abstract DataType getDataType(T value);

    protected abstract boolean isObject(T value);

    protected abstract boolean isArray(T value);

    protected abstract boolean isEmptyArray(T value);

    protected abstract void forEachFieldInRecord(T rawRecord, BiConsumer<String, T> fieldConsumer);

    protected abstract void forEachRawRecordInArray(T arrayRecord, Consumer<T> rawRecordConsumer);

    protected abstract String getRootName(T rawRecord);
}
