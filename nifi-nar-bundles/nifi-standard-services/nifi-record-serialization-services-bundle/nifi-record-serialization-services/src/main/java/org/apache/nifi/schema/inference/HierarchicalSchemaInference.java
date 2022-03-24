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

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
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

        return createSchema(typeMap, rootElementName);
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
            final FieldTypeInference arrayElementTypeInference = new FieldTypeInference();
            forEachRawRecordInArray(value, arrayElement -> inferType(arrayElement, arrayElementTypeInference));

            final DataType elementDataType = arrayElementTypeInference.toDataType();
            final DataType arrayDataType = RecordFieldType.ARRAY.getArrayDataType(elementDataType);
            typeInference.addPossibleDataType(arrayDataType);
        } else {
            typeInference.addPossibleDataType(getDataType(value));
        }
    }

    private void inferType(final T value, final FieldTypeInference typeInference) {
        if (isObject(value)) {
            final RecordSchema schema = createSchema(value);
            final DataType fieldDataType = RecordFieldType.RECORD.getRecordDataType(schema);
            typeInference.addPossibleDataType(fieldDataType);
        } else if (isArray(value)) {
            final FieldTypeInference arrayElementTypeInference = new FieldTypeInference();
            forEachRawRecordInArray(value, arrayElement -> inferType(arrayElement, arrayElementTypeInference));

            final DataType elementDataType = arrayElementTypeInference.toDataType();
            final DataType arrayDataType = RecordFieldType.ARRAY.getArrayDataType(elementDataType);
            typeInference.addPossibleDataType(arrayDataType);
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

    protected abstract void forEachFieldInRecord(T rawRecord, BiConsumer<String, T> fieldConsumer);

    protected abstract void forEachRawRecordInArray(T arrayRecord, Consumer<T> rawRecordConsumer);

    protected abstract String getRootName(T rawRecord);
}
