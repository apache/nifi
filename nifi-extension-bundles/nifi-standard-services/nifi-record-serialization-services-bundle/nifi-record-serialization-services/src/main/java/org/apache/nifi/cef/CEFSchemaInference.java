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
package org.apache.nifi.cef;

import com.fluenda.parcefone.event.CEFHandlingException;
import com.fluenda.parcefone.event.CommonEvent;
import org.apache.nifi.schema.inference.FieldTypeInference;
import org.apache.nifi.schema.inference.RecordSource;
import org.apache.nifi.schema.inference.SchemaInferenceEngine;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

final class CEFSchemaInference implements SchemaInferenceEngine<CommonEvent> {
    private final boolean includeExtensions;
    private final boolean includeCustomExtensions;
    private final CEFCustomExtensionTypeResolver typeResolver;
    private final String rawMessageField;
    private final String invalidField;

    CEFSchemaInference(
            final boolean includeExtensions,
            final boolean includeCustomExtensions,
            final CEFCustomExtensionTypeResolver typeResolver,
            final String rawMessageField,
            final String invalidField) {
        this.includeExtensions = includeExtensions;
        this.includeCustomExtensions = includeCustomExtensions;
        this.typeResolver = typeResolver;
        this.rawMessageField = rawMessageField;
        this.invalidField = invalidField;
    }

    @Override
    public RecordSchema inferSchema(final RecordSource<CommonEvent> recordSource) throws IOException {
        try {
            CommonEvent event;

            // Header fields are always part of the schema
            final List<RecordField> fields = new ArrayList<>(CEFSchemaUtil.getHeaderFields());
            final Set<String> extensionFields = new HashSet<>();
            final Map<String, FieldTypeInference> customExtensionTypes = new LinkedHashMap<>();

            // Even though we need to process this only when we want extensions, the records are being read in order to check if the are valid
            while ((event = recordSource.next()) != null) {
                if (includeExtensions) {
                    for (final Map.Entry<String, Object> field : event.getExtension(true, includeCustomExtensions).entrySet()) {
                        final Optional<RecordField> extensionField = getExtensionField(field.getKey());

                        if (extensionField.isPresent()) {
                            // We add extension field to the schema only at the first time
                            if (!extensionFields.contains(field.getKey())) {
                                extensionFields.add(field.getKey());
                                fields.add(extensionField.get());
                            }
                        } else if (includeCustomExtensions) {
                            // CommonEvent will always return custom extensions as String values
                            final FieldTypeInference typeInference = customExtensionTypes.computeIfAbsent(field.getKey(), key -> new FieldTypeInference());
                            typeInference.addPossibleDataType(typeResolver.resolve((String) field.getValue()));
                        }
                    }
                }
            }

            final List<RecordField> customExtensionFields = new ArrayList<>(customExtensionTypes.size());
            customExtensionTypes.forEach((fieldName, type) -> customExtensionFields.add(new RecordField(fieldName, type.toDataType(), true)));
            fields.addAll(customExtensionFields);

            if (rawMessageField != null) {
                fields.add(new RecordField(rawMessageField, RecordFieldType.STRING.getDataType()));
            }

            if (invalidField != null) {
                fields.add(new RecordField(invalidField, RecordFieldType.STRING.getDataType()));
            }

            return new SimpleRecordSchema(fields);
        } catch (final CEFHandlingException e) {
            throw new IOException(e);
        }
    }

    private Optional<RecordField> getExtensionField(final String fieldName) {
        for (final Map.Entry<Set<String>, DataType> entry : CEFSchemaUtil.getExtensionTypeMapping().entrySet()) {
            if (entry.getKey().contains(fieldName)) {
                return Optional.of(new RecordField(fieldName, entry.getValue()));
            }
        }

        return Optional.empty();
    }


}
