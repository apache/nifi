/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.json.schema.validation;

import org.apache.nifi.schema.validation.SchemaValidationContext;
import org.apache.nifi.schema.validation.StandardSchemaValidator;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.RecordDataType;
import org.apache.nifi.serialization.record.validation.DefaultValidationError;
import org.apache.nifi.serialization.record.validation.RecordValidator;
import org.apache.nifi.serialization.record.validation.SchemaValidationResult;
import org.apache.nifi.serialization.record.validation.ValidationError;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Applies JSON Schema {@code patternProperties} validation rules to records.
 */
public class PatternPropertiesValidator implements RecordValidator {
    private final List<PatternPropertyDefinition> patternDefinitions;

    public PatternPropertiesValidator(final Collection<PatternPropertyDefinition> patternDefinitions) {
        Objects.requireNonNull(patternDefinitions, "Pattern definitions required");
        this.patternDefinitions = List.copyOf(patternDefinitions);
    }

    @Override
    public Collection<ValidationError> validate(final Record record, final RecordSchema schema, final String fieldPath) {
        final List<ValidationError> validationErrors = new ArrayList<>();
        final Set<String> explicitFieldNames = Set.copyOf(schema.getFieldNames());

        for (final PatternPropertyDefinition definition : patternDefinitions) {
            final Pattern pattern = definition.pattern();

            for (final String rawFieldName : record.getRawFieldNames()) {
                if (explicitFieldNames.contains(rawFieldName)) {
                    continue;
                }

                if (pattern.matcher(rawFieldName).find()) {
                    final Object value = record.getValue(rawFieldName);
                    final Collection<ValidationError> errors = definition.validate(fieldPath, rawFieldName, value);
                    if (!errors.isEmpty()) {
                        validationErrors.addAll(errors);
                    }
                }
            }
        }

        return validationErrors;
    }

    @Override
    public String getDescription() {
        return "Pattern properties validator";
    }

    public record PatternPropertyDefinition(Pattern pattern, RecordField templateField) {
        public PatternPropertyDefinition {
            Objects.requireNonNull(pattern, "Pattern is required");
            Objects.requireNonNull(templateField, "Template field is required");
        }

        public Collection<ValidationError> validate(final String basePath, final String actualFieldName, final Object value) {
            final List<ValidationError> collectedErrors = new ArrayList<>();

            final RecordField actualField = new RecordField(actualFieldName,
                    templateField.getDataType(),
                    templateField.getDefaultValue(),
                    templateField.getAliases(),
                    templateField.isNullable(),
                    templateField.getFieldValidators());

            final SimpleRecordSchema singleFieldSchema = new SimpleRecordSchema(List.of(actualField));
            final SchemaValidationContext context = new SchemaValidationContext(singleFieldSchema, true, false);
            final StandardSchemaValidator validator = new StandardSchemaValidator(context);

            Object normalizedValue = value;
            if (actualField.getDataType().getFieldType() == RecordFieldType.RECORD && value instanceof Map<?, ?> mapValue) {
                final RecordDataType recordDataType = (RecordDataType) actualField.getDataType();
                final RecordSchema childSchema = recordDataType.getChildSchema();
                final Map<String, Object> normalizedMap = new java.util.HashMap<>();
                for (final Map.Entry<?, ?> entry : mapValue.entrySet()) {
                    normalizedMap.put(String.valueOf(entry.getKey()), entry.getValue());
                }
                normalizedValue = new MapRecord(childSchema, normalizedMap);
            }

            final MapRecord singleRecord = new MapRecord(singleFieldSchema, Map.of(actualFieldName, normalizedValue));
            final SchemaValidationResult result = validator.validate(singleRecord);

            if (result.isValid()) {
                return collectedErrors;
            }

            for (final ValidationError error : result.getValidationErrors()) {
                final Optional<String> originalFieldPath = error.getFieldName();
                final String normalizedPath = originalFieldPath
                        .map(name -> name.startsWith("/") ? name.substring(1) : name)
                        .orElse(actualFieldName);
                final String combinedPath = ValidatorUtils.buildFieldPath(basePath, normalizedPath);

                final DefaultValidationError rewritten = DefaultValidationError.builder()
                        .fieldName(combinedPath)
                        .inputValue(error.getInputValue().orElse(null))
                        .type(error.getType())
                        .explanation(error.getExplanation())
                        .build();
                collectedErrors.add(rewritten);
            }

            return collectedErrors;
        }
    }
}
