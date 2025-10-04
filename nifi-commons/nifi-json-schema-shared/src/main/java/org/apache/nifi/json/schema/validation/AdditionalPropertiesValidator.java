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

import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.validation.RecordValidator;
import org.apache.nifi.serialization.record.validation.ValidationError;
import org.apache.nifi.serialization.record.validation.ValidationErrorType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Enforces the JSON Schema {@code additionalProperties: false} constraint.
 */
public class AdditionalPropertiesValidator implements RecordValidator {
    private final Set<String> allowedFields;
    private final List<Pattern> allowedPatterns;

    public AdditionalPropertiesValidator(final Set<String> allowedFields) {
        this(allowedFields, List.of());
    }

    public AdditionalPropertiesValidator(final Set<String> allowedFields, final Collection<Pattern> allowedPatterns) {
        this.allowedFields = Set.copyOf(allowedFields);
        this.allowedPatterns = List.copyOf(allowedPatterns);
    }

    @Override
    public Collection<ValidationError> validate(final Record record, final RecordSchema schema, final String fieldPath) {
        final Collection<ValidationError> errors = new ArrayList<>();
        for (final String rawFieldName : record.getRawFieldNames()) {
            if (!allowedFields.contains(rawFieldName) && allowedPatterns.stream().noneMatch(pattern -> pattern.matcher(rawFieldName).find())) {
                final String fullFieldPath = ValidatorUtils.buildFieldPath(fieldPath, rawFieldName);
                final ValidationError error = ValidatorUtils.createError(fullFieldPath, record.getValue(rawFieldName), ValidationErrorType.EXTRA_FIELD,
                        "Field is not defined in schema");
                errors.add(error);
            }
        }
        return errors;
    }

    @Override
    public String getDescription() {
        return "Additional properties disallowed";
    }
}
