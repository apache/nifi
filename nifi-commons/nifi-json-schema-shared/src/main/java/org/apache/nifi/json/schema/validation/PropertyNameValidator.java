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

package org.apache.nifi.json.schema.validation;

import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.validation.RecordValidator;
import org.apache.nifi.serialization.record.validation.ValidationError;
import org.apache.nifi.serialization.record.validation.ValidationErrorType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Validates each property name of a record against JSON Schema {@code propertyNames} constraints.
 */
public class PropertyNameValidator implements RecordValidator {
    private final Integer minLength;
    private final Integer maxLength;
    private final Pattern pattern;
    private final Set<String> allowedNames;
    private final String constValue;
    private final String description;

    public PropertyNameValidator(final Integer minLength, final Integer maxLength, final Pattern pattern,
                                 final Set<String> allowedNames, final String constValue, final String description) {
        this.minLength = minLength;
        this.maxLength = maxLength;
        this.pattern = pattern;
        this.allowedNames = allowedNames == null ? Collections.emptySet() : Set.copyOf(allowedNames);
        this.constValue = constValue;
        this.description = description;
    }

    @Override
    public Collection<ValidationError> validate(final Record record, final RecordSchema schema, final String fieldPath) {
        if (record == null) {
            return Collections.emptyList();
        }

        final Collection<ValidationError> errors = new ArrayList<>();
        for (final String propertyName : record.getRawFieldNames()) {
            final ValidationError error = validatePropertyName(propertyName, fieldPath);
            if (error != null) {
                errors.add(error);
            }
        }

        return errors;
    }

    @Override
    public String getDescription() {
        return description;
    }

    private ValidationError validatePropertyName(final String propertyName, final String fieldPath) {
        Objects.requireNonNull(propertyName, "Property name cannot be null");

        if (constValue != null && !constValue.equals(propertyName)) {
            return ValidatorUtils.createError(ValidatorUtils.buildFieldPath(fieldPath, propertyName), propertyName,
                    ValidationErrorType.INVALID_FIELD, "Property name must equal '" + constValue + "'");
        }

        if (!allowedNames.isEmpty() && !allowedNames.contains(propertyName)) {
            return ValidatorUtils.createError(ValidatorUtils.buildFieldPath(fieldPath, propertyName), propertyName,
                    ValidationErrorType.INVALID_FIELD, "Property name is not one of the allowed values");
        }

        final int length = propertyName.length();
        if (minLength != null && length < minLength) {
            return ValidatorUtils.createError(ValidatorUtils.buildFieldPath(fieldPath, propertyName), propertyName,
                    ValidationErrorType.INVALID_FIELD, "Property name length " + length + " is less than minimum " + minLength);
        }

        if (maxLength != null && length > maxLength) {
            return ValidatorUtils.createError(ValidatorUtils.buildFieldPath(fieldPath, propertyName), propertyName,
                    ValidationErrorType.INVALID_FIELD, "Property name length " + length + " exceeds maximum " + maxLength);
        }

        if (pattern != null && !pattern.matcher(propertyName).matches()) {
            return ValidatorUtils.createError(ValidatorUtils.buildFieldPath(fieldPath, propertyName), propertyName,
                    ValidationErrorType.INVALID_FIELD, "Property name does not match required pattern");
        }

        return null;
    }
}
