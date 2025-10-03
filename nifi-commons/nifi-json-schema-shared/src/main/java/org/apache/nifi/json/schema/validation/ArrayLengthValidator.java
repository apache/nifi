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

import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.validation.FieldValidator;
import org.apache.nifi.serialization.record.validation.ValidationError;
import org.apache.nifi.serialization.record.validation.ValidationErrorType;

import java.util.Collection;

/**
 * Validates {@code minItems} and {@code maxItems} constraints for array fields.
 */
public class ArrayLengthValidator implements FieldValidator {
    private final Integer minItems;
    private final Integer maxItems;

    public ArrayLengthValidator(final Integer minItems, final Integer maxItems) {
        this.minItems = minItems;
        this.maxItems = maxItems;
    }

    @Override
    public Collection<ValidationError> validate(final RecordField field, final String fieldPath, final Object value) {
        final int size = determineSize(value);
        if (size < 0) {
            return ValidatorUtils.errorCollection(null);
        }

        if (minItems != null && size < minItems) {
            final String explanation = String.format("Array must contain at least %d items", minItems);
            final ValidationError error = ValidatorUtils.createError(fieldPath, value, ValidationErrorType.INVALID_FIELD, explanation);
            return ValidatorUtils.errorCollection(error);
        }

        if (maxItems != null && size > maxItems) {
            final String explanation = String.format("Array must contain no more than %d items", maxItems);
            final ValidationError error = ValidatorUtils.createError(fieldPath, value, ValidationErrorType.INVALID_FIELD, explanation);
            return ValidatorUtils.errorCollection(error);
        }

        return ValidatorUtils.errorCollection(null);
    }

    @Override
    public String getDescription() {
        final StringBuilder description = new StringBuilder("Array length validator");
        if (minItems != null) {
            description.append(", minItems=").append(minItems);
        }
        if (maxItems != null) {
            description.append(", maxItems=").append(maxItems);
        }
        return description.toString();
    }

    private int determineSize(final Object value) {
        if (value == null) {
            return -1;
        }

        if (value instanceof Object[]) {
            return ((Object[]) value).length;
        }

        if (value instanceof java.util.Collection<?>) {
            return ((java.util.Collection<?>) value).size();
        }

        return -1;
    }
}
