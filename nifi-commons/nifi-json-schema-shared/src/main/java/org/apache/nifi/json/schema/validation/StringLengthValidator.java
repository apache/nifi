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
 * Validates {@code minLength} and {@code maxLength} constraints for string fields.
 */
public class StringLengthValidator implements FieldValidator {
    private final Integer minLength;
    private final Integer maxLength;

    public StringLengthValidator(final Integer minLength, final Integer maxLength) {
        this.minLength = minLength;
        this.maxLength = maxLength;
    }

    @Override
    public Collection<ValidationError> validate(final RecordField field, final String fieldPath, final Object value) {
        if (!(value instanceof CharSequence)) {
            return ValidatorUtils.errorCollection(null);
        }

        final int length = ((CharSequence) value).length();

        if (minLength != null && length < minLength) {
            final String explanation = String.format("String length must be at least %d", minLength);
            final ValidationError error = ValidatorUtils.createError(fieldPath, value, ValidationErrorType.INVALID_FIELD, explanation);
            return ValidatorUtils.errorCollection(error);
        }

        if (maxLength != null && length > maxLength) {
            final String explanation = String.format("String length must be at most %d", maxLength);
            final ValidationError error = ValidatorUtils.createError(fieldPath, value, ValidationErrorType.INVALID_FIELD, explanation);
            return ValidatorUtils.errorCollection(error);
        }

        return ValidatorUtils.errorCollection(null);
    }

    @Override
    public String getDescription() {
        final StringBuilder description = new StringBuilder("String length validator");
        if (minLength != null) {
            description.append(", minLength=").append(minLength);
        }
        if (maxLength != null) {
            description.append(", maxLength=").append(maxLength);
        }
        return description.toString();
    }
}
