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
 * Validates that the field value matches the JSON Schema {@code const} keyword.
 */
public class ConstValidator implements FieldValidator {
    private final String canonicalValue;

    public ConstValidator(final Object constantValue) {
        this.canonicalValue = EnumValidator.canonicalize(constantValue);
    }

    @Override
    public Collection<ValidationError> validate(final RecordField field, final String fieldPath, final Object value) {
        final String candidate = EnumValidator.canonicalize(value);
        if (!canonicalValue.equals(candidate)) {
            final String explanation = String.format("Value must equal %s", canonicalValue);
            final ValidationError error = ValidatorUtils.createError(fieldPath, value, ValidationErrorType.INVALID_FIELD, explanation);
            return ValidatorUtils.errorCollection(error);
        }

        return ValidatorUtils.errorCollection(null);
    }

    @Override
    public String getDescription() {
        return "Const validator: value=" + canonicalValue;
    }
}
