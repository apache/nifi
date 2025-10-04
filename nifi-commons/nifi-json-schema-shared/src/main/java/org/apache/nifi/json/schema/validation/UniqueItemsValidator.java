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
import java.util.HashSet;
import java.util.Set;

/**
 * Validates the JSON Schema {@code uniqueItems} constraint for arrays.
 */
public class UniqueItemsValidator implements FieldValidator {

    @Override
    public Collection<ValidationError> validate(final RecordField field, final String fieldPath, final Object value) {
        final Object[] array = toArray(value);
        if (array == null) {
            return ValidatorUtils.errorCollection(null);
        }

        final Set<String> seen = new HashSet<>();
        for (final Object element : array) {
            final String canonical = EnumValidator.canonicalize(element);
            if (!seen.add(canonical)) {
                final String explanation = "Array elements must be unique";
                final ValidationError error = ValidatorUtils.createError(fieldPath, value, ValidationErrorType.INVALID_FIELD, explanation);
                return ValidatorUtils.errorCollection(error);
            }
        }

        return ValidatorUtils.errorCollection(null);
    }

    @Override
    public String getDescription() {
        return "Unique items validator";
    }

    private Object[] toArray(final Object value) {
        if (value instanceof Object[]) {
            return (Object[]) value;
        }
        if (value instanceof java.util.Collection<?>) {
            return ((java.util.Collection<?>) value).toArray();
        }
        return null;
    }
}
