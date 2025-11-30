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

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Validates a value against a finite set of allowed values as defined by the JSON Schema {@code enum} keyword.
 */
public class EnumValidator implements FieldValidator {
    private final Set<String> allowedValues;
    private final String description;

    public EnumValidator(final Collection<String> allowedValues) {
        this.allowedValues = Set.copyOf(allowedValues);
        this.description = "Enum validator" + allowedValues;
    }

    @Override
    public Collection<ValidationError> validate(final RecordField field, final String fieldPath, final Object value) {
        final String canonicalValue = canonicalize(value);
        if (!allowedValues.contains(canonicalValue)) {
            final String explanation = String.format("Value must be one of %s", allowedValues);
            final ValidationError error = ValidatorUtils.createError(fieldPath, value, ValidationErrorType.INVALID_FIELD, explanation);
            return ValidatorUtils.errorCollection(error);
        }

        return ValidatorUtils.errorCollection(null);
    }

    @Override
    public String getDescription() {
        return description;
    }

    public static Collection<String> canonicalizeAll(final Collection<Object> values) {
        return values.stream().map(EnumValidator::canonicalizeStatic).collect(Collectors.toSet());
    }

    public static String canonicalize(final Object value) {
        return canonicalizeStatic(value);
    }

    private static String canonicalizeStatic(final Object value) {
        if (value == null) {
            return "null";
        }

        final Class<?> valueClass = value.getClass();
        if (valueClass.isArray()) {
            if (value instanceof Object[]) {
                return Arrays.deepToString((Object[]) value);
            }
            if (value instanceof byte[]) {
                return Arrays.toString((byte[]) value);
            }
            if (value instanceof short[]) {
                return Arrays.toString((short[]) value);
            }
            if (value instanceof int[]) {
                return Arrays.toString((int[]) value);
            }
            if (value instanceof long[]) {
                return Arrays.toString((long[]) value);
            }
            if (value instanceof float[]) {
                return Arrays.toString((float[]) value);
            }
            if (value instanceof double[]) {
                return Arrays.toString((double[]) value);
            }
            if (value instanceof char[]) {
                return Arrays.toString((char[]) value);
            }
            if (value instanceof boolean[]) {
                return Arrays.toString((boolean[]) value);
            }
        }

        return String.valueOf(value);
    }
}
