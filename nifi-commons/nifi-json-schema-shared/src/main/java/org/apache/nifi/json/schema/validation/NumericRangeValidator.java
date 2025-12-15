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

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;

/**
 * Validates numeric minimum and maximum constraints according to JSON Schema semantics.
 */
public class NumericRangeValidator implements FieldValidator {
    private final BigDecimal minimum;
    private final boolean exclusiveMinimum;
    private final BigDecimal maximum;
    private final boolean exclusiveMaximum;

    public NumericRangeValidator(final BigDecimal minimum, final boolean exclusiveMinimum, final BigDecimal maximum, final boolean exclusiveMaximum) {
        this.minimum = minimum;
        this.exclusiveMinimum = exclusiveMinimum;
        this.maximum = maximum;
        this.exclusiveMaximum = exclusiveMaximum;
    }

    @Override
    public Collection<ValidationError> validate(final RecordField field, final String fieldPath, final Object value) {
        final BigDecimal decimalValue = toBigDecimal(value);
        if (decimalValue == null) {
            return ValidatorUtils.errorCollection(null);
        }

        if (minimum != null) {
            final int comparison = decimalValue.compareTo(minimum);
            if (comparison < 0 || (exclusiveMinimum && comparison == 0)) {
                final String explanation = exclusiveMinimum
                        ? String.format("Value must be greater than %s", minimum)
                        : String.format("Value must be greater than or equal to %s", minimum);
                final ValidationError error = ValidatorUtils.createError(fieldPath, value, ValidationErrorType.INVALID_FIELD, explanation);
                return ValidatorUtils.errorCollection(error);
            }
        }

        if (maximum != null) {
            final int comparison = decimalValue.compareTo(maximum);
            if (comparison > 0 || (exclusiveMaximum && comparison == 0)) {
                final String explanation = exclusiveMaximum
                        ? String.format("Value must be less than %s", maximum)
                        : String.format("Value must be less than or equal to %s", maximum);
                final ValidationError error = ValidatorUtils.createError(fieldPath, value, ValidationErrorType.INVALID_FIELD, explanation);
                return ValidatorUtils.errorCollection(error);
            }
        }

        return ValidatorUtils.errorCollection(null);
    }

    @Override
    public String getDescription() {
        final StringBuilder description = new StringBuilder("Numeric range validator");
        if (minimum != null) {
            description.append(exclusiveMinimum ? ", exclusive min=" : ", min=").append(minimum);
        }
        if (maximum != null) {
            description.append(exclusiveMaximum ? ", exclusive max=" : ", max=").append(maximum);
        }
        return description.toString();
    }

    private BigDecimal toBigDecimal(final Object value) {
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
        if (value instanceof BigInteger) {
            return new BigDecimal((BigInteger) value);
        }
        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
            return BigDecimal.valueOf(((Number) value).longValue());
        }
        if (value instanceof Float || value instanceof Double) {
            return BigDecimal.valueOf(((Number) value).doubleValue());
        }

        return null;
    }
}
