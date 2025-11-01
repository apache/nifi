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
 * Validates the JSON Schema {@code multipleOf} constraint for numeric fields.
 */
public class MultipleOfValidator implements FieldValidator {
    private final BigDecimal divisor;

    public MultipleOfValidator(final BigDecimal divisor) {
        if (divisor == null || BigDecimal.ZERO.compareTo(divisor) == 0) {
            throw new IllegalArgumentException("multipleOf divisor must be a non-zero number");
        }
        this.divisor = divisor.stripTrailingZeros();
    }

    @Override
    public Collection<ValidationError> validate(final RecordField field, final String fieldPath, final Object value) {
        final BigDecimal decimalValue = toBigDecimal(value);
        if (decimalValue == null) {
            return ValidatorUtils.errorCollection(null);
        }

        final BigDecimal remainder = decimalValue.remainder(divisor);
        if (remainder.compareTo(BigDecimal.ZERO) != 0) {
            final String explanation = String.format("Value must be a multiple of %s", divisor);
            final ValidationError error = ValidatorUtils.createError(fieldPath, value, ValidationErrorType.INVALID_FIELD, explanation);
            return ValidatorUtils.errorCollection(error);
        }

        return ValidatorUtils.errorCollection(null);
    }

    @Override
    public String getDescription() {
        return "Multiple-of validator: divisor=" + divisor;
    }

    private BigDecimal toBigDecimal(final Object value) {
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
        if (value instanceof BigInteger) {
            return new BigDecimal((BigInteger) value);
        }
        if (value instanceof Number) {
            return BigDecimal.valueOf(((Number) value).doubleValue());
        }
        return null;
    }
}
