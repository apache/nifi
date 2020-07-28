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

package org.apache.nifi.schema.validation;

import java.util.Objects;
import java.util.Optional;

import org.apache.nifi.serialization.record.validation.ValidationError;
import org.apache.nifi.serialization.record.validation.ValidationErrorType;

public class StandardValidationError implements ValidationError {
    private final Optional<String> fieldName;
    private final Optional<Object> inputValue;
    private final String explanation;
    private final ValidationErrorType type;


    public StandardValidationError(final String fieldName, final Object value, final ValidationErrorType type, final String explanation) {
        this.fieldName = Optional.ofNullable(fieldName);
        this.inputValue = Optional.ofNullable(value);
        this.type = type;
        this.explanation = explanation;
    }

    public StandardValidationError(final String fieldName, final ValidationErrorType type, final String explanation) {
        this.fieldName = Optional.ofNullable(fieldName);
        this.inputValue = Optional.empty();
        this.type = type;
        this.explanation = Objects.requireNonNull(explanation);
    }

    public StandardValidationError(final ValidationErrorType type, final String explanation) {
        this.fieldName = Optional.empty();
        this.inputValue = Optional.empty();
        this.type = type;
        this.explanation = Objects.requireNonNull(explanation);
    }

    @Override
    public ValidationErrorType getType() {
        return type;
    }

    @Override
    public Optional<String> getFieldName() {
        return fieldName;
    }

    @Override
    public Optional<Object> getInputValue() {
        return inputValue;
    }

    @Override
    public String getExplanation() {
        return explanation;
    }

    @Override
    public String toString() {
        if (fieldName.isPresent()) {
            if (inputValue.isPresent()) {
                final Object input = inputValue.get();
                if (input instanceof Object[]) {
                    final StringBuilder sb = new StringBuilder("[");
                    final Object[] array = (Object[]) input;
                    for (int i=0; i < array.length; i++) {

                        final Object arrayValue = array[i];
                        if (arrayValue instanceof String) {
                            sb.append('"').append(array[i]).append('"');
                        } else {
                            sb.append(array[i]);
                        }

                        if (i < array.length - 1) {
                            sb.append(", ");
                        }
                    }
                    sb.append("]");

                    return sb.toString() + " is not a valid value for " + fieldName.get() + ": " + explanation;
                } else {
                    return inputValue.get() + " is not a valid value for " + fieldName.get() + ": " + explanation;
                }
            } else {
                return fieldName.get() + " is invalid due to: " + explanation;
            }
        }

        return explanation;
    }

    @Override
    public int hashCode() {
        return 31 + 17 * fieldName.hashCode() + 17 * inputValue.hashCode() + 17 * explanation.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof ValidationError)) {
            return false;
        }

        final ValidationError other = (ValidationError) obj;
        return getFieldName().equals(other.getFieldName()) && getInputValue().equals(other.getInputValue()) && getExplanation().equals(other.getExplanation());
    }
}
