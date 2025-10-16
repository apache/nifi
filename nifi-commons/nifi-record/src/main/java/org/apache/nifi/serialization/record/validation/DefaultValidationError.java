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
package org.apache.nifi.serialization.record.validation;

import java.util.Objects;
import java.util.Optional;

/**
 * Basic implementation of {@link ValidationError} that can be used by validators in modules that
 * cannot depend on higher level utility classes. Instances are immutable and thread-safe.
 */
public class DefaultValidationError implements ValidationError {
    private final Optional<String> fieldName;
    private final Optional<Object> inputValue;
    private final String explanation;
    private final ValidationErrorType type;

    private DefaultValidationError(final Builder builder) {
        this.fieldName = Optional.ofNullable(builder.fieldName);
        this.inputValue = Optional.ofNullable(builder.inputValue);
        this.explanation = Objects.requireNonNull(builder.explanation, "Explanation is required");
        this.type = Objects.requireNonNull(builder.type, "Validation error type is required");
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
    public ValidationErrorType getType() {
        return type;
    }

    /**
     * Creates a builder for constructing immutable {@link DefaultValidationError} instances.
     *
     * @return builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    public static final class Builder {
        private String fieldName;
        private Object inputValue;
        private String explanation;
        private ValidationErrorType type = ValidationErrorType.INVALID_FIELD;

        private Builder() {
        }

        public Builder fieldName(final String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        public Builder inputValue(final Object inputValue) {
            this.inputValue = inputValue;
            return this;
        }

        public Builder explanation(final String explanation) {
            this.explanation = explanation;
            return this;
        }

        public Builder type(final ValidationErrorType type) {
            this.type = type;
            return this;
        }

        public DefaultValidationError build() {
            return new DefaultValidationError(this);
        }
    }
}
