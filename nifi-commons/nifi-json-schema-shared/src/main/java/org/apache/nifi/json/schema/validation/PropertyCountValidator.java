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

import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.validation.RecordValidator;
import org.apache.nifi.serialization.record.validation.ValidationError;
import org.apache.nifi.serialization.record.validation.ValidationErrorType;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Validates {@code minProperties} and {@code maxProperties} constraints for JSON objects.
 */
public class PropertyCountValidator implements RecordValidator {
    private final Integer minProperties;
    private final Integer maxProperties;

    public PropertyCountValidator(final Integer minProperties, final Integer maxProperties) {
        this.minProperties = minProperties;
        this.maxProperties = maxProperties;
    }

    @Override
    public Collection<ValidationError> validate(final Record record, final RecordSchema schema, final String fieldPath) {
        final int propertyCount = record.getRawFieldNames().size();
        final Collection<ValidationError> errors = new ArrayList<>();

        if (minProperties != null && propertyCount < minProperties) {
            final String explanation = String.format("Record must contain at least %d properties", minProperties);
            errors.add(ValidatorUtils.createError(fieldPath, propertyCount, ValidationErrorType.OTHER, explanation));
        }

        if (maxProperties != null && propertyCount > maxProperties) {
            final String explanation = String.format("Record must contain no more than %d properties", maxProperties);
            errors.add(ValidatorUtils.createError(fieldPath, propertyCount, ValidationErrorType.OTHER, explanation));
        }

        return errors;
    }

    @Override
    public String getDescription() {
        final StringBuilder description = new StringBuilder("Property count validator");
        if (minProperties != null) {
            description.append(", minProperties=").append(minProperties);
        }
        if (maxProperties != null) {
            description.append(", maxProperties=").append(maxProperties);
        }
        return description.toString();
    }
}
