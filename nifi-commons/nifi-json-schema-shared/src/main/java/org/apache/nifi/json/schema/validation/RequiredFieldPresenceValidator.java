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
import java.util.Set;

/**
 * Ensures that required JSON properties are actually present in the record, regardless of their value.
 */
public class RequiredFieldPresenceValidator implements RecordValidator {
    private final Set<String> requiredFields;

    public RequiredFieldPresenceValidator(final Set<String> requiredFields) {
        this.requiredFields = Set.copyOf(requiredFields);
    }

    @Override
    public Collection<ValidationError> validate(final Record record, final RecordSchema schema, final String fieldPath) {
        final Collection<ValidationError> errors = new ArrayList<>();
        final Set<String> rawFieldNames = record.getRawFieldNames();

        for (final String requiredField : requiredFields) {
            if (!rawFieldNames.contains(requiredField)) {
                final String fullPath = ValidatorUtils.buildFieldPath(fieldPath, requiredField);
                errors.add(ValidatorUtils.createError(fullPath, null, ValidationErrorType.MISSING_FIELD, "Field is required"));
            }
        }

        return errors;
    }

    @Override
    public String getDescription() {
        return "Required field presence validator: " + requiredFields;
    }
}
