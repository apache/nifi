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
import java.util.regex.Pattern;

/**
 * Validates values against the JSON Schema {@code pattern} expression.
 */
public class PatternValidator implements FieldValidator {
    private final Pattern pattern;

    public PatternValidator(final String regex) {
        this.pattern = Pattern.compile(regex);
    }

    @Override
    public Collection<ValidationError> validate(final RecordField field, final String fieldPath, final Object value) {
        if (!(value instanceof CharSequence)) {
            return ValidatorUtils.errorCollection(null);
        }

        final CharSequence text = (CharSequence) value;
        if (!pattern.matcher(text).matches()) {
            final String explanation = String.format("Value does not match pattern '%s'", pattern.pattern());
            final ValidationError error = ValidatorUtils.createError(fieldPath, value, ValidationErrorType.INVALID_FIELD, explanation);
            return ValidatorUtils.errorCollection(error);
        }

        return ValidatorUtils.errorCollection(null);
    }

    @Override
    public String getDescription() {
        return "Pattern validator: pattern='" + pattern.pattern() + "'";
    }
}
