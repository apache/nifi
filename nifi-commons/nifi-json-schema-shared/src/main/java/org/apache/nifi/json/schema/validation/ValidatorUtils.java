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

import org.apache.nifi.serialization.record.validation.DefaultValidationError;
import org.apache.nifi.serialization.record.validation.ValidationError;
import org.apache.nifi.serialization.record.validation.ValidationErrorType;

import java.util.Collection;
import java.util.List;

public final class ValidatorUtils {
    private ValidatorUtils() {
    }

    public static String buildFieldPath(final String basePath, final String fieldName) {
        if (fieldName == null || fieldName.isEmpty()) {
            return basePath == null ? "" : basePath;
        }

        if (basePath == null || basePath.isEmpty()) {
            return "/" + fieldName;
        }

        if (basePath.endsWith("/")) {
            return basePath + fieldName;
        }

        return basePath + "/" + fieldName;
    }

    public static Collection<ValidationError> errorCollection(final ValidationError error) {
        if (error == null) {
            return List.of();
        }
        return List.of(error);
    }

    public static ValidationError createError(final String fieldPath, final Object value, final ValidationErrorType type, final String explanation) {
        return DefaultValidationError.builder()
                .fieldName(fieldPath)
                .inputValue(value)
                .type(type)
                .explanation(explanation)
                .build();
    }
}
