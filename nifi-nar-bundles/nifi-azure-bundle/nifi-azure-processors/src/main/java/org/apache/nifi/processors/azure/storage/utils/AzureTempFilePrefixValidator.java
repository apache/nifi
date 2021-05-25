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
package org.apache.nifi.processors.azure.storage.utils;

import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;

import java.util.regex.Pattern;

public class AzureTempFilePrefixValidator implements Validator {
    private static final Pattern TEMP_FILE_PREFIX_PATTERN = Pattern.compile("^[^\"\\\\/:|<>*?]*$");

    @Override
    public ValidationResult validate(String subject, String input, ValidationContext context) {
        PropertyValue prefixProperty = context.newPropertyValue(input);
        final String prefix;
        if (prefixProperty.isExpressionLanguagePresent()) {
            try {
                prefix = prefixProperty.evaluateAttributeExpressions().getValue();
            } catch (final Exception e) {
                return new ValidationResult.Builder()
                        .subject(subject)
                        .input(input)
                        .valid(false)
                        .explanation("Failed to evaluate the Attribute Expression Language due to " + e.toString())
                        .build();
            }
        } else {
            prefix = prefixProperty.getValue();
        }

        if (TEMP_FILE_PREFIX_PATTERN.matcher(prefix).matches()) {
            return new ValidationResult.Builder()
                    .subject(subject)
                    .input(prefix)
                    .valid(true)
                    .build();
        } else {
            return new ValidationResult.Builder()
                    .subject(subject)
                    .input(prefix)
                    .valid(false)
                    .explanation("Prefix cannot contain \" / \\ < > : | * ? characters.")
                    .build();
        }
    }
}
