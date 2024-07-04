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
package org.apache.nifi.cef;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;

import java.util.Locale;

public class ValidateLocale implements Validator {
    @Override
    public ValidationResult validate(String subject, String input, ValidationContext context) {
        if (null == input || input.isEmpty()) {
            return new ValidationResult.Builder().subject(subject).input(input).valid(false)
                    .explanation(subject + " cannot be empty").build();
        }
        final Locale testLocale = Locale.forLanguageTag(input);
        final Locale[] availableLocales = Locale.getAvailableLocales();

        // Check if the provided Locale is valid by checking against the first value of the array (i.e. "null" locale)
        if (availableLocales[0].equals(testLocale)) {
            // Locale matches the "null" locale so it is treated as invalid
            return new ValidationResult.Builder().subject(subject).input(input).valid(false)
                    .explanation(input + " is not a valid locale format.").build();
        } else {
            return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();

        }

    }
}