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
package org.apache.nifi.processors.jwt;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.File;

public class CustomValidators {

    public static final Validator DIRECTORY_HAS_PUBLIC_KEYS_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
            // allow expression language if present
            if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
                return new ValidationResult.Builder().subject(subject).input(input)
                        .explanation("Expression Language Present").valid(true).build();
            }
            // allow empty
            final ValidationResult nonEmptyValidatorResult = StandardValidators.NON_EMPTY_VALIDATOR.validate(subject,
                    input, context);
            if (!nonEmptyValidatorResult.isValid()) {
                return new ValidationResult.Builder().subject(subject).input(input)
                        .explanation("property is empty").valid(true).build();
            }
            // if not empty, ensure it is a valid path and is a directory
            final ValidationResult pathValidatorResult = StandardValidators.FILE_EXISTS_VALIDATOR.validate(subject,
                    input,
                    context);
            if (!pathValidatorResult.isValid()) {
                return pathValidatorResult;
            }
            // path is directory
            final File directory = new File(input);
            if (!directory.isDirectory()) {
                return new ValidationResult.Builder().subject(subject).input(input)
                        .explanation(subject + " must be a directory").valid(false).build();
            }
            return new ValidationResult.Builder().subject(subject).input(input)
                    .explanation("Valid public key directory").valid(true).build();
        }
    };
}