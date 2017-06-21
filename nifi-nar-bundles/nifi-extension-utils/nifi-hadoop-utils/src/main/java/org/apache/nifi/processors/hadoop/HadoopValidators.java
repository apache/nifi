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
package org.apache.nifi.processors.hadoop;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;

import java.io.File;

/**
 * Validators for Hadoop related processors.
 */
public interface HadoopValidators {

    /*
    * Validates that one or more files exist, as specified in a single property.
    */
    Validator ONE_OR_MORE_FILE_EXISTS_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(String subject, String input, ValidationContext context) {
            if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
                return new ValidationResult.Builder().subject(subject).input(input).explanation("Expression Language Present").valid(true).build();
            }

            final String[] files = input.split(",");
            for (String filename : files) {
                try {
                    final File file = new File(filename.trim());
                    final boolean valid = file.exists() && file.isFile();
                    if (!valid) {
                        final String message = "File " + file + " does not exist or is not a file";
                        return new ValidationResult.Builder().subject(subject).input(input).valid(false).explanation(message).build();
                    }
                } catch (SecurityException e) {
                    final String message = "Unable to access " + filename + " due to " + e.getMessage();
                    return new ValidationResult.Builder().subject(subject).input(input).valid(false).explanation(message).build();
                }
            }
            return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
        }

    };

    /*
     * Validates that a property is a valid umask, i.e. a short octal number that is not negative.
     */
    Validator UMASK_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String value, final ValidationContext context) {
            String reason = null;
            try {
                final short shortVal = Short.parseShort(value, 8);
                if (shortVal < 0) {
                    reason = "octal umask [" + value + "] cannot be negative";
                } else if (shortVal > 511) {
                    // HDFS umask has 9 bits: rwxrwxrwx ; the sticky bit cannot be umasked
                    reason = "octal umask [" + value + "] is not a valid umask";
                }
            } catch (final NumberFormatException e) {
                reason = "[" + value + "] is not a valid short octal number";
            }
            return new ValidationResult.Builder().subject(subject).input(value).explanation(reason).valid(reason == null)
                    .build();
        }
    };

    /*
     * Validates that a property is a valid short number greater than 0.
     */
    Validator POSITIVE_SHORT_VALIDATOR = new Validator() {
        @Override
        public ValidationResult validate(final String subject, final String value, final ValidationContext context) {
            String reason = null;
            try {
                final short shortVal = Short.parseShort(value);
                if (shortVal <= 0) {
                    reason = "short integer must be greater than zero";
                }
            } catch (final NumberFormatException e) {
                reason = "[" + value + "] is not a valid short integer";
            }
            return new ValidationResult.Builder().subject(subject).input(value).explanation(reason).valid(reason == null)
                    .build();
        }
    };

}
