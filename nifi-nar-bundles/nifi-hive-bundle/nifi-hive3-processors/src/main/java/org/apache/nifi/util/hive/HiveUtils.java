/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.nifi.util.hive;


import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class HiveUtils {
    private static final Logger LOG = LoggerFactory.getLogger(HiveUtils.class);

    public static final Validator GREATER_THAN_ONE_VALIDATOR = (subject, value, context) -> {
        if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(value)) {
            return new ValidationResult.Builder().subject(subject).input(value).explanation("Expression Language Present").valid(true).build();
        }

        String reason = null;
        try {
            final int intVal = Integer.parseInt(value);

            if (intVal < 2) {
                reason = "value is less than 2";
            }
        } catch (final NumberFormatException e) {
            reason = "value is not a valid integer";
        }

        return new ValidationResult.Builder().subject(subject).input(value).explanation(reason).valid(reason == null).build();
    };

    /**
     * Validates that one or more files exist, as specified in a single property.
     */
    public static Validator createMultipleFilesExistValidator() {
        return (subject, input, context) -> {
            if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
                return new ValidationResult.Builder().subject(subject).input(input).explanation("Expression Language Present").valid(true).build();
            }
            final String[] files = input.split("\\s*,\\s*");
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
        };
    }
}
