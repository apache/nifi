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
package org.apache.nifi.dbcp;

import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;

import java.util.Collections;
import java.util.Set;

/**
 * Database Driver Class Validator supports system attribute expressions and evaluates class names against unsupported values
 */
public class DriverClassValidator implements Validator {
    private static final Set<String> UNSUPPORTED_CLASSES = Collections.singleton("org.h2.Driver");

    @Override
    public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
        final ValidationResult.Builder builder = new ValidationResult.Builder().subject(subject).input(input);

        if (input == null || input.isEmpty()) {
            builder.valid(false);
            builder.explanation("Driver Class required");
        } else {
            final String driverClass = context.newPropertyValue(input).evaluateAttributeExpressions().getValue().trim();

            if (isDriverClassUnsupported(driverClass)) {
                builder.valid(false);
                builder.explanation(String.format("Driver Class is listed as unsupported %s", UNSUPPORTED_CLASSES));
            } else {
                builder.valid(true);
                builder.explanation("Driver Class is valid");
            }
        }

        return builder.build();
    }

    private boolean isDriverClassUnsupported(final String driverClass) {
        return UNSUPPORTED_CLASSES.contains(driverClass);
    }
}
