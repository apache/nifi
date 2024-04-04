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
 * Database Connection URL Validator supports system attribute expressions and evaluates URL formatting
 */
public class ConnectionUrlValidator implements Validator {
    private static final Set<String> UNSUPPORTED_SCHEMES = Collections.singleton("jdbc:h2");

    @Override
    public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
        final ValidationResult.Builder builder = new ValidationResult.Builder().subject(subject).input(input);

        if (input == null || input.isEmpty()) {
            builder.valid(false);
            builder.explanation("Connection URL required");
        } else {
            final String url = context.newPropertyValue(input).evaluateAttributeExpressions().getValue().trim();

            if (isUrlUnsupported(url)) {
                builder.valid(false);
                builder.explanation(String.format("Connection URL contains an unsupported scheme %s", UNSUPPORTED_SCHEMES));
            } else {
                builder.valid(true);
                builder.explanation("Connection URL is valid");
            }
        }

        return builder.build();
    }

    private boolean isUrlUnsupported(final String url) {
        boolean unsupported = false;

        for (final String unsupportedScheme : UNSUPPORTED_SCHEMES) {
            if (url.contains(unsupportedScheme)) {
                unsupported = true;
                break;
            }
        }

        return unsupported;
    }
}
