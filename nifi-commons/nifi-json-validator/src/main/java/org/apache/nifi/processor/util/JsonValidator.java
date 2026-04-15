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

package org.apache.nifi.processor.util;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;

/**
 * Property Validator expecting JSON Objects or Arrays with support for Expression Language when configured
 */
public class JsonValidator implements Validator {
    public static final JsonValidator INSTANCE = new JsonValidator();

    private static final JsonFactory JSON_FACTORY = new JsonFactory();

    @Override
    public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
        final ValidationResult.Builder builder = new ValidationResult.Builder()
                .subject(subject)
                .input(input);

        if (input == null || input.isBlank()) {
            builder.valid(false);
            builder.explanation("JSON input not found in null or blank string");
        } else if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
            builder.valid(true);
            builder.explanation("Expression Language found");
        } else {
            try (JsonParser parser = JSON_FACTORY.createParser(input)) {
                final JsonToken firstToken = parser.nextToken();
                if (JsonToken.START_OBJECT == firstToken || JsonToken.START_ARRAY == firstToken) {
                    parser.skipChildren();

                    // Read Next Token expecting end of object or array
                    final JsonToken lastToken = parser.nextToken();
                    if (lastToken == null) {
                        builder.valid(true);
                        builder.explanation("JSON object or array found");
                    } else {
                        builder.valid(false);
                        builder.explanation("Trailing content found after JSON object or array");
                    }
                } else {
                    builder.valid(false);
                    builder.explanation("JSON object or array not found");
                }

            } catch (final Exception e) {
                builder.valid(false);
                builder.explanation("JSON input validation failed: %s".formatted(e.getMessage()));
            }
        }

        return builder.build();
    }
}
