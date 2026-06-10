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
package org.apache.nifi.processors.gcp.credentials.factory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.processor.util.JsonValidator;

/**
 * Validates that a property value is a JSON object describing a Google Service Account key, that is a JSON object
 * whose {@code type} field is {@code service_account}. Other Google credential types, such as {@code external_account},
 * are rejected so that external identity credentials are configured through the Workload Identity Federation strategy.
 */
public class ServiceAccountJsonValidator implements Validator {
    public static final ServiceAccountJsonValidator INSTANCE = new ServiceAccountJsonValidator();

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static final String TYPE_FIELD = "type";
    private static final String SERVICE_ACCOUNT_TYPE = "service_account";

    @Override
    public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
        final ValidationResult jsonResult = JsonValidator.INSTANCE.validate(subject, input, context);
        if (!jsonResult.isValid()) {
            return jsonResult;
        }

        if (context.isExpressionLanguageSupported(subject) && context.isExpressionLanguagePresent(input)) {
            return jsonResult;
        }

        final ValidationResult.Builder builder = new ValidationResult.Builder()
                .subject(subject)
                .input(input);

        try {
            final JsonNode rootNode = OBJECT_MAPPER.readTree(input);
            final JsonNode typeNode = rootNode.get(TYPE_FIELD);
            if (typeNode != null && SERVICE_ACCOUNT_TYPE.equals(typeNode.asText())) {
                builder.valid(true);
                builder.explanation("Service Account JSON found");
            } else {
                final String foundType = typeNode == null ? "none" : typeNode.asText();
                builder.valid(false);
                builder.explanation(("Expected a Service Account key with \"type\": \"service_account\" but found type: %s. "
                        + "Use the Workload Identity Federation strategy for external account credentials.").formatted(foundType));
            }
        } catch (final Exception e) {
            builder.valid(false);
            builder.explanation("Service Account JSON validation failed: %s".formatted(e.getMessage()));
        }

        return builder.build();
    }
}
