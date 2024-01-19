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
package org.apache.nifi.processors.cipher.age;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.resource.ResourceReference;
import org.apache.nifi.components.resource.ResourceReferences;
import org.apache.nifi.components.resource.ResourceType;
import org.apache.nifi.components.resource.StandardResourceReferences;
import org.apache.nifi.components.resource.Utf8TextResource;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

/**
 * Component Property Validator for age-encryption X25519 keys encoded using Bech32
 */
public class AgeKeyValidator implements Validator {
    private static final String FILE_RESOURCE_EXPLANATION = "File resource validation passed";

    private static final String RESOURCE_EXCEPTION = "Read failed: %s";

    private static final String NOT_FOUND_EXPLANATION = "Encoded keys not found";

    private static final String INVALID_EXPLANATION = "Invalid keys found [%d]";

    private static final String VALID_EXPLANATION = "Valid keys found";

    private static final String EXPLANATION_SEPARATOR = " and ";

    private final AgeKeyIndicator ageKeyIndicator;
    /**
     * Key Validator with prefix for initial line filtering and pattern for subsequent matching
     *
     * @param ageKeyIndicator Key Indicator
     */
    public AgeKeyValidator(final AgeKeyIndicator ageKeyIndicator) {
        this.ageKeyIndicator = ageKeyIndicator;
    }

    /**
     * Validate property reads one or more lines from a newline-delimited string and requires at least one valid key
     *
     * @param subject Property to be validated
     * @param input Property Value to be validated
     * @param context Validation Context for property resolution
     * @return Validation Result based on finding at least one valid key
     */
    @Override
    public ValidationResult validate(final String subject, final String input, final ValidationContext context) {
        final PropertyValue propertyValue = getPropertyValue(subject, context);
        ResourceReferences resources = propertyValue.asResources();

        if (resources == null) {
            final ResourceReference resourceReference = new Utf8TextResource(input);
            resources = new StandardResourceReferences(Collections.singletonList(resourceReference));
        }

        final ValidationResult.Builder builder = new ValidationResult.Builder();
        builder.subject(subject);

        final Set<ValidationResult> results = resources.asList().stream().map(this::validateResource).collect(Collectors.toSet());
        final String invalidExplanation = results.stream()
                .filter(Predicate.not(ValidationResult::isValid))
                .map(ValidationResult::getExplanation)
                .collect(Collectors.joining(EXPLANATION_SEPARATOR));

        if (invalidExplanation.isEmpty()) {
            builder.explanation(VALID_EXPLANATION).valid(true);
        } else {
            builder.explanation(invalidExplanation).valid(false);
        }

        return builder.build();
    }

    private PropertyValue getPropertyValue(final String subject, final ValidationContext context) {
        final Optional<PropertyDescriptor> propertyFound = context.getProperties()
                .keySet()
                .stream()
                .filter(s -> s.getName().contentEquals(subject))
                .findFirst();

        final String message = String.format("Property [%s] not found", subject);
        final PropertyDescriptor propertyDescriptor = propertyFound.orElseThrow(() -> new IllegalArgumentException(message));
        return context.getProperty(propertyDescriptor);
    }

    private ValidationResult validateResource(final ResourceReference resource) {
        final ValidationResult.Builder builder = new ValidationResult.Builder();

        final ResourceType resourceType = resource.getResourceType();
        if (ResourceType.TEXT == resourceType) {
            try (final BufferedReader reader = new BufferedReader(new InputStreamReader(resource.read()))) {
                final Set<String> prefixedLines = reader.lines()
                        .filter(line -> line.startsWith(ageKeyIndicator.getPrefix()))
                        .collect(Collectors.toSet());

                final long invalid = prefixedLines.stream()
                        .map(ageKeyIndicator.getPattern()::matcher)
                        .filter(Predicate.not(Matcher::matches))
                        .count();

                if (prefixedLines.isEmpty()) {
                    builder.explanation(NOT_FOUND_EXPLANATION).valid(false);
                } else if (invalid == 0) {
                    builder.explanation(VALID_EXPLANATION).valid(true);
                } else {
                    final String explanation = String.format(INVALID_EXPLANATION, invalid);
                    builder.explanation(explanation).valid(false);
                }
            } catch (final Exception e) {
                final String explanation = String.format(RESOURCE_EXCEPTION, e.getMessage());
                builder.explanation(explanation).valid(false);
            }
        } else {
            builder.explanation(FILE_RESOURCE_EXPLANATION).valid(true);
        }

        return builder.build();
    }
}
