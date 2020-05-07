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
package org.apache.nifi.atlas.resolver;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class RegexNamespaceResolver implements NamespaceResolver {

    public static final String PATTERN_PROPERTY_PREFIX = "hostnamePattern.";
    public static final String PATTERN_PROPERTY_PREFIX_DESC = "White space delimited (including new line) Regular Expressions" +
            " to resolve a namespace from a hostname or IP address of a transit URI of NiFi provenance record.";
    private Map<String, Set<Pattern>> namespacePatterns;

    @Override
    public PropertyDescriptor getSupportedDynamicPropertyDescriptor(String propertyDescriptorName) {
        if (propertyDescriptorName.startsWith(PATTERN_PROPERTY_PREFIX)) {
            return new PropertyDescriptor
                    .Builder().name(propertyDescriptorName)
                    .description(PATTERN_PROPERTY_PREFIX_DESC)
                    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                    .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
                    .dynamic(true)
                    .sensitive(false)
                    .build();
        }
        return null;
    }

    @Override
    public Collection<ValidationResult> validate(ValidationContext validationContext) {
        final List<ValidationResult> validationResults = new ArrayList<>();
        consumeConfigurations(validationContext.getAllProperties(),
                (namespacePatterns, patterns) -> {},
                (entry, e) -> {
                    final ValidationResult result = new ValidationResult.Builder()
                            .subject(entry.getKey())
                            .input(entry.getValue())
                            .explanation(e.getMessage())
                            .valid(false)
                            .build();
                    validationResults.add(result);
                });
        return validationResults;
    }

    @Override
    public void configure(PropertyContext context) {

        namespacePatterns = new HashMap<>();
        consumeConfigurations(context.getAllProperties(),
                (namespace, patterns) -> namespacePatterns.put(namespace, patterns),
                null);

    }

    private void consumeConfigurations(final Map<String, String> allProperties,
                                               final BiConsumer<String, Set<Pattern>> consumer,
                                               final BiConsumer<Map.Entry<String, String>, RuntimeException> errorHandler) {
        allProperties.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(PATTERN_PROPERTY_PREFIX))
                .forEach(entry -> {
                    final String namespace;
                    final Set<Pattern> patterns;
                    try {
                        namespace = entry.getKey().substring(PATTERN_PROPERTY_PREFIX.length());
                        final String[] regexsArray = entry.getValue().split("\\s");
                        final List<String> regexs = Arrays.stream(regexsArray)
                                .map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toList());
                        patterns = parseNamespacePatterns(namespace, regexs);
                        consumer.accept(namespace, patterns);
                    } catch (RuntimeException e) {
                        if (errorHandler != null) {
                            errorHandler.accept(entry, e);
                        } else {
                            throw e;
                        }
                    }
                });
    }

    private Set<Pattern> parseNamespacePatterns(final String namespace, List<String> regexs) {
        if (namespace == null || namespace.isEmpty()) {
            throw new IllegalArgumentException("Empty namespace is not allowed.");
        }

        if (regexs.size() == 0) {
            throw new IllegalArgumentException(
                    String.format("At least one namespace pattern is required, [%s].", namespace));
        }

        return regexs.stream().map(Pattern::compile).collect(Collectors.toSet());
    }

    @Override
    public String fromHostNames(String ... hostNames) {
        for (Map.Entry<String, Set<Pattern>> entry : namespacePatterns.entrySet()) {
            for (Pattern pattern : entry.getValue()) {
                for (String hostname : hostNames) {
                    if (pattern.matcher(hostname).matches()) {
                        return entry.getKey();
                    }
                }
            }
        }
        return null;
    }



}
