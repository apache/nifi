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

public class RegexClusterResolver implements ClusterResolver {

    public static final String PATTERN_PROPERTY_PREFIX = "hostnamePattern.";
    public static final String PATTERN_PROPERTY_PREFIX_DESC = "White space delimited (including new line) Regular Expressions" +
            " to resolve a 'Cluster Name' from a hostname or IP address of a transit URI of NiFi provenance record.";
    private Map<String, Set<Pattern>> clusterNamePatterns;

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
                (clusterNamePatterns, patterns) -> {},
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

        clusterNamePatterns = new HashMap<>();
        consumeConfigurations(context.getAllProperties(),
                (clusterName, patterns) -> clusterNamePatterns.put(clusterName, patterns),
                null);

    }

    private void consumeConfigurations(final Map<String, String> allProperties,
                                               final BiConsumer<String, Set<Pattern>> consumer,
                                               final BiConsumer<Map.Entry<String, String>, RuntimeException> errorHandler) {
        allProperties.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(PATTERN_PROPERTY_PREFIX))
                .forEach(entry -> {
                    final String clusterName;
                    final Set<Pattern> patterns;
                    try {
                        clusterName = entry.getKey().substring(PATTERN_PROPERTY_PREFIX.length());
                        final String[] regexsArray = entry.getValue().split("\\s");
                        final List<String> regexs = Arrays.stream(regexsArray)
                                .map(String::trim).filter(s -> !s.isEmpty()).collect(Collectors.toList());
                        patterns = parseClusterNamePatterns(clusterName, regexs);
                        consumer.accept(clusterName, patterns);
                    } catch (RuntimeException e) {
                        if (errorHandler != null) {
                            errorHandler.accept(entry, e);
                        } else {
                            throw e;
                        }
                    }
                });
    }

    private Set<Pattern> parseClusterNamePatterns(final String clusterName, List<String> regexs) {
        if (clusterName == null || clusterName.isEmpty()) {
            throw new IllegalArgumentException("Empty cluster name is not allowed.");
        }

        if (regexs.size() == 0) {
            throw new IllegalArgumentException(
                    String.format("At least one cluster name pattern is required, [%s].", clusterName));
        }

        return regexs.stream().map(Pattern::compile).collect(Collectors.toSet());
    }

    @Override
    public String fromHostNames(String ... hostNames) {
        for (Map.Entry<String, Set<Pattern>> entry : clusterNamePatterns.entrySet()) {
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
