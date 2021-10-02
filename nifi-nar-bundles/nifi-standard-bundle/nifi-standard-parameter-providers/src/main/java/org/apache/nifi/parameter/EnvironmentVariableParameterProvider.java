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
package org.apache.nifi.parameter;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Tags({"environment", "variable"})
@CapabilityDescription("Fetches parameters from environment variables")
public class EnvironmentVariableParameterProvider extends AbstractParameterProvider {
    private final Map<String, String> environmentVariables = System.getenv();

    public static final PropertyDescriptor INCLUDE_REGEX = new PropertyDescriptor.Builder()
            .name("include-regex")
            .displayName("Include Regex")
            .description("A Regular Expression indicating which Environment Variables to include as parameters.")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .required(true)
            .defaultValue(".*")
            .build();
    public static final PropertyDescriptor EXCLUDE_REGEX = new PropertyDescriptor.Builder()
            .name("exclude-regex")
            .displayName("Exclude Regex")
            .description("A Regular Expression indicating which Environment Variables to exclude as parameters.")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .required(false)
            .build();

    private List<PropertyDescriptor> properties;

    @Override
    protected void init(final ParameterProviderInitializationContext config) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(INCLUDE_REGEX);
        properties.add(EXCLUDE_REGEX);

        this.properties = Collections.unmodifiableList(properties);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public List<Parameter> fetchParameters(final ConfigurationContext context) {
        final Pattern includePattern = Pattern.compile(context.getProperty(INCLUDE_REGEX).getValue());
        final Pattern excludePattern = context.getProperty(EXCLUDE_REGEX).isSet()
                ? Pattern.compile(context.getProperty(EXCLUDE_REGEX).getValue())
                : null;

        return environmentVariables.entrySet().stream()
                .filter(entry -> includePattern.matcher(entry.getKey()).matches())
                .filter(entry -> excludePattern == null || !excludePattern.matcher(entry.getKey()).matches())
                .map(entry -> {
                    final ParameterDescriptor parameterDescriptor = new ParameterDescriptor.Builder()
                            .name(entry.getKey())
                            .build();
                    return new Parameter(parameterDescriptor, entry.getValue(), null, true);
                })
                .collect(Collectors.toList());
    }
}
