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

import org.apache.nifi.annotation.behavior.Restricted;
import org.apache.nifi.annotation.behavior.Restriction;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.components.RequiredPermission;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Tags({"environment", "variable"})
@CapabilityDescription("Fetches parameters from environment variables")

@Restricted(
        restrictions = {
                @Restriction(
                        requiredPermission = RequiredPermission.ACCESS_ENVIRONMENT_CREDENTIALS,
                        explanation = "Provides operator the ability to read environment variables, which may contain environment credentials.")
        }
)
public class EnvironmentVariableParameterProvider extends AbstractParameterProvider implements VerifiableParameterProvider {
    private final Map<String, String> environmentVariables = new ConcurrentHashMap<>();

    private static final AllowableValue INCLUDE_ALL_STRATEGY = new AllowableValue("include-all", "Include All",
            "All Environment Variables will be included");
    private static final AllowableValue COMMA_SEPARATED_STRATEGY = new AllowableValue("comma-separated", "Comma-Separated",
            "List comma-separated Environment Variable names to include");
    private static final AllowableValue REGEX_STRATEGY = new AllowableValue("regex", "Regular Expression",
            "Include Environment Variable names that match a Regular Expression");

    private enum InclusionStrategyValue {
        INCLUDE_ALL(EnvironmentVariableParameterProvider.INCLUDE_ALL_STRATEGY.getValue(), IncludeAllEnvironmentVariableInclusionStrategy::new),
        COMMA_SEPARATED(EnvironmentVariableParameterProvider.COMMA_SEPARATED_STRATEGY.getValue(), CommaSeparatedEnvironmentVariableInclusionStrategy::new),
        REGEX_STRATEGY(EnvironmentVariableParameterProvider.REGEX_STRATEGY.getValue(), RegexEnvironmentVariableInclusionStrategy::new);

        private String name;
        private Function<String, EnvironmentVariableInclusionStrategy> factoryMethod;

        InclusionStrategyValue(final String name, final Function<String, EnvironmentVariableInclusionStrategy> factoryMethod) {
            this.name = name;
            this.factoryMethod = factoryMethod;
        }

        EnvironmentVariableInclusionStrategy getStrategy(final String inclusionText) {
            return factoryMethod.apply(inclusionText);
        }

        static InclusionStrategyValue fromValue(final String value) {
            if (value == null) {
                throw new IllegalArgumentException("Inclusion strategy value is required");
            }
            for (final InclusionStrategyValue v : values()) {
                if (v.name.equals(value)) {
                    return v;
                }
            }
            throw new IllegalArgumentException("Unrecognized inclusion strategy value");
        }
    }

    public static final PropertyDescriptor ENVIRONMENT_VARIABLE_INCLUSION_STRATEGY = new PropertyDescriptor.Builder()
            .name("environment-variable-inclusion-strategy")
            .displayName("Environment Variable Inclusion Strategy")
            .description("Indicates how Environment Variables should be included")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .allowableValues(new AllowableValue[] {INCLUDE_ALL_STRATEGY, COMMA_SEPARATED_STRATEGY, REGEX_STRATEGY})
            .defaultValue(INCLUDE_ALL_STRATEGY.getValue())
            .required(true)
            .build();
    public static final PropertyDescriptor INCLUDE_ENVIRONMENT_VARIABLES = new PropertyDescriptor.Builder()
            .name("include-environment-variables")
            .displayName("Include Environment Variables")
            .description("Specifies environment variable names that should be included from the fetched environment variables.")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .required(false)
            .dependsOn(ENVIRONMENT_VARIABLE_INCLUSION_STRATEGY, REGEX_STRATEGY, COMMA_SEPARATED_STRATEGY)
            .build();
    public static final PropertyDescriptor PARAMETER_GROUP_NAME = new PropertyDescriptor.Builder()
            .name("parameter-group-name")
            .displayName("Parameter Group Name")
            .description("The name of the parameter group that will be fetched.  This indicates the name of the Parameter Context that may receive " +
                    "the fetched parameters.")
            .addValidator(StandardValidators.createRegexMatchingValidator(Pattern.compile("^[a-zA-Z0-9_. -]+$")))
            .defaultValue("Environment Variables")
            .required(true)
            .build();

    private List<PropertyDescriptor> properties;

    @Override
    protected void init(final ParameterProviderInitializationContext config) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(PARAMETER_GROUP_NAME);
        properties.add(ENVIRONMENT_VARIABLE_INCLUSION_STRATEGY);
        properties.add(INCLUDE_ENVIRONMENT_VARIABLES);

        this.properties = Collections.unmodifiableList(properties);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public List<ParameterGroup> fetchParameters(final ConfigurationContext context) {
        environmentVariables.clear();
        environmentVariables.putAll(System.getenv());

        final EnvironmentVariableInclusionStrategy inclusionStrategy = getStrategy(context);
        final String parameterGroupName = context.getProperty(PARAMETER_GROUP_NAME).getValue();

        final List<Parameter> parameters = new ArrayList<>();
        environmentVariables
                .forEach( (key, value) -> {
                    if (inclusionStrategy.include(key)) {
                        parameters.add(new Parameter.Builder()
                            .name(key)
                            .value(value)
                            .provided(true)
                            .build());
                    }
        });
        return Collections.singletonList(new ParameterGroup(parameterGroupName, parameters));
    }

    @Override
    public List<ConfigVerificationResult> verify(final ConfigurationContext context, final ComponentLog verificationLogger) {
        final List<ConfigVerificationResult> results = new ArrayList<>();

        try {
            final List<ParameterGroup> parameterGroups = fetchParameters(context);
            final int parameterCount = parameterGroups.get(0).getParameters().size();
            results.add(new ConfigVerificationResult.Builder()
                    .outcome(ConfigVerificationResult.Outcome.SUCCESSFUL)
                    .verificationStepName("Fetch Environment Variables")
                    .explanation(String.format("Fetched %s Environment Variables as parameters", parameterCount))
                    .build());
        } catch (final IllegalArgumentException e) {
            verificationLogger.error("Failed to fetch parameters", e);
            results.add(new ConfigVerificationResult.Builder()
                    .outcome(ConfigVerificationResult.Outcome.FAILED)
                    .verificationStepName("Fetch Environment Variables")
                    .explanation("Failed to fetch parameters: " + e.getMessage())
                    .build());
        }
        return results;
    }

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        final List<ValidationResult> results = new ArrayList<>(super.customValidate(validationContext));

        final PropertyValue includeEnvironmentVariables = validationContext.getProperty(INCLUDE_ENVIRONMENT_VARIABLES);
        if (includeEnvironmentVariables.isSet()) {
            final String inclusionStrategy = validationContext.getProperty(ENVIRONMENT_VARIABLE_INCLUSION_STRATEGY).getValue();
            if (REGEX_STRATEGY.getValue().equals(inclusionStrategy)) {
                results.add(StandardValidators.REGULAR_EXPRESSION_VALIDATOR.validate(INCLUDE_ENVIRONMENT_VARIABLES.getDisplayName(),
                        includeEnvironmentVariables.getValue(), validationContext));
            } else if (COMMA_SEPARATED_STRATEGY.getValue().equals(inclusionStrategy)) {
                results.add(StandardValidators.NON_EMPTY_VALIDATOR.validate(INCLUDE_ENVIRONMENT_VARIABLES.getDisplayName(),
                        includeEnvironmentVariables.getValue(), validationContext));
            }
        }
        return results;
    }

    private EnvironmentVariableInclusionStrategy getStrategy(final ConfigurationContext context) {
        return InclusionStrategyValue
                .fromValue(context.getProperty(ENVIRONMENT_VARIABLE_INCLUSION_STRATEGY).getValue())
                .getStrategy(context.getProperty(INCLUDE_ENVIRONMENT_VARIABLES).getValue());
    }

    interface EnvironmentVariableInclusionStrategy {
        boolean include(String variableName);
    }

    static class IncludeAllEnvironmentVariableInclusionStrategy implements EnvironmentVariableInclusionStrategy {

        IncludeAllEnvironmentVariableInclusionStrategy(final String inclusionText) {

        }

        @Override
        public boolean include(final String variableName) {
            return true;
        }
    }

    static class RegexEnvironmentVariableInclusionStrategy implements EnvironmentVariableInclusionStrategy {

        private final Pattern pattern;

        RegexEnvironmentVariableInclusionStrategy(final String regex) {
            if (regex == null) {
                throw new IllegalArgumentException("Regular Expression is required");
            }
            this.pattern = Pattern.compile(regex);
        }
        @Override
        public boolean include(final String variableName) {
            return pattern.matcher(variableName).matches();
        }

    }

    static class CommaSeparatedEnvironmentVariableInclusionStrategy implements EnvironmentVariableInclusionStrategy {

        private final Set<String> includedNames;

        CommaSeparatedEnvironmentVariableInclusionStrategy(final String includedNames) {
            if (includedNames == null) {
                throw new IllegalArgumentException("Comma-separated list of included names is required");
            }
            this.includedNames = new HashSet<>(Arrays.asList(includedNames.split(",")).stream().map(String::trim).collect(Collectors.toList()));
        }
        @Override
        public boolean include(final String variableName) {
            return includedNames.contains(variableName);
        }

    }
}
