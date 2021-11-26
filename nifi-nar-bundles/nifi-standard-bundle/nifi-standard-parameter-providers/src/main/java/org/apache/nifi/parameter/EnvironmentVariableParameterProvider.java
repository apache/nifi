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

import org.apache.nifi.annotation.behavior.DynamicProperties;
import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

@Tags({"environment", "variable"})
@CapabilityDescription("Fetches parameters from environment variables")

@DynamicProperties(
    @DynamicProperty(name = "Raw parameter name", value = "Mapped parameter name",
        description = "Maps a raw fetched parameter from the external system to the name of a parameter inside the dataflow")
)
public class EnvironmentVariableParameterProvider extends AbstractParameterProvider implements VerifiableParameterProvider {
    private final Map<String, String> environmentVariables = System.getenv();

    public static final PropertyDescriptor SENSITIVE_PARAMETER_REGEX = new PropertyDescriptor.Builder()
            .name("sensitive-parameter-regex")
            .displayName("Sensitive Parameter Regex")
            .description("A Regular Expression indicating which Environment Variables to include as sensitive parameters.  Any that this pattern " +
                    " are automatically excluded from the non-sensitive parameters.  If not specified, no sensitive parameters will be included.")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .required(false)
            .build();
    public static final PropertyDescriptor NON_SENSITIVE_PARAMETER_REGEX = new PropertyDescriptor.Builder()
            .name("non-sensitive-parameter-regex")
            .displayName("Non-Sensitive Parameter Regex")
            .description("A Regular Expression indicating which Environment Variables to include as non-sensitive parameters.  If the Sensitive Parameter Regex " +
                    "is specified, Environment Variables matching that Regex will be excluded from the non-sensitive parameters.  " +
                    "If not specified, no non-sensitive parameters will be included.")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .required(false)
            .build();

    private List<PropertyDescriptor> properties;

    @Override
    protected void init(final ParameterProviderInitializationContext config) {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(SENSITIVE_PARAMETER_REGEX);
        properties.add(NON_SENSITIVE_PARAMETER_REGEX);

        this.properties = Collections.unmodifiableList(properties);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public List<ProvidedParameterGroup> fetchParameters(final ConfigurationContext context) {
        final Pattern sensitivePattern = context.getProperty(SENSITIVE_PARAMETER_REGEX).isSet()
                ? Pattern.compile(context.getProperty(SENSITIVE_PARAMETER_REGEX).getValue())
                : null;
        final Pattern nonSensitivePattern = context.getProperty(NON_SENSITIVE_PARAMETER_REGEX).isSet()
                ? Pattern.compile(context.getProperty(NON_SENSITIVE_PARAMETER_REGEX).getValue())
                : null;

        final List<Parameter> sensitiveParameters = new ArrayList<>();
        final List<Parameter> nonSensitiveParameters = new ArrayList<>();
        environmentVariables.forEach( (key, value) -> {
            final ParameterDescriptor parameterDescriptor = new ParameterDescriptor.Builder().name(key).build();
            final Parameter parameter = new Parameter(parameterDescriptor, value, null, true);
            boolean sensitiveMatch = false;
            if (sensitivePattern != null && sensitivePattern.matcher(key).matches()) {
                sensitiveParameters.add(parameter);
                sensitiveMatch = true;
            }
            if (nonSensitivePattern != null && nonSensitivePattern.matcher(key).matches()) {
                if (!sensitiveMatch) {
                    nonSensitiveParameters.add(parameter);
                }
            }
        });
        return Arrays.asList(
                new ProvidedParameterGroup(ParameterSensitivity.SENSITIVE, sensitiveParameters),
                new ProvidedParameterGroup(ParameterSensitivity.NON_SENSITIVE, nonSensitiveParameters));
    }

    @Override
    public List<ConfigVerificationResult> verify(final ConfigurationContext context, final ComponentLog verificationLogger) {
        final List<ConfigVerificationResult> results = new ArrayList<>();

        try {
            final List<ProvidedParameterGroup> parameterGroups = fetchParameters(context);
            int sensitiveCount = 0;
            int nonSensitiveCount = 0;
            for (final ProvidedParameterGroup group : parameterGroups) {
                if (group.getGroupKey().getSensitivity() == ParameterSensitivity.SENSITIVE) {
                    sensitiveCount += group.getItems().size();
                }
                if (group.getGroupKey().getSensitivity() == ParameterSensitivity.NON_SENSITIVE) {
                    nonSensitiveCount += group.getItems().size();
                }
            }
            results.add(new ConfigVerificationResult.Builder()
                    .outcome(ConfigVerificationResult.Outcome.SUCCESSFUL)
                    .verificationStepName("Fetch Environment Variables")
                    .explanation(String.format("Fetched %s Environment Variables as sensitive parameters and %s Environment Variables as non-sensitive parameters", sensitiveCount, nonSensitiveCount))
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
}
