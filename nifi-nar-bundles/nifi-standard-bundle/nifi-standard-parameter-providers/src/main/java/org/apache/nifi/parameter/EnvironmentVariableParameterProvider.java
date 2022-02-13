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
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

@Tags({"environment", "variable"})
@CapabilityDescription("Fetches parameters from environment variables")
public class EnvironmentVariableParameterProvider extends AbstractParameterProvider implements VerifiableParameterProvider {
    private final Map<String, String> environmentVariables = System.getenv();

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

        this.properties = Collections.unmodifiableList(properties);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public List<ParameterGroup> fetchParameters(final ConfigurationContext context) {
        final String parameterGroupName = context.getProperty(PARAMETER_GROUP_NAME).getValue();

        final List<Parameter> parameters = new ArrayList<>();
        environmentVariables.forEach( (key, value) -> {
            final ParameterDescriptor parameterDescriptor = new ParameterDescriptor.Builder().name(key).build();
            parameters.add(new Parameter(parameterDescriptor, value, null, true));
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
}
