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

package org.apache.nifi.components.connector;

import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.connector.components.ParameterContextFacade;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterLookup;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class StandardConnectorValidationContext implements ConnectorValidationContext {
    private final DescribedValueProvider allowableValueProvider;
    private final Map<String, StepConfiguration> stepConfigurations;
    private final ParameterLookup parameterLookup;

    public StandardConnectorValidationContext(final ConnectorConfiguration connectorConfiguration, final DescribedValueProvider allowableValueProvider,
                final ParameterContextFacade parameterContextFacade) {

        this.allowableValueProvider = allowableValueProvider;

        stepConfigurations = new HashMap<>();
        for (final NamedStepConfiguration stepConfiguration : connectorConfiguration.getNamedStepConfigurations()) {
            stepConfigurations.put(stepConfiguration.stepName(), stepConfiguration.configuration());
        }

        parameterLookup = new ParameterLookupBridge(parameterContextFacade);
    }

    @Override
    public ValidationContext createValidationContext(final String stepName, final String propertyGroupName) {
        final StepConfiguration stepConfig = stepConfigurations.get(stepName);
        final Map<String, ConnectorValueReference> stepProperties = stepConfig != null ? stepConfig.getPropertyValues() : Collections.emptyMap();

        final Map<String, String> stringValues = new HashMap<>();
        for (final Map.Entry<String, ConnectorValueReference> entry : stepProperties.entrySet()) {
            final ConnectorValueReference valueRef = entry.getValue();

            if (valueRef instanceof StringLiteralValue stringLiteral) {
                stringValues.put(entry.getKey(), stringLiteral.getValue());
            } else {
                stringValues.put(entry.getKey(), null);
            }
        }

        return new ConnectorValidationContextBridge(stringValues, parameterLookup);
    }

    @Override
    public List<DescribedValue> fetchAllowableValues(final String stepName, final String propertyName) {
        return allowableValueProvider.fetchAllowableValues(stepName, propertyName);
    }

    private static class ParameterLookupBridge implements ParameterLookup {
        private final ParameterContextFacade parameterContextFacade;

        public ParameterLookupBridge(final ParameterContextFacade parameterContextFacade) {
            this.parameterContextFacade = parameterContextFacade;
        }

        @Override
        public Optional<Parameter> getParameter(final String parameterName) {
            final String parameterValue = parameterContextFacade.getValue(parameterName);
            if (parameterValue == null) {
                return Optional.empty();
            }

            final Parameter parameter = new Parameter.Builder()
                .name(parameterName)
                .provided(false)
                .sensitive(parameterContextFacade.isSensitive(parameterName))
                .value(parameterValue)
                .build();
            return Optional.of(parameter);
        }

        @Override
        public boolean isEmpty() {
            return parameterContextFacade.getDefinedParameterNames().isEmpty();
        }

        @Override
        public long getVersion() {
            return 0;
        }
    }
}
