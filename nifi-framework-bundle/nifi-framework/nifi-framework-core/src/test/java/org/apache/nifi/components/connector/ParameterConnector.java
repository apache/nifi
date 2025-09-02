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

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.components.connector.components.ParameterValue;
import org.apache.nifi.components.connector.util.VersionedFlowUtils;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.List;
import java.util.Map;

public class ParameterConnector extends AbstractConnector {
    private volatile boolean initialized = false;

    static final ConnectorPropertyDescriptor TEXT_PROPERTY = new ConnectorPropertyDescriptor.Builder()
        .name("Text")
        .description("The text to write to FlowFiles")
        .type(PropertyType.STRING)
        .addValidator(Validator.VALID)
        .required(true)
        .defaultValue("Hello World")
        .build();

    static final ConnectorPropertyDescriptor SLEEP_DURATION = new ConnectorPropertyDescriptor.Builder()
        .name("Sleep Duration")
        .description("The duration to sleep when the Sleep Processor is stopped")
        .type(PropertyType.STRING)
        .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
        .required(true)
        .defaultValue("1 sec")
        .build();

    private static final ConnectorPropertyGroup TEXT_GROUP = new ConnectorPropertyGroup.Builder()
        .name("Text Settings")
        .description("Settings for the text to write to FlowFiles")
        .addProperty(TEXT_PROPERTY)
        .addProperty(SLEEP_DURATION)
        .build();

    private static final ConfigurationStep TEXT_STEP = new ConfigurationStep.Builder()
        .name("Text Configuration")
        .description("Configure the text to be written to FlowFiles")
        .propertyGroups(List.of(TEXT_GROUP))
        .build();

    @Override
    protected void init() {
        initialized = true;
    }

    public boolean isInitialized() {
        return initialized;
    }

    @Override
    public VersionedExternalFlow getInitialFlow() {
        return VersionedFlowUtils.loadFlowFromResource("flows/generate-and-log-with-parameter.json");
    }

    @Override
    public List<ConfigurationStep> getConfigurationSteps() {
        return List.of(TEXT_STEP);
    }

    @Override
    public void applyUpdate(final FlowContext workingContext, final FlowContext activeContext) {
        try {
            updateTextParameter(workingContext, activeContext);
        } catch (final FlowUpdateException e) {
            getLogger().error("Failed to update parameters", e);
            throw new RuntimeException("Failed to update parameters", e);
        }
    }

    @Override
    public void onStepConfigured(final String stepName, final FlowContext workingContext) {
    }

    @Override
    public void prepareForUpdate(final FlowContext workingContext, final FlowContext activeContext) {
    }

    @Override
    public void abortUpdate(final FlowContext workingContext, final Throwable throwable) {
    }

    @Override
    public List<ConfigVerificationResult> verifyConfigurationStep(final String stepName, final Map<String, String> overrides, final FlowContext workingContext) {
        return List.of();
    }

    private void updateTextParameter(final FlowContext workingContext, final FlowContext activeContext) throws FlowUpdateException {
        final ConnectorConfigurationContext configContext = workingContext.getConfigurationContext();
        final String textValue = configContext.getProperty(TEXT_STEP, TEXT_PROPERTY).getValue();

        // Update the "Text" parameter with the configured property value
        final ParameterValue textParameter = new ParameterValue.Builder()
            .name("Text")
            .value(textValue)
            .sensitive(false)
            .build();

        final ParameterValue sleepDurationParameter = new ParameterValue.Builder()
            .name("Sleep Duration")
            .value(configContext.getProperty(TEXT_STEP, SLEEP_DURATION).getValue())
            .sensitive(false)
            .build();

        final List<ParameterValue> parameterValues = List.of(textParameter, sleepDurationParameter);
        activeContext.getParameterContext().updateParameters(parameterValues);
    }
}
