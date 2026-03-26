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
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.components.connector.util.VersionedFlowUtils;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.List;
import java.util.Map;

/**
 * A test connector that creates a flow with an OnPropertyModifiedTracker processor.
 * The processor's Configured Number property is bound to a parameter, allowing this connector
 * to test that onPropertyModified is called when a Connector's applyUpdate changes a parameter value.
 */
public class OnPropertyModifiedConnector extends AbstractConnector {

    private static final String PARAMETER_NAME = "CONFIGURED_NUMBER";

    static final ConnectorPropertyDescriptor NUMBER_VALUE = new ConnectorPropertyDescriptor.Builder()
            .name("Number Value")
            .description("The number value to set for the Configured Number parameter")
            .type(PropertyType.STRING)
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .required(true)
            .build();

    private static final ConnectorPropertyGroup PROPERTY_GROUP = new ConnectorPropertyGroup.Builder()
            .name("Configuration")
            .description("Configuration properties for the OnPropertyModified test")
            .addProperty(NUMBER_VALUE)
            .build();

    private static final ConfigurationStep CONFIG_STEP = new ConfigurationStep.Builder()
            .name("Configuration")
            .description("Configure the number value for testing onPropertyModified")
            .propertyGroups(List.of(PROPERTY_GROUP))
            .build();

    @Override
    public VersionedExternalFlow getInitialFlow() {
        return VersionedFlowUtils.loadFlowFromResource("flows/on-property-modified-tracker.json");
    }

    @Override
    public List<ConfigurationStep> getConfigurationSteps() {
        return List.of(CONFIG_STEP);
    }

    @Override
    protected void onStepConfigured(final String stepName, final FlowContext workingContext) throws FlowUpdateException {
        final VersionedExternalFlow versionedExternalFlow = getInitialFlow();
        final String number = workingContext.getConfigurationContext().getProperty(CONFIG_STEP, NUMBER_VALUE).getValue();
        VersionedFlowUtils.setParameterValue(versionedExternalFlow, PARAMETER_NAME, number);
        getInitializationContext().updateFlow(workingContext, versionedExternalFlow);
    }

    @Override
    public void applyUpdate(final FlowContext workingContext, final FlowContext activeContext) throws FlowUpdateException {
        final VersionedExternalFlow versionedExternalFlow = getInitialFlow();
        final String number = workingContext.getConfigurationContext().getProperty(CONFIG_STEP, NUMBER_VALUE).getValue();
        VersionedFlowUtils.setParameterValue(versionedExternalFlow, PARAMETER_NAME, number);
        getInitializationContext().updateFlow(activeContext, versionedExternalFlow);
    }

    @Override
    public List<ConfigVerificationResult> verifyConfigurationStep(final String stepName, final Map<String, String> overrides, final FlowContext workingContext) {
        return List.of();
    }
}
