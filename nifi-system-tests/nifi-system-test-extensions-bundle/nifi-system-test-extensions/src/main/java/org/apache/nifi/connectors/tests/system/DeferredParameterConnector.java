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

package org.apache.nifi.connectors.tests.system;

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.connector.AbstractConnector;
import org.apache.nifi.components.connector.BundleCompatibility;
import org.apache.nifi.components.connector.ConfigurationStep;
import org.apache.nifi.components.connector.ConnectorPropertyDescriptor;
import org.apache.nifi.components.connector.ConnectorPropertyGroup;
import org.apache.nifi.components.connector.FlowUpdateException;
import org.apache.nifi.components.connector.PropertyType;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.components.connector.components.ParameterValue;
import org.apache.nifi.components.connector.util.VersionedFlowUtils;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.List;
import java.util.Map;

/**
 * A test connector that resolves value-derived flow state onto its managed Parameter Context during
 * {@link #applyUpdate(FlowContext, FlowContext)} while leaving that value blank in the side-effect-free
 * {@link #getActiveFlow(FlowContext)}.
 *
 * <p>The managed flow contains a single {@code WriteToFile} processor whose required {@code Filename} property
 * references the Parameter {@code #{resolved_value}}. {@code getActiveFlow} declares that Parameter as blank, so a
 * restore of the authoritative flow alone leaves the processor INVALID. {@code applyUpdate} resolves
 * {@code resolved_value} from the inherited configuration onto the live Parameter Context, making the processor and
 * Connector valid and startable.
 */
public class DeferredParameterConnector extends AbstractConnector {

    private static final String CONFIGURATION_STEP_NAME = "Deferred Parameter Configuration";
    private static final String RESOLVED_PARAMETER_NAME = "resolved_value";
    private static final String FLOW_RESOURCE = "flows/deferred-parameter-flow.json";

    static final ConnectorPropertyDescriptor RESOLVED_VALUE = new ConnectorPropertyDescriptor.Builder()
            .name("Resolved Value")
            .description("The value that applyUpdate resolves onto the managed Parameter Context. It is intentionally "
                    + "absent from the authoritative flow returned by getActiveFlow, which declares the Parameter as blank.")
            .required(true)
            .type(PropertyType.STRING)
            .defaultValue("resolved-content")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final ConnectorPropertyGroup PROPERTY_GROUP = new ConnectorPropertyGroup.Builder()
            .name("Deferred Parameter Configuration")
            .description("Configuration for deferred parameter resolution testing")
            .properties(List.of(RESOLVED_VALUE))
            .build();

    private static final ConfigurationStep CONFIG_STEP = new ConfigurationStep.Builder()
            .name(CONFIGURATION_STEP_NAME)
            .description("Configure the value that applyUpdate resolves onto the managed Parameter Context")
            .propertyGroups(List.of(PROPERTY_GROUP))
            .build();

    @Override
    protected void onStepConfigured(final String stepName, final FlowContext workingContext) throws FlowUpdateException {
    }

    @Override
    public VersionedExternalFlow getInitialFlow() {
        return buildFlow();
    }

    @Override
    public VersionedExternalFlow getActiveFlow(final FlowContext activeFlowContext) {
        // Declares the derived Parameter as blank. Apply-time resolution happens only in applyUpdate.
        return buildFlow();
    }

    @Override
    public List<ConfigVerificationResult> verifyConfigurationStep(final String stepName, final Map<String, String> propertyValueOverrides, final FlowContext flowContext) {
        return List.of();
    }

    @Override
    public List<ConfigurationStep> getConfigurationSteps() {
        return List.of(CONFIG_STEP);
    }

    @Override
    public void applyUpdate(final FlowContext workingContext, final FlowContext activeContext) throws FlowUpdateException {
        final VersionedExternalFlow flow = buildFlow();
        getInitializationContext().updateFlow(activeContext, flow, BundleCompatibility.RESOLVE_BUNDLE);

        // Resolve the derived Parameter onto the live managed Parameter Context so that the processor's required
        // property becomes valid. Installing the flow above first resets the Parameter to its blank authoritative
        // value, so this resolution must run afterward.
        final String resolvedValue = workingContext.getConfigurationContext().getProperty(CONFIG_STEP, RESOLVED_VALUE).getValue();
        activeContext.getParameterContext().updateParameters(List.of(
                new ParameterValue.Builder().name(RESOLVED_PARAMETER_NAME).value(resolvedValue).sensitive(false).build()));
    }

    private VersionedExternalFlow buildFlow() {
        // The managed flow is defined declaratively in the JSON resource: a single WriteToFile processor whose required
        // Filename property references #{resolved_value}, plus a Parameter Context that declares resolved_value as blank.
        return VersionedFlowUtils.loadFlowFromResource(FLOW_RESOURCE);
    }
}
