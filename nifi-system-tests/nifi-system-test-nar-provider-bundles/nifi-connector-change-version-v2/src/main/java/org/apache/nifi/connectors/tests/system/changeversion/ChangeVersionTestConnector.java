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
package org.apache.nifi.connectors.tests.system.changeversion;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.connector.AbstractConnector;
import org.apache.nifi.components.connector.ConfigurationStep;
import org.apache.nifi.components.connector.ConnectorPropertyDescriptor;
import org.apache.nifi.components.connector.ConnectorPropertyGroup;
import org.apache.nifi.components.connector.PropertyType;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.processor.util.StandardValidators;

import java.util.List;
import java.util.Map;

@Tags({"test", "change-version"})
@CapabilityDescription("Test Connector used to verify changing NAR version at runtime.")
public class ChangeVersionTestConnector extends AbstractConnector {

    static final String SETTINGS_STEP = "Settings";
    static final String SHARED_PROPERTY = "Shared Property";
    static final String NEW_PROPERTY = "New Property";
    static final String FLOW_GROUP_NAME = "Change Version Flow v2";

    private static final ConnectorPropertyDescriptor SHARED_PROPERTY_DESCRIPTOR = new ConnectorPropertyDescriptor.Builder()
            .name(SHARED_PROPERTY)
            .description("A property that exists in every version of this Connector.")
            .required(true)
            .defaultValue("shared-default")
            .type(PropertyType.STRING)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final ConnectorPropertyDescriptor NEW_PROPERTY_DESCRIPTOR = new ConnectorPropertyDescriptor.Builder()
            .name(NEW_PROPERTY)
            .description("A property introduced in version 2.0.0.")
            .required(true)
            .defaultValue("version-two-default")
            .type(PropertyType.STRING)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final ConnectorPropertyGroup SETTINGS_GROUP = new ConnectorPropertyGroup.Builder()
            .name("Settings")
            .description("Settings")
            .properties(List.of(SHARED_PROPERTY_DESCRIPTOR, NEW_PROPERTY_DESCRIPTOR))
            .build();

    private static final ConfigurationStep SETTINGS = new ConfigurationStep.Builder()
            .name(SETTINGS_STEP)
            .propertyGroups(List.of(SETTINGS_GROUP))
            .build();

    @Override
    protected void onStepConfigured(final String stepName, final FlowContext workingContext) {
    }

    @Override
    public VersionedExternalFlow getInitialFlow() {
        final VersionedProcessGroup group = new VersionedProcessGroup();
        group.setName(FLOW_GROUP_NAME);

        final VersionedExternalFlow flow = new VersionedExternalFlow();
        flow.setFlowContents(group);
        return flow;
    }

    @Override
    public VersionedExternalFlow getActiveFlow(final FlowContext activeFlowContext) {
        return getInitialFlow();
    }

    @Override
    public List<ConfigVerificationResult> verifyConfigurationStep(final String stepName, final Map<String, String> propertyValueOverrides, final FlowContext flowContext) {
        return List.of(new ConfigVerificationResult.Builder()
                .outcome(Outcome.SUCCESSFUL)
                .subject(stepName)
                .verificationStepName("Change Version Verification")
                .explanation("Successful verification")
                .build());
    }

    @Override
    public List<ConfigurationStep> getConfigurationSteps() {
        return List.of(SETTINGS);
    }

    @Override
    public void applyUpdate(final FlowContext workingFlowContext, final FlowContext activeFlowContext) {
    }
}
