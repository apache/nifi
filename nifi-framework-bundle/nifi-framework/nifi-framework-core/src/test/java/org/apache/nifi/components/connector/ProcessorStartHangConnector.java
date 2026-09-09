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
import org.apache.nifi.components.connector.processors.ManagedStartHangProcessor;
import org.apache.nifi.components.connector.util.VersionedFlowUtils;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.Position;
import org.apache.nifi.flow.ScheduledState;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class ProcessorStartHangConnector extends AbstractConnector {

    static final String ROOT_GROUP = "ROOT_GROUP";
    static final String CHILD_GROUP = "CHILD_GROUP";
    static final String CONFIGURATION_STEP_NAME = "Configuration";
    static final String HANGING_PROCESSOR_NAME = "Hanging Start Processor";

    static final ConnectorPropertyDescriptor PROCESSOR_PLACEMENT = new ConnectorPropertyDescriptor.Builder()
        .name("Processor Placement")
        .description("Where the hanging Processor should be placed within the managed flow")
        .type(PropertyType.STRING)
        .required(true)
        .defaultValue(ROOT_GROUP)
        .allowableValues(ROOT_GROUP, CHILD_GROUP)
        .build();

    private static final ConnectorPropertyGroup PROPERTY_GROUP = new ConnectorPropertyGroup.Builder()
        .name("Processor Settings")
        .addProperty(PROCESSOR_PLACEMENT)
        .build();

    private static final ConfigurationStep CONFIGURATION_STEP = new ConfigurationStep.Builder()
        .name(CONFIGURATION_STEP_NAME)
        .propertyGroups(List.of(PROPERTY_GROUP))
        .build();

    @Override
    public List<ConfigurationStep> getConfigurationSteps() {
        return List.of(CONFIGURATION_STEP);
    }

    @Override
    public VersionedExternalFlow getInitialFlow() {
        return buildFlow(ROOT_GROUP);
    }

    @Override
    public VersionedExternalFlow getActiveFlow(final FlowContext activeFlowContext) {
        final String placement = activeFlowContext.getConfigurationContext().getProperty(CONFIGURATION_STEP, PROCESSOR_PLACEMENT).getValue();
        return buildFlow(placement);
    }

    @Override
    public void applyUpdate(final FlowContext workingContext, final FlowContext activeContext) throws FlowUpdateException {
        getInitializationContext().updateFlow(activeContext, getActiveFlow(workingContext));
    }

    @Override
    public void onStepConfigured(final String stepName, final FlowContext workingContext) throws FlowUpdateException {
        getInitializationContext().updateFlow(workingContext, getActiveFlow(workingContext));
    }

    @Override
    public List<ConfigVerificationResult> verifyConfigurationStep(final String stepName, final Map<String, String> overrides, final FlowContext flowContext) {
        return List.of();
    }

    private VersionedExternalFlow buildFlow(final String placement) {
        final VersionedExternalFlow externalFlow = VersionedFlowUtils.loadFlowFromResource("flows/generate-duplicate-log-flow.json");
        final VersionedProcessGroup rootGroup = externalFlow.getFlowContents();

        final Bundle systemBundle = new Bundle();
        systemBundle.setArtifact("system");
        systemBundle.setGroup("default");
        systemBundle.setVersion("unversioned");

        final VersionedProcessGroup targetGroup = ROOT_GROUP.equals(placement) ? rootGroup : rootGroup.getProcessGroups().iterator().next();

        final VersionedProcessor hangingProcessor = VersionedFlowUtils.addProcessor(targetGroup, ManagedStartHangProcessor.class.getName(),
            systemBundle, HANGING_PROCESSOR_NAME, new Position(0.0, 0.0));
        hangingProcessor.setAutoTerminatedRelationships(Set.of("success"));
        hangingProcessor.setScheduledState(ScheduledState.ENABLED);

        return externalFlow;
    }
}
