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
import org.apache.nifi.components.connector.processors.CreateDummyFlowFile;
import org.apache.nifi.components.connector.services.impl.BlockingEnableDisableCounterService;
import org.apache.nifi.components.connector.services.impl.BlockingEnablingCounterService;
import org.apache.nifi.components.connector.services.impl.FailingEnablingCounterService;
import org.apache.nifi.components.connector.util.VersionedFlowUtils;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;

import java.util.List;
import java.util.Map;

public class ControllerServiceEnablingConnector extends AbstractConnector {

    static final String BLOCKING_ENABLING = "BLOCKING_ENABLING";
    static final String FAILING_ENABLING = "FAILING_ENABLING";
    static final String ORDERED_BLOCKING = "ORDERED_BLOCKING";
    static final String CONFIGURATION_STEP_NAME = "Configuration";

    static final ConnectorPropertyDescriptor ENABLING_BEHAVIOR = new ConnectorPropertyDescriptor.Builder()
        .name("Enabling Behavior")
        .description("How the managed Controller Service should behave while enabling")
        .type(PropertyType.STRING)
        .required(true)
        .defaultValue(BLOCKING_ENABLING)
        .allowableValues(BLOCKING_ENABLING, FAILING_ENABLING, ORDERED_BLOCKING)
        .build();

    private static final ConnectorPropertyGroup PROPERTY_GROUP = new ConnectorPropertyGroup.Builder()
        .name("Controller Service Settings")
        .addProperty(ENABLING_BEHAVIOR)
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
        return VersionedFlowUtils.loadFlowFromResource("flows/generate-duplicate-log-flow.json");
    }

    @Override
    public VersionedExternalFlow getActiveFlow(final FlowContext activeFlowContext) {
        final String enablingBehavior = activeFlowContext.getConfigurationContext().getProperty(CONFIGURATION_STEP, ENABLING_BEHAVIOR).getValue();
        return buildFlow(enablingBehavior);
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

    private VersionedExternalFlow buildFlow(final String enablingBehavior) {
        final VersionedExternalFlow externalFlow = VersionedFlowUtils.loadFlowFromResource("flows/generate-duplicate-log-flow.json");
        final VersionedProcessGroup rootGroup = externalFlow.getFlowContents();

        final Bundle systemBundle = new Bundle();
        systemBundle.setArtifact("system");
        systemBundle.setGroup("default");
        systemBundle.setVersion("unversioned");

        final String serviceType = switch (enablingBehavior) {
            case BLOCKING_ENABLING -> BlockingEnablingCounterService.class.getName();
            case ORDERED_BLOCKING -> BlockingEnableDisableCounterService.class.getName();
            default -> FailingEnablingCounterService.class.getName();
        };

        final VersionedControllerService controllerService = VersionedFlowUtils.addControllerService(rootGroup, serviceType, systemBundle, "Managed Service");

        final VersionedProcessor generateProcessor = VersionedFlowUtils.findProcessor(rootGroup,
            processor -> processor.getType().equals(CreateDummyFlowFile.class.getName())).orElseThrow();

        generateProcessor.getProperties().put("Counter Service", controllerService.getIdentifier());

        return externalFlow;
    }
}
