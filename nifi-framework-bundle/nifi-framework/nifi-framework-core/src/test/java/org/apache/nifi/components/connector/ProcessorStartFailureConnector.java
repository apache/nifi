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
import org.apache.nifi.components.connector.services.impl.StandardCounterService;
import org.apache.nifi.components.connector.util.VersionedFlowUtils;
import org.apache.nifi.controller.scheduling.processors.FailOnScheduledProcessor;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;

import java.util.List;
import java.util.Map;

public class ProcessorStartFailureConnector extends AbstractConnector {

    @Override
    public List<ConfigurationStep> getConfigurationSteps() {
        return List.of();
    }

    @Override
    public VersionedExternalFlow getInitialFlow() {
        return buildFlow();
    }

    @Override
    public VersionedExternalFlow getActiveFlow(final FlowContext activeFlowContext) {
        return buildFlow();
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

    private VersionedExternalFlow buildFlow() {
        final VersionedExternalFlow externalFlow = VersionedFlowUtils.loadFlowFromResource("flows/generate-duplicate-log-flow.json");
        final VersionedProcessGroup rootGroup = externalFlow.getFlowContents();

        final Bundle systemBundle = new Bundle();
        systemBundle.setArtifact("system");
        systemBundle.setGroup("default");
        systemBundle.setVersion("unversioned");

        final VersionedControllerService controllerService = VersionedFlowUtils.addControllerService(
            rootGroup, StandardCounterService.class.getName(), systemBundle, "Managed Service");
        final VersionedProcessor processor = VersionedFlowUtils.findProcessor(rootGroup,
            candidate -> candidate.getType().equals(CreateDummyFlowFile.class.getName())).orElseThrow();
        processor.setType(FailOnScheduledProcessor.class.getName());
        processor.setName("Start Failure Processor");
        processor.getProperties().clear();
        processor.getProperties().put(FailOnScheduledProcessor.MANAGED_SERVICE.getName(), controllerService.getIdentifier());
        processor.getPropertyDescriptors().clear();

        for (final VersionedProcessGroup childGroup : rootGroup.getProcessGroups()) {
            if (childGroup.getIdentifier().equals(processor.getGroupIdentifier())) {
                childGroup.getConnections().removeIf(connection -> connection.getSource().getId().equals(processor.getIdentifier()));
                break;
            }
        }

        return externalFlow;
    }
}
