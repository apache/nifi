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

package org.apache.nifi.mock.connectors;

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.connector.AbstractConnector;
import org.apache.nifi.components.connector.ConfigurationStep;
import org.apache.nifi.components.connector.FlowUpdateException;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.Position;
import org.apache.nifi.flow.ScheduledState;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * A test connector that returns an initial flow containing a processor with a bundle that does not exist.
 * This is used to test the behavior when a connector's initial flow references unavailable components.
 */
public class MissingBundleConnector extends AbstractConnector {

    @Override
    public VersionedExternalFlow getInitialFlow() {
        final VersionedProcessGroup rootGroup = new VersionedProcessGroup();
        rootGroup.setIdentifier(UUID.randomUUID().toString());
        rootGroup.setInstanceIdentifier(UUID.randomUUID().toString());
        rootGroup.setName("Missing Bundle Connector Flow");
        rootGroup.setPosition(new Position(0.0, 0.0));
        rootGroup.setProcessGroups(new HashSet<>());
        rootGroup.setConnections(new HashSet<>());
        rootGroup.setInputPorts(new HashSet<>());
        rootGroup.setOutputPorts(new HashSet<>());
        rootGroup.setControllerServices(new HashSet<>());
        rootGroup.setFunnels(new HashSet<>());
        rootGroup.setLabels(new HashSet<>());

        final VersionedProcessor missingProcessor = new VersionedProcessor();
        missingProcessor.setIdentifier(UUID.randomUUID().toString());
        missingProcessor.setInstanceIdentifier(UUID.randomUUID().toString());
        missingProcessor.setName("Missing Processor");
        missingProcessor.setType("com.example.nonexistent.MissingProcessor");
        missingProcessor.setPosition(new Position(100.0, 100.0));
        missingProcessor.setScheduledState(ScheduledState.ENABLED);
        missingProcessor.setSchedulingPeriod("0 sec");
        missingProcessor.setSchedulingStrategy("TIMER_DRIVEN");
        missingProcessor.setExecutionNode("ALL");
        missingProcessor.setPenaltyDuration("30 sec");
        missingProcessor.setYieldDuration("1 sec");
        missingProcessor.setBulletinLevel("WARN");
        missingProcessor.setRunDurationMillis(0L);
        missingProcessor.setConcurrentlySchedulableTaskCount(1);
        missingProcessor.setAutoTerminatedRelationships(new HashSet<>());
        missingProcessor.setProperties(Map.of());
        missingProcessor.setPropertyDescriptors(Map.of());
        missingProcessor.setGroupIdentifier(rootGroup.getIdentifier());

        final Bundle missingBundle = new Bundle();
        missingBundle.setGroup("com.example.nonexistent");
        missingBundle.setArtifact("missing-nar");
        missingBundle.setVersion("1.0.0");
        missingProcessor.setBundle(missingBundle);

        rootGroup.setProcessors(new HashSet<>(List.of(missingProcessor)));

        final VersionedExternalFlow externalFlow = new VersionedExternalFlow();
        externalFlow.setFlowContents(rootGroup);
        return externalFlow;
    }

    @Override
    protected void onStepConfigured(final String stepName, final FlowContext flowContext) {
    }

    @Override
    public List<ConfigurationStep> getConfigurationSteps() {
        return List.of();
    }

    @Override
    public void applyUpdate(final FlowContext workingContext, final FlowContext activeContext) throws FlowUpdateException {
    }

    @Override
    public List<ConfigVerificationResult> verifyConfigurationStep(final String stepName, final Map<String, String> overrides, final FlowContext workingContext) {
        return List.of();
    }
}
