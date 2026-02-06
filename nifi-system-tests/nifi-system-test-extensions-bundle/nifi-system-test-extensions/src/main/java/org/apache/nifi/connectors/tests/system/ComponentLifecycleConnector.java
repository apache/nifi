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
import org.apache.nifi.components.connector.ConfigurationStep;
import org.apache.nifi.components.connector.FlowUpdateException;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.components.connector.util.VersionedFlowUtils;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.ControllerServiceAPI;
import org.apache.nifi.flow.ExecutionEngine;
import org.apache.nifi.flow.Position;
import org.apache.nifi.flow.ScheduledState;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedPort;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Test Connector designed to verify the complete component lifecycle.
 * Creates a flow with:
 * - A processor at the root level
 * - A child process group with input and output ports
 * - A processor within the child group
 * - A stateless group with a processor
 *
 * This allows testing that start/stop operations properly handle all component types recursively.
 */
public class ComponentLifecycleConnector extends AbstractConnector {

    private static final Bundle SYSTEM_TEST_EXTENSIONS_BUNDLE = new Bundle("org.apache.nifi", "nifi-system-test-extensions-nar", "2.8.0-SNAPSHOT");

    @Override
    protected void onStepConfigured(final String stepName, final FlowContext workingContext) {
    }

    @Override
    public VersionedExternalFlow getInitialFlow() {
        final VersionedProcessGroup rootGroup = createRootGroup();
        final VersionedExternalFlow flow = new VersionedExternalFlow();
        flow.setFlowContents(rootGroup);
        return flow;
    }

    private VersionedProcessGroup createRootGroup() {
        final VersionedProcessGroup rootGroup = VersionedFlowUtils.createProcessGroup(UUID.randomUUID().toString(), "Component Lifecycle Root");
        rootGroup.setPosition(new Position(0, 0));
        rootGroup.setRemoteProcessGroups(new HashSet<>());
        rootGroup.setScheduledState(ScheduledState.ENABLED);
        rootGroup.setExecutionEngine(ExecutionEngine.STANDARD);

        final VersionedControllerService rootControllerService = VersionedFlowUtils.addControllerService(rootGroup,
            "org.apache.nifi.cs.tests.system.StandardCountService", SYSTEM_TEST_EXTENSIONS_BUNDLE, "Root Count Service");
        rootControllerService.setScheduledState(ScheduledState.ENABLED);
        final ControllerServiceAPI rootServiceApi = new ControllerServiceAPI();
        rootServiceApi.setType("org.apache.nifi.cs.tests.system.CountService");
        rootServiceApi.setBundle(SYSTEM_TEST_EXTENSIONS_BUNDLE);
        rootControllerService.setControllerServiceApis(Collections.singletonList(rootServiceApi));

        final VersionedProcessor rootProcessor = VersionedFlowUtils.addProcessor(rootGroup,
            "org.apache.nifi.processors.tests.system.GenerateFlowFile", SYSTEM_TEST_EXTENSIONS_BUNDLE, "Root GenerateFlowFile", new Position(100, 100));
        rootProcessor.setSchedulingPeriod("10 sec");

        final VersionedProcessor rootTerminateProcessor = VersionedFlowUtils.addProcessor(rootGroup,
            "org.apache.nifi.processors.tests.system.TerminateFlowFile", SYSTEM_TEST_EXTENSIONS_BUNDLE, "Root TerminateFlowFile", new Position(300, 100));

        final VersionedProcessGroup childGroup = createChildGroup(rootGroup.getIdentifier());
        rootGroup.getProcessGroups().add(childGroup);

        final VersionedPort childInputPort = childGroup.getInputPorts().iterator().next();
        final VersionedPort childOutputPort = childGroup.getOutputPorts().iterator().next();

        VersionedFlowUtils.addConnection(rootGroup, VersionedFlowUtils.createConnectableComponent(rootProcessor),
            VersionedFlowUtils.createConnectableComponent(childInputPort), Set.of("success"));
        VersionedFlowUtils.addConnection(rootGroup, VersionedFlowUtils.createConnectableComponent(childOutputPort),
            VersionedFlowUtils.createConnectableComponent(rootTerminateProcessor), Set.of(""));

        return rootGroup;
    }

    private VersionedProcessGroup createChildGroup(final String parentGroupId) {
        final VersionedProcessGroup childGroup = VersionedFlowUtils.createProcessGroup("child-group-id", "Child Group");
        childGroup.setPosition(new Position(100, 300));
        childGroup.setRemoteProcessGroups(new HashSet<>());
        childGroup.setScheduledState(ScheduledState.ENABLED);
        childGroup.setExecutionEngine(ExecutionEngine.STANDARD);
        childGroup.setGroupIdentifier(parentGroupId);

        final VersionedControllerService childControllerService = VersionedFlowUtils.addControllerService(childGroup,
            "org.apache.nifi.cs.tests.system.StandardCountService", SYSTEM_TEST_EXTENSIONS_BUNDLE, "Child Count Service");
        childControllerService.setScheduledState(ScheduledState.ENABLED);
        final ControllerServiceAPI childServiceApi = new ControllerServiceAPI();
        childServiceApi.setType("org.apache.nifi.cs.tests.system.CountService");
        childServiceApi.setBundle(SYSTEM_TEST_EXTENSIONS_BUNDLE);
        childControllerService.setControllerServiceApis(Collections.singletonList(childServiceApi));

        final VersionedPort inputPort = VersionedFlowUtils.addInputPort(childGroup, "Child Input", new Position(0, 0));
        final VersionedPort outputPort = VersionedFlowUtils.addOutputPort(childGroup, "Child Output", new Position(200, 0));

        final VersionedProcessor childProcessor = VersionedFlowUtils.addProcessor(childGroup,
            "org.apache.nifi.processors.tests.system.PassThrough", SYSTEM_TEST_EXTENSIONS_BUNDLE, "Child Terminate", new Position(100, 100));

        final VersionedProcessGroup statelessGroup = createStatelessGroup(childGroup.getIdentifier());
        childGroup.getProcessGroups().add(statelessGroup);

        final VersionedPort statelessInputPort = statelessGroup.getInputPorts().iterator().next();

        VersionedFlowUtils.addConnection(childGroup, VersionedFlowUtils.createConnectableComponent(inputPort),
            VersionedFlowUtils.createConnectableComponent(childProcessor), Set.of(""));
        VersionedFlowUtils.addConnection(childGroup, VersionedFlowUtils.createConnectableComponent(inputPort),
            VersionedFlowUtils.createConnectableComponent(statelessInputPort), Set.of(""));
        VersionedFlowUtils.addConnection(childGroup, VersionedFlowUtils.createConnectableComponent(childProcessor),
            VersionedFlowUtils.createConnectableComponent(outputPort), Set.of("success"));

        return childGroup;
    }

    private VersionedProcessGroup createStatelessGroup(final String parentGroupId) {
        final VersionedProcessGroup statelessGroup = VersionedFlowUtils.createProcessGroup("stateless-group-id", "Stateless Group");
        statelessGroup.setPosition(new Position(400, 100));
        statelessGroup.setRemoteProcessGroups(new HashSet<>());
        statelessGroup.setScheduledState(ScheduledState.ENABLED);
        statelessGroup.setExecutionEngine(ExecutionEngine.STATELESS);
        statelessGroup.setStatelessFlowTimeout("1 min");
        statelessGroup.setGroupIdentifier(parentGroupId);

        final VersionedPort statelessInput = VersionedFlowUtils.addInputPort(statelessGroup, "Stateless Input", new Position(0, 0));

        final VersionedProcessor statelessProcessor = VersionedFlowUtils.addProcessor(statelessGroup,
            "org.apache.nifi.processors.tests.system.TerminateFlowFile", SYSTEM_TEST_EXTENSIONS_BUNDLE, "Stateless Terminate", new Position(100, 100));

        VersionedFlowUtils.addConnection(statelessGroup, VersionedFlowUtils.createConnectableComponent(statelessInput),
            VersionedFlowUtils.createConnectableComponent(statelessProcessor), Set.of(""));

        return statelessGroup;
    }

    @Override
    public List<ConfigVerificationResult> verifyConfigurationStep(final String stepName, final Map<String, String> propertyValueOverrides, final FlowContext flowContext) {
        return List.of();
    }

    @Override
    public List<ConfigurationStep> getConfigurationSteps() {
        return List.of();
    }

    @Override
    public void applyUpdate(final FlowContext workingFlowContext, final FlowContext activeFlowContext) throws FlowUpdateException {
        getInitializationContext().updateFlow(activeFlowContext, getInitialFlow());
    }
}
