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

/**
 * Test Connector designed to verify the complete component lifecycle.
 * Creates a flow with:
 * - A processor at the root level
 * - A child process group with input and output ports
 * - A processor within the child group
 * - A stateless group with a processor
 * - A pair of nested stateless groups, where only the inner one holds a processor
 *
 * This allows testing that start/stop operations properly handle all component types recursively.
 */
public class ComponentLifecycleConnector extends AbstractConnector {

    private static final Bundle SYSTEM_TEST_EXTENSIONS_BUNDLE = new Bundle("org.apache.nifi", "nifi-system-test-extensions-nar", "2.8.0-SNAPSHOT");
    private static final String ROOT_GROUP_ID = "component-lifecycle-root-group";

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

    @Override
    public VersionedExternalFlow getActiveFlow(final FlowContext activeFlowContext) {
        // This Connector's flow is fully determined statically and does not depend on runtime configuration.
        return getInitialFlow();
    }

    private VersionedProcessGroup createRootGroup() {
        final VersionedProcessGroup rootGroup = VersionedFlowUtils.createProcessGroup(ROOT_GROUP_ID, "Component Lifecycle Root");
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

        final VersionedProcessGroup childGroup = createChildGroup(rootGroup.getIdentifier(), rootControllerService.getIdentifier());
        rootGroup.getProcessGroups().add(childGroup);

        final VersionedPort childInputPort = childGroup.getInputPorts().iterator().next();
        final VersionedPort childOutputPort = childGroup.getOutputPorts().iterator().next();

        VersionedFlowUtils.addConnection(rootGroup, VersionedFlowUtils.createConnectableComponent(rootProcessor),
            VersionedFlowUtils.createConnectableComponent(childInputPort), Set.of("success"));
        VersionedFlowUtils.addConnection(rootGroup, VersionedFlowUtils.createConnectableComponent(childOutputPort),
            VersionedFlowUtils.createConnectableComponent(rootTerminateProcessor), Set.of(""));

        return rootGroup;
    }

    private VersionedProcessGroup createChildGroup(final String parentGroupId, final String rootCountServiceId) {
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

        final VersionedProcessGroup statelessGroup = createStatelessGroup(childGroup.getIdentifier(), rootCountServiceId);
        childGroup.getProcessGroups().add(statelessGroup);

        final VersionedPort statelessInputPort = statelessGroup.getInputPorts().iterator().next();

        final VersionedProcessGroup nestedOuterGroup = createNestedStatelessGroups(childGroup.getIdentifier(), rootCountServiceId);
        childGroup.getProcessGroups().add(nestedOuterGroup);

        final VersionedPort nestedOuterInputPort = nestedOuterGroup.getInputPorts().iterator().next();

        VersionedFlowUtils.addConnection(childGroup, VersionedFlowUtils.createConnectableComponent(inputPort),
            VersionedFlowUtils.createConnectableComponent(childProcessor), Set.of(""));
        VersionedFlowUtils.addConnection(childGroup, VersionedFlowUtils.createConnectableComponent(inputPort),
            VersionedFlowUtils.createConnectableComponent(statelessInputPort), Set.of(""));
        VersionedFlowUtils.addConnection(childGroup, VersionedFlowUtils.createConnectableComponent(inputPort),
            VersionedFlowUtils.createConnectableComponent(nestedOuterInputPort), Set.of(""));
        VersionedFlowUtils.addConnection(childGroup, VersionedFlowUtils.createConnectableComponent(childProcessor),
            VersionedFlowUtils.createConnectableComponent(outputPort), Set.of("success"));

        return childGroup;
    }

    private VersionedProcessGroup createStatelessGroup(final String parentGroupId, final String rootCountServiceId) {
        final VersionedProcessGroup statelessGroup = createStatelessGroupShell("stateless-group-id", "Stateless", new Position(400, 100), parentGroupId);
        addCountingFlow(statelessGroup, "Stateless", rootCountServiceId);
        return statelessGroup;
    }

    private VersionedProcessGroup createNestedStatelessGroups(final String parentGroupId, final String rootCountServiceId) {
        final VersionedProcessGroup outerGroup = createStatelessGroupShell("nested-stateless-outer-group-id", "Nested Outer",
            new Position(400, 300), parentGroupId);

        // Only the inner group holds a processor referencing the root service, so a resolver that stops at the inner
        // group silently no-ops instead of transitioning the subtree.
        final VersionedProcessGroup innerGroup = createStatelessGroupShell("nested-stateless-inner-group-id", "Nested Inner",
            new Position(200, 0), outerGroup.getIdentifier());
        addCountingFlow(innerGroup, "Nested Inner", rootCountServiceId);
        outerGroup.getProcessGroups().add(innerGroup);

        VersionedFlowUtils.addConnection(outerGroup, VersionedFlowUtils.createConnectableComponent(getInputPort(outerGroup)),
            VersionedFlowUtils.createConnectableComponent(getInputPort(innerGroup)), Set.of(""));

        return outerGroup;
    }

    private VersionedProcessGroup createStatelessGroupShell(final String identifier, final String namePrefix, final Position position,
                                                            final String parentGroupId) {
        final VersionedProcessGroup group = VersionedFlowUtils.createProcessGroup(identifier, namePrefix + " Group");
        group.setPosition(position);
        group.setRemoteProcessGroups(new HashSet<>());
        group.setScheduledState(ScheduledState.ENABLED);
        group.setExecutionEngine(ExecutionEngine.STATELESS);
        group.setStatelessFlowTimeout("1 min");
        group.setGroupIdentifier(parentGroupId);

        VersionedFlowUtils.addInputPort(group, namePrefix + " Input", new Position(0, 0));
        return group;
    }

    /**
     * Wires {@code input port -> CountFlowFiles -> TerminateFlowFile} inside the given group, with the CountFlowFiles
     * processor referencing a Controller Service that lives outside the group.
     */
    private void addCountingFlow(final VersionedProcessGroup group, final String namePrefix, final String rootCountServiceId) {
        final VersionedProcessor countProcessor = VersionedFlowUtils.addProcessor(group,
            "org.apache.nifi.processors.tests.system.CountFlowFiles", SYSTEM_TEST_EXTENSIONS_BUNDLE, namePrefix + " Count", new Position(100, 50));
        countProcessor.getProperties().put("Count Service", rootCountServiceId);

        final VersionedProcessor terminateProcessor = VersionedFlowUtils.addProcessor(group,
            "org.apache.nifi.processors.tests.system.TerminateFlowFile", SYSTEM_TEST_EXTENSIONS_BUNDLE, namePrefix + " Terminate", new Position(100, 100));

        VersionedFlowUtils.addConnection(group, VersionedFlowUtils.createConnectableComponent(getInputPort(group)),
            VersionedFlowUtils.createConnectableComponent(countProcessor), Set.of(""));
        VersionedFlowUtils.addConnection(group, VersionedFlowUtils.createConnectableComponent(countProcessor),
            VersionedFlowUtils.createConnectableComponent(terminateProcessor), Set.of("success"));
    }

    private VersionedPort getInputPort(final VersionedProcessGroup group) {
        return group.getInputPorts().iterator().next();
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
