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
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.ComponentType;
import org.apache.nifi.flow.ConnectableComponent;
import org.apache.nifi.flow.ConnectableComponentType;
import org.apache.nifi.flow.ControllerServiceAPI;
import org.apache.nifi.flow.ExecutionEngine;
import org.apache.nifi.flow.PortType;
import org.apache.nifi.flow.Position;
import org.apache.nifi.flow.ScheduledState;
import org.apache.nifi.flow.VersionedConnection;
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

    public static final String ROOT_PROCESSOR_ID = "root-processor-id";
    public static final String CHILD_GROUP_ID = "child-group-id";
    public static final String CHILD_INPUT_PORT_ID = "child-input-port-id";
    public static final String CHILD_OUTPUT_PORT_ID = "child-output-port-id";
    public static final String CHILD_PROCESSOR_ID = "child-processor-id";
    public static final String STATELESS_GROUP_ID = "stateless-group-id";
    public static final String STATELESS_PROCESSOR_ID = "stateless-processor-id";
    public static final String STATELESS_INPUT_PORT_ID = "stateless-input-port-id";
    public static final String ROOT_CONTROLLER_SERVICE_ID = "root-controller-service-id";
    public static final String CHILD_CONTROLLER_SERVICE_ID = "child-controller-service-id";

    private static final Bundle SYSTEM_TEST_EXTENSIONS_BUNDLE = createBundle();

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
        final VersionedProcessGroup rootGroup = new VersionedProcessGroup();
        rootGroup.setIdentifier(UUID.randomUUID().toString());
        rootGroup.setName("Component Lifecycle Root");
        rootGroup.setPosition(new Position(0, 0));
        rootGroup.setProcessors(new HashSet<>());
        rootGroup.setProcessGroups(new HashSet<>());
        rootGroup.setConnections(new HashSet<>());
        rootGroup.setInputPorts(new HashSet<>());
        rootGroup.setOutputPorts(new HashSet<>());
        rootGroup.setControllerServices(new HashSet<>());
        rootGroup.setLabels(new HashSet<>());
        rootGroup.setFunnels(new HashSet<>());
        rootGroup.setRemoteProcessGroups(new HashSet<>());
        rootGroup.setScheduledState(ScheduledState.ENABLED);
        rootGroup.setExecutionEngine(ExecutionEngine.STANDARD);
        rootGroup.setComponentType(ComponentType.PROCESS_GROUP);

        // Create root-level Controller Service
        final VersionedControllerService rootControllerService = createControllerService(ROOT_CONTROLLER_SERVICE_ID, "Root Count Service", rootGroup.getIdentifier());
        rootGroup.getControllerServices().add(rootControllerService);

        // Create root-level processor (GenerateFlowFile)
        final VersionedProcessor rootProcessor = createProcessor(ROOT_PROCESSOR_ID, "Root GenerateFlowFile",
            "org.apache.nifi.processors.tests.system.GenerateFlowFile", new Position(100, 100));
        rootProcessor.setGroupIdentifier(rootGroup.getIdentifier());
        rootProcessor.setSchedulingPeriod("10 sec");
        rootGroup.getProcessors().add(rootProcessor);

        // Create root-level processor (TerminateFlowFile)
        final VersionedProcessor rootTerminateProcessor = createProcessor("root-terminate-processor-id", "Root TerminateFlowFile",
            "org.apache.nifi.processors.tests.system.TerminateFlowFile", new Position(300, 100));
        rootTerminateProcessor.setGroupIdentifier(rootGroup.getIdentifier());
        rootGroup.getProcessors().add(rootTerminateProcessor);

        // Create child process group with ports and processor
        final VersionedProcessGroup childGroup = createChildGroup(rootGroup.getIdentifier());
        rootGroup.getProcessGroups().add(childGroup);

        // Create connection from root processor to child group's input port
        final VersionedConnection rootToChildConnection = createConnection(
            createConnectableComponent(ROOT_PROCESSOR_ID, "Root GenerateFlowFile", ConnectableComponentType.PROCESSOR, rootGroup.getIdentifier()),
            Set.of("success"),
            createConnectableComponent(CHILD_INPUT_PORT_ID, "Child Input", ConnectableComponentType.INPUT_PORT, CHILD_GROUP_ID),
            rootGroup.getIdentifier()
        );
        rootGroup.getConnections().add(rootToChildConnection);

        // Create connection from child group's output port to root terminate processor
        final VersionedConnection childToRootConnection = createConnection(
            createConnectableComponent(CHILD_OUTPUT_PORT_ID, "Child Output", ConnectableComponentType.OUTPUT_PORT, CHILD_GROUP_ID),
            Set.of(""),
            createConnectableComponent("root-terminate-processor-id", "Root TerminateFlowFile", ConnectableComponentType.PROCESSOR, rootGroup.getIdentifier()),
            rootGroup.getIdentifier()
        );
        rootGroup.getConnections().add(childToRootConnection);

        return rootGroup;
    }

    private VersionedProcessGroup createChildGroup(final String parentGroupId) {
        final VersionedProcessGroup childGroup = new VersionedProcessGroup();
        childGroup.setIdentifier(CHILD_GROUP_ID);
        childGroup.setName("Child Group");
        childGroup.setPosition(new Position(100, 300));
        childGroup.setProcessors(new HashSet<>());
        childGroup.setProcessGroups(new HashSet<>());
        childGroup.setConnections(new HashSet<>());
        childGroup.setInputPorts(new HashSet<>());
        childGroup.setOutputPorts(new HashSet<>());
        childGroup.setControllerServices(new HashSet<>());
        childGroup.setLabels(new HashSet<>());
        childGroup.setFunnels(new HashSet<>());
        childGroup.setRemoteProcessGroups(new HashSet<>());
        childGroup.setScheduledState(ScheduledState.ENABLED);
        childGroup.setExecutionEngine(ExecutionEngine.STANDARD);
        childGroup.setComponentType(ComponentType.PROCESS_GROUP);
        childGroup.setGroupIdentifier(parentGroupId);

        // Create Controller Service in child group
        final VersionedControllerService childControllerService = createControllerService(CHILD_CONTROLLER_SERVICE_ID, "Child Count Service", CHILD_GROUP_ID);
        childGroup.getControllerServices().add(childControllerService);

        // Create input port
        final VersionedPort inputPort = createPort(CHILD_INPUT_PORT_ID, "Child Input", true, CHILD_GROUP_ID);
        childGroup.getInputPorts().add(inputPort);

        // Create output port
        final VersionedPort outputPort = createPort(CHILD_OUTPUT_PORT_ID, "Child Output", false, CHILD_GROUP_ID);
        childGroup.getOutputPorts().add(outputPort);

        // Create processor in child group
        final VersionedProcessor childProcessor = createProcessor(CHILD_PROCESSOR_ID, "Child Terminate",
            "org.apache.nifi.processors.tests.system.PassThrough", new Position(100, 100));
        childProcessor.setGroupIdentifier(CHILD_GROUP_ID);
        childGroup.getProcessors().add(childProcessor);

        // Create stateless group
        final VersionedProcessGroup statelessGroup = createStatelessGroup(CHILD_GROUP_ID);
        childGroup.getProcessGroups().add(statelessGroup);

        // Connection: input port -> child processor
        final VersionedConnection inputToProcessor = createConnection(
            createConnectableComponent(CHILD_INPUT_PORT_ID, "Child Input", ConnectableComponentType.INPUT_PORT, CHILD_GROUP_ID),
            Set.of(""),
            createConnectableComponent(CHILD_PROCESSOR_ID, "Child Terminate", ConnectableComponentType.PROCESSOR, CHILD_GROUP_ID),
            CHILD_GROUP_ID
        );
        childGroup.getConnections().add(inputToProcessor);

        // Connection: input port -> stateless group
        final VersionedConnection inputToStateless = createConnection(
            createConnectableComponent(CHILD_INPUT_PORT_ID, "Child Input", ConnectableComponentType.INPUT_PORT, CHILD_GROUP_ID),
            Set.of(""),
            createConnectableComponent(STATELESS_INPUT_PORT_ID, "Stateless Input", ConnectableComponentType.INPUT_PORT, STATELESS_GROUP_ID),
            CHILD_GROUP_ID
        );
        childGroup.getConnections().add(inputToStateless);

        // Connection: child processor -> output port
        final VersionedConnection processorToOutput = createConnection(
            createConnectableComponent(CHILD_PROCESSOR_ID, "Child Terminate", ConnectableComponentType.PROCESSOR, CHILD_GROUP_ID),
            Set.of("success"),
            createConnectableComponent(CHILD_OUTPUT_PORT_ID, "Child Output", ConnectableComponentType.OUTPUT_PORT, CHILD_GROUP_ID),
            CHILD_GROUP_ID
        );
        childGroup.getConnections().add(processorToOutput);

        return childGroup;
    }

    private VersionedProcessGroup createStatelessGroup(final String parentGroupId) {
        final VersionedProcessGroup statelessGroup = new VersionedProcessGroup();
        statelessGroup.setIdentifier(STATELESS_GROUP_ID);
        statelessGroup.setName("Stateless Group");
        statelessGroup.setPosition(new Position(400, 100));
        statelessGroup.setProcessors(new HashSet<>());
        statelessGroup.setProcessGroups(new HashSet<>());
        statelessGroup.setConnections(new HashSet<>());
        statelessGroup.setInputPorts(new HashSet<>());
        statelessGroup.setOutputPorts(new HashSet<>());
        statelessGroup.setControllerServices(new HashSet<>());
        statelessGroup.setLabels(new HashSet<>());
        statelessGroup.setFunnels(new HashSet<>());
        statelessGroup.setRemoteProcessGroups(new HashSet<>());
        statelessGroup.setScheduledState(ScheduledState.ENABLED);
        statelessGroup.setExecutionEngine(ExecutionEngine.STATELESS);
        statelessGroup.setStatelessFlowTimeout("1 min");
        statelessGroup.setComponentType(ComponentType.PROCESS_GROUP);
        statelessGroup.setGroupIdentifier(parentGroupId);

        // Create input port for stateless group
        final VersionedPort statelessInput = createPort(STATELESS_INPUT_PORT_ID, "Stateless Input", true, STATELESS_GROUP_ID);
        statelessGroup.getInputPorts().add(statelessInput);

        // Create processor in stateless group
        final VersionedProcessor statelessProcessor = createProcessor(STATELESS_PROCESSOR_ID, "Stateless Terminate",
            "org.apache.nifi.processors.tests.system.TerminateFlowFile", new Position(100, 100));
        statelessProcessor.setGroupIdentifier(STATELESS_GROUP_ID);
        statelessGroup.getProcessors().add(statelessProcessor);

        // Connection: input port -> processor
        final VersionedConnection inputToProcessor = createConnection(
            createConnectableComponent(STATELESS_INPUT_PORT_ID, "Stateless Input", ConnectableComponentType.INPUT_PORT, STATELESS_GROUP_ID),
            Set.of(""),
            createConnectableComponent(STATELESS_PROCESSOR_ID, "Stateless Terminate", ConnectableComponentType.PROCESSOR, STATELESS_GROUP_ID),
            STATELESS_GROUP_ID
        );
        statelessGroup.getConnections().add(inputToProcessor);

        return statelessGroup;
    }

    private VersionedProcessor createProcessor(final String id, final String name, final String type, final Position position) {
        final VersionedProcessor processor = new VersionedProcessor();
        processor.setIdentifier(id);
        processor.setName(name);
        processor.setType(type);
        processor.setBundle(SYSTEM_TEST_EXTENSIONS_BUNDLE);
        processor.setPosition(position);
        processor.setProperties(Map.of());
        processor.setPropertyDescriptors(Map.of());
        processor.setSchedulingPeriod("0 sec");
        processor.setSchedulingStrategy("TIMER_DRIVEN");
        processor.setExecutionNode("ALL");
        processor.setPenaltyDuration("30 sec");
        processor.setYieldDuration("1 sec");
        processor.setBulletinLevel("WARN");
        processor.setRunDurationMillis(0L);
        processor.setConcurrentlySchedulableTaskCount(1);
        processor.setAutoTerminatedRelationships(Set.of());
        processor.setScheduledState(ScheduledState.ENABLED);
        processor.setRetryCount(0);
        processor.setRetriedRelationships(Set.of());
        processor.setComponentType(ComponentType.PROCESSOR);
        return processor;
    }

    private VersionedPort createPort(final String id, final String name, final boolean isInput, final String groupId) {
        final VersionedPort port = new VersionedPort();
        port.setIdentifier(id);
        port.setName(name);
        port.setPosition(new Position(isInput ? 0 : 200, 0));
        port.setType(isInput ? PortType.INPUT_PORT : PortType.OUTPUT_PORT);
        port.setComponentType(isInput ? ComponentType.INPUT_PORT : ComponentType.OUTPUT_PORT);
        port.setConcurrentlySchedulableTaskCount(1);
        port.setScheduledState(ScheduledState.ENABLED);
        port.setAllowRemoteAccess(false);
        port.setGroupIdentifier(groupId);
        return port;
    }

    private VersionedControllerService createControllerService(final String id, final String name, final String groupId) {
        final VersionedControllerService service = new VersionedControllerService();
        service.setIdentifier(id);
        service.setName(name);
        service.setType("org.apache.nifi.cs.tests.system.StandardCountService");
        service.setBundle(SYSTEM_TEST_EXTENSIONS_BUNDLE);
        service.setGroupIdentifier(groupId);
        service.setProperties(Map.of());
        service.setPropertyDescriptors(Map.of());
        service.setScheduledState(ScheduledState.ENABLED);
        service.setBulletinLevel("WARN");

        final ControllerServiceAPI serviceApi = new ControllerServiceAPI();
        serviceApi.setType("org.apache.nifi.cs.tests.system.CountService");
        serviceApi.setBundle(SYSTEM_TEST_EXTENSIONS_BUNDLE);
        service.setControllerServiceApis(Collections.singletonList(serviceApi));

        return service;
    }

    private VersionedConnection createConnection(final ConnectableComponent source, final Set<String> relationships,
                                                  final ConnectableComponent destination, final String groupId) {
        final VersionedConnection connection = new VersionedConnection();
        connection.setIdentifier(UUID.randomUUID().toString());
        connection.setName("");
        connection.setSource(source);
        connection.setDestination(destination);
        connection.setSelectedRelationships(relationships);
        connection.setBackPressureObjectThreshold(10000L);
        connection.setBackPressureDataSizeThreshold("1 GB");
        connection.setFlowFileExpiration("0 sec");
        connection.setLabelIndex(0);
        connection.setzIndex(0L);
        connection.setComponentType(ComponentType.CONNECTION);
        connection.setGroupIdentifier(groupId);
        return connection;
    }

    private ConnectableComponent createConnectableComponent(final String id, final String name, final ConnectableComponentType type, final String groupId) {
        final ConnectableComponent component = new ConnectableComponent();
        component.setId(id);
        component.setName(name);
        component.setType(type);
        component.setGroupId(groupId);
        return component;
    }

    private static Bundle createBundle() {
        final Bundle bundle = new Bundle();
        bundle.setGroup("org.apache.nifi");
        bundle.setArtifact("nifi-system-test-extensions-nar");
        bundle.setVersion("2.8.0-SNAPSHOT");
        return bundle;
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
