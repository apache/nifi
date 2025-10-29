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
package org.apache.nifi.util;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.behavior.DynamicRelationship;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.flow.ComponentType;
import org.apache.nifi.flow.ScheduledState;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedPort;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flow.VersionedPropertyDescriptor;
import org.apache.nifi.flow.VersionedRemoteGroupPort;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.registry.flow.diff.DifferenceType;
import org.apache.nifi.registry.flow.diff.FlowDifference;
import org.apache.nifi.registry.flow.diff.StandardFlowDifference;
import org.apache.nifi.registry.flow.mapping.InstantiatedVersionedConnection;
import org.apache.nifi.registry.flow.mapping.InstantiatedVersionedControllerService;
import org.apache.nifi.registry.flow.mapping.InstantiatedVersionedProcessor;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestFlowDifferenceFilters {

    @Test
    public void testFilterAddedRemotePortsWithRemoteInputPortAsComponentB() {
        VersionedRemoteGroupPort remoteGroupPort = new VersionedRemoteGroupPort();
        remoteGroupPort.setComponentType(ComponentType.REMOTE_INPUT_PORT);

        StandardFlowDifference flowDifference = new StandardFlowDifference(
                DifferenceType.COMPONENT_ADDED, null, remoteGroupPort, null, null, "");

        // predicate should return false because we don't want to include changes for adding a remote input port
        assertFalse(FlowDifferenceFilters.FILTER_ADDED_REMOVED_REMOTE_PORTS.test(flowDifference));
    }

    @Test
    public void testFilterAddedRemotePortsWithRemoteInputPortAsComponentA() {
        VersionedRemoteGroupPort remoteGroupPort = new VersionedRemoteGroupPort();
        remoteGroupPort.setComponentType(ComponentType.REMOTE_INPUT_PORT);

        StandardFlowDifference flowDifference = new StandardFlowDifference(
                DifferenceType.COMPONENT_ADDED, remoteGroupPort, null, null, null, "");

        // predicate should return false because we don't want to include changes for adding a remote input port
        assertFalse(FlowDifferenceFilters.FILTER_ADDED_REMOVED_REMOTE_PORTS.test(flowDifference));
    }

    @Test
    public void testFilterAddedRemotePortsWithRemoteOutputPort() {
        VersionedRemoteGroupPort remoteGroupPort = new VersionedRemoteGroupPort();
        remoteGroupPort.setComponentType(ComponentType.REMOTE_OUTPUT_PORT);

        StandardFlowDifference flowDifference = new StandardFlowDifference(
                DifferenceType.COMPONENT_ADDED, null, remoteGroupPort, null, null, "");

        // predicate should return false because we don't want to include changes for adding a remote input port
        assertFalse(FlowDifferenceFilters.FILTER_ADDED_REMOVED_REMOTE_PORTS.test(flowDifference));
    }

    @Test
    public void testFilterAddedRemotePortsWithNonRemoteInputPort() {
        VersionedProcessor versionedProcessor = new VersionedProcessor();
        versionedProcessor.setComponentType(ComponentType.PROCESSOR);

        StandardFlowDifference flowDifference = new StandardFlowDifference(
                DifferenceType.COMPONENT_ADDED, null, versionedProcessor, null, null, "");

        // predicate should return true because we do want to include changes for adding a non-port
        assertTrue(FlowDifferenceFilters.FILTER_ADDED_REMOVED_REMOTE_PORTS.test(flowDifference));
    }


    @Test
    public void testFilterPublicPortNameChangeWhenNotNameChange() {
        final VersionedPort portA = new VersionedPort();
        final VersionedPort portB = new VersionedPort();

        final StandardFlowDifference flowDifference = new StandardFlowDifference(
                DifferenceType.VERSIONED_FLOW_COORDINATES_CHANGED,
                portA, portB,
                "http://localhost:18080", "http://localhost:17080",
                "");

        assertTrue(FlowDifferenceFilters.FILTER_PUBLIC_PORT_NAME_CHANGES.test(flowDifference));
    }

    @Test
    public void testFilterPublicPortNameChangeWhenNotAllowRemoteAccess() {
        final VersionedPort portA = new VersionedPort();
        final VersionedPort portB = new VersionedPort();

        final StandardFlowDifference flowDifference = new StandardFlowDifference(
                DifferenceType.NAME_CHANGED,
                portA, portB,
                "Port A", "Port B",
                "");

        assertTrue(FlowDifferenceFilters.FILTER_PUBLIC_PORT_NAME_CHANGES.test(flowDifference));
    }

    @Test
    public void testFilterPublicPortNameChangeWhenAllowRemoteAccess() {
        final VersionedPort portA = new VersionedPort();
        portA.setAllowRemoteAccess(Boolean.TRUE);

        final VersionedPort portB = new VersionedPort();
        portB.setAllowRemoteAccess(Boolean.FALSE);

        final StandardFlowDifference flowDifference = new StandardFlowDifference(
                DifferenceType.NAME_CHANGED,
                portA, portB,
                "Port A", "Port B",
                "");

        assertFalse(FlowDifferenceFilters.FILTER_PUBLIC_PORT_NAME_CHANGES.test(flowDifference));
    }

    @Test
    public void testFilterControllerServiceStatusChangeWhenNewStateIntroduced() {
        final VersionedControllerService controllerServiceA = new VersionedControllerService();
        final VersionedControllerService controllerServiceB = new VersionedControllerService();
        controllerServiceA.setScheduledState(null);
        controllerServiceB.setScheduledState(ScheduledState.DISABLED);

        final StandardFlowDifference flowDifference = new StandardFlowDifference(
                DifferenceType.SCHEDULED_STATE_CHANGED,
                controllerServiceA, controllerServiceB,
                controllerServiceA.getScheduledState(), controllerServiceB.getScheduledState(),
                "");

        assertTrue(FlowDifferenceFilters.isScheduledStateNew(flowDifference));
    }

    @Test
    public void testIsLocalScheduleStateChangeWithNullComponentADoesNotNPE() {
        // Simulate DEEP comparison producing a scheduled state change for a newly added component (no local A)
        final FlowDifference flowDifference = Mockito.mock(FlowDifference.class);
        Mockito.when(flowDifference.getDifferenceType()).thenReturn(DifferenceType.SCHEDULED_STATE_CHANGED);
        Mockito.when(flowDifference.getComponentA()).thenReturn(null);
        Mockito.when(flowDifference.getValueA()).thenReturn("RUNNING");
        Mockito.when(flowDifference.getValueB()).thenReturn("RUNNING");

        // Should not throw and should return false since no local component
        assertFalse(FlowDifferenceFilters.isLocalScheduleStateChange(flowDifference));
    }

    @Test
    public void testIsStaticPropertyRemovedFromDefinitionWhenPropertyDropped() {
        final FlowManager flowManager = Mockito.mock(FlowManager.class);
        final ProcessorNode processorNode = Mockito.mock(ProcessorNode.class);
        final ConfigurableComponent configurableComponent = Mockito.mock(ConfigurableComponent.class);

        final String propertyName = "Obsolete Property";
        final String instanceId = "processor-instance";

        Mockito.when(flowManager.getProcessorNode(instanceId)).thenReturn(processorNode);
        Mockito.when(processorNode.getComponent()).thenReturn(configurableComponent);
        Mockito.when(configurableComponent.getPropertyDescriptors()).thenReturn(List.of(new PropertyDescriptor.Builder().name("Retained Property").build()));
        Mockito.when(configurableComponent.getPropertyDescriptor(propertyName)).thenReturn(null);

        final InstantiatedVersionedProcessor localProcessor = new InstantiatedVersionedProcessor(instanceId, "group-id");
        final FlowDifference difference = new StandardFlowDifference(
                DifferenceType.PROPERTY_REMOVED,
                localProcessor,
                localProcessor,
                propertyName,
                "old",
                null,
                "Property removed in component definition");

        assertTrue(FlowDifferenceFilters.isStaticPropertyRemoved(difference, flowManager));
    }

    @Test
    public void testIsStaticPropertyRemovedFromDefinitionWhenDescriptorStillExists() {
        final FlowManager flowManager = Mockito.mock(FlowManager.class);
        final ProcessorNode processorNode = Mockito.mock(ProcessorNode.class);
        final ConfigurableComponent configurableComponent = Mockito.mock(ConfigurableComponent.class);

        final String propertyName = "Still Supported";
        final String instanceId = "processor-instance";

        Mockito.when(flowManager.getProcessorNode(instanceId)).thenReturn(processorNode);
        Mockito.when(processorNode.getComponent()).thenReturn(configurableComponent);
        Mockito.when(configurableComponent.getPropertyDescriptors()).thenReturn(List.of(new PropertyDescriptor.Builder().name(propertyName).build()));

        final InstantiatedVersionedProcessor localProcessor = new InstantiatedVersionedProcessor(instanceId, "group-id");
        final FlowDifference difference = new StandardFlowDifference(
                DifferenceType.PROPERTY_REMOVED,
                localProcessor,
                localProcessor,
                propertyName,
                "old",
                null,
                "Property still defined");

        assertFalse(FlowDifferenceFilters.isStaticPropertyRemoved(difference, flowManager));
    }

    @Test
    public void testIsStaticPropertyRemovedFromDefinitionWhenDynamicSupported() {
        final FlowManager flowManager = Mockito.mock(FlowManager.class);
        final ProcessorNode processorNode = Mockito.mock(ProcessorNode.class);
        final ConfigurableComponent configurableComponent = new DynamicAnnotationProcessor();

        final String propertyName = "Dynamic Property";
        final String instanceId = "processor-instance";

        Mockito.when(flowManager.getProcessorNode(instanceId)).thenReturn(processorNode);
        Mockito.when(processorNode.getComponent()).thenReturn(configurableComponent);

        final InstantiatedVersionedProcessor localProcessor = new InstantiatedVersionedProcessor(instanceId, "group-id");
        final FlowDifference difference = new StandardFlowDifference(
                DifferenceType.PROPERTY_REMOVED,
                localProcessor,
                localProcessor,
                propertyName,
                "old",
                null,
                "Dynamic property removed");
        assertFalse(FlowDifferenceFilters.isStaticPropertyRemoved(difference, flowManager));
    }

    @Test
    public void testControllerServiceCreationPairedWithPropertyAdditionIsEnvironmentalChange() {
        final FlowManager flowManager = Mockito.mock(FlowManager.class);
        final ProcessorNode processorNode = Mockito.mock(ProcessorNode.class);
        final ControllerServiceNode controllerServiceNode = Mockito.mock(ControllerServiceNode.class);

        final String processorId = "processor-instance";
        final String groupId = "group-id";
        final String propertyName = "ABC";
        final String controllerServiceId = "controller-service-id";

        Mockito.when(flowManager.getProcessorNode(processorId)).thenReturn(processorNode);
        Mockito.when(flowManager.getControllerServiceNode(controllerServiceId)).thenReturn(controllerServiceNode);

        final PropertyDescriptor propertyDescriptor = new PropertyDescriptor.Builder()
                .name(propertyName)
                .identifiesControllerService(ControllerService.class)
                .build();
        Mockito.when(processorNode.getPropertyDescriptor(propertyName)).thenReturn(propertyDescriptor);

        final InstantiatedVersionedProcessor instantiatedProcessor = new InstantiatedVersionedProcessor(processorId, groupId);
        instantiatedProcessor.setComponentType(ComponentType.PROCESSOR);

        final FlowDifference propertyDifference = new StandardFlowDifference(
                DifferenceType.PROPERTY_ADDED,
                instantiatedProcessor,
                instantiatedProcessor,
                propertyName,
                null,
                controllerServiceId,
                "Controller service reference added");

        final InstantiatedVersionedControllerService instantiatedControllerService = new InstantiatedVersionedControllerService(controllerServiceId, groupId);
        instantiatedControllerService.setComponentType(ComponentType.CONTROLLER_SERVICE);

        final FlowDifference controllerServiceDifference = new StandardFlowDifference(
                DifferenceType.COMPONENT_ADDED,
                null,
                instantiatedControllerService,
                null,
                null,
                "Controller service created");

        final FlowDifferenceFilters.EnvironmentalChangeContext context = FlowDifferenceFilters.buildEnvironmentalChangeContext(
                List.of(propertyDifference, controllerServiceDifference), flowManager);

        assertFalse(FlowDifferenceFilters.isControllerServiceCreatedForNewProperty(propertyDifference, FlowDifferenceFilters.EnvironmentalChangeContext.empty()));
        assertTrue(FlowDifferenceFilters.isControllerServiceCreatedForNewProperty(propertyDifference, context));
        assertTrue(FlowDifferenceFilters.isControllerServiceCreatedForNewProperty(controllerServiceDifference, context));

        assertFalse(FlowDifferenceFilters.isEnvironmentalChange(propertyDifference, null, flowManager));
        assertTrue(FlowDifferenceFilters.isEnvironmentalChange(propertyDifference, null, flowManager, context));
        assertTrue(FlowDifferenceFilters.isEnvironmentalChange(controllerServiceDifference, null, flowManager, context));
    }

    @Test
    public void testControllerServiceCreationEnvironmentalChangeWithoutComponentNode() {
        final FlowManager flowManager = Mockito.mock(FlowManager.class);

        final String groupId = "group-id";
        final String propertyName = "Request Rate Manager";
        final String controllerServiceInstanceId = "service-instance-id";
        final String controllerServiceVersionedId = "service-versioned-id";

        final InstantiatedVersionedControllerService controllerServiceWithNewProperty = new InstantiatedVersionedControllerService("component-instance", groupId);
        controllerServiceWithNewProperty.setComponentType(ComponentType.CONTROLLER_SERVICE);
        controllerServiceWithNewProperty.setIdentifier(controllerServiceVersionedId);
        controllerServiceWithNewProperty.setProperties(Map.of(propertyName, controllerServiceVersionedId));

        final VersionedPropertyDescriptor versionedPropertyDescriptor = new VersionedPropertyDescriptor();
        versionedPropertyDescriptor.setName(propertyName);
        versionedPropertyDescriptor.setDisplayName(propertyName);
        versionedPropertyDescriptor.setDynamic(false);
        versionedPropertyDescriptor.setIdentifiesControllerService(true);
        controllerServiceWithNewProperty.setPropertyDescriptors(Map.of(propertyName, versionedPropertyDescriptor));

        final FlowDifference propertyDifference = new StandardFlowDifference(
                DifferenceType.PROPERTY_ADDED,
                null,
                controllerServiceWithNewProperty,
                propertyName,
                null,
                controllerServiceVersionedId,
                "Controller service reference added");

        final InstantiatedVersionedControllerService instantiatedControllerService = new InstantiatedVersionedControllerService(controllerServiceInstanceId, groupId);
        instantiatedControllerService.setComponentType(ComponentType.CONTROLLER_SERVICE);
        instantiatedControllerService.setIdentifier(controllerServiceVersionedId);

        final FlowDifference controllerServiceDifference = new StandardFlowDifference(
                DifferenceType.COMPONENT_ADDED,
                null,
                instantiatedControllerService,
                null,
                null,
                "Controller service created");

        final FlowDifferenceFilters.EnvironmentalChangeContext context = FlowDifferenceFilters.buildEnvironmentalChangeContext(
                List.of(propertyDifference, controllerServiceDifference), flowManager);

        assertTrue(FlowDifferenceFilters.isEnvironmentalChange(propertyDifference, null, flowManager, context));
        assertTrue(FlowDifferenceFilters.isEnvironmentalChange(controllerServiceDifference, null, flowManager, context));
    }

    @Test
    public void testPropertyRenameWithParameterizationObservedAsEnvironmentalChange() {
        final FlowManager flowManager = Mockito.mock(FlowManager.class);
        final ProcessorNode processorNode = Mockito.mock(ProcessorNode.class);

        final String processorInstanceId = "processor-instance";

        Mockito.when(flowManager.getProcessorNode(processorInstanceId)).thenReturn(processorNode);

        final PropertyDescriptor renamedDescriptor = new PropertyDescriptor.Builder()
                .name("Access Key ID")
                .build();

        Mockito.when(processorNode.getPropertyDescriptor("Access Key ID")).thenReturn(renamedDescriptor);
        Mockito.when(processorNode.getPropertyDescriptor("Access Key")).thenReturn(null);

        final VersionedProcessor versionedProcessorA = new VersionedProcessor();
        versionedProcessorA.setComponentType(ComponentType.PROCESSOR);
        versionedProcessorA.setIdentifier("versioned-id");
        versionedProcessorA.setProperties(Map.of("Access Key", "#{AWS Access Key ID}"));

        final InstantiatedVersionedProcessor instantiatedProcessorB = new InstantiatedVersionedProcessor(processorInstanceId, "group-id");
        instantiatedProcessorB.setComponentType(ComponentType.PROCESSOR);
        instantiatedProcessorB.setIdentifier("versioned-id");
        instantiatedProcessorB.setProperties(Map.of("Access Key ID", "#{AWS Access Key ID}"));

        final FlowDifference parameterizationRemoved = new StandardFlowDifference(
                DifferenceType.PROPERTY_PARAMETERIZATION_REMOVED,
                versionedProcessorA,
                instantiatedProcessorB,
                "Access Key ID",
                null,
                null,
                "Property parameterization removed for Access Key");

        final FlowDifference parameterized = new StandardFlowDifference(
                DifferenceType.PROPERTY_PARAMETERIZED,
                versionedProcessorA,
                instantiatedProcessorB,
                "Access Key ID",
                null,
                null,
                "Property parameterized for Access Key ID");

        final List<FlowDifference> differences = List.of(parameterizationRemoved, parameterized);

        final FlowDifferenceFilters.EnvironmentalChangeContext context = FlowDifferenceFilters.buildEnvironmentalChangeContext(differences, flowManager);

        assertFalse(FlowDifferenceFilters.isEnvironmentalChange(parameterizationRemoved, null, flowManager));
        assertTrue(FlowDifferenceFilters.isEnvironmentalChange(parameterizationRemoved, null, flowManager, context));
        assertTrue(FlowDifferenceFilters.isEnvironmentalChange(parameterized, null, flowManager, context));
    }

    @Test
    public void testSelectedRelationshipChangeForNewRelationshipObservedAsEnvironmentalChange() {
        final FlowManager flowManager = Mockito.mock(FlowManager.class);
        final ProcessGroup processGroup = Mockito.mock(ProcessGroup.class);
        final Connection connection = Mockito.mock(Connection.class);
        final ProcessorNode processorNode = Mockito.mock(ProcessorNode.class);

        final String connectionInstanceId = "connection-instance";
        final String connectionGroupId = "group-id";
        final String processorInstanceId = "processor-instance";
        final String existingRelationshipName = "retry";
        final String newRelationshipName = "restrictions changed";

        final VersionedConnection connectionA = new VersionedConnection();
        connectionA.setSelectedRelationships(Set.of(existingRelationshipName));

        final InstantiatedVersionedConnection connectionB = new InstantiatedVersionedConnection(connectionInstanceId, connectionGroupId);
        connectionB.setSelectedRelationships(Set.of(existingRelationshipName, newRelationshipName));

        final FlowDifference difference = new StandardFlowDifference(
                DifferenceType.SELECTED_RELATIONSHIPS_CHANGED,
                connectionA,
                connectionB,
                null,
                Set.of(existingRelationshipName),
                Set.of(existingRelationshipName, newRelationshipName),
                "Selected relationships updated");

        final Relationship newRelationship = new Relationship.Builder().name(newRelationshipName).build();

        Mockito.when(flowManager.getGroup(connectionGroupId)).thenReturn(processGroup);
        Mockito.when(processGroup.getConnection(connectionInstanceId)).thenReturn(connection);
        Mockito.when(connection.getIdentifier()).thenReturn(connectionInstanceId);
        Mockito.when(connection.getSource()).thenReturn(processorNode);

        Mockito.when(processorNode.getIdentifier()).thenReturn(processorInstanceId);
        Mockito.when(processorNode.getConnectableType()).thenReturn(ConnectableType.PROCESSOR);
        Mockito.when(flowManager.getProcessorNode(processorInstanceId)).thenReturn(processorNode);
        Mockito.when(processorNode.getRelationship(newRelationshipName)).thenReturn(newRelationship);
        Mockito.when(processorNode.isAutoTerminated(newRelationship)).thenReturn(false);
        Mockito.when(processorNode.getProcessor()).thenReturn(new NonDynamicProcessor());
        Mockito.when(processorNode.getConnections(newRelationship)).thenReturn(new HashSet<>(Set.of(connection)));

        assertTrue(FlowDifferenceFilters.isEnvironmentalChange(difference, null, flowManager));
    }

    @Test
    public void testPropertyRenameWithMatchingValueObservedAsEnvironmentalChange() {
        final FlowManager flowManager = Mockito.mock(FlowManager.class);
        final ProcessorNode processorNode = Mockito.mock(ProcessorNode.class);

        final String processorInstanceId = "processor-instance";

        Mockito.when(flowManager.getProcessorNode(processorInstanceId)).thenReturn(processorNode);

        final String groupId = "group-id";
        final String versionedId = "versioned-id";
        final String controllerServiceId = "service-id";
        final String legacyPropertyName = "box-client-service";
        final String renamedPropertyName = "Box Client Service";

        final VersionedProcessor versionedProcessor = new VersionedProcessor();
        versionedProcessor.setComponentType(ComponentType.PROCESSOR);
        versionedProcessor.setIdentifier(versionedId);
        versionedProcessor.setProperties(Map.of(legacyPropertyName, controllerServiceId));

        final InstantiatedVersionedProcessor instantiatedProcessor = new InstantiatedVersionedProcessor(processorInstanceId, groupId);
        instantiatedProcessor.setComponentType(ComponentType.PROCESSOR);
        instantiatedProcessor.setIdentifier(versionedId);
        instantiatedProcessor.setProperties(Map.of(renamedPropertyName, controllerServiceId));

        final FlowDifference propertyRemoved = new StandardFlowDifference(
                DifferenceType.PROPERTY_REMOVED,
                versionedProcessor,
                instantiatedProcessor,
                legacyPropertyName,
                controllerServiceId,
                null,
                "Legacy property removed");

        final FlowDifference propertyAdded = new StandardFlowDifference(
                DifferenceType.PROPERTY_ADDED,
                versionedProcessor,
                instantiatedProcessor,
                renamedPropertyName,
                null,
                controllerServiceId,
                "Renamed property added");

        final List<FlowDifference> differences = List.of(propertyRemoved, propertyAdded);

        final FlowDifferenceFilters.EnvironmentalChangeContext context = FlowDifferenceFilters.buildEnvironmentalChangeContext(differences, flowManager);

        assertTrue(FlowDifferenceFilters.isEnvironmentalChange(propertyRemoved, null, flowManager, context));
        assertTrue(FlowDifferenceFilters.isEnvironmentalChange(propertyAdded, null, flowManager, context));
    }

    @Test
    public void testSelectedRelationshipChangeNotEnvironmentalWhenRelationshipUsedElsewhere() {
        final FlowManager flowManager = Mockito.mock(FlowManager.class);
        final ProcessGroup processGroup = Mockito.mock(ProcessGroup.class);
        final Connection connection = Mockito.mock(Connection.class);
        final Connection otherConnection = Mockito.mock(Connection.class);
        final ProcessorNode processorNode = Mockito.mock(ProcessorNode.class);

        final String connectionInstanceId = "connection-instance";
        final String otherConnectionId = "connection-other";
        final String connectionGroupId = "group-id";
        final String processorInstanceId = "processor-instance";
        final String existingRelationshipName = "retry";
        final String newRelationshipName = "restrictions changed";

        final VersionedConnection connectionA = new VersionedConnection();
        connectionA.setSelectedRelationships(Set.of(existingRelationshipName));

        final InstantiatedVersionedConnection connectionB = new InstantiatedVersionedConnection(connectionInstanceId, connectionGroupId);
        connectionB.setSelectedRelationships(Set.of(existingRelationshipName, newRelationshipName));

        final FlowDifference difference = new StandardFlowDifference(
                DifferenceType.SELECTED_RELATIONSHIPS_CHANGED,
                connectionA,
                connectionB,
                null,
                Set.of(existingRelationshipName),
                Set.of(existingRelationshipName, newRelationshipName),
                "Selected relationships updated");

        final Relationship newRelationship = new Relationship.Builder().name(newRelationshipName).build();

        Mockito.when(flowManager.getGroup(connectionGroupId)).thenReturn(processGroup);
        Mockito.when(processGroup.getConnection(connectionInstanceId)).thenReturn(connection);
        Mockito.when(connection.getIdentifier()).thenReturn(connectionInstanceId);
        Mockito.when(connection.getSource()).thenReturn(processorNode);

        Mockito.when(otherConnection.getIdentifier()).thenReturn(otherConnectionId);

        Mockito.when(processorNode.getIdentifier()).thenReturn(processorInstanceId);
        Mockito.when(processorNode.getConnectableType()).thenReturn(ConnectableType.PROCESSOR);
        Mockito.when(flowManager.getProcessorNode(processorInstanceId)).thenReturn(processorNode);
        Mockito.when(processorNode.getRelationship(newRelationshipName)).thenReturn(newRelationship);
        Mockito.when(processorNode.isAutoTerminated(newRelationship)).thenReturn(false);
        Mockito.when(processorNode.getProcessor()).thenReturn(new NonDynamicProcessor());
        Mockito.when(processorNode.getConnections(newRelationship)).thenReturn(new HashSet<>(Set.of(connection, otherConnection)));

        assertFalse(FlowDifferenceFilters.isEnvironmentalChange(difference, null, flowManager));
    }

    @Test
    public void testSelectedRelationshipChangeNotEnvironmentalWhenProcessorHasDynamicRelationships() {
        final FlowManager flowManager = Mockito.mock(FlowManager.class);
        final ProcessGroup processGroup = Mockito.mock(ProcessGroup.class);
        final Connection connection = Mockito.mock(Connection.class);
        final ProcessorNode processorNode = Mockito.mock(ProcessorNode.class);

        final String connectionInstanceId = "connection-instance";
        final String connectionGroupId = "group-id";
        final String processorInstanceId = "processor-instance";
        final String existingRelationshipName = "retry";
        final String newRelationshipName = "dynamic";

        final VersionedConnection connectionA = new VersionedConnection();
        connectionA.setSelectedRelationships(Set.of(existingRelationshipName));

        final InstantiatedVersionedConnection connectionB = new InstantiatedVersionedConnection(connectionInstanceId, connectionGroupId);
        connectionB.setSelectedRelationships(Set.of(existingRelationshipName, newRelationshipName));

        final FlowDifference difference = new StandardFlowDifference(
                DifferenceType.SELECTED_RELATIONSHIPS_CHANGED,
                connectionA,
                connectionB,
                null,
                Set.of(existingRelationshipName),
                Set.of(existingRelationshipName, newRelationshipName),
                "Selected relationships updated");

        final Relationship newRelationship = new Relationship.Builder().name(newRelationshipName).build();

        Mockito.when(flowManager.getGroup(connectionGroupId)).thenReturn(processGroup);
        Mockito.when(processGroup.getConnection(connectionInstanceId)).thenReturn(connection);
        Mockito.when(connection.getIdentifier()).thenReturn(connectionInstanceId);
        Mockito.when(connection.getSource()).thenReturn(processorNode);

        Mockito.when(processorNode.getIdentifier()).thenReturn(processorInstanceId);
        Mockito.when(processorNode.getConnectableType()).thenReturn(ConnectableType.PROCESSOR);
        Mockito.when(flowManager.getProcessorNode(processorInstanceId)).thenReturn(processorNode);
        Mockito.when(processorNode.getRelationship(newRelationshipName)).thenReturn(newRelationship);
        Mockito.when(processorNode.isAutoTerminated(newRelationship)).thenReturn(false);
        Mockito.when(processorNode.getConnections(newRelationship)).thenReturn(new HashSet<>(Set.of(connection)));
        Mockito.when(processorNode.getProcessor()).thenReturn(new DynamicRelationshipProcessor());

        assertFalse(FlowDifferenceFilters.isEnvironmentalChange(difference, null, flowManager));
    }

    @Test
    public void testSelectedRelationshipChangeWithRemovedRelationshipObservedAsEnvironmentalChange() {
        final FlowManager flowManager = Mockito.mock(FlowManager.class);
        final ProcessGroup processGroup = Mockito.mock(ProcessGroup.class);
        final Connection connection = Mockito.mock(Connection.class);
        final ProcessorNode processorNode = Mockito.mock(ProcessorNode.class);

        final String connectionInstanceId = "connection-instance";
        final String connectionGroupId = "group-id";
        final String processorInstanceId = "processor-instance";
        final String oldRelationshipName = "old";
        final String firstNewRelationshipName = "newA";
        final String secondNewRelationshipName = "newB";

        final VersionedConnection connectionA = new VersionedConnection();
        connectionA.setSelectedRelationships(Set.of(oldRelationshipName));

        final InstantiatedVersionedConnection connectionB = new InstantiatedVersionedConnection(connectionInstanceId, connectionGroupId);
        connectionB.setSelectedRelationships(Set.of(firstNewRelationshipName, secondNewRelationshipName));

        final FlowDifference difference = new StandardFlowDifference(
                DifferenceType.SELECTED_RELATIONSHIPS_CHANGED,
                connectionA,
                connectionB,
                null,
                Set.of(oldRelationshipName),
                Set.of(firstNewRelationshipName, secondNewRelationshipName),
                "Selected relationships updated");

        final Relationship firstNewRelationship = new Relationship.Builder().name(firstNewRelationshipName).build();
        final Relationship secondNewRelationship = new Relationship.Builder().name(secondNewRelationshipName).build();

        Mockito.when(flowManager.getGroup(connectionGroupId)).thenReturn(processGroup);
        Mockito.when(processGroup.getConnection(connectionInstanceId)).thenReturn(connection);
        Mockito.when(connection.getIdentifier()).thenReturn(connectionInstanceId);
        Mockito.when(connection.getSource()).thenReturn(processorNode);

        Mockito.when(processorNode.getIdentifier()).thenReturn(processorInstanceId);
        Mockito.when(processorNode.getConnectableType()).thenReturn(ConnectableType.PROCESSOR);
        Mockito.when(flowManager.getProcessorNode(processorInstanceId)).thenReturn(processorNode);
        Mockito.when(processorNode.getProcessor()).thenReturn(new NonDynamicProcessor());
        Mockito.when(processorNode.getRelationship(firstNewRelationshipName)).thenReturn(firstNewRelationship);
        Mockito.when(processorNode.getRelationship(secondNewRelationshipName)).thenReturn(secondNewRelationship);
        Mockito.when(processorNode.getRelationship(oldRelationshipName)).thenReturn(null);
        Mockito.when(processorNode.isAutoTerminated(firstNewRelationship)).thenReturn(false);
        Mockito.when(processorNode.isAutoTerminated(secondNewRelationship)).thenReturn(false);
        Mockito.when(processorNode.getConnections(firstNewRelationship)).thenReturn(new HashSet<>(Set.of(connection)));
        Mockito.when(processorNode.getConnections(secondNewRelationship)).thenReturn(new HashSet<>(Set.of(connection)));

        assertTrue(FlowDifferenceFilters.isEnvironmentalChange(difference, null, flowManager));
    }

    @DynamicProperty(name = "Dynamic Property", value = "Value", description = "Allows dynamic properties")
    private static class DynamicAnnotationProcessor extends AbstractProcessor {
        @Override
        public void onTrigger(final ProcessContext context, final ProcessSession session) {
            // No-op for testing
        }
    }

    private static class NonDynamicProcessor extends AbstractProcessor {
        @Override
        public void onTrigger(final ProcessContext context, final ProcessSession session) {
            // No-op for testing
        }
    }

    @DynamicRelationship(name = "dynamic", description = "dynamic")
    private static class DynamicRelationshipProcessor extends AbstractProcessor {
        @Override
        public void onTrigger(final ProcessContext context, final ProcessSession session) {
            // No-op for testing
        }
    }
}
