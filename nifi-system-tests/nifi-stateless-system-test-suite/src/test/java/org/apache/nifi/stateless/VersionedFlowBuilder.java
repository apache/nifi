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

package org.apache.nifi.stateless;

import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.ComponentType;
import org.apache.nifi.flow.ConnectableComponent;
import org.apache.nifi.flow.ConnectableComponentType;
import org.apache.nifi.flow.ControllerServiceAPI;
import org.apache.nifi.flow.PortType;
import org.apache.nifi.flow.Position;
import org.apache.nifi.flow.ScheduledState;
import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.apache.nifi.registry.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedPort;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flow.VersionedPropertyDescriptor;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

public class VersionedFlowBuilder {
    private VersionedFlowSnapshot flowSnapshot;
    private VersionedProcessGroup rootGroup;

    public VersionedFlowBuilder() {
         rootGroup = new VersionedProcessGroup();
         rootGroup.setIdentifier("root");
         rootGroup.setName("root");

         flowSnapshot = new VersionedFlowSnapshot();
         flowSnapshot.setParameterContexts(new HashMap<>());
         flowSnapshot.setFlowContents(rootGroup);
    }

    public VersionedProcessGroup createProcessGroup(final String name) {
        return createProcessGroup(name, rootGroup);
    }

    public VersionedProcessGroup createProcessGroup(final String name, final VersionedProcessGroup group) {
        final VersionedProcessGroup childGroup = new VersionedProcessGroup();
        childGroup.setIdentifier(UUID.randomUUID().toString());
        childGroup.setName(name);
        childGroup.setGroupIdentifier(group.getIdentifier());

        group.getProcessGroups().add(childGroup);
        return childGroup;
    }

    public VersionedProcessor createProcessor(final Bundle bundle, final String type) {
        return createProcessor(bundle, type, rootGroup);
    }

    public VersionedPort createOutputPort(final String portName) {
        return createOutputPort(portName, rootGroup);
    }

    public VersionedPort createOutputPort(final String portName, final VersionedProcessGroup group) {
        final VersionedPort port = new VersionedPort();
        port.setAllowRemoteAccess(false);
        port.setComponentType(ComponentType.OUTPUT_PORT);
        port.setConcurrentlySchedulableTaskCount(1);
        port.setGroupIdentifier(group.getIdentifier());
        port.setIdentifier(UUID.randomUUID().toString());
        port.setName(portName);
        port.setPosition(new Position(0, 0));
        port.setScheduledState(ScheduledState.ENABLED);
        port.setType(PortType.OUTPUT_PORT);

        group.getOutputPorts().add(port);
        return port;
    }

    public VersionedPort createInputPort(final String portName) {
        return createInputPort(portName, rootGroup);
    }

    public VersionedPort createInputPort(final String portName, final VersionedProcessGroup group) {
        final VersionedPort port = new VersionedPort();
        port.setAllowRemoteAccess(false);
        port.setComponentType(ComponentType.INPUT_PORT);
        port.setConcurrentlySchedulableTaskCount(1);
        port.setGroupIdentifier(group.getIdentifier());
        port.setIdentifier(UUID.randomUUID().toString());
        port.setName(portName);
        port.setPosition(new Position(0, 0));
        port.setScheduledState(ScheduledState.ENABLED);
        port.setType(PortType.INPUT_PORT);

        group.getInputPorts().add(port);
        return port;
    }

    public VersionedProcessor createSimpleProcessor(final String simpleType) {
        return createSimpleProcessor(simpleType, rootGroup);
    }

    public VersionedProcessor createSimpleProcessor(final String simpleType, final VersionedProcessGroup group) {
        return createProcessor(StatelessSystemIT.SYSTEM_TEST_EXTENSIONS_BUNDLE, "org.apache.nifi.processors.tests.system." + simpleType, group);
    }

    public VersionedProcessor createProcessor(final Bundle bundle, final String type, final VersionedProcessGroup group) {
        final VersionedProcessor processor = new VersionedProcessor();
        processor.setBundle(bundle);
        processor.setIdentifier(UUID.randomUUID().toString());
        processor.setName(substringAfterLast(type, "."));
        processor.setAutoTerminatedRelationships(Collections.emptySet());
        processor.setConcurrentlySchedulableTaskCount(1);
        processor.setBulletinLevel("WARN");
        processor.setComponentType(ComponentType.PROCESSOR);
        processor.setExecutionNode("ALL");
        processor.setGroupIdentifier(group.getIdentifier());
        processor.setPenaltyDuration("30 sec");
        processor.setPosition(new Position(0, 0));
        processor.setProperties(Collections.emptyMap());
        processor.setPropertyDescriptors(Collections.emptyMap());
        processor.setRunDurationMillis(0L);
        processor.setScheduledState(ScheduledState.ENABLED);
        processor.setSchedulingPeriod("0 sec");
        processor.setStyle(Collections.emptyMap());
        processor.setType(type);
        processor.setYieldDuration("1 sec");
        processor.setSchedulingStrategy("TIMER_DRIVEN");
        processor.setRetryCounts(0);
        processor.setBackoffMechanism("PENALIZE_FLOWFILE");
        processor.setRetriedRelationships(new HashSet<>());
        processor.setMaxBackoffPeriod("0 sec");

        group.getProcessors().add(processor);
        return processor;
    }

    public VersionedConnection createConnection(final VersionedComponent source, final VersionedComponent destination, final String relationship) {
        return createConnection(source, destination, Collections.singleton(relationship));
    }

    public VersionedConnection createConnection(final VersionedComponent source, final VersionedComponent destination, final Set<String> relationships) {
        return createConnection(source, destination, relationships, rootGroup);
    }

    public VersionedConnection createConnection(final VersionedComponent source, final VersionedComponent destination, final String relationship, final VersionedProcessGroup group) {
        return createConnection(source, destination, Collections.singleton(relationship), group);
    }

    public VersionedConnection createConnection(final VersionedComponent source, final VersionedComponent destination, final Set<String> relationships, final VersionedProcessGroup group) {
        final VersionedConnection connection = new VersionedConnection();
        connection.setBackPressureDataSizeThreshold("0 GB");
        connection.setBackPressureObjectThreshold(0L);
        connection.setBends(Collections.emptyList());
        connection.setComponentType(ComponentType.CONNECTION);
        connection.setDestination(createConnectableComponent(destination));
        connection.setFlowFileExpiration("0 sec");
        connection.setIdentifier(UUID.randomUUID().toString());
        connection.setGroupIdentifier(group.getIdentifier());
        connection.setLabelIndex(1);
        connection.setPosition(new Position(0, 0));
        connection.setPrioritizers(Collections.emptyList());
        connection.setSelectedRelationships(relationships);
        connection.setSource(createConnectableComponent(source));
        connection.setzIndex(1L);

        group.getConnections().add(connection);
        return connection;
    }

    private ConnectableComponent createConnectableComponent(final VersionedComponent component) {
        final ConnectableComponent connectable = new ConnectableComponent();
        connectable.setGroupId(component.getGroupIdentifier());
        connectable.setId(component.getIdentifier());
        connectable.setName(component.getName());
        connectable.setType(ConnectableComponentType.valueOf(component.getComponentType().name()));
        return connectable;
    }

    public VersionedParameterContext createParameterContext(final String name) {
        if (flowSnapshot.getParameterContexts().containsKey(name)) {
            throw new IllegalStateException("Parameter Context already exists with name " + name);
        }

        final VersionedParameterContext parameterContext = new VersionedParameterContext();
        parameterContext.setName(name);
        parameterContext.setParameters(new HashSet<>());

        flowSnapshot.getParameterContexts().put(name, parameterContext);
        return parameterContext;
    }


    public VersionedControllerService createSimpleControllerService(final String simpleType, final String apiSimpleType) {
        return createSimpleControllerService(simpleType, apiSimpleType, rootGroup);
    }

    public VersionedControllerService createSimpleControllerService(final String simpleType, final String apiSimpleType, final VersionedProcessGroup group) {
        return createControllerService(StatelessSystemIT.SYSTEM_TEST_EXTENSIONS_BUNDLE, "org.apache.nifi.cs.tests.system." + simpleType,
            StatelessSystemIT.SYSTEM_TEST_EXTENSIONS_BUNDLE, "org.apache.nifi.cs.tests.system." + apiSimpleType, group);
    }

    public VersionedControllerService createControllerService(final Bundle bundle, final String type, final Bundle apiBundle, final String apiType, final VersionedProcessGroup group) {
        final VersionedControllerService service = new VersionedControllerService();
        service.setBundle(bundle);
        service.setComponentType(ComponentType.CONTROLLER_SERVICE);
        service.setControllerServiceApis(Collections.singletonList(createControllerServiceAPI(apiBundle, apiType)));
        service.setGroupIdentifier(group.getIdentifier());
        service.setIdentifier(UUID.randomUUID().toString());
        service.setName(type);
        service.setPosition(new Position(0, 0));
        service.setProperties(new HashMap<>());
        service.setPropertyDescriptors(new HashMap<>());
        service.setType(type);

        group.getControllerServices().add(service);
        return service;
    }

    private ControllerServiceAPI createControllerServiceAPI(final Bundle bundle, final String type) {
        final ControllerServiceAPI serviceApi = new ControllerServiceAPI();
        serviceApi.setBundle(bundle);
        serviceApi.setType(type);
        return serviceApi;
    }

    public void addControllerServiceReference(final VersionedControllerService referencingComponent, final VersionedControllerService referenced, final String propertyName) {
        final Map<String, String> existingProperties = referencingComponent.getProperties();
        final Map<String, String> updatedProperties = new HashMap<>(existingProperties);
        updatedProperties.put(propertyName, referenced.getIdentifier());

        referencingComponent.setProperties(updatedProperties);

        final Map<String, VersionedPropertyDescriptor> existingDescriptors = referencingComponent.getPropertyDescriptors();
        final Map<String, VersionedPropertyDescriptor> updatedDescriptors = new HashMap<>(existingDescriptors);

        final VersionedPropertyDescriptor versionedPropertyDescriptor = new VersionedPropertyDescriptor();
        versionedPropertyDescriptor.setDisplayName(propertyName);
        versionedPropertyDescriptor.setName(propertyName);
        versionedPropertyDescriptor.setIdentifiesControllerService(true);
        updatedDescriptors.put(propertyName, versionedPropertyDescriptor);

        referencingComponent.setPropertyDescriptors(updatedDescriptors);
    }

    public void addControllerServiceReference(final VersionedProcessor referencingComponent, final VersionedControllerService referenced, final String propertyName) {
        final Map<String, String> existingProperties = referencingComponent.getProperties();
        final Map<String, String> updatedProperties = new HashMap<>(existingProperties);
        updatedProperties.put(propertyName, referenced.getIdentifier());

        referencingComponent.setProperties(updatedProperties);

        final Map<String, VersionedPropertyDescriptor> existingDescriptors = referencingComponent.getPropertyDescriptors();
        final Map<String, VersionedPropertyDescriptor> updatedDescriptors = new HashMap<>(existingDescriptors);

        final VersionedPropertyDescriptor versionedPropertyDescriptor = new VersionedPropertyDescriptor();
        versionedPropertyDescriptor.setDisplayName(propertyName);
        versionedPropertyDescriptor.setName(propertyName);
        versionedPropertyDescriptor.setIdentifiesControllerService(true);
        updatedDescriptors.put(propertyName, versionedPropertyDescriptor);

        referencingComponent.setPropertyDescriptors(updatedDescriptors);
    }

    public VersionedProcessGroup getRootGroup() {
        return rootGroup;
    }

    public VersionedFlowSnapshot getFlowSnapshot() {
        return flowSnapshot;
    }

    private static String substringAfterLast(final String input, final String delimiter) {
        final int loc = input.lastIndexOf(delimiter);
        if (loc < 0) {
            return input;
        }

        return input.substring(loc + delimiter.length());
    }
}
