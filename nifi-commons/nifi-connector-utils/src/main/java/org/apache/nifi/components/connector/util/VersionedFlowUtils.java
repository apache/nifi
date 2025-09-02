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

package org.apache.nifi.components.connector.util;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.ComponentType;
import org.apache.nifi.flow.ConnectableComponent;
import org.apache.nifi.flow.ConnectableComponentType;
import org.apache.nifi.flow.Position;
import org.apache.nifi.flow.ScheduledState;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedParameter;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedPort;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;

public class VersionedFlowUtils {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public static VersionedExternalFlow loadFlowFromResource(final String resourceName) {
        try (final InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(resourceName)) {
            if (in == null) {
                throw new IllegalArgumentException("Resource not found: " + resourceName);
            }

            return OBJECT_MAPPER.readValue(in, VersionedExternalFlow.class);
        } catch (final Exception e) {
            throw new IllegalStateException("Unable to load resource: " + resourceName, e);
        }
    }

    public static Optional<VersionedProcessor> findProcessor(final VersionedProcessGroup group, final Predicate<VersionedProcessor> predicate) {
        final List<VersionedProcessor> processors = findProcessors(group, predicate);
        if (processors.size() == 1) {
            return Optional.of(processors.getFirst());
        }
        return Optional.empty();
    }

    public static List<VersionedProcessor> findProcessors(final VersionedProcessGroup group, final Predicate<VersionedProcessor> predicate) {
        final List<VersionedProcessor> processors = new ArrayList<>();
        findProcessors(group, predicate, processors);
        return processors;
    }

    private static void findProcessors(final VersionedProcessGroup group, final Predicate<VersionedProcessor> predicate, final List<VersionedProcessor> processors) {
        for (final VersionedProcessor processor : group.getProcessors()) {
            if (predicate.test(processor)) {
                processors.add(processor);
            }
        }

        for (final VersionedProcessGroup childGroup : group.getProcessGroups()) {
            findProcessors(childGroup, predicate, processors);
        }
    }

    public static ConnectableComponent createConnectableComponent(final VersionedProcessor processor) {
        final ConnectableComponent component = new ConnectableComponent();
        component.setId(processor.getIdentifier());
        component.setName(processor.getName());
        component.setType(ConnectableComponentType.PROCESSOR);
        component.setGroupId(processor.getGroupIdentifier());
        return component;
    }

    public static ConnectableComponent createConnectableComponent(final VersionedPort port) {
        final ConnectableComponent component = new ConnectableComponent();
        component.setId(port.getIdentifier());
        component.setName(port.getName());
        component.setType(port.getComponentType() == ComponentType.INPUT_PORT ? ConnectableComponentType.INPUT_PORT : ConnectableComponentType.OUTPUT_PORT);
        component.setGroupId(port.getGroupIdentifier());
        return component;
    }

    public static void addConnection(final VersionedProcessGroup group, final ConnectableComponent source, final ConnectableComponent destination, final Set<String> relationships) {
        final VersionedConnection connection = new VersionedConnection();
        connection.setSource(source);
        connection.setDestination(destination);
        connection.setSelectedRelationships(relationships);
        connection.setBends(List.of());
        connection.setLabelIndex(0);
        connection.setzIndex(0L);
        connection.setGroupIdentifier(group.getIdentifier());
        connection.setLoadBalanceStrategy("DO_NOT_LOAD_BALANCE");
        connection.setBackPressureDataSizeThreshold("1 GB");
        connection.setBackPressureObjectThreshold(10000L);
        connection.setFlowFileExpiration("0 sec");
        connection.setPrioritizers(new ArrayList<>());
        connection.setComponentType(ComponentType.CONNECTION);

        Set<VersionedConnection> connections = group.getConnections();
        if (connections == null) {
            connections = new HashSet<>();
            group.setConnections(connections);
        }
        connections.add(connection);

        final String uuid = generateDeterministicUuid(group, ComponentType.CONNECTION);
        connection.setIdentifier(uuid);
    }

    public static List<VersionedConnection> findOutboundConnections(final VersionedProcessGroup group, final VersionedProcessor processor) {
        final VersionedProcessGroup processorGroup = findGroupForProcessor(group, processor);
        if (processorGroup == null) {
            return List.of();
        }

        final List<VersionedConnection> outboundConnections = new ArrayList<>();
        final Set<VersionedConnection> connections = processorGroup.getConnections();
        if (connections == null) {
            return outboundConnections;
        }

        for (final VersionedConnection connection : connections) {
            final ConnectableComponent source = connection.getSource();
            if (Objects.equals(source.getId(), processor.getIdentifier()) && source.getType() == ConnectableComponentType.PROCESSOR) {
                outboundConnections.add(connection);
            }
        }

        return outboundConnections;
    }

    public static VersionedProcessGroup findGroupForProcessor(final VersionedProcessGroup rootGroup, final VersionedProcessor processor) {
        if (rootGroup.getProcessors().contains(processor)) {
            return rootGroup;
        }

        for (final VersionedProcessGroup childGroup : rootGroup.getProcessGroups()) {
            final VersionedProcessGroup foundGroup = findGroupForProcessor(childGroup, processor);
            if (foundGroup != null) {
                return foundGroup;
            }
        }

        return null;
    }

    public static String generateDeterministicUuid(final VersionedProcessGroup group, final ComponentType componentType) {
        final int componentCount = getComponentCount(group, componentType);
        final String uuidSeed = "%s-%s-%d".formatted(group.getIdentifier(), componentType.name(), componentCount);
        return UUID.nameUUIDFromBytes(uuidSeed.getBytes(StandardCharsets.UTF_8)).toString();
    }

    private static int getComponentCount(final VersionedProcessGroup group, final ComponentType componentType) {
        final Collection<?> components = switch (componentType) {
            case PROCESSOR -> group.getProcessors();
            case INPUT_PORT -> group.getInputPorts();
            case OUTPUT_PORT -> group.getOutputPorts();
            case CONNECTION -> group.getConnections();
            case FUNNEL -> group.getFunnels();
            case LABEL -> group.getLabels();
            case PROCESS_GROUP -> group.getProcessGroups();
            case CONTROLLER_SERVICE -> group.getControllerServices();
            default -> List.of();
        };

        return components == null ? 0 : components.size();
    }

    public static VersionedProcessor addProcessor(final VersionedProcessGroup group, final String processorType, final Bundle bundle, final String name, final Position position) {
        final VersionedProcessor processor = new VersionedProcessor();

        // Generate deterministic UUID based on group and component type
        processor.setIdentifier(generateDeterministicUuid(group, ComponentType.PROCESSOR));

        processor.setName(name);
        processor.setType(processorType);
        processor.setPosition(position);
        processor.setBundle(bundle);

        // Set default processor configuration
        processor.setProperties(new HashMap<>());
        processor.setPropertyDescriptors(new HashMap<>());
        processor.setStyle(new HashMap<>());
        processor.setSchedulingPeriod("0 sec");
        processor.setSchedulingStrategy("TIMER_DRIVEN");
        processor.setExecutionNode("ALL");
        processor.setPenaltyDuration("30 sec");
        processor.setYieldDuration("1 sec");
        processor.setBulletinLevel("WARN");
        processor.setRunDurationMillis(25L);
        processor.setConcurrentlySchedulableTaskCount(1);
        processor.setAutoTerminatedRelationships(new HashSet<>());
        processor.setScheduledState(ScheduledState.ENABLED);
        processor.setRetryCount(10);
        processor.setRetriedRelationships(new HashSet<>());
        processor.setBackoffMechanism("PENALIZE_FLOWFILE");
        processor.setMaxBackoffPeriod("10 mins");
        processor.setComponentType(ComponentType.PROCESSOR);
        processor.setGroupIdentifier(group.getIdentifier());

        group.getProcessors().add(processor);
        return processor;
    }

    public static VersionedControllerService addControllerService(final VersionedProcessGroup group, final String serviceType, final Bundle bundle, final String name) {
        final VersionedControllerService controllerService = new VersionedControllerService();

        // Generate deterministic UUID based on group and component type
        controllerService.setIdentifier(generateDeterministicUuid(group, ComponentType.CONTROLLER_SERVICE));

        controllerService.setName(name);
        controllerService.setType(serviceType);
        controllerService.setBundle(bundle);
        controllerService.setComponentType(ComponentType.CONTROLLER_SERVICE);

        // Set default controller service configuration
        controllerService.setProperties(new HashMap<>());
        controllerService.setPropertyDescriptors(new HashMap<>());
        controllerService.setControllerServiceApis(new ArrayList<>());
        controllerService.setAnnotationData(null);
        controllerService.setScheduledState(ScheduledState.DISABLED);
        controllerService.setBulletinLevel("WARN");
        controllerService.setComments(null);
        controllerService.setGroupIdentifier(group.getIdentifier());

        // Initialize controller services collection if it doesn't exist
        Set<VersionedControllerService> controllerServices = group.getControllerServices();
        if (controllerServices == null) {
            controllerServices = new HashSet<>();
            group.setControllerServices(controllerServices);
        }
        controllerServices.add(controllerService);

        return controllerService;
    }

    public static Set<VersionedControllerService> getReferencedControllerServices(final VersionedProcessGroup group) {
        final Set<VersionedControllerService> referencedServices = new HashSet<>();
        collectReferencedControllerServices(group, referencedServices);
        return referencedServices;
    }

    private static void collectReferencedControllerServices(final VersionedProcessGroup group, final Set<VersionedControllerService> referencedServices) {
        final Map<String, VersionedControllerService> serviceMap = new HashMap<>();
        for (final VersionedControllerService service : group.getControllerServices()) {
            serviceMap.put(service.getIdentifier(), service);
        }

        for (final VersionedProcessor processor : group.getProcessors()) {
            for (final String propertyValue : processor.getProperties().values()) {
                final VersionedControllerService referencedService = serviceMap.get(propertyValue);
                if (referencedService != null) {
                    referencedServices.add(referencedService);
                }
            }
        }

        while (true) {
            final Set<VersionedControllerService> newlyAddedServices = new HashSet<>();

            for (final VersionedControllerService service : referencedServices) {
                for (final String propertyValue : service.getProperties().values()) {
                    final VersionedControllerService referencedService = serviceMap.get(propertyValue);
                    if (referencedService != null && !referencedServices.contains(referencedService)) {
                        newlyAddedServices.add(referencedService);
                    }
                }
            }

            referencedServices.addAll(newlyAddedServices);
            if (newlyAddedServices.isEmpty()) {
                break;
            }
        }

        for (final VersionedProcessGroup childGroup : group.getProcessGroups()) {
            collectReferencedControllerServices(childGroup, referencedServices);
        }
    }

    public static void removeControllerServiceReferences(final VersionedProcessGroup processGroup, final String serviceIdentifier) {
        for (final VersionedProcessor processor : processGroup.getProcessors()) {
            removeValuesFromMap(processor.getProperties(), serviceIdentifier);
        }

        for (final VersionedControllerService service : processGroup.getControllerServices()) {
            removeValuesFromMap(service.getProperties(), serviceIdentifier);
        }

        for (final VersionedProcessGroup childGroup : processGroup.getProcessGroups()) {
            removeControllerServiceReferences(childGroup, serviceIdentifier);
        }
    }

    private static void removeValuesFromMap(final Map<String, String> properties, final String valueToRemove) {
        final List<String> keysToRemove = new ArrayList<>();
        for (final Map.Entry<String, String> entry : properties.entrySet()) {
            if (Objects.equals(entry.getValue(), valueToRemove)) {
                keysToRemove.add(entry.getKey());
            }
        }

        keysToRemove.forEach(properties::remove);
    }

    public static void setParameterValue(final VersionedExternalFlow externalFlow, final String parameterName, final String parameterValue) {
        for (final VersionedParameterContext context : externalFlow.getParameterContexts().values()) {
            setParameterValue(context, parameterName, parameterValue);
        }
    }

    public static void setParameterValue(final VersionedParameterContext parameterContext, final String parameterName, final String parameterValue) {
        final Set<VersionedParameter> parameters = parameterContext.getParameters();
        for (final VersionedParameter parameter : parameters) {
            if (parameter.getName().equals(parameterName)) {
                parameter.setValue(parameterValue);
            }
        }
    }

    public static void removeUnreferencedControllerServices(final VersionedProcessGroup processGroup) {
        final Set<VersionedControllerService> referencedServices = getReferencedControllerServices(processGroup);
        final Set<String> referencedServiceIds = new HashSet<>();
        for (final VersionedControllerService service : referencedServices) {
            referencedServiceIds.add(service.getIdentifier());
        }

        removeUnreferencedControllerServices(processGroup, referencedServiceIds);
    }

    private static void removeUnreferencedControllerServices(final VersionedProcessGroup processGroup, final Set<String> referencedServiceIds) {
        processGroup.getControllerServices().removeIf(service -> !referencedServiceIds.contains(service.getIdentifier()));

        for (final VersionedProcessGroup childGroup : processGroup.getProcessGroups()) {
            removeUnreferencedControllerServices(childGroup, referencedServiceIds);
        }
    }
}
