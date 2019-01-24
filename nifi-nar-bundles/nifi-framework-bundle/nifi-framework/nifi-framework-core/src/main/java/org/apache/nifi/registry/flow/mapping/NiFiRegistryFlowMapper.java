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

package org.apache.nifi.registry.flow.mapping;

import org.apache.commons.lang3.ClassUtils;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.label.Label;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.registry.VariableDescriptor;
import org.apache.nifi.registry.flow.BatchSize;
import org.apache.nifi.registry.flow.Bundle;
import org.apache.nifi.registry.flow.ComponentType;
import org.apache.nifi.registry.flow.ConnectableComponent;
import org.apache.nifi.registry.flow.ConnectableComponentType;
import org.apache.nifi.registry.flow.ControllerServiceAPI;
import org.apache.nifi.registry.flow.FlowRegistry;
import org.apache.nifi.registry.flow.FlowRegistryClient;
import org.apache.nifi.registry.flow.PortType;
import org.apache.nifi.registry.flow.Position;
import org.apache.nifi.registry.flow.VersionControlInformation;
import org.apache.nifi.registry.flow.VersionedConnection;
import org.apache.nifi.registry.flow.VersionedControllerService;
import org.apache.nifi.registry.flow.VersionedFlowCoordinates;
import org.apache.nifi.registry.flow.VersionedFunnel;
import org.apache.nifi.registry.flow.VersionedLabel;
import org.apache.nifi.registry.flow.VersionedPort;
import org.apache.nifi.registry.flow.VersionedProcessGroup;
import org.apache.nifi.registry.flow.VersionedProcessor;
import org.apache.nifi.registry.flow.VersionedPropertyDescriptor;
import org.apache.nifi.registry.flow.VersionedRemoteGroupPort;
import org.apache.nifi.registry.flow.VersionedRemoteProcessGroup;
import org.apache.nifi.remote.RemoteGroupPort;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;


public class NiFiRegistryFlowMapper {

    private final ExtensionManager extensionManager;

    // We need to keep a mapping of component id to versionedComponentId as we transform these objects. This way, when
    // we call #mapConnectable, instead of generating a new UUID for the ConnectableComponent, we can lookup the 'versioned'
    // identifier based on the comopnent's actual id. We do connections last, so that all components will already have been
    // created before attempting to create the connection, where the ConnectableDTO is converted.
    private Map<String, String> versionedComponentIds = new HashMap<>();

    public NiFiRegistryFlowMapper(final ExtensionManager extensionManager) {
        this.extensionManager = extensionManager;
    }

    public InstantiatedVersionedProcessGroup mapProcessGroup(final ProcessGroup group, final ControllerServiceProvider serviceProvider, final FlowRegistryClient registryClient,
                                                             final boolean mapDescendantVersionedFlows) {
        versionedComponentIds.clear();
        final InstantiatedVersionedProcessGroup mapped = mapGroup(group, serviceProvider, registryClient, true, mapDescendantVersionedFlows);

        populateReferencedAncestorVariables(group, mapped);

        return mapped;
    }

    private void populateReferencedAncestorVariables(final ProcessGroup group, final VersionedProcessGroup versionedGroup) {
        final Set<String> ancestorVariableNames = new HashSet<>();
        populateVariableNames(group.getParent(), ancestorVariableNames);

        final Map<String, String> implicitlyDefinedVariables = new HashMap<>();
        for (final String variableName : ancestorVariableNames) {
            final boolean isReferenced = !group.getComponentsAffectedByVariable(variableName).isEmpty();
            if (isReferenced) {
                final String value = group.getVariableRegistry().getVariableValue(variableName);
                implicitlyDefinedVariables.put(variableName, value);
            }
        }

        if (!implicitlyDefinedVariables.isEmpty()) {
            // Merge the implicit variables with the explicitly defined variables for the Process Group
            // and set those as the Versioned Group's variables.
            if (versionedGroup.getVariables() != null) {
                implicitlyDefinedVariables.putAll(versionedGroup.getVariables());
            }

            versionedGroup.setVariables(implicitlyDefinedVariables);
        }
    }

    private void populateVariableNames(final ProcessGroup group, final Set<String> variableNames) {
        if (group == null) {
            return;
        }

        group.getVariableRegistry().getVariableMap().keySet().stream()
            .map(VariableDescriptor::getName)
            .forEach(variableNames::add);

        populateVariableNames(group.getParent(), variableNames);
    }


    private InstantiatedVersionedProcessGroup mapGroup(final ProcessGroup group, final ControllerServiceProvider serviceLookup, final FlowRegistryClient registryClient,
            final boolean topLevel, final boolean mapDescendantVersionedFlows) {

        final InstantiatedVersionedProcessGroup versionedGroup = new InstantiatedVersionedProcessGroup(group.getIdentifier(), group.getProcessGroupIdentifier());
        versionedGroup.setIdentifier(getId(group.getVersionedComponentId(), group.getIdentifier()));
        versionedGroup.setGroupIdentifier(getGroupId(group.getProcessGroupIdentifier()));
        versionedGroup.setName(group.getName());
        versionedGroup.setComments(group.getComments());
        versionedGroup.setPosition(mapPosition(group.getPosition()));

        // If we are at the 'top level', meaning that the given Process Group is the group that we are creating a VersionedProcessGroup for,
        // then we don't want to include the RemoteFlowCoordinates; we want to include the group contents. The RemoteFlowCoordinates will be used
        // only for a child group that is itself version controlled.
        if (!topLevel) {
            final VersionControlInformation versionControlInfo = group.getVersionControlInformation();
            if (versionControlInfo != null) {
                final VersionedFlowCoordinates coordinates = new VersionedFlowCoordinates();
                final String registryId = versionControlInfo.getRegistryIdentifier();
                final FlowRegistry registry = registryClient.getFlowRegistry(registryId);
                if (registry == null) {
                    throw new IllegalStateException("Process Group refers to a Flow Registry with ID " + registryId + " but no Flow Registry exists with that ID. Cannot resolve to a URL.");
                }

                coordinates.setRegistryUrl(registry.getURL());
                coordinates.setBucketId(versionControlInfo.getBucketIdentifier());
                coordinates.setFlowId(versionControlInfo.getFlowIdentifier());
                coordinates.setVersion(versionControlInfo.getVersion());
                versionedGroup.setVersionedFlowCoordinates(coordinates);

                // We need to register the Port ID -> Versioned Component ID's in our versionedComponentIds member variable for all input & output ports.
                // Otherwise, we will not be able to lookup the port when connecting to it.
                for (final Port port : group.getInputPorts()) {
                    getId(port.getVersionedComponentId(), port.getIdentifier());
                }
                for (final Port port : group.getOutputPorts()) {
                    getId(port.getVersionedComponentId(), port.getIdentifier());
                }

                // If the Process Group itself is remotely versioned, then we don't want to include its contents
                // because the contents are remotely managed and not part of the versioning of this Process Group
                if (!mapDescendantVersionedFlows) {
                    return versionedGroup;
                }
            }
        }

        versionedGroup.setControllerServices(group.getControllerServices(false).stream()
            .map(service -> mapControllerService(service, serviceLookup))
            .collect(Collectors.toCollection(LinkedHashSet::new)));

        versionedGroup.setFunnels(group.getFunnels().stream()
            .map(this::mapFunnel)
            .collect(Collectors.toCollection(LinkedHashSet::new)));

        versionedGroup.setInputPorts(group.getInputPorts().stream()
            .map(this::mapPort)
            .collect(Collectors.toCollection(LinkedHashSet::new)));

        versionedGroup.setOutputPorts(group.getOutputPorts().stream()
            .map(this::mapPort)
            .collect(Collectors.toCollection(LinkedHashSet::new)));

        versionedGroup.setLabels(group.getLabels().stream()
            .map(this::mapLabel)
            .collect(Collectors.toCollection(LinkedHashSet::new)));

        versionedGroup.setProcessors(group.getProcessors().stream()
            .map(processor -> mapProcessor(processor, serviceLookup))
            .collect(Collectors.toCollection(LinkedHashSet::new)));

        versionedGroup.setRemoteProcessGroups(group.getRemoteProcessGroups().stream()
            .map(this::mapRemoteProcessGroup)
            .collect(Collectors.toCollection(LinkedHashSet::new)));

        versionedGroup.setProcessGroups(group.getProcessGroups().stream()
            .map(grp -> mapGroup(grp, serviceLookup, registryClient, false, mapDescendantVersionedFlows))
            .collect(Collectors.toCollection(LinkedHashSet::new)));

        versionedGroup.setConnections(group.getConnections().stream()
            .map(this::mapConnection)
            .collect(Collectors.toCollection(LinkedHashSet::new)));

        versionedGroup.setVariables(group.getVariableRegistry().getVariableMap().entrySet().stream()
            .collect(Collectors.toMap(entry -> entry.getKey().getName(), Map.Entry::getValue)));

        return versionedGroup;
    }

    private String getId(final Optional<String> currentVersionedId, final String componentId) {
        final String versionedId;
        if (currentVersionedId.isPresent()) {
            versionedId = currentVersionedId.get();
        } else {
            versionedId = UUID.nameUUIDFromBytes(componentId.getBytes(StandardCharsets.UTF_8)).toString();
        }

        versionedComponentIds.put(componentId, versionedId);
        return versionedId;
    }

    private <E extends Exception> String getIdOrThrow(final Optional<String> currentVersionedId, final String componentId, final Supplier<E> exceptionSupplier) throws E {
        if (currentVersionedId.isPresent()) {
            return currentVersionedId.get();
        } else {
            final String resolved = versionedComponentIds.get(componentId);
            if (resolved == null) {
                throw exceptionSupplier.get();
            }

            return resolved;
        }
    }


    private String getGroupId(final String groupId) {
        return versionedComponentIds.get(groupId);
    }

    public VersionedConnection mapConnection(final Connection connection) {
        final FlowFileQueue queue = connection.getFlowFileQueue();

        final VersionedConnection versionedConnection = new InstantiatedVersionedConnection(connection.getIdentifier(), connection.getProcessGroup().getIdentifier());
        versionedConnection.setIdentifier(getId(connection.getVersionedComponentId(), connection.getIdentifier()));
        versionedConnection.setGroupIdentifier(getGroupId(connection.getProcessGroup().getIdentifier()));
        versionedConnection.setName(connection.getName());
        versionedConnection.setBackPressureDataSizeThreshold(queue.getBackPressureDataSizeThreshold());
        versionedConnection.setBackPressureObjectThreshold(queue.getBackPressureObjectThreshold());
        versionedConnection.setFlowFileExpiration(queue.getFlowFileExpiration());
        versionedConnection.setLabelIndex(connection.getLabelIndex());
        versionedConnection.setPrioritizers(queue.getPriorities().stream().map(p -> p.getClass().getName()).collect(Collectors.toList()));
        versionedConnection.setSelectedRelationships(connection.getRelationships().stream().map(Relationship::getName).collect(Collectors.toSet()));
        versionedConnection.setzIndex(connection.getZIndex());

        final FlowFileQueue flowFileQueue = connection.getFlowFileQueue();
        versionedConnection.setLoadBalanceStrategy(flowFileQueue.getLoadBalanceStrategy().name());
        versionedConnection.setPartitioningAttribute(flowFileQueue.getPartitioningAttribute());
        versionedConnection.setLoadBalanceCompression(flowFileQueue.getLoadBalanceCompression().name());

        versionedConnection.setBends(connection.getBendPoints().stream()
            .map(this::mapPosition)
            .collect(Collectors.toList()));

        versionedConnection.setSource(mapConnectable(connection.getSource()));
        versionedConnection.setDestination(mapConnectable(connection.getDestination()));

        return versionedConnection;
    }

    public ConnectableComponent mapConnectable(final Connectable connectable) {
        final ConnectableComponent component = new InstantiatedConnectableComponent(connectable.getIdentifier(), connectable.getProcessGroupIdentifier());

        final String versionedId = getIdOrThrow(connectable.getVersionedComponentId(), connectable.getIdentifier(),
            () -> new IllegalArgumentException("Unable to map Connectable Component with identifier " + connectable.getIdentifier() + " to any version-controlled component"));
        component.setId(versionedId);

        component.setComments(connectable.getComments());

        final String groupId;
        if (connectable instanceof RemoteGroupPort) {
            final RemoteGroupPort port = (RemoteGroupPort) connectable;
            final RemoteProcessGroup rpg = port.getRemoteProcessGroup();
            final Optional<String> rpgVersionedId = rpg.getVersionedComponentId();
            groupId = getIdOrThrow(rpgVersionedId, rpg.getIdentifier(),
                () -> new IllegalArgumentException("Unable to find the Versioned Component ID for Remote Process Group that " + connectable + " belongs to"));

        } else {
            groupId = getIdOrThrow(connectable.getProcessGroup().getVersionedComponentId(), connectable.getProcessGroupIdentifier(),
                () -> new IllegalArgumentException("Unable to find the Versioned Component ID for the Process Group that " + connectable + " belongs to"));
        }

        component.setGroupId(groupId);

        component.setName(connectable.getName());
        component.setType(ConnectableComponentType.valueOf(connectable.getConnectableType().name()));
        return component;
    }

    public VersionedControllerService mapControllerService(final ControllerServiceNode controllerService, final ControllerServiceProvider serviceProvider) {
        final VersionedControllerService versionedService = new InstantiatedVersionedControllerService(controllerService.getIdentifier(), controllerService.getProcessGroupIdentifier());
        versionedService.setIdentifier(getId(controllerService.getVersionedComponentId(), controllerService.getIdentifier()));
        versionedService.setGroupIdentifier(getGroupId(controllerService.getProcessGroupIdentifier()));
        versionedService.setName(controllerService.getName());
        versionedService.setAnnotationData(controllerService.getAnnotationData());
        versionedService.setBundle(mapBundle(controllerService.getBundleCoordinate()));
        versionedService.setComments(controllerService.getComments());

        versionedService.setControllerServiceApis(mapControllerServiceApis(controllerService));
        versionedService.setProperties(mapProperties(controllerService, serviceProvider));
        versionedService.setPropertyDescriptors(mapPropertyDescriptors(controllerService));
        versionedService.setType(controllerService.getCanonicalClassName());

        return versionedService;
    }

    private Map<String, String> mapProperties(final ComponentNode component, final ControllerServiceProvider serviceProvider) {
        final Map<String, String> mapped = new HashMap<>();

        component.getProperties().keySet().stream()
            .filter(property -> !property.isSensitive())
            .forEach(property -> {
                String value = component.getProperty(property);
                if (value == null) {
                    value = property.getDefaultValue();
                }

                if (value != null && property.getControllerServiceDefinition() != null) {
                    // Property references a Controller Service. Instead of storing the existing value, we want
                    // to store the Versioned Component ID of the service.
                    final ControllerServiceNode controllerService = serviceProvider.getControllerServiceNode(value);
                    if (controllerService != null) {
                        value = getId(controllerService.getVersionedComponentId(), controllerService.getIdentifier());
                    }
                }

                mapped.put(property.getName(), value);
            });

        return mapped;
    }

    private Map<String, VersionedPropertyDescriptor> mapPropertyDescriptors(final ComponentNode component) {
        final Map<String, VersionedPropertyDescriptor> descriptors = new HashMap<>();
        for (final PropertyDescriptor descriptor : component.getProperties().keySet()) {
            final VersionedPropertyDescriptor versionedDescriptor = new VersionedPropertyDescriptor();
            versionedDescriptor.setName(descriptor.getName());
            versionedDescriptor.setDisplayName(descriptor.getDisplayName());
            versionedDescriptor.setIdentifiesControllerService(descriptor.getControllerServiceDefinition() != null);
            descriptors.put(descriptor.getName(), versionedDescriptor);
        }
        return descriptors;
    }

    private Bundle mapBundle(final BundleCoordinate coordinate) {
        final Bundle versionedBundle = new Bundle();
        versionedBundle.setGroup(coordinate.getGroup());
        versionedBundle.setArtifact(coordinate.getId());
        versionedBundle.setVersion(coordinate.getVersion());
        return versionedBundle;
    }

    private List<ControllerServiceAPI> mapControllerServiceApis(final ControllerServiceNode service) {
        final Class<?> serviceClass = service.getControllerServiceImplementation().getClass();

        final Set<Class<?>> serviceApiClasses = new HashSet<>();
        // get all of it's interfaces to determine the controller service api's it implements
        final List<Class<?>> interfaces = ClassUtils.getAllInterfaces(serviceClass);
        for (final Class<?> i : interfaces) {
            // add all controller services that's not ControllerService itself
            if (ControllerService.class.isAssignableFrom(i) && !ControllerService.class.equals(i)) {
                serviceApiClasses.add(i);
            }
        }


        final List<ControllerServiceAPI> serviceApis = new ArrayList<>();
        for (final Class<?> serviceApiClass : serviceApiClasses) {
            final BundleCoordinate bundleCoordinate = extensionManager.getBundle(serviceApiClass.getClassLoader()).getBundleDetails().getCoordinate();

            final ControllerServiceAPI serviceApi = new ControllerServiceAPI();
            serviceApi.setType(serviceApiClass.getName());
            serviceApi.setBundle(mapBundle(bundleCoordinate));
            serviceApis.add(serviceApi);
        }
        return serviceApis;
    }


    public VersionedFunnel mapFunnel(final Funnel funnel) {
        final VersionedFunnel versionedFunnel = new InstantiatedVersionedFunnel(funnel.getIdentifier(), funnel.getProcessGroupIdentifier());
        versionedFunnel.setIdentifier(getId(funnel.getVersionedComponentId(), funnel.getIdentifier()));
        versionedFunnel.setGroupIdentifier(getGroupId(funnel.getProcessGroupIdentifier()));
        versionedFunnel.setPosition(mapPosition(funnel.getPosition()));

        return versionedFunnel;
    }

    public VersionedLabel mapLabel(final Label label) {
        final VersionedLabel versionedLabel = new InstantiatedVersionedLabel(label.getIdentifier(), label.getProcessGroupIdentifier());
        versionedLabel.setIdentifier(getId(label.getVersionedComponentId(), label.getIdentifier()));
        versionedLabel.setGroupIdentifier(getGroupId(label.getProcessGroupIdentifier()));
        versionedLabel.setHeight(label.getSize().getHeight());
        versionedLabel.setWidth(label.getSize().getWidth());
        versionedLabel.setLabel(label.getValue());
        versionedLabel.setPosition(mapPosition(label.getPosition()));
        versionedLabel.setStyle(label.getStyle());

        return versionedLabel;
    }

    public VersionedPort mapPort(final Port port) {
        final VersionedPort versionedPort = new InstantiatedVersionedPort(port.getIdentifier(), port.getProcessGroupIdentifier());
        versionedPort.setIdentifier(getId(port.getVersionedComponentId(), port.getIdentifier()));
        versionedPort.setGroupIdentifier(getGroupId(port.getProcessGroupIdentifier()));
        versionedPort.setComments(port.getComments());
        versionedPort.setConcurrentlySchedulableTaskCount(port.getMaxConcurrentTasks());
        versionedPort.setName(port.getName());
        versionedPort.setPosition(mapPosition(port.getPosition()));
        versionedPort.setType(PortType.valueOf(port.getConnectableType().name()));
        return versionedPort;
    }

    public Position mapPosition(final org.apache.nifi.connectable.Position pos) {
        final Position position = new Position();
        position.setX(pos.getX());
        position.setY(pos.getY());
        return position;
    }

    public VersionedProcessor mapProcessor(final ProcessorNode procNode, final ControllerServiceProvider serviceProvider) {
        final VersionedProcessor processor = new InstantiatedVersionedProcessor(procNode.getIdentifier(), procNode.getProcessGroupIdentifier());
        processor.setIdentifier(getId(procNode.getVersionedComponentId(), procNode.getIdentifier()));
        processor.setGroupIdentifier(getGroupId(procNode.getProcessGroupIdentifier()));
        processor.setType(procNode.getCanonicalClassName());
        processor.setAnnotationData(procNode.getAnnotationData());
        processor.setAutoTerminatedRelationships(procNode.getAutoTerminatedRelationships().stream().map(Relationship::getName).collect(Collectors.toSet()));
        processor.setBulletinLevel(procNode.getBulletinLevel().name());
        processor.setBundle(mapBundle(procNode.getBundleCoordinate()));
        processor.setComments(procNode.getComments());
        processor.setConcurrentlySchedulableTaskCount(procNode.getMaxConcurrentTasks());
        processor.setExecutionNode(procNode.getExecutionNode().name());
        processor.setName(procNode.getName());
        processor.setPenaltyDuration(procNode.getPenalizationPeriod());
        processor.setPosition(mapPosition(procNode.getPosition()));
        processor.setProperties(mapProperties(procNode, serviceProvider));
        processor.setPropertyDescriptors(mapPropertyDescriptors(procNode));
        processor.setRunDurationMillis(procNode.getRunDuration(TimeUnit.MILLISECONDS));
        processor.setSchedulingPeriod(procNode.getSchedulingPeriod());
        processor.setSchedulingStrategy(procNode.getSchedulingStrategy().name());
        processor.setStyle(procNode.getStyle());
        processor.setYieldDuration(procNode.getYieldPeriod());

        return processor;
    }

    public VersionedRemoteProcessGroup mapRemoteProcessGroup(final RemoteProcessGroup remoteGroup) {
        final VersionedRemoteProcessGroup rpg = new InstantiatedVersionedRemoteProcessGroup(remoteGroup.getIdentifier(), remoteGroup.getProcessGroupIdentifier());
        rpg.setIdentifier(getId(remoteGroup.getVersionedComponentId(), remoteGroup.getIdentifier()));
        rpg.setGroupIdentifier(getGroupId(remoteGroup.getProcessGroupIdentifier()));
        rpg.setComments(remoteGroup.getComments());
        rpg.setCommunicationsTimeout(remoteGroup.getCommunicationsTimeout());
        rpg.setLocalNetworkInterface(remoteGroup.getNetworkInterface());
        rpg.setName(remoteGroup.getName());
        rpg.setInputPorts(remoteGroup.getInputPorts().stream()
            .map(port -> mapRemotePort(port, ComponentType.REMOTE_INPUT_PORT))
            .collect(Collectors.toSet()));
        rpg.setOutputPorts(remoteGroup.getOutputPorts().stream()
            .map(port -> mapRemotePort(port, ComponentType.REMOTE_OUTPUT_PORT))
            .collect(Collectors.toSet()));
        rpg.setPosition(mapPosition(remoteGroup.getPosition()));
        rpg.setProxyHost(remoteGroup.getProxyHost());
        rpg.setProxyPort(remoteGroup.getProxyPort());
        rpg.setProxyUser(remoteGroup.getProxyUser());
        rpg.setTargetUri(remoteGroup.getTargetUri());
        rpg.setTargetUris(remoteGroup.getTargetUris());
        rpg.setTransportProtocol(remoteGroup.getTransportProtocol().name());
        rpg.setYieldDuration(remoteGroup.getYieldDuration());
        return rpg;
    }

    public VersionedRemoteGroupPort mapRemotePort(final RemoteGroupPort remotePort, final ComponentType componentType) {
        final VersionedRemoteGroupPort port = new InstantiatedVersionedRemoteGroupPort(remotePort.getIdentifier(), remotePort.getRemoteProcessGroup().getIdentifier());
        port.setIdentifier(getId(remotePort.getVersionedComponentId(), remotePort.getIdentifier()));
        port.setGroupIdentifier(getGroupId(remotePort.getRemoteProcessGroup().getIdentifier()));
        port.setComments(remotePort.getComments());
        port.setConcurrentlySchedulableTaskCount(remotePort.getMaxConcurrentTasks());
        port.setRemoteGroupId(getGroupId(remotePort.getRemoteProcessGroup().getIdentifier()));
        port.setName(remotePort.getName());
        port.setUseCompression(remotePort.isUseCompression());
        port.setBatchSize(mapBatchSettings(remotePort));
        port.setTargetId(remotePort.getTargetIdentifier());
        port.setComponentType(componentType);
        return port;
    }

    private BatchSize mapBatchSettings(final RemoteGroupPort remotePort) {
        final BatchSize batchSize = new BatchSize();
        batchSize.setCount(remotePort.getBatchCount());
        batchSize.setDuration(remotePort.getBatchDuration());
        batchSize.setSize(remotePort.getBatchSize());
        return batchSize;
    }
}
