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
import org.apache.nifi.components.resource.ResourceCardinality;
import org.apache.nifi.components.resource.ResourceDefinition;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.ComponentNode;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.ParameterProviderNode;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.PropertyConfiguration;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.label.Label;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.flow.BatchSize;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.ComponentType;
import org.apache.nifi.flow.ConnectableComponent;
import org.apache.nifi.flow.ConnectableComponentType;
import org.apache.nifi.flow.ControllerServiceAPI;
import org.apache.nifi.flow.ExternalControllerServiceReference;
import org.apache.nifi.flow.ParameterProviderReference;
import org.apache.nifi.flow.PortType;
import org.apache.nifi.flow.Position;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedFlowCoordinates;
import org.apache.nifi.flow.VersionedFlowRegistryClient;
import org.apache.nifi.flow.VersionedFunnel;
import org.apache.nifi.flow.VersionedLabel;
import org.apache.nifi.flow.VersionedParameter;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedParameterProvider;
import org.apache.nifi.flow.VersionedPort;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flow.VersionedPropertyDescriptor;
import org.apache.nifi.flow.VersionedRemoteGroupPort;
import org.apache.nifi.flow.VersionedRemoteProcessGroup;
import org.apache.nifi.flow.VersionedReportingTask;
import org.apache.nifi.flow.VersionedResourceCardinality;
import org.apache.nifi.flow.VersionedResourceDefinition;
import org.apache.nifi.flow.VersionedResourceType;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterDescriptor;
import org.apache.nifi.parameter.ParameterProvider;
import org.apache.nifi.parameter.ParameterProviderConfiguration;
import org.apache.nifi.parameter.ParameterReferencedControllerServiceData;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.registry.VariableDescriptor;
import org.apache.nifi.registry.flow.FlowRegistryClientNode;
import org.apache.nifi.registry.flow.VersionControlInformation;
import org.apache.nifi.remote.PublicPort;
import org.apache.nifi.remote.RemoteGroupPort;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class NiFiRegistryFlowMapper {
    private static final String ENCRYPTED_PREFIX = "enc{";
    private static final String ENCRYPTED_SUFFIX = "}";

    private final ExtensionManager extensionManager;
    private final FlowMappingOptions flowMappingOptions;

    // We need to keep a mapping of component id to versionedComponentId as we transform these objects. This way, when
    // we call #mapConnectable, instead of generating a new UUID for the ConnectableComponent, we can lookup the 'versioned'
    // identifier based on the component's actual id. We do connections last, so that all components will already have been
    // created before attempting to create the connection, where the ConnectableDTO is converted.
    private Map<String, String> versionedComponentIds = new HashMap<>();

    public NiFiRegistryFlowMapper(final ExtensionManager extensionManager) {
        this(extensionManager, FlowMappingOptions.DEFAULT_OPTIONS);
    }

    public NiFiRegistryFlowMapper(final ExtensionManager extensionManager, final FlowMappingOptions flowMappingOptions) {
        this.extensionManager = extensionManager;
        this.flowMappingOptions = flowMappingOptions;
    }

    /**
     * Map the given process group to a versioned process group without any use of an actual flow registry even if the
     * group is currently versioned in a registry.
     *
     * @param group             the process group to map
     * @param serviceProvider   the controller service provider to use for mapping
     * @return a complete versioned process group without any registry related details
     */
    public InstantiatedVersionedProcessGroup mapNonVersionedProcessGroup(final ProcessGroup group, final ControllerServiceProvider serviceProvider) {
        versionedComponentIds.clear();

        // always include descendant flows and do not apply any registry versioning info that may be present in the group
        return mapGroup(group, serviceProvider, (processGroup, versionedGroup) -> true);
    }

    /**
     * Map the given process group to a versioned process group using the provided registry client.
     *
     * @param group             the process group to map
     * @param serviceProvider   the controller service provider to use for mapping
     * @param flowManager    the registry client to use when retrieving versioning details
     * @param mapDescendantVersionedFlows  true in order to include descendant flows in the mapped result
     * @return a complete versioned process group with applicable registry related details
     */
    public InstantiatedVersionedProcessGroup mapProcessGroup(final ProcessGroup group, final ControllerServiceProvider serviceProvider,
                                                             final FlowManager flowManager,
                                                             final boolean mapDescendantVersionedFlows) {
        versionedComponentIds.clear();

        // apply registry versioning according to the lambda below
        // NOTE: lambda refers to registry client and map descendant boolean which will not change during recursion
        return mapGroup(group, serviceProvider, (processGroup, versionedGroup) -> {
            final VersionControlInformation versionControlInfo = processGroup.getVersionControlInformation();
            if (versionControlInfo != null) {
                final VersionedFlowCoordinates coordinates = new VersionedFlowCoordinates();
                final String registryId = versionControlInfo.getRegistryIdentifier();
                final FlowRegistryClientNode registry = flowManager.getFlowRegistryClient(registryId);
                if (registry == null) {
                    throw new IllegalStateException("Process Group refers to a Flow Registry with ID " + registryId + " but no Flow Registry exists with that ID. Cannot resolve to a URL.");
                }

                if (flowMappingOptions.isMapFlowRegistryClientId()) {
                    coordinates.setRegistryId(registryId);
                }

                coordinates.setRegistryUrl(getRegistryUrl(registry));
                coordinates.setStorageLocation(versionControlInfo.getStorageLocation() == null ?getRegistryUrl(registry) : versionControlInfo.getStorageLocation());
                coordinates.setBucketId(versionControlInfo.getBucketIdentifier());
                coordinates.setFlowId(versionControlInfo.getFlowIdentifier());
                coordinates.setVersion(versionControlInfo.getVersion());
                versionedGroup.setVersionedFlowCoordinates(coordinates);

                // We need to register the Port ID -> Versioned Component ID's in our versionedComponentIds member variable for all input & output ports.
                // Otherwise, we will not be able to lookup the port when connecting to it.
                for (final Port port : processGroup.getInputPorts()) {
                    getId(port.getVersionedComponentId(), port.getIdentifier());
                }
                for (final Port port : processGroup.getOutputPorts()) {
                    getId(port.getVersionedComponentId(), port.getIdentifier());
                }

                // If the Process Group itself is remotely versioned, then we don't want to include its contents
                // because the contents are remotely managed and not part of the versioning of this Process Group
                return mapDescendantVersionedFlows;
            }
            return true;
        });
    }


    // This is specific for the {@code NifiRegistryFlowRegistryClient}, purely for backward compatibility
    private String getRegistryUrl(final FlowRegistryClientNode registry) {
        return registry.getComponentType().equals("org.apache.nifi.registry.flow.NifiRegistryFlowRegistryClient") ? registry.getRawPropertyValue(registry.getPropertyDescriptor("URL")) : "";
    }

    private InstantiatedVersionedProcessGroup mapGroup(final ProcessGroup group, final ControllerServiceProvider serviceProvider,
                                                       final BiFunction<ProcessGroup, VersionedProcessGroup, Boolean> applyVersionControlInfo) {
        final Set<String> allIncludedGroupsIds = group.findAllProcessGroups().stream()
                .map(ProcessGroup::getIdentifier)
                .collect(Collectors.toSet());
        allIncludedGroupsIds.add(group.getIdentifier());

        final Map<String, ExternalControllerServiceReference> externalControllerServiceReferences = new HashMap<>();
        final InstantiatedVersionedProcessGroup versionedGroup =
                mapGroup(group, serviceProvider, applyVersionControlInfo, true, allIncludedGroupsIds, externalControllerServiceReferences);

        populateReferencedAncestorVariables(group, versionedGroup);

        return versionedGroup;
    }

    private InstantiatedVersionedProcessGroup mapGroup(final ProcessGroup group, final ControllerServiceProvider serviceProvider,
                                                       final BiFunction<ProcessGroup, VersionedProcessGroup, Boolean> applyVersionControlInfo,
                                                       final boolean topLevel, final Set<String> includedGroupIds,
                                                       final Map<String, ExternalControllerServiceReference> externalControllerServiceReferences) {

        final InstantiatedVersionedProcessGroup versionedGroup = new InstantiatedVersionedProcessGroup(group.getIdentifier(), group.getProcessGroupIdentifier());
        versionedGroup.setIdentifier(getId(group.getVersionedComponentId(), group.getIdentifier()));
        if (flowMappingOptions.isMapInstanceIdentifiers()) {
            versionedGroup.setInstanceIdentifier(group.getIdentifier());
        }
        versionedGroup.setGroupIdentifier(getGroupId(group.getProcessGroupIdentifier()));
        versionedGroup.setName(group.getName());
        versionedGroup.setComments(group.getComments());
        versionedGroup.setPosition(mapPosition(group.getPosition()));
        versionedGroup.setFlowFileConcurrency(group.getFlowFileConcurrency().name());
        versionedGroup.setFlowFileOutboundPolicy(group.getFlowFileOutboundPolicy().name());
        versionedGroup.setDefaultFlowFileExpiration(group.getDefaultFlowFileExpiration());
        versionedGroup.setDefaultBackPressureObjectThreshold(group.getDefaultBackPressureObjectThreshold());
        versionedGroup.setDefaultBackPressureDataSizeThreshold(group.getDefaultBackPressureDataSizeThreshold());

        final ParameterContext parameterContext = group.getParameterContext();
        versionedGroup.setParameterContextName(parameterContext == null ? null : parameterContext.getName());

        // If we are at the 'top level', meaning that the given Process Group is the group that we are creating a VersionedProcessGroup for,
        // then we don't want to include the RemoteFlowCoordinates; we want to include the group contents. The RemoteFlowCoordinates will be used
        // only for a child group that is itself version controlled.
        if (!topLevel) {
            final boolean mapDescendantVersionedFlows = applyVersionControlInfo.apply(group, versionedGroup);

            // return here if we do not want to include remotely managed descendant flows
            if (!mapDescendantVersionedFlows) {
                return versionedGroup;
            }
        }

        versionedGroup.setControllerServices(group.getControllerServices(false).stream()
                .map(service -> mapControllerService(service, serviceProvider, includedGroupIds, externalControllerServiceReferences))
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
                .map(processor -> mapProcessor(processor, serviceProvider, includedGroupIds, externalControllerServiceReferences))
                .collect(Collectors.toCollection(LinkedHashSet::new)));

        versionedGroup.setRemoteProcessGroups(group.getRemoteProcessGroups().stream()
                .map(this::mapRemoteProcessGroup)
                .collect(Collectors.toCollection(LinkedHashSet::new)));

        versionedGroup.setProcessGroups(group.getProcessGroups().stream()
                .map(grp -> mapGroup(grp, serviceProvider, applyVersionControlInfo, false, includedGroupIds, externalControllerServiceReferences))
                .collect(Collectors.toCollection(LinkedHashSet::new)));

        versionedGroup.setConnections(group.getConnections().stream()
                .map(this::mapConnection)
                .collect(Collectors.toCollection(LinkedHashSet::new)));

        versionedGroup.setVariables(group.getVariableRegistry().getVariableMap().entrySet().stream()
                .collect(Collectors.toMap(entry -> entry.getKey().getName(), Map.Entry::getValue)));

        if (topLevel) {
            versionedGroup.setExternalControllerServiceReferences(externalControllerServiceReferences);
        }

        return versionedGroup;
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

    private String getId(final Optional<String> currentVersionedId, final String componentId) {
        final String versionedId = flowMappingOptions.getComponentIdLookup().getComponentId(currentVersionedId, componentId);
        versionedComponentIds.put(componentId, versionedId);
        return versionedId;
    }

    /**
     * Generate a versioned component identifier based on the given component identifier. The result for a given
     * component identifier is deterministic.
     *
     * @param componentId the component identifier to generate a versioned component identifier for
     * @return a deterministic versioned component identifier
     */
    public static String generateVersionedComponentId(final String componentId) {
        return UUID.nameUUIDFromBytes(componentId.getBytes(StandardCharsets.UTF_8)).toString();
    }

    private <E extends Exception> String getIdOrThrow(final String componentId, final Supplier<E> exceptionSupplier) throws E {
        final String resolved = versionedComponentIds.get(componentId);
        if (resolved == null) {
            throw exceptionSupplier.get();
        }

        return resolved;
    }


    public String getGroupId(final String groupId) {
        return versionedComponentIds.get(groupId);
    }

    public VersionedConnection mapConnection(final Connection connection) {
        final FlowFileQueue queue = connection.getFlowFileQueue();

        final VersionedConnection versionedConnection = new InstantiatedVersionedConnection(connection.getIdentifier(), connection.getProcessGroup().getIdentifier());
        versionedConnection.setIdentifier(getId(connection.getVersionedComponentId(), connection.getIdentifier()));
        if (flowMappingOptions.isMapInstanceIdentifiers()) {
            versionedConnection.setInstanceIdentifier(connection.getIdentifier());
        }
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

        final String versionedId = getIdOrThrow(connectable.getIdentifier(),
            () -> new IllegalArgumentException("Unable to map Connectable Component with identifier " + connectable.getIdentifier() + " to any version-controlled component"));
        component.setId(versionedId);
        if (flowMappingOptions.isMapInstanceIdentifiers()) {
            component.setInstanceIdentifier(connectable.getIdentifier());
        }

        component.setComments(connectable.getComments());

        final String groupId;
        if (connectable instanceof RemoteGroupPort) {
            final RemoteGroupPort port = (RemoteGroupPort) connectable;
            final RemoteProcessGroup rpg = port.getRemoteProcessGroup();
            groupId = getIdOrThrow(rpg.getIdentifier(),
                () -> new IllegalArgumentException("Unable to find the Versioned Component ID for Remote Process Group that " + connectable + " belongs to"));

        } else {
            groupId = getIdOrThrow(connectable.getProcessGroupIdentifier(),
                () -> new IllegalArgumentException("Unable to find the Versioned Component ID for the Process Group that " + connectable + " belongs to"));
        }

        component.setGroupId(groupId);

        component.setName(connectable.getName());
        component.setType(ConnectableComponentType.valueOf(connectable.getConnectableType().name()));
        return component;
    }

    public VersionedReportingTask mapReportingTask(final ReportingTaskNode taskNode, final ControllerServiceProvider serviceProvider) {
        final VersionedReportingTask versionedTask = new VersionedReportingTask();
        versionedTask.setIdentifier(taskNode.getIdentifier());
        if (flowMappingOptions.isMapInstanceIdentifiers()) {
            versionedTask.setInstanceIdentifier(taskNode.getIdentifier());
        }
        versionedTask.setAnnotationData(taskNode.getAnnotationData());
        versionedTask.setBundle(mapBundle(taskNode.getBundleCoordinate()));
        versionedTask.setComments(taskNode.getComments());
        versionedTask.setComponentType(ComponentType.REPORTING_TASK);
        versionedTask.setName(taskNode.getName());

        versionedTask.setProperties(mapProperties(taskNode, serviceProvider));
        versionedTask.setPropertyDescriptors(mapPropertyDescriptors(taskNode, serviceProvider, Collections.emptySet(), Collections.emptyMap()));
        versionedTask.setSchedulingPeriod(taskNode.getSchedulingPeriod());
        versionedTask.setSchedulingStrategy(taskNode.getSchedulingStrategy().name());
        versionedTask.setType(taskNode.getCanonicalClassName());
        versionedTask.setScheduledState(flowMappingOptions.getStateLookup().getState(taskNode));

        return versionedTask;
    }

    public VersionedParameterProvider mapParameterProvider(final ParameterProviderNode parameterProviderNode, final ControllerServiceProvider serviceProvider) {
        final VersionedParameterProvider versionedParameterProvider = new VersionedParameterProvider();
        versionedParameterProvider.setIdentifier(parameterProviderNode.getIdentifier());
        if (flowMappingOptions.isMapInstanceIdentifiers()) {
            versionedParameterProvider.setInstanceIdentifier(parameterProviderNode.getIdentifier());
        }
        versionedParameterProvider.setAnnotationData(parameterProviderNode.getAnnotationData());
        versionedParameterProvider.setBundle(mapBundle(parameterProviderNode.getBundleCoordinate()));
        versionedParameterProvider.setComments(parameterProviderNode.getComments());
        versionedParameterProvider.setComponentType(ComponentType.PARAMETER_PROVIDER);
        versionedParameterProvider.setName(parameterProviderNode.getName());

        versionedParameterProvider.setProperties(mapProperties(parameterProviderNode, serviceProvider));
        versionedParameterProvider.setPropertyDescriptors(mapPropertyDescriptors(parameterProviderNode, serviceProvider, Collections.emptySet(), Collections.emptyMap()));
        versionedParameterProvider.setType(parameterProviderNode.getCanonicalClassName());

        return versionedParameterProvider;
    }

    public VersionedFlowRegistryClient mapFlowRegistryClient(final FlowRegistryClientNode clientNode, final ControllerServiceProvider serviceProvider) {
        final VersionedFlowRegistryClient versionedClient = new VersionedFlowRegistryClient();
        versionedClient.setIdentifier(clientNode.getIdentifier());

        if (flowMappingOptions.isMapInstanceIdentifiers()) {
            versionedClient.setInstanceIdentifier(clientNode.getIdentifier());
        }

        versionedClient.setAnnotationData(clientNode.getAnnotationData());
        versionedClient.setBundle(mapBundle(clientNode.getBundleCoordinate()));
        versionedClient.setComponentType(ComponentType.FLOW_REGISTRY_CLIENT);
        versionedClient.setName(clientNode.getName());
        versionedClient.setDescription(clientNode.getDescription());

        versionedClient.setProperties(mapProperties(clientNode, serviceProvider));
        versionedClient.setPropertyDescriptors(mapPropertyDescriptors(clientNode, serviceProvider, Collections.emptySet(), Collections.emptyMap()));
        versionedClient.setType(clientNode.getCanonicalClassName());

        return versionedClient;
    }

    public VersionedControllerService mapControllerService(final ControllerServiceNode controllerService, final ControllerServiceProvider serviceProvider, final Set<String> includedGroupIds,
                                                           final Map<String, ExternalControllerServiceReference> externalControllerServiceReferences) {
        final VersionedControllerService versionedService = new InstantiatedVersionedControllerService(controllerService.getIdentifier(), controllerService.getProcessGroupIdentifier());
        versionedService.setIdentifier(getId(controllerService.getVersionedComponentId(), controllerService.getIdentifier()));
        if (flowMappingOptions.isMapInstanceIdentifiers()) {
            versionedService.setInstanceIdentifier(controllerService.getIdentifier());
        }
        versionedService.setGroupIdentifier(getGroupId(controllerService.getProcessGroupIdentifier()));
        versionedService.setName(controllerService.getName());
        versionedService.setAnnotationData(controllerService.getAnnotationData());
        versionedService.setBundle(mapBundle(controllerService.getBundleCoordinate()));
        versionedService.setComments(controllerService.getComments());
        versionedService.setBulletinLevel(controllerService.getBulletinLevel().name());

        versionedService.setControllerServiceApis(mapControllerServiceApis(controllerService));
        versionedService.setProperties(mapProperties(controllerService, serviceProvider));
        versionedService.setPropertyDescriptors(mapPropertyDescriptors(controllerService, serviceProvider, includedGroupIds, externalControllerServiceReferences));
        versionedService.setType(controllerService.getCanonicalClassName());
        versionedService.setScheduledState(flowMappingOptions.getStateLookup().getState(controllerService));

        return versionedService;
    }

    private Map<String, String> mapProperties(final ComponentNode component, final ControllerServiceProvider serviceProvider) {
        final Map<String, String> mapped = new HashMap<>();

        component.getProperties().keySet().stream()
            .filter(property -> isMappable(property, component.getProperty(property)))
            .forEach(property -> {
                String value = component.getRawPropertyValue(property);
                if (value == null) {
                    value = property.getDefaultValue();
                }

                if (value != null && property.getControllerServiceDefinition() != null && flowMappingOptions.isMapControllerServiceReferencesToVersionedId()) {
                    // Property references a Controller Service. Instead of storing the existing value, we want
                    // to store the Versioned Component ID of the service.
                    final ControllerServiceNode controllerService = serviceProvider.getControllerServiceNode(value);
                    if (controllerService != null) {
                        value = getId(controllerService.getVersionedComponentId(), controllerService.getIdentifier());
                    }
                }

                if (property.isSensitive()) {
                    value = encrypt(value);
                }

                mapped.put(property.getName(), value);
            });

        return mapped;
    }

    private String encrypt(final String value) {
        if (value == null) {
            return null;
        }

        final SensitiveValueEncryptor encryptor = flowMappingOptions.getSensitiveValueEncryptor();
        if (encryptor == null) {
            // This will happen only if the given property is mappable, which means that it is a parameter reference.
            return value;
        }

        final String encrypted = encryptor.encrypt(value);
        return ENCRYPTED_PREFIX + encrypted + ENCRYPTED_SUFFIX;
    }

    private boolean isMappable(final PropertyDescriptor propertyDescriptor, final PropertyConfiguration propertyConfiguration) {
        if (!propertyDescriptor.isSensitive()) { // If the property is not sensitive, it can be mapped.
            return true;
        }

        if (flowMappingOptions.isMapSensitiveConfiguration()) {
            return true;
        }

        if (propertyConfiguration == null) {
            return false;
        }

        // Sensitive properties can be mapped if and only if they reference a Parameter. If a sensitive property references a parameter, it cannot contain any other value around it.
        // For example, for a non-sensitive property, a value of "hello#{param}123" is valid, but for a sensitive property, it is invalid. Only something like "hello123" or "#{param}" is valid.
        // Thus, we will map sensitive properties only if they reference a parameter.
        return !propertyConfiguration.getParameterReferences().isEmpty();
    }

    private Map<String, VersionedPropertyDescriptor> mapPropertyDescriptors(final ComponentNode component, final ControllerServiceProvider serviceProvider, final Set<String> includedGroupIds,
                                                                            final Map<String, ExternalControllerServiceReference> externalControllerServiceReferences) {

        if (!flowMappingOptions.isMapPropertyDescriptors()) {
            return Collections.emptyMap();
        }

        final Map<String, VersionedPropertyDescriptor> descriptors = new HashMap<>();
        for (final PropertyDescriptor descriptor : component.getProperties().keySet()) {
            final VersionedPropertyDescriptor versionedDescriptor = new VersionedPropertyDescriptor();
            versionedDescriptor.setName(descriptor.getName());
            versionedDescriptor.setDisplayName(descriptor.getDisplayName());
            versionedDescriptor.setSensitive(descriptor.isSensitive());

            final VersionedResourceDefinition versionedResourceDefinition = mapResourceDefinition(descriptor.getResourceDefinition());
            versionedDescriptor.setResourceDefinition(versionedResourceDefinition);

            final Class<?> referencedServiceType = descriptor.getControllerServiceDefinition();
            versionedDescriptor.setIdentifiesControllerService(referencedServiceType != null);

            if (referencedServiceType != null) {
                final String value = component.getProperty(descriptor).getRawValue();
                if (value != null) {
                    final ControllerServiceNode serviceNode = serviceProvider.getControllerServiceNode(value);
                    if (serviceNode == null) {
                        continue;
                    }

                    final String serviceGroupId = serviceNode.getProcessGroupIdentifier();
                    if (!includedGroupIds.contains(serviceGroupId)) {
                        final String serviceId = getId(serviceNode.getVersionedComponentId(), serviceNode.getIdentifier());

                        final ExternalControllerServiceReference controllerServiceReference = new ExternalControllerServiceReference();
                        controllerServiceReference.setIdentifier(serviceId);
                        controllerServiceReference.setName(serviceNode.getName());
                        externalControllerServiceReferences.put(serviceId, controllerServiceReference);
                    }
                }
            }

            descriptors.put(descriptor.getName(), versionedDescriptor);
        }

        return descriptors;
    }

    private VersionedResourceDefinition mapResourceDefinition(final ResourceDefinition resourceDefinition) {
        if (resourceDefinition == null) {
            return null;
        }

        final ResourceCardinality cardinality = resourceDefinition.getCardinality();
        final VersionedResourceCardinality versionedCardinality = VersionedResourceCardinality.valueOf(cardinality.name());

        final Set<VersionedResourceType> versionedResourceTypes = new HashSet<>();
        resourceDefinition.getResourceTypes().forEach(resourceType -> versionedResourceTypes.add(VersionedResourceType.valueOf(resourceType.name())));

        final VersionedResourceDefinition versionedResourceDefinition = new VersionedResourceDefinition();
        versionedResourceDefinition.setCardinality(versionedCardinality);
        versionedResourceDefinition.setResourceTypes(versionedResourceTypes);
        return versionedResourceDefinition;
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
        if (flowMappingOptions.isMapInstanceIdentifiers()) {
            versionedFunnel.setInstanceIdentifier(funnel.getIdentifier());
        }
        versionedFunnel.setGroupIdentifier(getGroupId(funnel.getProcessGroupIdentifier()));
        versionedFunnel.setPosition(mapPosition(funnel.getPosition()));

        return versionedFunnel;
    }

    public VersionedLabel mapLabel(final Label label) {
        final VersionedLabel versionedLabel = new InstantiatedVersionedLabel(label.getIdentifier(), label.getProcessGroupIdentifier());
        versionedLabel.setIdentifier(getId(label.getVersionedComponentId(), label.getIdentifier()));
        if (flowMappingOptions.isMapInstanceIdentifiers()) {
            versionedLabel.setInstanceIdentifier(label.getIdentifier());
        }
        versionedLabel.setGroupIdentifier(getGroupId(label.getProcessGroupIdentifier()));
        versionedLabel.setHeight(label.getSize().getHeight());
        versionedLabel.setWidth(label.getSize().getWidth());
        versionedLabel.setLabel(label.getValue());
        versionedLabel.setPosition(mapPosition(label.getPosition()));
        versionedLabel.setStyle(label.getStyle());
        versionedLabel.setzIndex(label.getZIndex());

        return versionedLabel;
    }

    public VersionedPort mapPort(final Port port) {
        final VersionedPort versionedPort = new InstantiatedVersionedPort(port.getIdentifier(), port.getProcessGroupIdentifier());
        versionedPort.setIdentifier(getId(port.getVersionedComponentId(), port.getIdentifier()));
        if (flowMappingOptions.isMapInstanceIdentifiers()) {
            versionedPort.setInstanceIdentifier(port.getIdentifier());
        }
        versionedPort.setGroupIdentifier(getGroupId(port.getProcessGroupIdentifier()));
        versionedPort.setComments(port.getComments());
        versionedPort.setConcurrentlySchedulableTaskCount(port.getMaxConcurrentTasks());
        versionedPort.setName(port.getName());
        versionedPort.setPosition(mapPosition(port.getPosition()));
        versionedPort.setType(PortType.valueOf(port.getConnectableType().name()));
        versionedPort.setScheduledState(mapScheduledState(port.getScheduledState()));
        versionedPort.setAllowRemoteAccess(port instanceof PublicPort);
        versionedPort.setScheduledState(flowMappingOptions.getStateLookup().getState(port));

        return versionedPort;
    }

    public Position mapPosition(final org.apache.nifi.connectable.Position pos) {
        final Position position = new Position();
        position.setX(pos.getX());
        position.setY(pos.getY());
        return position;
    }

    public VersionedProcessor mapProcessor(final ProcessorNode procNode, final ControllerServiceProvider serviceProvider, final Set<String> includedGroupIds,
                                           final Map<String, ExternalControllerServiceReference> externalControllerServiceReferences) {
        final VersionedProcessor processor = new InstantiatedVersionedProcessor(procNode.getIdentifier(), procNode.getProcessGroupIdentifier());
        processor.setIdentifier(getId(procNode.getVersionedComponentId(), procNode.getIdentifier()));
        if (flowMappingOptions.isMapInstanceIdentifiers()) {
            processor.setInstanceIdentifier(procNode.getIdentifier());
        }
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
        processor.setPropertyDescriptors(mapPropertyDescriptors(procNode, serviceProvider, includedGroupIds, externalControllerServiceReferences));
        processor.setRunDurationMillis(procNode.getRunDuration(TimeUnit.MILLISECONDS));
        processor.setSchedulingPeriod(procNode.getSchedulingPeriod());
        processor.setSchedulingStrategy(procNode.getSchedulingStrategy().name());
        processor.setStyle(procNode.getStyle());
        processor.setYieldDuration(procNode.getYieldPeriod());
        processor.setScheduledState(flowMappingOptions.getStateLookup().getState(procNode));
        processor.setRetryCount(procNode.getRetryCount());
        processor.setRetriedRelationships(procNode.getRetriedRelationships());
        processor.setBackoffMechanism(procNode.getBackoffMechanism().name());
        processor.setMaxBackoffPeriod(procNode.getMaxBackoffPeriod());

        return processor;
    }

    public VersionedRemoteProcessGroup mapRemoteProcessGroup(final RemoteProcessGroup remoteGroup) {
        final VersionedRemoteProcessGroup rpg = new InstantiatedVersionedRemoteProcessGroup(remoteGroup.getIdentifier(), remoteGroup.getProcessGroupIdentifier());
        rpg.setIdentifier(getId(remoteGroup.getVersionedComponentId(), remoteGroup.getIdentifier()));
        if (flowMappingOptions.isMapInstanceIdentifiers()) {
            rpg.setInstanceIdentifier(remoteGroup.getIdentifier());
        }
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
        rpg.setProxyPassword(encrypt(remoteGroup.getProxyPassword()));
        rpg.setTargetUri(remoteGroup.getTargetUri());
        rpg.setTargetUris(remoteGroup.getTargetUris());
        rpg.setTransportProtocol(remoteGroup.getTransportProtocol().name());
        rpg.setYieldDuration(remoteGroup.getYieldDuration());
        return rpg;
    }

    public VersionedRemoteGroupPort mapRemotePort(final RemoteGroupPort remotePort, final ComponentType componentType) {
        final VersionedRemoteGroupPort port = new InstantiatedVersionedRemoteGroupPort(remotePort.getIdentifier(), remotePort.getRemoteProcessGroup().getIdentifier());
        port.setIdentifier(getId(remotePort.getVersionedComponentId(), remotePort.getIdentifier()));
        if (flowMappingOptions.isMapInstanceIdentifiers()) {
            port.setInstanceIdentifier(remotePort.getIdentifier());
        }
        port.setGroupIdentifier(getGroupId(remotePort.getRemoteProcessGroup().getIdentifier()));
        port.setComments(remotePort.getComments());
        port.setConcurrentlySchedulableTaskCount(remotePort.getMaxConcurrentTasks());
        port.setRemoteGroupId(getGroupId(remotePort.getRemoteProcessGroup().getIdentifier()));
        port.setName(remotePort.getName());
        port.setUseCompression(remotePort.isUseCompression());
        port.setBatchSize(mapBatchSettings(remotePort));
        port.setTargetId(remotePort.getTargetIdentifier());
        port.setComponentType(componentType);
        port.setScheduledState(flowMappingOptions.getStateLookup().getState(remotePort));

        return port;
    }

    private BatchSize mapBatchSettings(final RemoteGroupPort remotePort) {
        final BatchSize batchSize = new BatchSize();
        batchSize.setCount(remotePort.getBatchCount());
        batchSize.setDuration(remotePort.getBatchDuration());
        batchSize.setSize(remotePort.getBatchSize());
        return batchSize;
    }

    public VersionedParameterContext mapParameterContext(final ParameterContext parameterContext) {
        final Set<VersionedParameter> versionedParameters = mapParameters(parameterContext);

        final VersionedParameterContext versionedParameterContext = new VersionedParameterContext();
        versionedParameterContext.setDescription(parameterContext.getDescription());
        versionedParameterContext.setName(parameterContext.getName());
        versionedParameterContext.setParameters(versionedParameters);
        final String versionedContextId = flowMappingOptions.getComponentIdLookup().getComponentId(Optional.empty(), parameterContext.getIdentifier());
        versionedParameterContext.setIdentifier(versionedContextId);
        versionedParameterContext.setInheritedParameterContexts(parameterContext.getInheritedParameterContextNames());
        configureParameterProvider(parameterContext, versionedParameterContext);

        if (flowMappingOptions.isMapInstanceIdentifiers()) {
            versionedParameterContext.setInstanceIdentifier(parameterContext.getIdentifier());
        }

        return versionedParameterContext;
    }

    private void configureParameterProvider(final ParameterContext parameterContext, final VersionedParameterContext versionedParameterContext) {
        final ParameterProviderConfiguration parameterProviderConfiguration = parameterContext.getParameterProviderConfiguration();
        if (parameterProviderConfiguration != null) {
            versionedParameterContext.setParameterGroupName(parameterProviderConfiguration.getParameterGroupName());
            versionedParameterContext.setParameterProvider(parameterProviderConfiguration.getParameterProviderId());
            versionedParameterContext.setSynchronized(parameterProviderConfiguration.isSynchronized());
        }
    }

    public Map<String, VersionedParameterContext> mapParameterContexts(final ProcessGroup processGroup,
                                                                       final boolean mapDescendantVersionedFlows,
                                                                       final Map<String, ParameterProviderReference> parameterProviderReferences) {
        // cannot use a set to enforce uniqueness of parameter contexts because VersionedParameterContext in the
        // registry data model doesn't currently implement hashcode/equals based on context name
        final Map<String, VersionedParameterContext> parameterContexts = new HashMap<>();
        mapParameterContexts(processGroup, mapDescendantVersionedFlows, parameterContexts, parameterProviderReferences);
        return parameterContexts;
    }

    private void mapParameterContexts(final ProcessGroup processGroup, final boolean mapDescendantVersionedFlows,
                                      final Map<String, VersionedParameterContext> parameterContexts,
                                      final Map<String, ParameterProviderReference> parameterProviderReferences) {
        final ParameterContext parameterContext = processGroup.getParameterContext();
        if (parameterContext != null) {
            mapParameterContext(parameterContext, parameterContexts, parameterProviderReferences);
        }

        for (final ProcessGroup child : processGroup.getProcessGroups()) {
            // only include child process group parameter contexts if boolean indicator is true or process group is unversioned
            if (mapDescendantVersionedFlows || child.getVersionControlInformation() == null) {
                mapParameterContexts(child, mapDescendantVersionedFlows, parameterContexts, parameterProviderReferences);
            }
        }
    }

    private void mapParameterContext(final ParameterContext parameterContext, final Map<String, VersionedParameterContext> parameterContexts,
                                     final Map<String, ParameterProviderReference> parameterProviderReferences) {
        // map this process group's parameter context and add to the collection
        final Set<VersionedParameter> parameters = mapParameters(parameterContext);

        final VersionedParameterContext versionedContext = new VersionedParameterContext();
        versionedContext.setName(parameterContext.getName());
        versionedContext.setParameters(parameters);
        versionedContext.setInheritedParameterContexts(parameterContext.getInheritedParameterContextNames());

        configureParameterProvider(parameterContext, versionedContext);
        if (versionedContext.getParameterProvider() != null) {
            parameterProviderReferences.put(versionedContext.getParameterProvider(), createParameterProviderReference(parameterContext));
        }
        for (final ParameterContext inheritedParameterContext : parameterContext.getInheritedParameterContexts()) {
            mapParameterContext(inheritedParameterContext, parameterContexts, parameterProviderReferences);
        }

        parameterContexts.put(versionedContext.getName(), versionedContext);
    }

    private Set<VersionedParameter> mapParameters(ParameterContext parameterContext) {
        final Set<VersionedParameter> parameters = parameterContext.getParameters().entrySet().stream()
                .map(descriptorAndParameter -> mapParameter(
                    parameterContext,
                    descriptorAndParameter.getKey(),
                    descriptorAndParameter.getValue())
                )
                .collect(Collectors.toSet());
        return parameters;
    }

    private VersionedParameter mapParameter(ParameterContext parameterContext, ParameterDescriptor parameterDescriptor, Parameter parameter) {
        VersionedParameter versionedParameter;

        if (this.flowMappingOptions.isMapControllerServiceReferencesToVersionedId()) {
            List<ParameterReferencedControllerServiceData> referencedControllerServiceData = parameterContext
                .getParameterReferenceManager()
                .getReferencedControllerServiceData(parameterContext, parameterDescriptor.getName());

            if (referencedControllerServiceData.isEmpty()) {
                versionedParameter = mapParameter(parameter);
            } else {
                versionedParameter = mapParameter(
                    parameter,
                    getId(Optional.ofNullable(referencedControllerServiceData.get(0).getVersionedServiceId()), parameter.getValue())
                );
            }
        } else {
            versionedParameter = mapParameter(parameter);
        }

        return versionedParameter;
    }

    private ParameterProviderReference createParameterProviderReference(final ParameterContext parameterContext) {
        if (parameterContext.getParameterProvider() == null) {
            return null;
        }

        final ParameterProviderReference reference = new ParameterProviderReference();
        final ParameterProvider parameterProvider = parameterContext.getParameterProvider();
        final ParameterProviderNode parameterProviderNode = parameterContext.getParameterProviderLookup().getParameterProvider(parameterProvider.getIdentifier());
        final BundleCoordinate bundleCoordinate = parameterProviderNode.getBundleCoordinate();

        reference.setIdentifier(parameterProvider.getIdentifier());
        reference.setName(parameterProviderNode.getName());
        reference.setType(parameterProvider.getClass().getName());
        reference.setBundle(new Bundle(bundleCoordinate.getGroup(), bundleCoordinate.getId(), bundleCoordinate.getVersion()));

        return reference;
    }

    private VersionedParameter mapParameter(final Parameter parameter) {
        return mapParameter(parameter, parameter.getValue());
    }

    private VersionedParameter mapParameter(final Parameter parameter, final String value) {
        final ParameterDescriptor descriptor = parameter.getDescriptor();

        final VersionedParameter versionedParameter = new VersionedParameter();
        versionedParameter.setDescription(descriptor.getDescription());
        versionedParameter.setName(descriptor.getName());
        versionedParameter.setSensitive(descriptor.isSensitive());
        versionedParameter.setProvided(parameter.isProvided());

        final boolean mapParameterValue = flowMappingOptions.isMapSensitiveConfiguration() || !descriptor.isSensitive();
        final String parameterValue;
        if (mapParameterValue) {
            if (descriptor.isSensitive()) {
                parameterValue = encrypt(value);
            } else {
                parameterValue = value;
            }
        } else {
            parameterValue = null;
        }

        versionedParameter.setValue(parameterValue);
        return versionedParameter;
    }

    private org.apache.nifi.flow.ScheduledState mapScheduledState(final ScheduledState scheduledState) {
         return scheduledState == ScheduledState.DISABLED
                 ? org.apache.nifi.flow.ScheduledState.DISABLED
                 : org.apache.nifi.flow.ScheduledState.ENABLED;
    }
}
