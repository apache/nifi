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
package org.apache.nifi.controller;

import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.connectable.Position;
import org.apache.nifi.connectable.Size;
import org.apache.nifi.controller.exception.ProcessorInstantiationException;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.label.Label;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.queue.LoadBalanceStrategy;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroupPortDescriptor;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.registry.flow.StandardVersionControlInformation;
import org.apache.nifi.registry.flow.VersionControlInformation;
import org.apache.nifi.remote.RootGroupPort;
import org.apache.nifi.remote.StandardRemoteProcessGroupPortDescriptor;
import org.apache.nifi.remote.protocol.SiteToSiteTransportProtocol;
import org.apache.nifi.scheduling.ExecutionNode;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.util.BundleUtils;
import org.apache.nifi.util.SnippetUtils;
import org.apache.nifi.web.api.dto.BatchSettingsDTO;
import org.apache.nifi.web.api.dto.BundleDTO;
import org.apache.nifi.web.api.dto.ConnectableDTO;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.FunnelDTO;
import org.apache.nifi.web.api.dto.LabelDTO;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.RelationshipDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupContentsDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupPortDTO;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class StandardFlowSnippet implements FlowSnippet {

    private final FlowSnippetDTO dto;
    private final ExtensionManager extensionManager;

    public StandardFlowSnippet(final FlowSnippetDTO dto, final ExtensionManager extensionManager) {
        this.dto = dto;
        this.extensionManager = extensionManager;
    }

    public void validate(final ProcessGroup group) {
        // validate the names of Input Ports
        for (final PortDTO port : dto.getInputPorts()) {
            if (group.getInputPortByName(port.getName()) != null) {
                throw new IllegalStateException("One or more of the proposed Port names is not available in the process group");
            }
        }

        // validate the names of Output Ports
        for (final PortDTO port : dto.getOutputPorts()) {
            if (group.getOutputPortByName(port.getName()) != null) {
                throw new IllegalStateException("One or more of the proposed Port names is not available in the process group");
            }
        }

        verifyComponentTypesInSnippet();

        SnippetUtils.verifyNoVersionControlConflicts(dto, group);
    }

    public void verifyComponentTypesInSnippet() {
        final Map<String, Set<BundleCoordinate>> processorClasses = new HashMap<>();
        for (final Class<?> c : extensionManager.getExtensions(Processor.class)) {
            final String name = c.getName();
            processorClasses.put(name, extensionManager.getBundles(name).stream().map(bundle -> bundle.getBundleDetails().getCoordinate()).collect(Collectors.toSet()));
        }
        verifyProcessorsInSnippet(dto, processorClasses);

        final Map<String, Set<BundleCoordinate>> controllerServiceClasses = new HashMap<>();
        for (final Class<?> c : extensionManager.getExtensions(ControllerService.class)) {
            final String name = c.getName();
            controllerServiceClasses.put(name, extensionManager.getBundles(name).stream().map(bundle -> bundle.getBundleDetails().getCoordinate()).collect(Collectors.toSet()));
        }
        verifyControllerServicesInSnippet(dto, controllerServiceClasses);

        final Set<String> prioritizerClasses = new HashSet<>();
        for (final Class<?> c : extensionManager.getExtensions(FlowFilePrioritizer.class)) {
            prioritizerClasses.add(c.getName());
        }

        final Set<ConnectionDTO> allConns = new HashSet<>();
        allConns.addAll(dto.getConnections());
        for (final ProcessGroupDTO childGroup : dto.getProcessGroups()) {
            allConns.addAll(findAllConnections(childGroup));
        }

        for (final ConnectionDTO conn : allConns) {
            final List<String> prioritizers = conn.getPrioritizers();
            if (prioritizers != null) {
                for (final String prioritizer : prioritizers) {
                    if (!prioritizerClasses.contains(prioritizer)) {
                        throw new IllegalStateException("Invalid FlowFile Prioritizer Type: " + prioritizer);
                    }
                }
            }
        }
    }

    public void instantiate(final FlowManager flowManager, final ProcessGroup group) throws ProcessorInstantiationException {
        instantiate(flowManager, group, true);
    }



    /**
     * Recursively finds all ConnectionDTO's
     *
     * @param group group
     * @return connection dtos
     */
    private Set<ConnectionDTO> findAllConnections(final ProcessGroupDTO group) {
        final Set<ConnectionDTO> conns = new HashSet<>();
        conns.addAll(group.getContents().getConnections());

        for (final ProcessGroupDTO childGroup : group.getContents().getProcessGroups()) {
            conns.addAll(findAllConnections(childGroup));
        }
        return conns;
    }

    private void verifyControllerServicesInSnippet(final FlowSnippetDTO templateContents, final Map<String, Set<BundleCoordinate>> supportedTypes) {
        if (templateContents.getControllerServices() != null) {
            templateContents.getControllerServices().forEach(controllerService -> {
                if (supportedTypes.containsKey(controllerService.getType())) {
                    if (controllerService.getBundle() == null) {
                        throw new IllegalArgumentException("Controller Service bundle must be specified.");
                    }

                    verifyBundleInSnippet(controllerService.getBundle(), supportedTypes.get(controllerService.getType()));
                } else {
                    throw new IllegalStateException("Invalid Controller Service Type: " + controllerService.getType());
                }
            });
        }

        if (templateContents.getProcessGroups() != null) {
            templateContents.getProcessGroups().forEach(processGroup -> verifyControllerServicesInSnippet(processGroup.getContents(), supportedTypes));
        }
    }

    private void verifyBundleInSnippet(final BundleDTO requiredBundle, final Set<BundleCoordinate> supportedBundles) {
        final BundleCoordinate requiredCoordinate = new BundleCoordinate(requiredBundle.getGroup(), requiredBundle.getArtifact(), requiredBundle.getVersion());
        if (!supportedBundles.contains(requiredCoordinate)) {
            throw new IllegalStateException("Unsupported bundle: " + requiredCoordinate);
        }
    }

    private void verifyProcessorsInSnippet(final FlowSnippetDTO templateContents, final Map<String, Set<BundleCoordinate>> supportedTypes) {
        if (templateContents.getProcessors() != null) {
            templateContents.getProcessors().forEach(processor -> {
                if (processor.getBundle() == null) {
                    throw new IllegalArgumentException("Processor bundle must be specified.");
                }

                if (supportedTypes.containsKey(processor.getType())) {
                    verifyBundleInSnippet(processor.getBundle(), supportedTypes.get(processor.getType()));
                } else {
                    throw new IllegalStateException("Invalid Processor Type: " + processor.getType());
                }
            });
        }

        if (templateContents.getProcessGroups() != null) {
            templateContents.getProcessGroups().forEach(processGroup -> verifyProcessorsInSnippet(processGroup.getContents(), supportedTypes));
        }
    }


    public void instantiate(final FlowManager flowManager, final ProcessGroup group, final boolean topLevel) {
        //
        // Instantiate Controller Services
        //
        final List<ControllerServiceNode> serviceNodes = new ArrayList<>();
        try {
            for (final ControllerServiceDTO controllerServiceDTO : dto.getControllerServices()) {
                final BundleCoordinate bundleCoordinate = BundleUtils.getBundle(extensionManager, controllerServiceDTO.getType(), controllerServiceDTO.getBundle());
                final ControllerServiceNode serviceNode = flowManager.createControllerService(controllerServiceDTO.getType(), controllerServiceDTO.getId(),
                    bundleCoordinate, Collections.emptySet(), true,true);

                serviceNode.pauseValidationTrigger();
                serviceNodes.add(serviceNode);

                serviceNode.setAnnotationData(controllerServiceDTO.getAnnotationData());
                serviceNode.setComments(controllerServiceDTO.getComments());
                serviceNode.setName(controllerServiceDTO.getName());
                if (!topLevel) {
                    serviceNode.setVersionedComponentId(controllerServiceDTO.getVersionedComponentId());
                }

                group.addControllerService(serviceNode);
            }

            // configure controller services. We do this after creating all of them in case 1 service
            // references another service.
            for (final ControllerServiceDTO controllerServiceDTO : dto.getControllerServices()) {
                final String serviceId = controllerServiceDTO.getId();
                final ControllerServiceNode serviceNode = flowManager.getControllerServiceNode(serviceId);
                serviceNode.setProperties(controllerServiceDTO.getProperties());
            }
        } finally {
            serviceNodes.forEach(ControllerServiceNode::resumeValidationTrigger);
        }

        //
        // Instantiate the labels
        //
        for (final LabelDTO labelDTO : dto.getLabels()) {
            final Label label = flowManager.createLabel(labelDTO.getId(), labelDTO.getLabel());
            label.setPosition(toPosition(labelDTO.getPosition()));
            if (labelDTO.getWidth() != null && labelDTO.getHeight() != null) {
                label.setSize(new Size(labelDTO.getWidth(), labelDTO.getHeight()));
            }

            label.setStyle(labelDTO.getStyle());
            if (!topLevel) {
                label.setVersionedComponentId(labelDTO.getVersionedComponentId());
            }

            group.addLabel(label);
        }

        // Instantiate the funnels
        for (final FunnelDTO funnelDTO : dto.getFunnels()) {
            final Funnel funnel = flowManager.createFunnel(funnelDTO.getId());
            funnel.setPosition(toPosition(funnelDTO.getPosition()));
            if (!topLevel) {
                funnel.setVersionedComponentId(funnelDTO.getVersionedComponentId());
            }

            group.addFunnel(funnel);
        }

        //
        // Instantiate Input Ports & Output Ports
        //
        for (final PortDTO portDTO : dto.getInputPorts()) {
            final Port inputPort;
            if (group.isRootGroup()) {
                inputPort = flowManager.createRemoteInputPort(portDTO.getId(), portDTO.getName());
                inputPort.setMaxConcurrentTasks(portDTO.getConcurrentlySchedulableTaskCount());
                if (portDTO.getGroupAccessControl() != null) {
                    ((RootGroupPort) inputPort).setGroupAccessControl(portDTO.getGroupAccessControl());
                }
                if (portDTO.getUserAccessControl() != null) {
                    ((RootGroupPort) inputPort).setUserAccessControl(portDTO.getUserAccessControl());
                }
            } else {
                inputPort = flowManager.createLocalInputPort(portDTO.getId(), portDTO.getName());
            }

            if (!topLevel) {
                inputPort.setVersionedComponentId(portDTO.getVersionedComponentId());
            }
            inputPort.setPosition(toPosition(portDTO.getPosition()));
            inputPort.setProcessGroup(group);
            inputPort.setComments(portDTO.getComments());
            group.addInputPort(inputPort);
        }

        for (final PortDTO portDTO : dto.getOutputPorts()) {
            final Port outputPort;
            if (group.isRootGroup()) {
                outputPort = flowManager.createRemoteOutputPort(portDTO.getId(), portDTO.getName());
                outputPort.setMaxConcurrentTasks(portDTO.getConcurrentlySchedulableTaskCount());
                if (portDTO.getGroupAccessControl() != null) {
                    ((RootGroupPort) outputPort).setGroupAccessControl(portDTO.getGroupAccessControl());
                }
                if (portDTO.getUserAccessControl() != null) {
                    ((RootGroupPort) outputPort).setUserAccessControl(portDTO.getUserAccessControl());
                }
            } else {
                outputPort = flowManager.createLocalOutputPort(portDTO.getId(), portDTO.getName());
            }

            if (!topLevel) {
                outputPort.setVersionedComponentId(portDTO.getVersionedComponentId());
            }
            outputPort.setPosition(toPosition(portDTO.getPosition()));
            outputPort.setProcessGroup(group);
            outputPort.setComments(portDTO.getComments());
            group.addOutputPort(outputPort);
        }

        //
        // Instantiate the processors
        //
        for (final ProcessorDTO processorDTO : dto.getProcessors()) {
            final BundleCoordinate bundleCoordinate = BundleUtils.getBundle(extensionManager, processorDTO.getType(), processorDTO.getBundle());
            final ProcessorNode procNode = flowManager.createProcessor(processorDTO.getType(), processorDTO.getId(), bundleCoordinate);
            procNode.pauseValidationTrigger();

            try {
                procNode.setPosition(toPosition(processorDTO.getPosition()));
                procNode.setProcessGroup(group);
                if (!topLevel) {
                    procNode.setVersionedComponentId(processorDTO.getVersionedComponentId());
                }

                final ProcessorConfigDTO config = processorDTO.getConfig();
                procNode.setComments(config.getComments());
                if (config.isLossTolerant() != null) {
                    procNode.setLossTolerant(config.isLossTolerant());
                }
                procNode.setName(processorDTO.getName());

                procNode.setYieldPeriod(config.getYieldDuration());
                procNode.setPenalizationPeriod(config.getPenaltyDuration());
                procNode.setBulletinLevel(LogLevel.valueOf(config.getBulletinLevel()));
                procNode.setAnnotationData(config.getAnnotationData());
                procNode.setStyle(processorDTO.getStyle());

                if (config.getRunDurationMillis() != null) {
                    procNode.setRunDuration(config.getRunDurationMillis(), TimeUnit.MILLISECONDS);
                }

                if (config.getSchedulingStrategy() != null) {
                    procNode.setSchedulingStrategy(SchedulingStrategy.valueOf(config.getSchedulingStrategy()));
                }

                if (config.getExecutionNode() != null) {
                    procNode.setExecutionNode(ExecutionNode.valueOf(config.getExecutionNode()));
                }

                if (processorDTO.getState().equals(ScheduledState.DISABLED.toString())) {
                    procNode.disable();
                }

                // ensure that the scheduling strategy is set prior to these values
                procNode.setMaxConcurrentTasks(config.getConcurrentlySchedulableTaskCount());
                procNode.setScheduldingPeriod(config.getSchedulingPeriod());

                final Set<Relationship> relationships = new HashSet<>();
                if (processorDTO.getRelationships() != null) {
                    for (final RelationshipDTO rel : processorDTO.getRelationships()) {
                        if (rel.isAutoTerminate()) {
                            relationships.add(procNode.getRelationship(rel.getName()));
                        }
                    }
                    procNode.setAutoTerminatedRelationships(relationships);
                }

                if (config.getProperties() != null) {
                    procNode.setProperties(config.getProperties());
                }

                group.addProcessor(procNode);
            } finally {
                procNode.resumeValidationTrigger();
            }
        }

        //
        // Instantiate Remote Process Groups
        //
        for (final RemoteProcessGroupDTO remoteGroupDTO : dto.getRemoteProcessGroups()) {
            final RemoteProcessGroup remoteGroup = flowManager.createRemoteProcessGroup(remoteGroupDTO.getId(), remoteGroupDTO.getTargetUris());
            remoteGroup.setComments(remoteGroupDTO.getComments());
            remoteGroup.setPosition(toPosition(remoteGroupDTO.getPosition()));
            remoteGroup.setCommunicationsTimeout(remoteGroupDTO.getCommunicationsTimeout());
            remoteGroup.setYieldDuration(remoteGroupDTO.getYieldDuration());
            if (!topLevel) {
                remoteGroup.setVersionedComponentId(remoteGroupDTO.getVersionedComponentId());
            }

            if (remoteGroupDTO.getTransportProtocol() == null) {
                remoteGroup.setTransportProtocol(SiteToSiteTransportProtocol.RAW);
            } else {
                remoteGroup.setTransportProtocol(SiteToSiteTransportProtocol.valueOf(remoteGroupDTO.getTransportProtocol()));
            }

            remoteGroup.setProxyHost(remoteGroupDTO.getProxyHost());
            remoteGroup.setProxyPort(remoteGroupDTO.getProxyPort());
            remoteGroup.setProxyUser(remoteGroupDTO.getProxyUser());
            remoteGroup.setProxyPassword(remoteGroupDTO.getProxyPassword());
            remoteGroup.setProcessGroup(group);

            // set the input/output ports
            if (remoteGroupDTO.getContents() != null) {
                final RemoteProcessGroupContentsDTO contents = remoteGroupDTO.getContents();

                // ensure there are input ports
                if (contents.getInputPorts() != null) {
                    remoteGroup.setInputPorts(convertRemotePort(contents.getInputPorts()), false);
                }

                // ensure there are output ports
                if (contents.getOutputPorts() != null) {
                    remoteGroup.setOutputPorts(convertRemotePort(contents.getOutputPorts()), false);
                }
            }

            group.addRemoteProcessGroup(remoteGroup);
        }

        //
        // Instantiate ProcessGroups
        //
        for (final ProcessGroupDTO groupDTO : dto.getProcessGroups()) {
            final ProcessGroup childGroup = flowManager.createProcessGroup(groupDTO.getId());
            childGroup.setParent(group);
            childGroup.setPosition(toPosition(groupDTO.getPosition()));
            childGroup.setComments(groupDTO.getComments());
            childGroup.setName(groupDTO.getName());
            if (groupDTO.getVariables() != null) {
                childGroup.setVariables(groupDTO.getVariables());
            }

            // If this Process Group is 'top level' then we do not set versioned component ID's.
            // We do this only if this component is the child of a Versioned Component.
            if (!topLevel) {
                childGroup.setVersionedComponentId(groupDTO.getVersionedComponentId());
            }

            group.addProcessGroup(childGroup);

            final FlowSnippetDTO contents = groupDTO.getContents();

            // we want this to be recursive, so we will create a new template that contains only
            // the contents of this child group and recursively call ourselves.
            final FlowSnippetDTO childTemplateDTO = new FlowSnippetDTO();
            childTemplateDTO.setConnections(contents.getConnections());
            childTemplateDTO.setInputPorts(contents.getInputPorts());
            childTemplateDTO.setLabels(contents.getLabels());
            childTemplateDTO.setOutputPorts(contents.getOutputPorts());
            childTemplateDTO.setProcessGroups(contents.getProcessGroups());
            childTemplateDTO.setProcessors(contents.getProcessors());
            childTemplateDTO.setFunnels(contents.getFunnels());
            childTemplateDTO.setRemoteProcessGroups(contents.getRemoteProcessGroups());
            childTemplateDTO.setControllerServices(contents.getControllerServices());

            final StandardFlowSnippet childSnippet = new StandardFlowSnippet(childTemplateDTO, extensionManager);
            childSnippet.instantiate(flowManager, childGroup, false);

            if (groupDTO.getVersionControlInformation() != null) {
                final VersionControlInformation vci = StandardVersionControlInformation.Builder
                    .fromDto(groupDTO.getVersionControlInformation())
                    .build();
                childGroup.setVersionControlInformation(vci, Collections.emptyMap());
            }
        }

        //
        // Instantiate Connections
        //
        for (final ConnectionDTO connectionDTO : dto.getConnections()) {
            final ConnectableDTO sourceDTO = connectionDTO.getSource();
            final ConnectableDTO destinationDTO = connectionDTO.getDestination();
            final Connectable source;
            final Connectable destination;

            // Locate the source and destination connectable. If this is a remote port we need to locate the remote process groups. Otherwise, we need to
            // find the connectable given its parent group.
            //
            // NOTE: (getConnectable returns ANY connectable, when the parent is not this group only input ports or output ports should be returned. If something
            // other than a port is returned, an exception will be thrown when adding the connection below.)

            // See if the source connectable is a remote port
            if (ConnectableType.REMOTE_OUTPUT_PORT.name().equals(sourceDTO.getType())) {
                final RemoteProcessGroup remoteGroup = group.getRemoteProcessGroup(sourceDTO.getGroupId());
                source = remoteGroup.getOutputPort(sourceDTO.getId());
            } else {
                final ProcessGroup sourceGroup = getConnectableParent(group, sourceDTO.getGroupId(), flowManager);
                source = sourceGroup.getConnectable(sourceDTO.getId());
            }

            // see if the destination connectable is a remote port
            if (ConnectableType.REMOTE_INPUT_PORT.name().equals(destinationDTO.getType())) {
                final RemoteProcessGroup remoteGroup = group.getRemoteProcessGroup(destinationDTO.getGroupId());
                destination = remoteGroup.getInputPort(destinationDTO.getId());
            } else {
                final ProcessGroup destinationGroup = getConnectableParent(group, destinationDTO.getGroupId(), flowManager);
                destination = destinationGroup.getConnectable(destinationDTO.getId());
            }

            // determine the selection relationships for this connection
            final Set<String> relationships = new HashSet<>();
            if (connectionDTO.getSelectedRelationships() != null) {
                relationships.addAll(connectionDTO.getSelectedRelationships());
            }

            final Connection connection = flowManager.createConnection(connectionDTO.getId(), connectionDTO.getName(), source, destination, relationships);
            if (!topLevel) {
                connection.setVersionedComponentId(connectionDTO.getVersionedComponentId());
            }

            if (connectionDTO.getBends() != null) {
                final List<Position> bendPoints = new ArrayList<>();
                for (final PositionDTO bend : connectionDTO.getBends()) {
                    bendPoints.add(new Position(bend.getX(), bend.getY()));
                }
                connection.setBendPoints(bendPoints);
            }

            final FlowFileQueue queue = connection.getFlowFileQueue();
            queue.setBackPressureDataSizeThreshold(connectionDTO.getBackPressureDataSizeThreshold());
            queue.setBackPressureObjectThreshold(connectionDTO.getBackPressureObjectThreshold());
            queue.setFlowFileExpiration(connectionDTO.getFlowFileExpiration());

            final List<String> prioritizers = connectionDTO.getPrioritizers();
            if (prioritizers != null) {
                final List<String> newPrioritizersClasses = new ArrayList<>(prioritizers);
                final List<FlowFilePrioritizer> newPrioritizers = new ArrayList<>();
                for (final String className : newPrioritizersClasses) {
                    try {
                        newPrioritizers.add(flowManager.createPrioritizer(className));
                    } catch (final ClassNotFoundException | InstantiationException | IllegalAccessException e) {
                        throw new IllegalArgumentException("Unable to set prioritizer " + className + ": " + e);
                    }
                }
                queue.setPriorities(newPrioritizers);
            }

            final String loadBalanceStrategyName = connectionDTO.getLoadBalanceStrategy();
            if (loadBalanceStrategyName != null) {
                final LoadBalanceStrategy loadBalanceStrategy = LoadBalanceStrategy.valueOf(loadBalanceStrategyName);
                final String partitioningAttribute = connectionDTO.getLoadBalancePartitionAttribute();
                queue.setLoadBalanceStrategy(loadBalanceStrategy, partitioningAttribute);
            }

            connection.setProcessGroup(group);
            group.addConnection(connection);
        }
    }

    private ProcessGroup getConnectableParent(final ProcessGroup group, final String parentGroupId, final FlowManager flowManager) {
        if (flowManager.areGroupsSame(group.getIdentifier(), parentGroupId)) {
            return group;
        } else {
            return group.getProcessGroup(parentGroupId);
        }
    }


    private Position toPosition(final PositionDTO dto) {
        return new Position(dto.getX(), dto.getY());
    }

    /**
     * Converts a set of ports into a set of remote process group ports.
     *
     * @param ports ports
     * @return group descriptors
     */
    private Set<RemoteProcessGroupPortDescriptor> convertRemotePort(final Set<RemoteProcessGroupPortDTO> ports) {
        Set<RemoteProcessGroupPortDescriptor> remotePorts = null;
        if (ports != null) {
            remotePorts = new LinkedHashSet<>(ports.size());
            for (final RemoteProcessGroupPortDTO port : ports) {
                final StandardRemoteProcessGroupPortDescriptor descriptor = new StandardRemoteProcessGroupPortDescriptor();
                descriptor.setId(port.getId());
                descriptor.setVersionedComponentId(port.getVersionedComponentId());
                descriptor.setTargetId(port.getTargetId());
                descriptor.setName(port.getName());
                descriptor.setComments(port.getComments());
                descriptor.setTargetRunning(port.isTargetRunning());
                descriptor.setConnected(port.isConnected());
                descriptor.setConcurrentlySchedulableTaskCount(port.getConcurrentlySchedulableTaskCount());
                descriptor.setTransmitting(port.isTransmitting());
                descriptor.setUseCompression(port.getUseCompression());
                final BatchSettingsDTO batchSettings = port.getBatchSettings();
                if (batchSettings != null) {
                    descriptor.setBatchCount(batchSettings.getCount());
                    descriptor.setBatchSize(batchSettings.getSize());
                    descriptor.setBatchDuration(batchSettings.getDuration());
                }
                remotePorts.add(descriptor);
            }
        }
        return remotePorts;
    }
}
