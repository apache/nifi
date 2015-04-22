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
package org.apache.nifi.web.util;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.nifi.cluster.context.ClusterContext;
import org.apache.nifi.cluster.context.ClusterContextThreadLocal;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.Snippet;
import org.apache.nifi.controller.label.Label;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.web.api.dto.ConnectableDTO;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.DtoFactory;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.FunnelDTO;
import org.apache.nifi.web.api.dto.LabelDTO;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.PropertyDescriptorDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupContentsDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupPortDTO;

/**
 * Template utilities.
 */
public final class SnippetUtils {

    private FlowController flowController;
    private DtoFactory dtoFactory;

    /**
     * Populates the specified snippet and returns the details.
     *
     * @param snippet
     * @param recurse
     * @return
     */
    public FlowSnippetDTO populateFlowSnippet(Snippet snippet, boolean recurse) {
        final FlowSnippetDTO snippetDto = new FlowSnippetDTO();
        final String groupId = snippet.getParentGroupId();
        final ProcessGroup processGroup = flowController.getGroup(groupId);

        // ensure the group could be found
        if (processGroup == null) {
            throw new IllegalStateException("The parent process group for this snippet could not be found.");
        }

        // add any processors
        if (!snippet.getProcessors().isEmpty()) {
            final Set<ProcessorDTO> processors = new LinkedHashSet<>();
            for (String processorId : snippet.getProcessors()) {
                final ProcessorNode processor = processGroup.getProcessor(processorId);
                if (processor == null) {
                    throw new IllegalStateException("A processor in this snippet could not be found.");
                }
                processors.add(dtoFactory.createProcessorDto(processor));
            }
            snippetDto.setProcessors(processors);
        }

        // add any connections
        if (!snippet.getConnections().isEmpty()) {
            final Set<ConnectionDTO> connections = new LinkedHashSet<>();
            for (String connectionId : snippet.getConnections()) {
                final Connection connection = processGroup.getConnection(connectionId);
                if (connection == null) {
                    throw new IllegalStateException("A connection in this snippet could not be found.");
                }
                connections.add(dtoFactory.createConnectionDto(connection));
            }
            snippetDto.setConnections(connections);
        }

        // add any funnels
        if (!snippet.getFunnels().isEmpty()) {
            final Set<FunnelDTO> funnels = new LinkedHashSet<>();
            for (String funnelId : snippet.getFunnels()) {
                final Funnel funnel = processGroup.getFunnel(funnelId);
                if (funnel == null) {
                    throw new IllegalStateException("A funnel in this snippet could not be found.");
                }
                funnels.add(dtoFactory.createFunnelDto(funnel));
            }
            snippetDto.setFunnels(funnels);
        }

        // add any input ports
        if (!snippet.getInputPorts().isEmpty()) {
            final Set<PortDTO> inputPorts = new LinkedHashSet<>();
            for (String inputPortId : snippet.getInputPorts()) {
                final Port inputPort = processGroup.getInputPort(inputPortId);
                if (inputPort == null) {
                    throw new IllegalStateException("An input port in this snippet could not be found.");
                }
                inputPorts.add(dtoFactory.createPortDto(inputPort));
            }
            snippetDto.setInputPorts(inputPorts);
        }

        // add any labels
        if (!snippet.getLabels().isEmpty()) {
            final Set<LabelDTO> labels = new LinkedHashSet<>();
            for (String labelId : snippet.getLabels()) {
                final Label label = processGroup.getLabel(labelId);
                if (label == null) {
                    throw new IllegalStateException("A label in this snippet could not be found.");
                }
                labels.add(dtoFactory.createLabelDto(label));
            }
            snippetDto.setLabels(labels);
        }

        // add any output ports
        if (!snippet.getOutputPorts().isEmpty()) {
            final Set<PortDTO> outputPorts = new LinkedHashSet<>();
            for (String outputPortId : snippet.getOutputPorts()) {
                final Port outputPort = processGroup.getOutputPort(outputPortId);
                if (outputPort == null) {
                    throw new IllegalStateException("An output port in this snippet could not be found.");
                }
                outputPorts.add(dtoFactory.createPortDto(outputPort));
            }
            snippetDto.setOutputPorts(outputPorts);
        }

        // add any process groups
        if (!snippet.getProcessGroups().isEmpty()) {
            final Set<ProcessGroupDTO> processGroups = new LinkedHashSet<>();
            for (String childGroupId : snippet.getProcessGroups()) {
                final ProcessGroup childGroup = processGroup.getProcessGroup(childGroupId);
                if (childGroup == null) {
                    throw new IllegalStateException("A process group in this snippet could not be found.");
                }
                processGroups.add(dtoFactory.createProcessGroupDto(childGroup, recurse));
            }
            snippetDto.setProcessGroups(processGroups);
        }

        // add any remote process groups
        if (!snippet.getRemoteProcessGroups().isEmpty()) {
            final Set<RemoteProcessGroupDTO> remoteProcessGroups = new LinkedHashSet<>();
            for (String remoteProcessGroupId : snippet.getRemoteProcessGroups()) {
                final RemoteProcessGroup remoteProcessGroup = processGroup.getRemoteProcessGroup(remoteProcessGroupId);
                if (remoteProcessGroup == null) {
                    throw new IllegalStateException("A remote process group in this snippet could not be found.");
                }
                remoteProcessGroups.add(dtoFactory.createRemoteProcessGroupDto(remoteProcessGroup));
            }
            snippetDto.setRemoteProcessGroups(remoteProcessGroups);
        }

        addControllerServicesToSnippet(snippetDto);
        
        return snippetDto;
    }
    
    private void addControllerServicesToSnippet(final FlowSnippetDTO snippetDto) {
        final Set<ProcessorDTO> processors = snippetDto.getProcessors();
        if ( processors != null ) {
	    	for ( final ProcessorDTO processorDto : processors ) {
	            addControllerServicesToSnippet(snippetDto, processorDto);
	        }
        }
        
        final Set<ProcessGroupDTO> childGroups = snippetDto.getProcessGroups();
        if ( childGroups != null ) {
	        for ( final ProcessGroupDTO processGroupDto : childGroups ) {
	            final FlowSnippetDTO childGroupDto = processGroupDto.getContents();
	            if ( childGroupDto != null ) {
	            	addControllerServicesToSnippet(childGroupDto);
	            }
	        }
        }
    }
    
    private void addControllerServicesToSnippet(final FlowSnippetDTO snippet, final ProcessorDTO processorDto) {
        final ProcessorConfigDTO configDto = processorDto.getConfig();
        if ( configDto == null ) {
            return;
        }
        
        final Map<String, PropertyDescriptorDTO> descriptors = configDto.getDescriptors();
        final Map<String, String> properties = configDto.getProperties();
        
        if ( properties != null && descriptors != null ) {
            for ( final Map.Entry<String, String> entry : properties.entrySet() ) {
                final String propName = entry.getKey();
                final String propValue = entry.getValue();
                if ( propValue == null ) {
                    continue;
                }
                
                final PropertyDescriptorDTO propertyDescriptorDto = descriptors.get(propName);
                if ( propertyDescriptorDto != null && propertyDescriptorDto.getIdentifiesControllerService() != null ) {
                    final ControllerServiceNode serviceNode = flowController.getControllerServiceNode(propValue);
                    if ( serviceNode != null ) {
                        addControllerServicesToSnippet(snippet, serviceNode);
                    }
                }
            }
        }
    }
    
    private void addControllerServicesToSnippet(final FlowSnippetDTO snippet, final ControllerServiceNode serviceNode) {
        if ( isServicePresent(serviceNode.getIdentifier(), snippet.getControllerServices()) ) {
            return;
        }
        
        final ControllerServiceDTO serviceNodeDto = dtoFactory.createControllerServiceDto(serviceNode);
        Set<ControllerServiceDTO> existingServiceDtos = snippet.getControllerServices();
        if ( existingServiceDtos == null ) {
            existingServiceDtos = new HashSet<>();
            snippet.setControllerServices(existingServiceDtos);
        }
        existingServiceDtos.add(serviceNodeDto);

        for ( final Map.Entry<PropertyDescriptor, String> entry : serviceNode.getProperties().entrySet() ) {
            final PropertyDescriptor descriptor = entry.getKey();
            final String propertyValue = entry.getValue();
            
            if ( descriptor.getControllerServiceDefinition() != null ) {
                final ControllerServiceNode referencedNode = flowController.getControllerServiceNode(propertyValue);
                if ( referencedNode == null ) {
                    throw new IllegalStateException("Controller Service with ID " + propertyValue + " is referenced in template but cannot be found");
                }
                
                final String referencedNodeId = referencedNode.getIdentifier();
                
                final boolean alreadyPresent = isServicePresent(referencedNodeId, snippet.getControllerServices());
                if ( !alreadyPresent ) {
                    addControllerServicesToSnippet(snippet, referencedNode);
                }
            }
        }
    }

    private boolean isServicePresent(final String serviceId, final Collection<ControllerServiceDTO> services) {
        if ( services == null ) {
            return false;
        }
        
        for ( final ControllerServiceDTO existingService : services ) {
            if ( serviceId.equals(existingService.getId()) ) {
                return true;
            }
        }
        
        return false;
    }
    
    
    public FlowSnippetDTO copy(final FlowSnippetDTO snippetContents, final ProcessGroup group) {
        final FlowSnippetDTO snippetCopy = copyContentsForGroup(snippetContents, group.getIdentifier(), null, null);
        resolveNameConflicts(snippetCopy, group);
        return snippetCopy;
    }

    private void resolveNameConflicts(final FlowSnippetDTO snippetContents, final ProcessGroup group) {
        // get a list of all names of ports so that we can rename the ports as needed.
        final List<String> existingPortNames = new ArrayList<>();
        for (final Port inputPort : group.getInputPorts()) {
            existingPortNames.add(inputPort.getName());
        }
        for (final Port outputPort : group.getOutputPorts()) {
            existingPortNames.add(outputPort.getName());
        }

        // rename ports
        if (snippetContents.getInputPorts() != null) {
            for (final PortDTO portDTO : snippetContents.getInputPorts()) {
                String portName = portDTO.getName();
                while (existingPortNames.contains(portName)) {
                    portName = "Copy of " + portName;
                }
                portDTO.setName(portName);
                existingPortNames.add(portDTO.getName());
            }
        }
        if (snippetContents.getOutputPorts() != null) {
            for (final PortDTO portDTO : snippetContents.getOutputPorts()) {
                String portName = portDTO.getName();
                while (existingPortNames.contains(portName)) {
                    portName = "Copy of " + portName;
                }
                portDTO.setName(portName);
                existingPortNames.add(portDTO.getName());
            }
        }

        // get a list of all names of process groups so that we can rename as needed.
        final List<String> groupNames = new ArrayList<>();
        for (final ProcessGroup childGroup : group.getProcessGroups()) {
            groupNames.add(childGroup.getName());
        }

        if (snippetContents.getProcessGroups() != null) {
            for (final ProcessGroupDTO groupDTO : snippetContents.getProcessGroups()) {
                String groupName = groupDTO.getName();
                while (groupNames.contains(groupName)) {
                    groupName = "Copy of " + groupName;
                }
                groupDTO.setName(groupName);
                groupNames.add(groupDTO.getName());
            }
        }
    }

    private FlowSnippetDTO copyContentsForGroup(final FlowSnippetDTO snippetContents, final String groupId, final Map<String, ConnectableDTO> parentConnectableMap, Map<String, String> serviceIdMap) {
        final FlowSnippetDTO snippetContentsCopy = new FlowSnippetDTO();

        //
        // Copy the Controller Services
        //
        if ( serviceIdMap == null ) {
            serviceIdMap = new HashMap<>();
            final Set<ControllerServiceDTO> services = new HashSet<>();
            if ( snippetContents.getControllerServices() != null ) {
                for (final ControllerServiceDTO serviceDTO : snippetContents.getControllerServices() ) {
                    final ControllerServiceDTO service = dtoFactory.copy(serviceDTO);
                    service.setId(generateId(serviceDTO.getId()));
                    service.setState(ControllerServiceState.DISABLED.name());
                    services.add(service);
                    
                    // Map old service ID to new service ID so that we can make sure that we reference the new ones.
                    serviceIdMap.put(serviceDTO.getId(), service.getId());
                }
            }
            
            // if there is any controller service that maps to another controller service, update the id's
            for ( final ControllerServiceDTO serviceDTO : services ) {
                final Map<String, String> properties = serviceDTO.getProperties();
                final Map<String, PropertyDescriptorDTO> descriptors = serviceDTO.getDescriptors();
                if ( properties != null && descriptors != null ) {
                    for ( final PropertyDescriptorDTO descriptor : descriptors.values() ) {
                        if ( descriptor.getIdentifiesControllerService() != null ) {
                            final String currentServiceId = properties.get(descriptor.getName());
                            if ( currentServiceId == null ) {
                                continue;
                            }
                            
                            final String newServiceId = serviceIdMap.get(currentServiceId);
                            properties.put(descriptor.getName(), newServiceId);
                        }
                    }
                }
            }
            snippetContentsCopy.setControllerServices(services);
        }
        
        //
        // Copy the labels
        //
        final Set<LabelDTO> labels = new HashSet<>();
        if (snippetContents.getLabels() != null) {
            for (final LabelDTO labelDTO : snippetContents.getLabels()) {
                final LabelDTO label = dtoFactory.copy(labelDTO);
                label.setId(generateId(labelDTO.getId()));
                label.setParentGroupId(groupId);
                labels.add(label);
            }
        }
        snippetContentsCopy.setLabels(labels);

        //
        // Copy connectable components
        //
        // maps a group ID-ID of a Connectable in the template to the new instance
        final Map<String, ConnectableDTO> connectableMap = new HashMap<>();

        //
        // Copy the funnels
        //
        final Set<FunnelDTO> funnels = new HashSet<>();
        if (snippetContents.getFunnels() != null) {
            for (final FunnelDTO funnelDTO : snippetContents.getFunnels()) {
                final FunnelDTO cp = dtoFactory.copy(funnelDTO);
                cp.setId(generateId(funnelDTO.getId()));
                cp.setParentGroupId(groupId);
                funnels.add(cp);

                connectableMap.put(funnelDTO.getParentGroupId() + "-" + funnelDTO.getId(), dtoFactory.createConnectableDto(cp));
            }
        }
        snippetContentsCopy.setFunnels(funnels);

        final Set<PortDTO> inputPorts = new HashSet<>();
        if (snippetContents.getInputPorts() != null) {
            for (final PortDTO portDTO : snippetContents.getInputPorts()) {
                final PortDTO cp = dtoFactory.copy(portDTO);
                cp.setId(generateId(portDTO.getId()));
                cp.setParentGroupId(groupId);
                cp.setState(ScheduledState.STOPPED.toString());
                inputPorts.add(cp);

                final ConnectableDTO portConnectable = dtoFactory.createConnectableDto(cp, ConnectableType.INPUT_PORT);
                connectableMap.put(portDTO.getParentGroupId() + "-" + portDTO.getId(), portConnectable);
                if (parentConnectableMap != null) {
                    parentConnectableMap.put(portDTO.getParentGroupId() + "-" + portDTO.getId(), portConnectable);
                }
            }
        }
        snippetContentsCopy.setInputPorts(inputPorts);

        final Set<PortDTO> outputPorts = new HashSet<>();
        if (snippetContents.getOutputPorts() != null) {
            for (final PortDTO portDTO : snippetContents.getOutputPorts()) {
                final PortDTO cp = dtoFactory.copy(portDTO);
                cp.setId(generateId(portDTO.getId()));
                cp.setParentGroupId(groupId);
                cp.setState(ScheduledState.STOPPED.toString());
                outputPorts.add(cp);

                final ConnectableDTO portConnectable = dtoFactory.createConnectableDto(cp, ConnectableType.OUTPUT_PORT);
                connectableMap.put(portDTO.getParentGroupId() + "-" + portDTO.getId(), portConnectable);
                if (parentConnectableMap != null) {
                    parentConnectableMap.put(portDTO.getParentGroupId() + "-" + portDTO.getId(), portConnectable);
                }
            }
        }
        snippetContentsCopy.setOutputPorts(outputPorts);

        //
        // Copy the processors
        //
        final Set<ProcessorDTO> processors = new HashSet<>();
        if (snippetContents.getProcessors() != null) {
            for (final ProcessorDTO processorDTO : snippetContents.getProcessors()) {
                final ProcessorDTO cp = dtoFactory.copy(processorDTO);
                cp.setId(generateId(processorDTO.getId()));
                cp.setParentGroupId(groupId);
                cp.setState(ScheduledState.STOPPED.toString());
                processors.add(cp);

                connectableMap.put(processorDTO.getParentGroupId() + "-" + processorDTO.getId(), dtoFactory.createConnectableDto(cp));
            }
        }
        snippetContentsCopy.setProcessors(processors);

        // if there is any controller service that maps to another controller service, update the id's
        updateControllerServiceIdentifiers(snippetContentsCopy, serviceIdMap);
        
        // 
        // Copy ProcessGroups
        //
        // instantiate the process groups, renaming as necessary
        final Set<ProcessGroupDTO> groups = new HashSet<>();
        if (snippetContents.getProcessGroups() != null) {
            for (final ProcessGroupDTO groupDTO : snippetContents.getProcessGroups()) {
                final ProcessGroupDTO cp = dtoFactory.copy(groupDTO, false);
                cp.setId(generateId(groupDTO.getId()));
                cp.setParentGroupId(groupId);

                // copy the contents of this group - we do not copy via the dto factory since we want to specify new ids
                final FlowSnippetDTO contentsCopy = copyContentsForGroup(groupDTO.getContents(), cp.getId(), connectableMap, serviceIdMap);
                cp.setContents(contentsCopy);
                groups.add(cp);
            }
        }
        snippetContentsCopy.setProcessGroups(groups);

        final Set<RemoteProcessGroupDTO> remoteGroups = new HashSet<>();
        if (snippetContents.getRemoteProcessGroups() != null) {
            for (final RemoteProcessGroupDTO remoteGroupDTO : snippetContents.getRemoteProcessGroups()) {
                final RemoteProcessGroupDTO cp = dtoFactory.copy(remoteGroupDTO);
                cp.setId(generateId(remoteGroupDTO.getId()));
                cp.setParentGroupId(groupId);

                final RemoteProcessGroupContentsDTO contents = cp.getContents();
                if (contents != null && contents.getInputPorts() != null) {
                    for (final RemoteProcessGroupPortDTO remotePort : contents.getInputPorts()) {
                        remotePort.setGroupId(cp.getId());
                        connectableMap.put(remoteGroupDTO.getId() + "-" + remotePort.getId(), dtoFactory.createConnectableDto(remotePort, ConnectableType.REMOTE_INPUT_PORT));
                    }
                }
                if (contents != null && contents.getOutputPorts() != null) {
                    for (final RemoteProcessGroupPortDTO remotePort : contents.getOutputPorts()) {
                        remotePort.setGroupId(cp.getId());
                        connectableMap.put(remoteGroupDTO.getId() + "-" + remotePort.getId(), dtoFactory.createConnectableDto(remotePort, ConnectableType.REMOTE_OUTPUT_PORT));
                    }
                }

                remoteGroups.add(cp);
            }
        }
        snippetContentsCopy.setRemoteProcessGroups(remoteGroups);

        final Set<ConnectionDTO> connections = new HashSet<>();
        if (snippetContents.getConnections() != null) {
            for (final ConnectionDTO connectionDTO : snippetContents.getConnections()) {
                final ConnectionDTO cp = dtoFactory.copy(connectionDTO);

                final ConnectableDTO source = connectableMap.get(cp.getSource().getGroupId() + "-" + cp.getSource().getId());
                final ConnectableDTO destination = connectableMap.get(cp.getDestination().getGroupId() + "-" + cp.getDestination().getId());

                cp.setId(generateId(connectionDTO.getId()));
                cp.setSource(source);
                cp.setDestination(destination);
                cp.setParentGroupId(groupId);
                connections.add(cp);
            }
        }
        snippetContentsCopy.setConnections(connections);

        return snippetContentsCopy;
    }
    
    
    private void updateControllerServiceIdentifiers(final FlowSnippetDTO snippet, final Map<String, String> serviceIdMap) {
        final Set<ProcessorDTO> processors = snippet.getProcessors();
        if ( processors != null ) {
            for ( final ProcessorDTO processor : processors ) {
                updateControllerServiceIdentifiers(processor.getConfig(), serviceIdMap);
            }
        }
        
        for ( final ProcessGroupDTO processGroupDto : snippet.getProcessGroups() ) {
            updateControllerServiceIdentifiers(processGroupDto.getContents(), serviceIdMap);
        }
    }
    
    private void updateControllerServiceIdentifiers(final ProcessorConfigDTO configDto, final Map<String, String> serviceIdMap) {
        if ( configDto == null ) {
            return;
        }
        
        final Map<String, String> properties = configDto.getProperties();
        final Map<String, PropertyDescriptorDTO> descriptors = configDto.getDescriptors();
        if ( properties != null && descriptors != null ) {
            for ( final PropertyDescriptorDTO descriptor : descriptors.values() ) {
                if ( descriptor.getIdentifiesControllerService() != null ) {
                    final String currentServiceId = properties.get(descriptor.getName());
                    if ( currentServiceId == null ) {
                        continue;
                    }
                    
                    final String newServiceId = serviceIdMap.get(currentServiceId);
                    properties.put(descriptor.getName(), newServiceId);
                }
            }
        }
    }
    

    /**
     * Generates a new id for the current id that is specified. If no seed is
     * found, a new random id will be created.
     *
     * @param currentId
     * @return
     */
    private String generateId(final String currentId) {
        final ClusterContext clusterContext = ClusterContextThreadLocal.getContext();
        if (clusterContext != null) {
            final String seed = clusterContext.getIdGenerationSeed() + "-" + currentId;
            return UUID.nameUUIDFromBytes(seed.getBytes(StandardCharsets.UTF_8)).toString();
        } else {
            return UUID.randomUUID().toString();
        }
    }

    /* setters */
    public void setDtoFactory(DtoFactory dtoFactory) {
        this.dtoFactory = dtoFactory;
    }

    public void setFlowController(FlowController flowController) {
        this.flowController = flowController;
    }

}
