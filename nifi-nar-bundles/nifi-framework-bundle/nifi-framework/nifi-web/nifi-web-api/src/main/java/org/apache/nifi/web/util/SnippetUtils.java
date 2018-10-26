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



import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.AccessPolicy;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.resource.ResourceFactory;
import org.apache.nifi.authorization.resource.ResourceType;
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
import org.apache.nifi.util.ComponentIdGenerator;
import org.apache.nifi.web.api.dto.AccessPolicyDTO;
import org.apache.nifi.web.api.dto.ComponentDTO;
import org.apache.nifi.web.api.dto.ConnectableDTO;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.DtoFactory;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.FunnelDTO;
import org.apache.nifi.web.api.dto.LabelDTO;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.PropertyDescriptorDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupContentsDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupPortDTO;
import org.apache.nifi.web.api.entity.TenantEntity;
import org.apache.nifi.web.dao.AccessPolicyDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Template utilities.
 */
public final class SnippetUtils {

    private static final Logger logger = LoggerFactory.getLogger(SnippetUtils.class);

    private static final SecureRandom randomGenerator = new SecureRandom();

    private FlowController flowController;
    private DtoFactory dtoFactory;
    private AccessPolicyDAO accessPolicyDAO;

    /**
     * Populates the specified snippet and returns the details.
     *
     * @param snippet snippet
     * @param recurse recurse
     * @param includeControllerServices whether or not to include controller services in the flow snippet dto
     * @return snippet
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public FlowSnippetDTO populateFlowSnippet(final Snippet snippet, final boolean recurse, final boolean includeControllerServices, boolean removeInstanceId) {
        final FlowSnippetDTO snippetDto = new FlowSnippetDTO(removeInstanceId);
        final String groupId = snippet.getParentGroupId();
        final ProcessGroup processGroup = flowController.getFlowManager().getGroup(groupId);

        // ensure the group could be found
        if (processGroup == null) {
            throw new IllegalStateException("The parent process group for this snippet could not be found.");
        }

        // We need to ensure that the Controller Services that are added get added to the proper group.
        // This can potentially get a little bit tricky. Consider this scenario:
        // We have a Process Group G1. Within Process Group G1 is a Controller Service C1.
        // Also within G1 is a child Process Group, G2. Within G2 is a child Process Group, G3.
        // Within G3 are two child Process Groups: G4 and G5. Within each of these children,
        // we have a Processor (P1, P2) that references the Controller Service C1, defined 3 levels above.
        // Now, we create a template that encompasses only Process Groups G4 and G5. We need to ensure
        // that the Controller Service C1 is included at the 'root' of the template so that those
        // Processors within G4 and G5 both have access to the same Controller Service. This can be drawn
        // out thus:
        //
        // G1 -- C1
        // |
        // |
        // G2
        // |
        // |
        // G3
        // |  \
        // |   \
        // G4   G5
        // |    |
        // |    |
        // P1   P2
        //
        // Both P1 and P2 reference C1.
        //
        // In order to accomplish this, we maintain two collections. First, we keep a Set of all Controller Services that have
        // been added. If we add a new Controller Service to the set, then we know it hasn't been added anywhere in the Snippet.
        // In that case, we determine the service's group ID. In the flow described above, if we template just groups G4 and G5,
        // then we need to include the Controller Service defined at G1. So we also keep a Map of Group ID to controller services
        // in that group. If the ParentGroupId of a Controller Service is not in our snippet, then we instead update the parent
        // ParentGroupId to be that of our highest-level process group (in this case G3, as that's where the template is created)
        // and then add the controller services to that group (NOTE: here, when we say we change the group ID and add to that group,
        // we are talking only about the DTO objects that make up the snippet. We do not actually modify the Process Group or the
        // Controller Services in our flow themselves!)
        final Set<ControllerServiceDTO> allServicesReferenced = new HashSet<>();
        final Map<String, FlowSnippetDTO> contentsByGroup = new HashMap<>();
        contentsByGroup.put(processGroup.getIdentifier(), snippetDto);

        // add any processors
        final Set<ControllerServiceDTO> controllerServices = new HashSet<>();
        final Set<ProcessorDTO> processors = new LinkedHashSet<>();
        if (!snippet.getProcessors().isEmpty()) {
            for (final String processorId : snippet.getProcessors().keySet()) {
                final ProcessorNode processor = processGroup.getProcessor(processorId);
                if (processor == null) {
                    throw new IllegalStateException("A processor in this snippet could not be found.");
                }
                processors.add(dtoFactory.createProcessorDto(processor));

                if (includeControllerServices) {
                    // Include all referenced services that are not already included in this snippet.
                    getControllerServices(processor.getProperties()).stream()
                        .filter(svc -> allServicesReferenced.add(svc))
                        .forEach(svc -> {
                            final String svcGroupId = svc.getParentGroupId();
                            final String destinationGroupId = contentsByGroup.containsKey(svcGroupId) ? svcGroupId : processGroup.getIdentifier();
                            svc.setParentGroupId(destinationGroupId);
                            controllerServices.add(svc);
                        });
                }
            }
        }

        // add any connections
        final Set<ConnectionDTO> connections = new LinkedHashSet<>();
        if (!snippet.getConnections().isEmpty()) {
            for (final String connectionId : snippet.getConnections().keySet()) {
                final Connection connection = processGroup.getConnection(connectionId);
                if (connection == null) {
                    throw new IllegalStateException("A connection in this snippet could not be found.");
                }
                connections.add(dtoFactory.createConnectionDto(connection));
            }
        }

        // add any funnels
        final Set<FunnelDTO> funnels = new LinkedHashSet<>();
        if (!snippet.getFunnels().isEmpty()) {
            for (final String funnelId : snippet.getFunnels().keySet()) {
                final Funnel funnel = processGroup.getFunnel(funnelId);
                if (funnel == null) {
                    throw new IllegalStateException("A funnel in this snippet could not be found.");
                }
                funnels.add(dtoFactory.createFunnelDto(funnel));
            }
        }

        // add any input ports
        final Set<PortDTO> inputPorts = new LinkedHashSet<>();
        if (!snippet.getInputPorts().isEmpty()) {
            for (final String inputPortId : snippet.getInputPorts().keySet()) {
                final Port inputPort = processGroup.getInputPort(inputPortId);
                if (inputPort == null) {
                    throw new IllegalStateException("An input port in this snippet could not be found.");
                }
                inputPorts.add(dtoFactory.createPortDto(inputPort));
            }
        }

        // add any labels
        final Set<LabelDTO> labels = new LinkedHashSet<>();
        if (!snippet.getLabels().isEmpty()) {
            for (final String labelId : snippet.getLabels().keySet()) {
                final Label label = processGroup.getLabel(labelId);
                if (label == null) {
                    throw new IllegalStateException("A label in this snippet could not be found.");
                }
                labels.add(dtoFactory.createLabelDto(label));
            }
        }

        // add any output ports
        final Set<PortDTO> outputPorts = new LinkedHashSet<>();
        if (!snippet.getOutputPorts().isEmpty()) {
            for (final String outputPortId : snippet.getOutputPorts().keySet()) {
                final Port outputPort = processGroup.getOutputPort(outputPortId);
                if (outputPort == null) {
                    throw new IllegalStateException("An output port in this snippet could not be found.");
                }
                outputPorts.add(dtoFactory.createPortDto(outputPort));
            }
        }

        // add any process groups
        final Set<ProcessGroupDTO> processGroups = new LinkedHashSet<>();
        if (!snippet.getProcessGroups().isEmpty()) {
            for (final String childGroupId : snippet.getProcessGroups().keySet()) {
                final ProcessGroup childGroup = processGroup.getProcessGroup(childGroupId);
                if (childGroup == null) {
                    throw new IllegalStateException("A process group in this snippet could not be found.");
                }

                final ProcessGroupDTO childGroupDto = dtoFactory.createProcessGroupDto(childGroup, recurse);
                processGroups.add(childGroupDto);

                // maintain a listing of visited groups starting with each group in the snippet. this is used to determine
                // whether a referenced controller service should be included in the resulting snippet. if the service is
                // defined at groupId or one of it's ancestors, its considered outside of this snippet and will only be included
                // when the includeControllerServices is set to true. this happens above when considering the processors in this snippet
                final Set<String> visitedGroupIds = new HashSet<>();
                addControllerServices(childGroup, childGroupDto, allServicesReferenced, includeControllerServices, visitedGroupIds, contentsByGroup, processGroup.getIdentifier());
            }
        }

        // add any remote process groups
        final Set<RemoteProcessGroupDTO> remoteProcessGroups = new LinkedHashSet<>();
        if (!snippet.getRemoteProcessGroups().isEmpty()) {
            for (final String remoteProcessGroupId : snippet.getRemoteProcessGroups().keySet()) {
                final RemoteProcessGroup remoteProcessGroup = processGroup.getRemoteProcessGroup(remoteProcessGroupId);
                if (remoteProcessGroup == null) {
                    throw new IllegalStateException("A remote process group in this snippet could not be found.");
                }
                remoteProcessGroups.add(dtoFactory.createRemoteProcessGroupDto(remoteProcessGroup));
            }
        }


        // Normalize the coordinates based on the locations of the other components
        final List<? extends ComponentDTO> components = new ArrayList<>();
        components.addAll((Set) processors);
        components.addAll((Set) connections);
        components.addAll((Set) funnels);
        components.addAll((Set) inputPorts);
        components.addAll((Set) labels);
        components.addAll((Set) outputPorts);
        components.addAll((Set) processGroups);
        components.addAll((Set) remoteProcessGroups);
        normalizeCoordinates(components);

        Set<ControllerServiceDTO> updatedControllerServices = snippetDto.getControllerServices();
        if (updatedControllerServices == null) {
            updatedControllerServices = new HashSet<>();
        }
        updatedControllerServices.addAll(controllerServices);
        snippetDto.setControllerServices(updatedControllerServices);

        snippetDto.setProcessors(processors);
        snippetDto.setConnections(connections);
        snippetDto.setFunnels(funnels);
        snippetDto.setInputPorts(inputPorts);
        snippetDto.setLabels(labels);
        snippetDto.setOutputPorts(outputPorts);
        snippetDto.setProcessGroups(processGroups);
        snippetDto.setRemoteProcessGroups(remoteProcessGroups);

        return snippetDto;
    }

    /**
     * Finds all Controller Services that are referenced in the given Process Group (and child Process Groups, recursively), and
     * adds them to the given servicesByGroup map
     *
     * @param group the Process Group to start from
     * @param dto the DTO representation of the Process Group
     * @param allServicesReferenced a Set of all Controller Service DTO's that have already been referenced; used to dedupe services
     * @param contentsByGroup a Map of Process Group ID to the Process Group's contents
     * @param highestGroupId the UUID of the 'highest' process group in the snippet
     */
    private void addControllerServices(final ProcessGroup group, final ProcessGroupDTO dto, final Set<ControllerServiceDTO> allServicesReferenced,
        final boolean includeControllerServices, final Set<String> visitedGroupIds, final Map<String, FlowSnippetDTO> contentsByGroup, final String highestGroupId) {

        final FlowSnippetDTO contents = dto.getContents();
        contentsByGroup.put(dto.getId(), contents);
        if (contents == null) {
            return;
        }

        // include this group in the ancestry for this snippet, services only get included if the includeControllerServices
        // flag is set or if the service is defined within this groups hierarchy within the snippet
        visitedGroupIds.add(group.getIdentifier());

        for (final ProcessorNode procNode : group.getProcessors()) {
            // Include all referenced services that are not already included in this snippet.
            getControllerServices(procNode.getProperties()).stream()
                .filter(svc -> allServicesReferenced.add(svc))
                .filter(svc -> includeControllerServices || visitedGroupIds.contains(svc.getParentGroupId()))
                .forEach(svc -> {
                    final String svcGroupId = svc.getParentGroupId();
                    final String destinationGroupId = contentsByGroup.containsKey(svcGroupId) ? svcGroupId : highestGroupId;
                    svc.setParentGroupId(destinationGroupId);
                    final FlowSnippetDTO snippetDto = contentsByGroup.get(destinationGroupId);
                    if (snippetDto != null) {
                        Set<ControllerServiceDTO> services = snippetDto.getControllerServices();
                        if (services == null) {
                            snippetDto.setControllerServices(Collections.singleton(svc));
                        } else {
                            services.add(svc);
                            snippetDto.setControllerServices(services);
                        }
                    }
                });
        }

        // Map child process group ID to the child process group for easy lookup
        final Map<String, ProcessGroupDTO> childGroupMap = contents.getProcessGroups().stream()
            .collect(Collectors.toMap(childGroupDto -> childGroupDto.getId(), childGroupDto -> childGroupDto));

        for (final ProcessGroup childGroup : group.getProcessGroups()) {
            final ProcessGroupDTO childDto = childGroupMap.get(childGroup.getIdentifier());
            if (childDto == null) {
                continue;
            }

            addControllerServices(childGroup, childDto, allServicesReferenced, includeControllerServices, visitedGroupIds, contentsByGroup, highestGroupId);
        }
    }

    private Set<ControllerServiceDTO> getControllerServices(final Map<PropertyDescriptor, String> componentProperties) {
        final Set<ControllerServiceDTO> serviceDtos = new HashSet<>();

        for (final Map.Entry<PropertyDescriptor, String> entry : componentProperties.entrySet()) {
            final PropertyDescriptor descriptor = entry.getKey();
            if (descriptor.getControllerServiceDefinition() != null) {
                final String controllerServiceId = entry.getValue();
                if (controllerServiceId != null) {
                    final ControllerServiceNode serviceNode = flowController.getFlowManager().getControllerServiceNode(controllerServiceId);
                    if (serviceNode != null) {
                        serviceDtos.add(dtoFactory.createControllerServiceDto(serviceNode));

                        final Set<ControllerServiceDTO> recursiveRefs = getControllerServices(serviceNode.getProperties());
                        serviceDtos.addAll(recursiveRefs);
                    }
                }
            }
        }

        return serviceDtos;
    }


    public FlowSnippetDTO copy(final FlowSnippetDTO snippetContents, final ProcessGroup group, final String idGenerationSeed, boolean isCopy) {
        final FlowSnippetDTO snippetCopy = copyContentsForGroup(snippetContents, group.getIdentifier(), null, null, idGenerationSeed, isCopy);
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
        final Set<String> groupNames = new HashSet<>();
        for (final ProcessGroup childGroup : group.getProcessGroups()) {
            groupNames.add(childGroup.getName());
        }

        if (snippetContents.getProcessGroups() != null) {
            for (final ProcessGroupDTO groupDTO : snippetContents.getProcessGroups()) {
                // If Version Control Information is present, then we don't want to rename the
                // Process Group - we want it to remain the same as the one in Version Control.
                // However, in order to disambiguate things, we generally do want to rename to
                // 'Copy of...' so we do this only if there is no Version Control Information present.
                if (groupDTO.getVersionControlInformation() == null) {
                    String groupName = groupDTO.getName();
                    while (groupNames.contains(groupName)) {
                        groupName = "Copy of " + groupName;
                    }
                    groupDTO.setName(groupName);
                }
                groupNames.add(groupDTO.getName());
            }
        }
    }

    private FlowSnippetDTO copyContentsForGroup(final FlowSnippetDTO snippetContents, final String groupId, final Map<String, ConnectableDTO> parentConnectableMap,
                                                Map<String, String> serviceIdMap, final String idGenerationSeed, boolean isCopy) {

        final FlowSnippetDTO snippetContentsCopy = new FlowSnippetDTO();
        try {
            //
            // Copy the Controller Services
            //
            if (serviceIdMap == null) {
                serviceIdMap = new HashMap<>();
            }

            final Set<ControllerServiceDTO> services = new HashSet<>();
            if (snippetContents.getControllerServices() != null) {
                for (final ControllerServiceDTO serviceDTO : snippetContents.getControllerServices()) {
                    final ControllerServiceDTO service = dtoFactory.copy(serviceDTO);
                    service.setId(generateId(serviceDTO.getId(), idGenerationSeed, isCopy));
                    service.setState(ControllerServiceState.DISABLED.name());
                    services.add(service);

                    // Map old service ID to new service ID so that we can make sure that we reference the new ones.
                    serviceIdMap.put(serviceDTO.getId(), service.getId());

                    // clone policies as appropriate
                    if (isCopy) {
                        cloneComponentSpecificPolicies(
                                ResourceFactory.getComponentResource(ResourceType.ControllerService, serviceDTO.getId(), serviceDTO.getName()),
                                ResourceFactory.getComponentResource(ResourceType.ControllerService, service.getId(), service.getName()), idGenerationSeed);
                    }
                }
            }

            // if there is any controller service that maps to another controller service, update the id's
            for (final ControllerServiceDTO serviceDTO : services) {
                final Map<String, String> properties = serviceDTO.getProperties();
                final Map<String, PropertyDescriptorDTO> descriptors = serviceDTO.getDescriptors();
                if (properties != null && descriptors != null) {
                    for (final PropertyDescriptorDTO descriptor : descriptors.values()) {
                        if (descriptor.getIdentifiesControllerService() != null) {
                            final String currentServiceId = properties.get(descriptor.getName());
                            if (currentServiceId == null) {
                                continue;
                            }

                            final String newServiceId = serviceIdMap.get(currentServiceId);
                            properties.put(descriptor.getName(), newServiceId);
                        }
                    }
                }
            }
            snippetContentsCopy.setControllerServices(services);

            //
            // Copy the labels
            //
            final Set<LabelDTO> labels = new HashSet<>();
            if (snippetContents.getLabels() != null) {
                for (final LabelDTO labelDTO : snippetContents.getLabels()) {
                    final LabelDTO label = dtoFactory.copy(labelDTO);
                    label.setId(generateId(labelDTO.getId(), idGenerationSeed, isCopy));
                    label.setParentGroupId(groupId);
                    labels.add(label);

                    // clone policies as appropriate
                    if (isCopy) {
                        cloneComponentSpecificPolicies(
                                ResourceFactory.getComponentResource(ResourceType.Label, labelDTO.getId(), labelDTO.getLabel()),
                                ResourceFactory.getComponentResource(ResourceType.Label, label.getId(), label.getLabel()), idGenerationSeed);
                    }
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
                    cp.setId(generateId(funnelDTO.getId(), idGenerationSeed, isCopy));
                    cp.setParentGroupId(groupId);
                    funnels.add(cp);

                    connectableMap.put(funnelDTO.getParentGroupId() + "-" + funnelDTO.getId(), dtoFactory.createConnectableDto(cp));

                    // clone policies as appropriate
                    if (isCopy) {
                        cloneComponentSpecificPolicies(
                                ResourceFactory.getComponentResource(ResourceType.Funnel, funnelDTO.getId(), funnelDTO.getId()),
                                ResourceFactory.getComponentResource(ResourceType.Funnel, cp.getId(), cp.getId()), idGenerationSeed);
                    }
                }
            }
            snippetContentsCopy.setFunnels(funnels);

            final Set<PortDTO> inputPorts = new HashSet<>();
            if (snippetContents.getInputPorts() != null) {
                for (final PortDTO portDTO : snippetContents.getInputPorts()) {
                    final PortDTO cp = dtoFactory.copy(portDTO);
                    cp.setId(generateId(portDTO.getId(), idGenerationSeed, isCopy));
                    cp.setParentGroupId(groupId);
                    cp.setState(ScheduledState.STOPPED.toString());
                    inputPorts.add(cp);

                    final ConnectableDTO portConnectable = dtoFactory.createConnectableDto(cp, ConnectableType.INPUT_PORT);
                    connectableMap.put(portDTO.getParentGroupId() + "-" + portDTO.getId(), portConnectable);
                    if (parentConnectableMap != null) {
                        parentConnectableMap.put(portDTO.getParentGroupId() + "-" + portDTO.getId(), portConnectable);
                    }

                    // clone policies as appropriate
                    if (isCopy) {
                        cloneComponentSpecificPolicies(
                                ResourceFactory.getComponentResource(ResourceType.InputPort, portDTO.getId(), portDTO.getName()),
                                ResourceFactory.getComponentResource(ResourceType.InputPort, cp.getId(), cp.getName()), idGenerationSeed);
                    }
                }
            }
            snippetContentsCopy.setInputPorts(inputPorts);

            final Set<PortDTO> outputPorts = new HashSet<>();
            if (snippetContents.getOutputPorts() != null) {
                for (final PortDTO portDTO : snippetContents.getOutputPorts()) {
                    final PortDTO cp = dtoFactory.copy(portDTO);
                    cp.setId(generateId(portDTO.getId(), idGenerationSeed, isCopy));
                    cp.setParentGroupId(groupId);
                    cp.setState(ScheduledState.STOPPED.toString());
                    outputPorts.add(cp);

                    final ConnectableDTO portConnectable = dtoFactory.createConnectableDto(cp, ConnectableType.OUTPUT_PORT);
                    connectableMap.put(portDTO.getParentGroupId() + "-" + portDTO.getId(), portConnectable);
                    if (parentConnectableMap != null) {
                        parentConnectableMap.put(portDTO.getParentGroupId() + "-" + portDTO.getId(), portConnectable);
                    }

                    // clone policies as appropriate
                    if (isCopy) {
                        cloneComponentSpecificPolicies(
                                ResourceFactory.getComponentResource(ResourceType.OutputPort, portDTO.getId(), portDTO.getName()),
                                ResourceFactory.getComponentResource(ResourceType.OutputPort, cp.getId(), cp.getName()), idGenerationSeed);
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
                    cp.setId(generateId(processorDTO.getId(), idGenerationSeed, isCopy));
                    cp.setParentGroupId(groupId);
                    if(processorDTO.getState() != null && processorDTO.getState().equals(ScheduledState.DISABLED.toString())) {
                        cp.setState(ScheduledState.DISABLED.toString());
                    } else {
                        cp.setState(ScheduledState.STOPPED.toString());
                    }
                    processors.add(cp);

                    connectableMap.put(processorDTO.getParentGroupId() + "-" + processorDTO.getId(), dtoFactory.createConnectableDto(cp));

                    // clone policies as appropriate
                    if (isCopy) {
                        cloneComponentSpecificPolicies(
                                ResourceFactory.getComponentResource(ResourceType.Processor, processorDTO.getId(), processorDTO.getName()),
                                ResourceFactory.getComponentResource(ResourceType.Processor, cp.getId(), cp.getName()), idGenerationSeed);
                    }
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
                    cp.setId(generateId(groupDTO.getId(), idGenerationSeed, isCopy));
                    cp.setParentGroupId(groupId);

                    // copy the contents of this group - we do not copy via the dto factory since we want to specify new ids
                    final FlowSnippetDTO contentsCopy = copyContentsForGroup(groupDTO.getContents(), cp.getId(), connectableMap, serviceIdMap, idGenerationSeed, isCopy);
                    cp.setContents(contentsCopy);
                    groups.add(cp);

                    // clone policies as appropriate
                    if (isCopy) {
                        cloneComponentSpecificPolicies(
                                ResourceFactory.getComponentResource(ResourceType.ProcessGroup, groupDTO.getId(), groupDTO.getName()),
                                ResourceFactory.getComponentResource(ResourceType.ProcessGroup, cp.getId(), cp.getName()), idGenerationSeed);
                    }
                }
            }
            snippetContentsCopy.setProcessGroups(groups);

            final Set<RemoteProcessGroupDTO> remoteGroups = new HashSet<>();
            if (snippetContents.getRemoteProcessGroups() != null) {
                for (final RemoteProcessGroupDTO remoteGroupDTO : snippetContents.getRemoteProcessGroups()) {
                    final RemoteProcessGroupDTO cp = dtoFactory.copy(remoteGroupDTO);
                    cp.setId(generateId(remoteGroupDTO.getId(), idGenerationSeed, isCopy));
                    cp.setParentGroupId(groupId);

                    final RemoteProcessGroupContentsDTO contents = cp.getContents();
                    if (contents != null && contents.getInputPorts() != null) {
                        for (final RemoteProcessGroupPortDTO remotePort : contents.getInputPorts()) {
                            remotePort.setGroupId(cp.getId());
                            final String originalId = remotePort.getId();
                            if (remotePort.getTargetId() == null) {
                                remotePort.setTargetId(originalId);
                            }
                            remotePort.setId(generateId(remotePort.getId(), idGenerationSeed, isCopy));

                            connectableMap.put(remoteGroupDTO.getId() + "-" + originalId, dtoFactory.createConnectableDto(remotePort, ConnectableType.REMOTE_INPUT_PORT));
                        }
                    }
                    if (contents != null && contents.getOutputPorts() != null) {
                        for (final RemoteProcessGroupPortDTO remotePort : contents.getOutputPorts()) {
                            remotePort.setGroupId(cp.getId());
                            final String originalId = remotePort.getId();
                            if (remotePort.getTargetId() == null) {
                                remotePort.setTargetId(originalId);
                            }
                            remotePort.setId(generateId(remotePort.getId(), idGenerationSeed, isCopy));
                            connectableMap.put(remoteGroupDTO.getId() + "-" + originalId, dtoFactory.createConnectableDto(remotePort, ConnectableType.REMOTE_OUTPUT_PORT));
                        }
                    }

                    remoteGroups.add(cp);

                    // clone policies as appropriate
                    if (isCopy) {
                        cloneComponentSpecificPolicies(
                                ResourceFactory.getComponentResource(ResourceType.RemoteProcessGroup, remoteGroupDTO.getId(), remoteGroupDTO.getName()),
                                ResourceFactory.getComponentResource(ResourceType.RemoteProcessGroup, cp.getId(), cp.getName()), idGenerationSeed);
                    }
                }
            }
            snippetContentsCopy.setRemoteProcessGroups(remoteGroups);

            final Set<ConnectionDTO> connections = new HashSet<>();
            if (snippetContents.getConnections() != null) {
                for (final ConnectionDTO connectionDTO : snippetContents.getConnections()) {
                    final ConnectionDTO cp = dtoFactory.copy(connectionDTO);

                    final ConnectableDTO source = connectableMap.get(cp.getSource().getGroupId() + "-" + cp.getSource().getId());
                    final ConnectableDTO destination = connectableMap.get(cp.getDestination().getGroupId() + "-" + cp.getDestination().getId());

                    // ensure all referenced components are present
                    if (source == null || destination == null) {
                        throw new IllegalArgumentException("The flow snippet contains a Connection that references a component that is not included.");
                    }

                    cp.setId(generateId(connectionDTO.getId(), idGenerationSeed, isCopy));
                    cp.setSource(source);
                    cp.setDestination(destination);
                    cp.setParentGroupId(groupId);
                    connections.add(cp);

                    // note - no need to copy policies of a connection as their permissions are inferred through the source and destination
                }
            }
            snippetContentsCopy.setConnections(connections);

            return snippetContentsCopy;
        } catch (Exception e) {
            // attempt to role back any policies of the copies that were created in preparation for the clone
            rollbackClonedPolicies(snippetContentsCopy);

            // rethrow the original exception
            throw e;
        }
    }

    /**
     * Clones all the component specified policies for the specified original component. This will include the component resource, data resource
     * for the component, view provenance for the component, data transfer resource for the component, and policy resource for the component.
     *
     * @param originalComponentResource original component resource
     * @param clonedComponentResource cloned component resource
     * @param idGenerationSeed id generation seed
     */
    private void cloneComponentSpecificPolicies(final Resource originalComponentResource, final Resource clonedComponentResource, final String idGenerationSeed) {
        if (!accessPolicyDAO.supportsConfigurableAuthorizer()) {
            return;
        }

        final Map<Resource, Resource> resources = new HashMap<>();
        resources.put(originalComponentResource, clonedComponentResource);
        resources.put(ResourceFactory.getDataResource(originalComponentResource), ResourceFactory.getDataResource(clonedComponentResource));
        resources.put(ResourceFactory.getProvenanceDataResource(originalComponentResource), ResourceFactory.getProvenanceDataResource(clonedComponentResource));
        resources.put(ResourceFactory.getDataTransferResource(originalComponentResource), ResourceFactory.getDataTransferResource(clonedComponentResource));
        resources.put(ResourceFactory.getPolicyResource(originalComponentResource), ResourceFactory.getPolicyResource(clonedComponentResource));

        for (final Entry<Resource, Resource> entry : resources.entrySet()) {
            final Resource originalResource = entry.getKey();
            final Resource cloneResource = entry.getValue();

            for (final RequestAction action : RequestAction.values()) {
                final AccessPolicy accessPolicy = accessPolicyDAO.getAccessPolicy(action, originalResource.getIdentifier());

                // if there is a component specific policy we want to clone it for the new component
                if (accessPolicy != null) {
                    final AccessPolicyDTO cloneAccessPolicy = new AccessPolicyDTO();
                    cloneAccessPolicy.setId(generateId(accessPolicy.getIdentifier(), idGenerationSeed, true));
                    cloneAccessPolicy.setAction(accessPolicy.getAction().toString());
                    cloneAccessPolicy.setResource(cloneResource.getIdentifier());

                    final Set<TenantEntity> users = new HashSet<>();
                    accessPolicy.getUsers().forEach(userId -> {
                        final TenantEntity entity = new TenantEntity();
                        entity.setId(userId);
                        users.add(entity);
                    });
                    cloneAccessPolicy.setUsers(users);

                    final Set<TenantEntity> groups = new HashSet<>();
                    accessPolicy.getGroups().forEach(groupId -> {
                        final TenantEntity entity = new TenantEntity();
                        entity.setId(groupId);
                        groups.add(entity);
                    });
                    cloneAccessPolicy.setUserGroups(groups);

                    // create the access policy for the cloned policy
                    accessPolicyDAO.createAccessPolicy(cloneAccessPolicy);
                }
            }
        }
    }

    /**
     * Attempts to roll back and in the specified snippet.
     *
     * @param snippet snippet
     */
    public void rollbackClonedPolicies(final FlowSnippetDTO snippet) {
        if (!accessPolicyDAO.supportsConfigurableAuthorizer()) {
            return;
        }

        snippet.getControllerServices().forEach(controllerServiceDTO -> {
            rollbackClonedPolicy(ResourceFactory.getComponentResource(ResourceType.ControllerService, controllerServiceDTO.getId(), controllerServiceDTO.getName()));
        });
        snippet.getFunnels().forEach(funnelDTO -> {
            rollbackClonedPolicy(ResourceFactory.getComponentResource(ResourceType.Funnel, funnelDTO.getId(), funnelDTO.getId()));
        });
        snippet.getInputPorts().forEach(inputPortDTO -> {
            rollbackClonedPolicy(ResourceFactory.getComponentResource(ResourceType.InputPort, inputPortDTO.getId(), inputPortDTO.getName()));
        });
        snippet.getLabels().forEach(labelDTO -> {
            rollbackClonedPolicy(ResourceFactory.getComponentResource(ResourceType.Label, labelDTO.getId(), labelDTO.getLabel()));
        });
        snippet.getOutputPorts().forEach(outputPortDTO -> {
            rollbackClonedPolicy(ResourceFactory.getComponentResource(ResourceType.OutputPort, outputPortDTO.getId(), outputPortDTO.getName()));
        });
        snippet.getProcessors().forEach(processorDTO -> {
            rollbackClonedPolicy(ResourceFactory.getComponentResource(ResourceType.Processor, processorDTO.getId(), processorDTO.getName()));
        });
        snippet.getRemoteProcessGroups().forEach(remoteProcessGroupDTO -> {
            rollbackClonedPolicy(ResourceFactory.getComponentResource(ResourceType.RemoteProcessGroup, remoteProcessGroupDTO.getId(), remoteProcessGroupDTO.getName()));
        });
        snippet.getProcessGroups().forEach(processGroupDTO -> {
            rollbackClonedPolicy(ResourceFactory.getComponentResource(ResourceType.ProcessGroup, processGroupDTO.getId(), processGroupDTO.getName()));

            // consider all descendant components
            if (processGroupDTO.getContents() != null) {
                rollbackClonedPolicies(processGroupDTO.getContents());
            }
        });
    }

    /**
     * Attempts to roll back all policies for the specified component. This includes the component resource, data resource
     * for the component, view provenance resource for the component, data transfer resource for the component, and policy resource for the component.
     *
     * @param componentResource component resource
     */
    private void rollbackClonedPolicy(final Resource componentResource) {
        if (!accessPolicyDAO.supportsConfigurableAuthorizer()) {
            return;
        }

        final List<Resource> resources = new ArrayList<>();
        resources.add(componentResource);
        resources.add(ResourceFactory.getDataResource(componentResource));
        resources.add(ResourceFactory.getProvenanceDataResource(componentResource));
        resources.add(ResourceFactory.getDataTransferResource(componentResource));
        resources.add(ResourceFactory.getPolicyResource(componentResource));

        for (final Resource resource : resources) {
            for (final RequestAction action : RequestAction.values()) {
                final AccessPolicy accessPolicy = accessPolicyDAO.getAccessPolicy(action, resource.getIdentifier());
                if (accessPolicy != null) {
                    try {
                        accessPolicyDAO.deleteAccessPolicy(accessPolicy.getIdentifier());
                    } catch (final Exception e) {
                        logger.warn(String.format("Unable to clean up cloned access policy for %s %s after failed copy/paste action.", action, componentResource.getIdentifier()), e);
                    }
                }
            }
        }
    }

    private void updateControllerServiceIdentifiers(final FlowSnippetDTO snippet, final Map<String, String> serviceIdMap) {
        final Set<ProcessorDTO> processors = snippet.getProcessors();
        if (processors != null) {
            for (final ProcessorDTO processor : processors) {
                updateControllerServiceIdentifiers(processor.getConfig(), serviceIdMap);
            }
        }

        for (final ProcessGroupDTO processGroupDto : snippet.getProcessGroups()) {
            updateControllerServiceIdentifiers(processGroupDto.getContents(), serviceIdMap);
        }
    }

    private void updateControllerServiceIdentifiers(final ProcessorConfigDTO configDto, final Map<String, String> serviceIdMap) {
        if (configDto == null) {
            return;
        }

        final Map<String, String> properties = configDto.getProperties();
        final Map<String, PropertyDescriptorDTO> descriptors = configDto.getDescriptors();
        if (properties != null && descriptors != null) {
            for (final PropertyDescriptorDTO descriptor : descriptors.values()) {
                if (descriptor.getIdentifiesControllerService() != null) {
                    final String currentServiceId = properties.get(descriptor.getName());
                    if (currentServiceId == null) {
                        continue;
                    }

                    // if this is a copy/paste action, we can continue to reference the same service, in this case
                    // the serviceIdMap will be empty
                    if (serviceIdMap.containsKey(currentServiceId)) {
                        final String newServiceId = serviceIdMap.get(currentServiceId);
                        properties.put(descriptor.getName(), newServiceId);
                    }
                }
            }
        }
    }

    /**
     * Generates a new type 1 id (UUID) for the current id that is specified. If
     * seed is provided, it will be incorporated into generation logic of the
     * new ID.
     * The contract of this method is as follows:
     * - The 'currentId' must never be null and it must be String representation
     *   of type-one UUID.
     * - If seed is provided, the new ID will be generated from the 'msb' extracted from
     *   the 'currentId' and the 'lsb' extracted from the UUID generated via
     *   UUID.nameUUIDFromBytes(currentId + seed).
     * - If seed is NOT provided and 'isCopy' flag is set the new ID will be generated from
     *   the 'msb' extracted from the 'currentId' and random integer as 'lsb'. In this case
     *   the new ID will always be > the previous ID essentially resulting in the new ID for
     *   the component that being copied (e.g., copy/paste).
     * - If seed is NOT provided and 'isCopy' flag is NOT set the new ID will be generated from
     *   the 'msb' extracted from the 'currentId' and random integer as 'lsb'.
     */
    private String generateId(final String currentId, final String seed, boolean isCopy) {
        long msb = UUID.fromString(currentId).getMostSignificantBits();

        UUID uuid;
        if (StringUtils.isBlank(seed)) {
            long lsb = randomGenerator.nextLong();
            if (isCopy) {
                uuid = ComponentIdGenerator.generateId(msb, lsb, true); // will increment msb if necessary
            } else {
                // since msb is extracted from type-one UUID, the type-one semantics will be preserved
                uuid = new UUID(msb, lsb);
            }
        } else {
            UUID seedId = UUID.nameUUIDFromBytes((currentId + seed).getBytes(StandardCharsets.UTF_8));
            if (isCopy) {
                // will ensure the type-one semantics for new UUID generated from msb extracted from seedId
                uuid = ComponentIdGenerator.generateId(seedId.getMostSignificantBits(), seedId.getLeastSignificantBits(), false);
            } else {
                uuid = new UUID(msb, seedId.getLeastSignificantBits());
            }
        }
        logger.debug("Generating UUID {} from currentId={}, seed={}, isCopy={}", uuid, currentId, seed, isCopy);
        return uuid.toString();
    }

    /* setters */
    public void setDtoFactory(final DtoFactory dtoFactory) {
        this.dtoFactory = dtoFactory;
    }

    public void setFlowController(final FlowController flowController) {
        this.flowController = flowController;
    }

    public void setAccessPolicyDAO(AccessPolicyDAO accessPolicyDAO) {
        this.accessPolicyDAO = accessPolicyDAO;
    }

    /**
     * Will normalize the coordinates of the components to ensure their
     * consistency across exports. It will do so by fist calculating the
     * smallest X and smallest Y and then subtracting it from all X's and Y's of
     * each component ensuring that coordinates are consistent across export
     * while preserving relative locations set by the user.
     */
    private void normalizeCoordinates(Collection<? extends ComponentDTO> components) {
        // determine the smallest x,y coordinates in the collection of components
        double smallestX = Double.MAX_VALUE;
        double smallestY = Double.MAX_VALUE;
        for (ComponentDTO component : components) {
            // Connections don't have positions themselves but their bendpoints do, so we need
            // to check those bend points for the smallest x,y coordinates
            if (component instanceof ConnectionDTO) {
                final ConnectionDTO connection = (ConnectionDTO) component;
                for (final PositionDTO position : connection.getBends()) {
                    smallestX = Math.min(smallestX, position.getX());
                    smallestY = Math.min(smallestY, position.getY());
                }
            } else {
                smallestX = Math.min(smallestX, component.getPosition().getX());
                smallestY = Math.min(smallestY, component.getPosition().getY());
            }
        }

        // position the components accordingly
        for (ComponentDTO component : components) {
            if (component instanceof ConnectionDTO) {
                final ConnectionDTO connection = (ConnectionDTO) component;
                for (final PositionDTO position : connection.getBends()) {
                    position.setX(position.getX() - smallestX);
                    position.setY(position.getY() - smallestY);
                }
            } else {
                component.getPosition().setX(component.getPosition().getX() - smallestX);
                component.getPosition().setY(component.getPosition().getY() - smallestY);
            }
        }
    }

}
