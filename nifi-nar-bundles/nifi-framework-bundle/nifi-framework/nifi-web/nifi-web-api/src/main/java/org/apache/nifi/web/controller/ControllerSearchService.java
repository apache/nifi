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
package org.apache.nifi.web.controller;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.registry.ComponentVariableRegistry;
import org.apache.nifi.registry.VariableDescriptor;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.remote.RootGroupPort;
import org.apache.nifi.scheduling.ExecutionNode;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.search.SearchContext;
import org.apache.nifi.search.SearchResult;
import org.apache.nifi.search.Searchable;
import org.apache.nifi.web.api.dto.search.ComponentSearchResultDTO;
import org.apache.nifi.web.api.dto.search.SearchResultGroupDTO;
import org.apache.nifi.web.api.dto.search.SearchResultsDTO;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * NiFi web controller's helper service that implements component search.
 */
public class ControllerSearchService {
    private FlowController flowController;
    private Authorizer authorizer;
    private VariableRegistry variableRegistry;

    /**
     * Searches term in the controller beginning from a given process group.
     *
     * @param results Search results
     * @param search The search term
     * @param group The init process group
     */
    public void search(final SearchResultsDTO results, final String search, final ProcessGroup group) {
        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        if (group.isAuthorized(authorizer, RequestAction.READ, user)) {
            final ComponentSearchResultDTO groupMatch = search(search, group);
            if (groupMatch != null) {
                // get the parent group, not the current one
                groupMatch.setParentGroup(buildResultGroup(group.getParent(), user));
                groupMatch.setVersionedGroup(buildVersionedGroup(group.getParent(), user));
                results.getProcessGroupResults().add(groupMatch);
            }
        }

        for (final ProcessorNode procNode : group.getProcessors()) {
            if (procNode.isAuthorized(authorizer, RequestAction.READ, user)) {
                final ComponentSearchResultDTO match = search(search, procNode);
                if (match != null) {
                    match.setGroupId(group.getIdentifier());
                    match.setParentGroup(buildResultGroup(group, user));
                    match.setVersionedGroup(buildVersionedGroup(group, user));
                    results.getProcessorResults().add(match);
                }
            }
        }

        for (final Connection connection : group.getConnections()) {
            if (connection.isAuthorized(authorizer, RequestAction.READ, user)) {
                final ComponentSearchResultDTO match = search(search, connection);
                if (match != null) {
                    match.setGroupId(group.getIdentifier());
                    match.setParentGroup(buildResultGroup(group, user));
                    match.setVersionedGroup(buildVersionedGroup(group, user));
                    results.getConnectionResults().add(match);
                }
            }
        }

        for (final RemoteProcessGroup remoteGroup : group.getRemoteProcessGroups()) {
            if (remoteGroup.isAuthorized(authorizer, RequestAction.READ, user)) {
                final ComponentSearchResultDTO match = search(search, remoteGroup);
                if (match != null) {
                    match.setGroupId(group.getIdentifier());
                    match.setParentGroup(buildResultGroup(group, user));
                    match.setVersionedGroup(buildVersionedGroup(group, user));
                    results.getRemoteProcessGroupResults().add(match);
                }
            }
        }

        for (final Port port : group.getInputPorts()) {
            if (port.isAuthorized(authorizer, RequestAction.READ, user)) {
                final ComponentSearchResultDTO match = search(search, port);
                if (match != null) {
                    match.setGroupId(group.getIdentifier());
                    match.setParentGroup(buildResultGroup(group, user));
                    match.setVersionedGroup(buildVersionedGroup(group, user));
                    results.getInputPortResults().add(match);
                }
            }
        }

        for (final Port port : group.getOutputPorts()) {
            if (port.isAuthorized(authorizer, RequestAction.READ, user)) {
                final ComponentSearchResultDTO match = search(search, port);
                if (match != null) {
                    match.setGroupId(group.getIdentifier());
                    match.setParentGroup(buildResultGroup(group, user));
                    match.setVersionedGroup(buildVersionedGroup(group, user));
                    results.getOutputPortResults().add(match);
                }
            }
        }

        for (final Funnel funnel : group.getFunnels()) {
            if (funnel.isAuthorized(authorizer, RequestAction.READ, user)) {
                final ComponentSearchResultDTO match = search(search, funnel);
                if (match != null) {
                    match.setGroupId(group.getIdentifier());
                    match.setParentGroup(buildResultGroup(group, user));
                    match.setVersionedGroup(buildVersionedGroup(group, user));
                    results.getFunnelResults().add(match);
                }
            }
        }

        for (final ProcessGroup processGroup : group.getProcessGroups()) {
            search(results, search, processGroup);
        }
    }

    private ComponentSearchResultDTO search(final String searchStr, final Port port) {
        final List<String> matches = new ArrayList<>();

        addIfAppropriate(searchStr, port.getIdentifier(), "Id", matches);
        addIfAppropriate(searchStr, port.getVersionedComponentId().orElse(null), "Version Control ID", matches);
        addIfAppropriate(searchStr, port.getName(), "Name", matches);
        addIfAppropriate(searchStr, port.getComments(), "Comments", matches);

        // consider scheduled state
        if (ScheduledState.DISABLED.equals(port.getScheduledState())) {
            if (StringUtils.containsIgnoreCase("disabled", searchStr)) {
                matches.add("Run status: Disabled");
            }
        } else {
            if (StringUtils.containsIgnoreCase("invalid", searchStr) && !port.isValid()) {
                matches.add("Run status: Invalid");
            } else if (ScheduledState.RUNNING.equals(port.getScheduledState()) && StringUtils.containsIgnoreCase("running", searchStr)) {
                matches.add("Run status: Running");
            } else if (ScheduledState.STOPPED.equals(port.getScheduledState()) && StringUtils.containsIgnoreCase("stopped", searchStr)) {
                matches.add("Run status: Stopped");
            }
        }

        if (port instanceof RootGroupPort) {
            final RootGroupPort rootGroupPort = (RootGroupPort) port;

            // user access controls
            for (final String userAccessControl : rootGroupPort.getUserAccessControl()) {
                addIfAppropriate(searchStr, userAccessControl, "User access control", matches);
            }

            // group access controls
            for (final String groupAccessControl : rootGroupPort.getGroupAccessControl()) {
                addIfAppropriate(searchStr, groupAccessControl, "Group access control", matches);
            }
        }

        if (matches.isEmpty()) {
            return null;
        }

        final ComponentSearchResultDTO dto = new ComponentSearchResultDTO();
        dto.setId(port.getIdentifier());
        dto.setName(port.getName());
        dto.setMatches(matches);
        return dto;
    }

    private ComponentSearchResultDTO search(final String searchStr, final ProcessorNode procNode) {
        final List<String> matches = new ArrayList<>();
        final Processor processor = procNode.getProcessor();

        addIfAppropriate(searchStr, procNode.getIdentifier(), "Id", matches);
        addIfAppropriate(searchStr, procNode.getVersionedComponentId().orElse(null), "Version Control ID", matches);
        addIfAppropriate(searchStr, procNode.getName(), "Name", matches);
        addIfAppropriate(searchStr, procNode.getComments(), "Comments", matches);

        // consider scheduling strategy
        if (SchedulingStrategy.EVENT_DRIVEN.equals(procNode.getSchedulingStrategy()) && StringUtils.containsIgnoreCase("event", searchStr)) {
            matches.add("Scheduling strategy: Event driven");
        } else if (SchedulingStrategy.TIMER_DRIVEN.equals(procNode.getSchedulingStrategy()) && StringUtils.containsIgnoreCase("timer", searchStr)) {
            matches.add("Scheduling strategy: Timer driven");
        } else if (SchedulingStrategy.PRIMARY_NODE_ONLY.equals(procNode.getSchedulingStrategy()) && StringUtils.containsIgnoreCase("primary", searchStr)) {
            // PRIMARY_NODE_ONLY has been deprecated as a SchedulingStrategy and replaced by PRIMARY as an ExecutionNode.
            matches.add("Scheduling strategy: On primary node");
        }

        // consider execution node
        if (ExecutionNode.PRIMARY.equals(procNode.getExecutionNode()) && StringUtils.containsIgnoreCase("primary", searchStr)) {
            matches.add("Execution node: primary");
        }

        // consider scheduled state
        if (ScheduledState.DISABLED.equals(procNode.getScheduledState())) {
            if (StringUtils.containsIgnoreCase("disabled", searchStr)) {
                matches.add("Run status: Disabled");
            }
        } else {
            if (StringUtils.containsIgnoreCase("invalid", searchStr) && procNode.getValidationStatus() == ValidationStatus.INVALID) {
                matches.add("Run status: Invalid");
            } else if (StringUtils.containsIgnoreCase("validating", searchStr) && procNode.getValidationStatus() == ValidationStatus.VALIDATING) {
                matches.add("Run status: Validating");
            } else if (ScheduledState.RUNNING.equals(procNode.getScheduledState()) && StringUtils.containsIgnoreCase("running", searchStr)) {
                matches.add("Run status: Running");
            } else if (ScheduledState.STOPPED.equals(procNode.getScheduledState()) && StringUtils.containsIgnoreCase("stopped", searchStr)) {
                matches.add("Run status: Stopped");
            }
        }

        for (final Relationship relationship : procNode.getRelationships()) {
            addIfAppropriate(searchStr, relationship.getName(), "Relationship", matches);
        }

        // Add both the actual class name and the component type. This allows us to search for 'Ghost'
        // to search for components that could not be instantiated.
        addIfAppropriate(searchStr, processor.getClass().getSimpleName(), "Type", matches);
        addIfAppropriate(searchStr, procNode.getComponentType(), "Type", matches);

        for (final Map.Entry<PropertyDescriptor, String> entry : procNode.getProperties().entrySet()) {
            final PropertyDescriptor descriptor = entry.getKey();

            addIfAppropriate(searchStr, descriptor.getName(), "Property name", matches);
            addIfAppropriate(searchStr, descriptor.getDescription(), "Property description", matches);

            // never include sensitive properties values in search results
            if (descriptor.isSensitive()) {
                continue;
            }

            String value = entry.getValue();

            // if unset consider default value
            if (value == null) {
                value = descriptor.getDefaultValue();
            }

            // evaluate if the value matches the search criteria
            if (StringUtils.containsIgnoreCase(value, searchStr)) {
                matches.add("Property value: " + descriptor.getName() + " - " + value);
            }
        }

        // consider searching the processor directly
        if (processor instanceof Searchable) {
            final Searchable searchable = (Searchable) processor;

            final SearchContext context = new StandardSearchContext(searchStr, procNode, flowController.getControllerServiceProvider(), variableRegistry);

            // search the processor using the appropriate thread context classloader
            try (final NarCloseable x = NarCloseable.withComponentNarLoader(flowController.getExtensionManager(), processor.getClass(), processor.getIdentifier())) {
                final Collection<SearchResult> searchResults = searchable.search(context);
                if (CollectionUtils.isNotEmpty(searchResults)) {
                    for (final SearchResult searchResult : searchResults) {
                        matches.add(searchResult.getLabel() + ": " + searchResult.getMatch());
                    }
                }
            } catch (final Throwable t) {
                // log this as error
            }
        }

        if (matches.isEmpty()) {
            return null;
        }

        final ComponentSearchResultDTO result = new ComponentSearchResultDTO();
        result.setId(procNode.getIdentifier());
        result.setMatches(matches);
        result.setName(procNode.getName());
        return result;
    }

    private ComponentSearchResultDTO search(final String searchStr, final ProcessGroup group) {
        final List<String> matches = new ArrayList<>();
        final ProcessGroup parent = group.getParent();
        if (parent == null) {
            return null;
        }

        addIfAppropriate(searchStr, group.getIdentifier(), "Id", matches);
        addIfAppropriate(searchStr, group.getVersionedComponentId().orElse(null), "Version Control ID", matches);
        addIfAppropriate(searchStr, group.getName(), "Name", matches);
        addIfAppropriate(searchStr, group.getComments(), "Comments", matches);

        final ComponentVariableRegistry varRegistry = group.getVariableRegistry();
        if (varRegistry != null) {
            final Map<VariableDescriptor, String> variableMap = varRegistry.getVariableMap();
            for (final Map.Entry<VariableDescriptor, String> entry : variableMap.entrySet()) {
                addIfAppropriate(searchStr, entry.getKey().getName(), "Variable Name", matches);
                addIfAppropriate(searchStr, entry.getValue(), "Variable Value", matches);
            }
        }

        if (matches.isEmpty()) {
            return null;
        }

        final ComponentSearchResultDTO result = new ComponentSearchResultDTO();
        result.setId(group.getIdentifier());
        result.setName(group.getName());
        result.setGroupId(parent.getIdentifier());
        result.setMatches(matches);
        return result;
    }

    private ComponentSearchResultDTO search(final String searchStr, final Connection connection) {
        final List<String> matches = new ArrayList<>();

        // search id and name
        addIfAppropriate(searchStr, connection.getIdentifier(), "Id", matches);
        addIfAppropriate(searchStr, connection.getVersionedComponentId().orElse(null), "Version Control ID", matches);
        addIfAppropriate(searchStr, connection.getName(), "Name", matches);

        // search relationships
        for (final Relationship relationship : connection.getRelationships()) {
            addIfAppropriate(searchStr, relationship.getName(), "Relationship", matches);
        }

        // search prioritizers
        final FlowFileQueue queue = connection.getFlowFileQueue();
        for (final FlowFilePrioritizer comparator : queue.getPriorities()) {
            addIfAppropriate(searchStr, comparator.getClass().getName(), "Prioritizer", matches);
        }

        // search expiration
        if (StringUtils.containsIgnoreCase("expires", searchStr) || StringUtils.containsIgnoreCase("expiration", searchStr)) {
            final int expirationMillis = connection.getFlowFileQueue().getFlowFileExpiration(TimeUnit.MILLISECONDS);
            if (expirationMillis > 0) {
                matches.add("FlowFile expiration: " + connection.getFlowFileQueue().getFlowFileExpiration());
            }
        }

        // search back pressure
        if (StringUtils.containsIgnoreCase("back pressure", searchStr) || StringUtils.containsIgnoreCase("pressure", searchStr)) {
            final String backPressureDataSize = connection.getFlowFileQueue().getBackPressureDataSizeThreshold();
            final Double backPressureBytes = DataUnit.parseDataSize(backPressureDataSize, DataUnit.B);
            if (backPressureBytes > 0) {
                matches.add("Back pressure data size: " + backPressureDataSize);
            }

            final long backPressureCount = connection.getFlowFileQueue().getBackPressureObjectThreshold();
            if (backPressureCount > 0) {
                matches.add("Back pressure count: " + backPressureCount);
            }
        }

        // search the source
        final Connectable source = connection.getSource();
        addIfAppropriate(searchStr, source.getIdentifier(), "Source id", matches);
        addIfAppropriate(searchStr, source.getName(), "Source name", matches);
        addIfAppropriate(searchStr, source.getComments(), "Source comments", matches);

        // search the destination
        final Connectable destination = connection.getDestination();
        addIfAppropriate(searchStr, destination.getIdentifier(), "Destination id", matches);
        addIfAppropriate(searchStr, destination.getName(), "Destination name", matches);
        addIfAppropriate(searchStr, destination.getComments(), "Destination comments", matches);

        if (matches.isEmpty()) {
            return null;
        }

        final ComponentSearchResultDTO result = new ComponentSearchResultDTO();
        result.setId(connection.getIdentifier());

        // determine the name of the search match
        if (StringUtils.isNotBlank(connection.getName())) {
            result.setName(connection.getName());
        } else if (!connection.getRelationships().isEmpty()) {
            final List<String> relationships = new ArrayList<>(connection.getRelationships().size());
            for (final Relationship relationship : connection.getRelationships()) {
                if (StringUtils.isNotBlank(relationship.getName())) {
                    relationships.add(relationship.getName());
                }
            }
            if (!relationships.isEmpty()) {
                result.setName(StringUtils.join(relationships, ", "));
            }
        }

        // ensure a name is added
        if (result.getName() == null) {
            result.setName("From source " + connection.getSource().getName());
        }

        result.setMatches(matches);
        return result;
    }

    private ComponentSearchResultDTO search(final String searchStr, final RemoteProcessGroup group) {
        final List<String> matches = new ArrayList<>();
        addIfAppropriate(searchStr, group.getIdentifier(), "Id", matches);
        addIfAppropriate(searchStr, group.getVersionedComponentId().orElse(null), "Version Control ID", matches);
        addIfAppropriate(searchStr, group.getName(), "Name", matches);
        addIfAppropriate(searchStr, group.getComments(), "Comments", matches);
        addIfAppropriate(searchStr, group.getTargetUris(), "URLs", matches);

        // consider the transmission status
        if ((StringUtils.containsIgnoreCase("transmitting", searchStr) || StringUtils.containsIgnoreCase("transmission enabled", searchStr)) && group.isTransmitting()) {
            matches.add("Transmission: On");
        } else if ((StringUtils.containsIgnoreCase("not transmitting", searchStr) || StringUtils.containsIgnoreCase("transmission disabled", searchStr)) && !group.isTransmitting()) {
            matches.add("Transmission: Off");
        }

        if (matches.isEmpty()) {
            return null;
        }

        final ComponentSearchResultDTO result = new ComponentSearchResultDTO();
        result.setId(group.getIdentifier());
        result.setName(group.getName());
        result.setMatches(matches);
        return result;
    }

    private ComponentSearchResultDTO search(final String searchStr, final Funnel funnel) {
        final List<String> matches = new ArrayList<>();
        addIfAppropriate(searchStr, funnel.getIdentifier(), "Id", matches);
        addIfAppropriate(searchStr, funnel.getVersionedComponentId().orElse(null), "Version Control ID", matches);

        if (matches.isEmpty()) {
            return null;
        }

        final ComponentSearchResultDTO dto = new ComponentSearchResultDTO();
        dto.setId(funnel.getIdentifier());
        dto.setName(funnel.getName());
        dto.setMatches(matches);
        return dto;
    }

    /**
     * Builds the nearest versioned parent result group for a given user.
     *
     * @param group The containing group
     * @param user The current NiFi user
     * @return Versioned parent group
     */
    private SearchResultGroupDTO buildVersionedGroup(final ProcessGroup group, final NiFiUser user) {
        if (group == null) {
            return null;
        }

        ProcessGroup tmpParent = group.getParent();
        ProcessGroup tmpGroup = group;

        // search for a versioned group by traversing the group tree up to the root
        while (!tmpGroup.isRootGroup()) {
            if (tmpGroup.getVersionControlInformation() != null) {
                return buildResultGroup(tmpGroup, user);
            }

            tmpGroup = tmpParent;
            tmpParent = tmpGroup.getParent();
        }

        // traversed all the way to the root
        return null;
    }

    /**
     * Builds result group for a given user.
     *
     * @param group The containing group
     * @param user The current NiFi user
     * @return Result group
     */
    private SearchResultGroupDTO buildResultGroup(final ProcessGroup group, final NiFiUser user) {
        if (group == null) {
            return null;
        }

        final SearchResultGroupDTO resultGroup = new SearchResultGroupDTO();
        resultGroup.setId(group.getIdentifier());

        // keep the group name confidential
        if (group.isAuthorized(authorizer, RequestAction.READ, user)) {
            resultGroup.setName(group.getName());
        }

        return resultGroup;
    }

    private void addIfAppropriate(final String searchStr, final String value, final String label, final List<String> matches) {
        if (StringUtils.containsIgnoreCase(value, searchStr)) {
            matches.add(label + ": " + value);
        }
    }

    public void setFlowController(FlowController flowController) {
        this.flowController = flowController;
    }

    public void setAuthorizer(Authorizer authorizer) {
        this.authorizer = authorizer;
    }

    public void setVariableRegistry(VariableRegistry variableRegistry) {
        this.variableRegistry = variableRegistry;
    }
}
