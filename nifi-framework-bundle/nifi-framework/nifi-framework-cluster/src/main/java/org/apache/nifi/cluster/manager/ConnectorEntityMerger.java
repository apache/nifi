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
package org.apache.nifi.cluster.manager;

import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.components.connector.ConnectorState;
import org.apache.nifi.components.connector.FlowEvolutionStatus;
import org.apache.nifi.web.api.dto.BundleDTO;
import org.apache.nifi.web.api.dto.ConfigurationStepConfigurationDTO;
import org.apache.nifi.web.api.dto.ConnectorActionDTO;
import org.apache.nifi.web.api.dto.ConnectorConfigurationDTO;
import org.apache.nifi.web.api.dto.ConnectorDTO;
import org.apache.nifi.web.api.dto.status.ConnectorStatusDTO;
import org.apache.nifi.web.api.dto.status.NodeConnectorStatusSnapshotDTO;
import org.apache.nifi.web.api.entity.ConnectorEntity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class ConnectorEntityMerger {
    public static final String LOADED_BUNDLE_MISMATCH_REASON = "Cluster nodes load different Connector versions: ";

    private static final String CHANGE_VERSION_ACTION = "CHANGE_VERSION";
    private static final String START_ACTION = "START";

    /**
     * Merges the ConnectorEntity responses.
     *
     * @param clientEntity the entity being returned to the client
     * @param entityMap all node responses
     */
    public static void merge(final ConnectorEntity clientEntity, final Map<NodeIdentifier, ConnectorEntity> entityMap) {
        final ConnectorDTO clientDto = clientEntity.getComponent();
        final Map<NodeIdentifier, ConnectorDTO> dtoMap = new HashMap<>();
        for (final Map.Entry<NodeIdentifier, ConnectorEntity> entry : entityMap.entrySet()) {
            dtoMap.put(entry.getKey(), entry.getValue().getComponent());
        }

        mergeDtos(clientDto, dtoMap);
        mergeStatus(clientEntity, entityMap);
    }

    private static void mergeStatus(final ConnectorEntity clientEntity, final Map<NodeIdentifier, ConnectorEntity> entityMap) {
        final ConnectorStatusDTO mergedStatus = clientEntity.getStatus();
        if (mergedStatus == null) {
            return;
        }

        mergedStatus.setNodeSnapshots(new ArrayList<>());

        // Identify the selected node (the one whose response was chosen as the client entity)
        final NodeIdentifier selectedNodeId = entityMap.entrySet().stream()
                .filter(e -> e.getValue() == clientEntity)
                .map(Map.Entry::getKey)
                .findFirst()
                .orElse(null);

        // Add the selected node's snapshot to the node snapshots list
        if (selectedNodeId != null && mergedStatus.getAggregateSnapshot() != null) {
            final NodeConnectorStatusSnapshotDTO selectedNodeSnapshot = new NodeConnectorStatusSnapshotDTO();
            selectedNodeSnapshot.setStatusSnapshot(mergedStatus.getAggregateSnapshot().clone());
            selectedNodeSnapshot.setAddress(selectedNodeId.getApiAddress());
            selectedNodeSnapshot.setApiPort(selectedNodeId.getApiPort());
            selectedNodeSnapshot.setNodeId(selectedNodeId.getId());
            mergedStatus.getNodeSnapshots().add(selectedNodeSnapshot);
        }

        // Merge snapshots from other nodes
        for (final Map.Entry<NodeIdentifier, ConnectorEntity> entry : entityMap.entrySet()) {
            final NodeIdentifier nodeId = entry.getKey();
            final ConnectorEntity nodeEntity = entry.getValue();
            if (nodeEntity == clientEntity || nodeEntity == null) {
                continue;
            }

            final ConnectorStatusDTO nodeStatus = nodeEntity.getStatus();
            if (nodeStatus == null) {
                continue;
            }

            final boolean clientReadable = clientEntity.getPermissions() != null && Boolean.TRUE.equals(clientEntity.getPermissions().getCanRead());
            final boolean nodeReadable = nodeEntity.getPermissions() != null && Boolean.TRUE.equals(nodeEntity.getPermissions().getCanRead());
            StatusMerger.merge(mergedStatus, clientReadable, nodeStatus, nodeReadable,
                    nodeId.getId(), nodeId.getApiAddress(), nodeId.getApiPort());
        }
    }

    private static void mergeDtos(final ConnectorDTO clientDto, final Map<NodeIdentifier, ConnectorDTO> dtoMap) {
        // if unauthorized for the client dto, simple return
        if (clientDto == null) {
            return;
        }

        for (final ConnectorDTO nodeConnector : dtoMap.values()) {
            if (nodeConnector != null) {
                if (clientDto.getMultipleVersionsAvailable() == null || !Boolean.TRUE.equals(nodeConnector.getMultipleVersionsAvailable())) {
                    clientDto.setMultipleVersionsAvailable(Boolean.FALSE);
                }
            }
        }

        mergeChangeVersion(clientDto, dtoMap);
        mergeLoadedBundles(clientDto, dtoMap);
        // Status is merged before state. State merge rewrites the client DTO, and the barrier has to see each node's own state.
        mergeFlowEvolutionStatus(clientDto, dtoMap);
        mergeState(clientDto, dtoMap);

        // Merge configuration steps to handle dynamic property descriptors
        mergeActiveConfiguration(clientDto, dtoMap);
        mergeWorkingConfiguration(clientDto, dtoMap);
    }

    /**
     * A version change is offered only when every node allows it. If any node is missing the action, or does not allow it, the merged action is not
     * allowed and keeps that node's reason.
     */
    private static void mergeChangeVersion(final ConnectorDTO clientDto, final Map<NodeIdentifier, ConnectorDTO> dtoMap) {
        for (final ConnectorDTO nodeConnector : dtoMap.values()) {
            if (nodeConnector == null) {
                continue;
            }

            final ConnectorActionDTO nodeAction = findAction(nodeConnector, CHANGE_VERSION_ACTION);
            if (nodeAction != null && Boolean.TRUE.equals(nodeAction.getAllowed())) {
                continue;
            }

            final ConnectorActionDTO clientAction = findOrCreateChangeVersionAction(clientDto);
            clientAction.setAllowed(Boolean.FALSE);
            clientAction.setReasonNotAllowed(nodeAction == null ? null : nodeAction.getReasonNotAllowed());
        }
    }

    /**
     * Nodes that load different Connector versions would evolve to different flows, so START is not allowed until every node
     * loads the same bundle.
     */
    private static void mergeLoadedBundles(final ConnectorDTO clientDto, final Map<NodeIdentifier, ConnectorDTO> dtoMap) {
        final Map<String, String> loadedBundles = new TreeMap<>();
        for (final Map.Entry<NodeIdentifier, ConnectorDTO> entry : dtoMap.entrySet()) {
            final ConnectorDTO nodeConnector = entry.getValue();
            if (nodeConnector == null || nodeConnector.getBundle() == null) {
                continue;
            }

            final BundleDTO bundle = nodeConnector.getBundle();
            loadedBundles.put(entry.getKey().getApiAddress() + ":" + entry.getKey().getApiPort(),
                    bundle.getGroup() + ":" + bundle.getArtifact() + ":" + bundle.getVersion());
        }

        if (new HashSet<>(loadedBundles.values()).size() <= 1) {
            return;
        }

        final ConnectorActionDTO startAction = findAction(clientDto, START_ACTION);
        if (startAction != null) {
            startAction.setAllowed(Boolean.FALSE);
            startAction.setReasonNotAllowed(LOADED_BUNDLE_MISMATCH_REASON + loadedBundles);
        }
    }

    private static ConnectorActionDTO findOrCreateChangeVersionAction(final ConnectorDTO clientDto) {
        final ConnectorActionDTO existingAction = findAction(clientDto, CHANGE_VERSION_ACTION);
        if (existingAction != null) {
            return existingAction;
        }

        final ConnectorActionDTO changeVersionAction = new ConnectorActionDTO();
        changeVersionAction.setName(CHANGE_VERSION_ACTION);
        changeVersionAction.setDescription("Change the connector version");

        final List<ConnectorActionDTO> actions = new ArrayList<>();
        if (clientDto.getAvailableActions() != null) {
            actions.addAll(clientDto.getAvailableActions());
        }
        actions.add(changeVersionAction);
        clientDto.setAvailableActions(actions);
        return changeVersionAction;
    }

    private static ConnectorActionDTO findAction(final ConnectorDTO connector, final String actionName) {
        final List<ConnectorActionDTO> actions = connector.getAvailableActions();
        if (actions == null) {
            return null;
        }

        for (final ConnectorActionDTO action : actions) {
            if (actionName.equals(action.getName())) {
                return action;
            }
        }

        return null;
    }

    /**
     * Merges state from cluster nodes using priority-based selection.
     * Priority (highest to lowest):
     * 1. UPDATE_FAILED - indicates a failed update that needs attention
     * 2. PREPARING_FOR_UPDATE - connector is preparing for update
     * 3. UPDATING - connector is actively updating
     * 4. UPDATED - connector has been updated
     * 5. STARTING/STOPPING - connector is transitioning between run states
     * 6. RUNNING/STOPPED/DISABLED - stable states
     */
    private static void mergeState(final ConnectorDTO clientDto, final Map<NodeIdentifier, ConnectorDTO> dtoMap) {
        String mergedState = clientDto.getState();

        for (final ConnectorDTO nodeConnector : dtoMap.values()) {
            if (nodeConnector != null) {
                final String nodeState = nodeConnector.getState();
                if (nodeState != null) {
                    if (getStatePriority(nodeState) > getStatePriority(mergedState)) {
                        mergedState = nodeState;
                    }
                }
            }
        }

        clientDto.setState(mergedState);
    }

    private static void mergeFlowEvolutionStatus(final ConnectorDTO clientDto, final Map<NodeIdentifier, ConnectorDTO> dtoMap) {
        boolean anyPhaseTwo = false;
        boolean anyEarlierPhase = false;
        for (final ConnectorDTO nodeConnector : dtoMap.values()) {
            final FlowEvolutionStatus status = reportedFlowEvolutionStatus(nodeConnector);
            if (status == FlowEvolutionStatus.PHASE_TWO_COMPLETE) {
                anyPhaseTwo = true;
            } else if (status == FlowEvolutionStatus.PHASE_ONE_PENDING || status == FlowEvolutionStatus.PHASE_ONE_COMPLETE) {
                anyEarlierPhase = true;
            }
        }
        // A node that has finished reports INACTIVE once it is RUNNING. While a peer is still at phase two, count the
        // finished node as phase two. Doing that for an earlier phase would hide a node that has not started.
        final boolean countRunningInactiveAsPhaseTwo = anyPhaseTwo && !anyEarlierPhase;

        final List<FlowEvolutionStatus> statuses = new ArrayList<>(dtoMap.size());
        for (final ConnectorDTO nodeConnector : dtoMap.values()) {
            statuses.add(barrierFlowEvolutionStatus(nodeConnector, countRunningInactiveAsPhaseTwo));
        }

        clientDto.setFlowEvolutionStatus(FlowEvolutionStatus.merge(statuses).name());
    }

    private static FlowEvolutionStatus reportedFlowEvolutionStatus(final ConnectorDTO nodeConnector) {
        if (nodeConnector == null || nodeConnector.getFlowEvolutionStatus() == null) {
            return FlowEvolutionStatus.INACTIVE;
        }
        return FlowEvolutionStatus.valueOf(nodeConnector.getFlowEvolutionStatus());
    }

    private static FlowEvolutionStatus barrierFlowEvolutionStatus(final ConnectorDTO nodeConnector, final boolean countRunningInactiveAsPhaseTwo) {
        final FlowEvolutionStatus status = reportedFlowEvolutionStatus(nodeConnector);
        if (countRunningInactiveAsPhaseTwo && status == FlowEvolutionStatus.INACTIVE && nodeConnector != null
                && ConnectorState.RUNNING.name().equals(nodeConnector.getState())) {
            return FlowEvolutionStatus.PHASE_TWO_COMPLETE;
        }
        return status;
    }

    private static int getStatePriority(final String state) {
        if (state == null) {
            return 0;
        }

        final ConnectorState connectorState = ConnectorState.valueOf(state);
        return switch (connectorState) {
            case UPDATE_FAILED -> 7;
            case PREPARING_FOR_UPDATE -> 6;
            case UPDATING -> 5;
            case DRAINING, PURGING -> 4;
            case UPDATED -> 3;
            case STARTING, STOPPING -> 2;
            default -> 1; // RUNNING, STOPPED
        };
    }

    private static void mergeActiveConfiguration(final ConnectorDTO clientDto, final Map<NodeIdentifier, ConnectorDTO> dtoMap) {
        final ConnectorConfigurationDTO clientConfig = clientDto.getActiveConfiguration();
        if (clientConfig == null || clientConfig.getConfigurationStepConfigurations() == null) {
            return;
        }

        // For each configuration step in the client's active configuration, merge it with the corresponding steps from all nodes
        for (final ConfigurationStepConfigurationDTO clientStep : clientConfig.getConfigurationStepConfigurations()) {
            final Map<NodeIdentifier, ConfigurationStepConfigurationDTO> stepDtoMap = new HashMap<>();

            for (final Map.Entry<NodeIdentifier, ConnectorDTO> nodeEntry : dtoMap.entrySet()) {
                final ConnectorDTO nodeDto = nodeEntry.getValue();
                if (nodeDto == null || nodeDto.getActiveConfiguration() == null || nodeDto.getActiveConfiguration().getConfigurationStepConfigurations() == null) {
                    continue;
                }

                // Find the matching configuration step in the node's active configuration
                nodeDto.getActiveConfiguration().getConfigurationStepConfigurations().stream()
                        .filter(step -> clientStep.getConfigurationStepName() != null
                                && clientStep.getConfigurationStepName().equals(step.getConfigurationStepName()))
                        .findFirst()
                        .ifPresent(step -> stepDtoMap.put(nodeEntry.getKey(), step));
            }

            // Merge property descriptors for this configuration step
            ConfigurationStepEntityMerger.mergePropertyDescriptors(clientStep, stepDtoMap);
        }
    }

    private static void mergeWorkingConfiguration(final ConnectorDTO clientDto, final Map<NodeIdentifier, ConnectorDTO> dtoMap) {
        final ConnectorConfigurationDTO clientConfig = clientDto.getWorkingConfiguration();
        if (clientConfig == null || clientConfig.getConfigurationStepConfigurations() == null) {
            return;
        }

        // For each configuration step in the client's working configuration, merge it with the corresponding steps from all nodes
        for (final ConfigurationStepConfigurationDTO clientStep : clientConfig.getConfigurationStepConfigurations()) {
            final Map<NodeIdentifier, ConfigurationStepConfigurationDTO> stepDtoMap = new HashMap<>();

            for (final Map.Entry<NodeIdentifier, ConnectorDTO> nodeEntry : dtoMap.entrySet()) {
                final ConnectorDTO nodeDto = nodeEntry.getValue();
                if (nodeDto == null || nodeDto.getWorkingConfiguration() == null || nodeDto.getWorkingConfiguration().getConfigurationStepConfigurations() == null) {
                    continue;
                }

                // Find the matching configuration step in the node's working configuration
                nodeDto.getWorkingConfiguration().getConfigurationStepConfigurations().stream()
                        .filter(step -> clientStep.getConfigurationStepName() != null
                                && clientStep.getConfigurationStepName().equals(step.getConfigurationStepName()))
                        .findFirst()
                        .ifPresent(step -> stepDtoMap.put(nodeEntry.getKey(), step));
            }

            // Merge property descriptors for this configuration step
            ConfigurationStepEntityMerger.mergePropertyDescriptors(clientStep, stepDtoMap);
        }
    }
}
