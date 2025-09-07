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
package org.apache.nifi.web.dao.impl;

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.controller.FlowAnalysisRuleNode;
import org.apache.nifi.controller.ParameterProviderNode;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.api.dto.ComponentStateDTO;
import org.apache.nifi.web.api.dto.StateMapDTO;
import org.apache.nifi.web.dao.ComponentStateDAO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Repository
public class StandardComponentStateDAO implements ComponentStateDAO {

    private StateManagerProvider stateManagerProvider;

    private StateMap getState(final String componentId, final Scope scope) {
        try {
            final StateManager manager = stateManagerProvider.getStateManager(componentId);
            if (manager == null) {
                throw new ResourceNotFoundException("State for the specified component %s could not be found.".formatted(componentId));
            }

            return manager.getState(scope);
        } catch (final IOException ioe) {
            throw new IllegalStateException("Unable to get the state for the specified component %s: %s".formatted(componentId, ioe), ioe);
        }
    }

    private void clearState(final String componentId, final ComponentStateDTO componentStateDTO) {
        try {
            final StateManager manager = stateManagerProvider.getStateManager(componentId);
            if (manager == null) {
                throw new ResourceNotFoundException("State for the specified component %s could not be found.".formatted(componentId));
            }

            // No DTO provided: clear both scopes
            if (componentStateDTO == null) {
                manager.clear(Scope.CLUSTER);
                manager.clear(Scope.LOCAL);
                return;
            }

            // Determine if there is existing local state
            final StateMap localStateMap = manager.getState(Scope.LOCAL);
            final boolean hasLocalState = localStateMap != null && !localStateMap.toMap().isEmpty();

            if (hasLocalState) {
                // Local state exists
                if (!stateManagerProvider.isClusterProviderEnabled()) {
                    // Standalone mode: allow selective local key removal
                    if (!manager.isStateKeyDropSupported()) {
                        throw new IllegalStateException("Selective state key removal is not supported for component %s with local state.".formatted(componentId));
                    }

                    final Map<String, String> newLocalState = toStateMap(componentStateDTO.getLocalState());
                    final Map<String, String> currentLocalState = localStateMap.toMap();

                    if (hasExactlyOneKeyRemoved(currentLocalState, newLocalState)) {
                        manager.setState(newLocalState, Scope.LOCAL);
                    } else {
                        throw new IllegalStateException("Unable to remove a state key for component %s. Exactly one key removal is supported.".formatted(componentId));
                    }
                } else {
                    // Cluster mode with existing local state: do not allow selective removal
                    throw new IllegalStateException("Selective state key removal is not supported for component %s with local state.".formatted(componentId));
                }
                return;
            }

            // No local state present, check for cluster state selective removal
            final StateMapDTO clusterStateMapDto = componentStateDTO.getClusterState();
            if (clusterStateMapDto != null && clusterStateMapDto.getState() != null && !clusterStateMapDto.getState().isEmpty()) {
                if (!manager.isStateKeyDropSupported()) {
                    throw new IllegalStateException("Selective state key removal is not supported for component %s with cluster state.".formatted(componentId));
                }

                final Map<String, String> newClusterState = toStateMap(clusterStateMapDto);
                final Map<String, String> currentClusterState = manager.getState(Scope.CLUSTER).toMap();

                if (hasExactlyOneKeyRemoved(currentClusterState, newClusterState)) {
                    manager.setState(newClusterState, Scope.CLUSTER);
                } else {
                    throw new IllegalStateException("Unable to remove a state key for component %s. Exactly one key removal is supported.".formatted(componentId));
                }

                // Ensure local state is cleared
                manager.clear(Scope.LOCAL);
                return;
            }

            // Default: clear both scopes
            manager.clear(Scope.CLUSTER);
            manager.clear(Scope.LOCAL);
        } catch (final IOException ioe) {
            throw new IllegalStateException("Unable to clear the state for the specified component %s: %s".formatted(componentId, ioe), ioe);
        }
    }

    private boolean hasExactlyOneKeyRemoved(Map<String, String> currentState, Map<String, String> newState) {
        // Check if newState has exactly one less key
        if (currentState.size() - newState.size() != 1) {
            return false;
        }

        // Check that newState is a subset of currentState
        return newState.entrySet()
                .stream()
                .allMatch(entry -> entry.getValue().equals(currentState.get(entry.getKey())));
    }

    private static Map<String, String> toStateMap(final StateMapDTO stateMapDTO) {
        final Map<String, String> map = new HashMap<>();
        if (stateMapDTO == null || stateMapDTO.getState() == null) {
            return map;
        }

        stateMapDTO.getState().forEach(entry -> map.put(entry.getKey(), entry.getValue()));
        return map;
    }

    @Override
    public StateMap getState(final ProcessorNode processor, final Scope scope) {
        return getState(processor.getIdentifier(), scope);
    }

    @Override
    public void clearState(final ProcessorNode processor, final ComponentStateDTO componentStateDTO) {
        clearState(processor.getIdentifier(), componentStateDTO);
    }

    @Override
    public StateMap getState(final ControllerServiceNode controllerService, final Scope scope) {
        return getState(controllerService.getIdentifier(), scope);
    }

    @Override
    public void clearState(final ControllerServiceNode controllerService, final ComponentStateDTO componentStateDTO) {
        clearState(controllerService.getIdentifier(), componentStateDTO);
    }

    @Override
    public StateMap getState(final ReportingTaskNode reportingTask, final Scope scope) {
        return getState(reportingTask.getIdentifier(), scope);
    }

    @Override
    public void clearState(final ReportingTaskNode reportingTask, final ComponentStateDTO componentStateDTO) {
        clearState(reportingTask.getIdentifier(), componentStateDTO);
    }

    @Override
    public StateMap getState(final FlowAnalysisRuleNode flowAnalysisRule, Scope scope) {
        return getState(flowAnalysisRule.getIdentifier(), scope);
    }

    @Override
    public void clearState(final FlowAnalysisRuleNode flowAnalysisRule, final ComponentStateDTO componentStateDTO) {
        clearState(flowAnalysisRule.getIdentifier(), componentStateDTO);
    }

    @Override
    public StateMap getState(final ParameterProviderNode parameterProvider, final Scope scope) {
        return getState(parameterProvider.getIdentifier(), scope);
    }

    @Override
    public void clearState(final ParameterProviderNode parameterProvider, final ComponentStateDTO componentStateDTO) {
        clearState(parameterProvider.getIdentifier(), componentStateDTO);
    }

    @Override
    public StateMap getState(final RemoteProcessGroup remoteProcessGroup, final Scope scope) {
        return getState(remoteProcessGroup.getIdentifier(), scope);
    }

    @Autowired
    public void setStateManagerProvider(final StateManagerProvider stateManagerProvider) {
        this.stateManagerProvider = stateManagerProvider;
    }
}
