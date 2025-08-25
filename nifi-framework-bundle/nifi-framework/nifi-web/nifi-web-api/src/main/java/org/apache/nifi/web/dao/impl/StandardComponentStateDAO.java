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
                throw new ResourceNotFoundException(String.format("State for the specified component %s could not be found.", componentId));
            }

            return manager.getState(scope);
        } catch (final IOException ioe) {
            throw new IllegalStateException(String.format("Unable to get the state for the specified component %s: %s", componentId, ioe), ioe);
        }
    }

    private void clearState(final String componentId, ComponentStateDTO componentStateDTO) {
        try {
            final StateManager manager = stateManagerProvider.getStateManager(componentId);
            if (manager == null) {
                throw new ResourceNotFoundException(String.format("State for the specified component %s could not be found.", componentId));
            }

            if (componentStateDTO == null) {
                // clear both state's at the same time
                manager.clear(Scope.CLUSTER);
                manager.clear(Scope.LOCAL);
            } else if (manager.getState(Scope.LOCAL) != null && !manager.getState(Scope.LOCAL).toMap().isEmpty() && !stateManagerProvider.isClusterProviderEnabled()) {
                // we are in standalone mode, all state is local
                if (manager.isStateKeyDropSupported()) {
                    final Map<String, String> newState = new HashMap<>();
                    componentStateDTO.getClusterState().getState().forEach(stateEntryDTO -> {
                        newState.put(stateEntryDTO.getKey(), stateEntryDTO.getValue());
                    });

                    final Map<String, String> currentState = manager.getState(Scope.LOCAL).toMap();

                    if (hasExactlyOneKeyRemoved(currentState, newState)) {
                        manager.setState(newState, Scope.LOCAL);
                    } else {
                        throw new IllegalStateException(String.format("Unable to remove key in the state of the specified component %s. Only one key can be removed.", componentId));
                    }
                } else {
                    throw new IllegalStateException(String.format("Removing specific keys for component %s with local state is not supported.", componentId));
                }
            } else if (manager.getState(Scope.LOCAL) != null && !manager.getState(Scope.LOCAL).toMap().isEmpty()) {
                throw new IllegalStateException(String.format("Removing specific keys for component %s with local state is not supported.", componentId));
            } else if (componentStateDTO.getClusterState() != null && !componentStateDTO.getClusterState().getState().isEmpty()) {
                if (manager.isStateKeyDropSupported()) {
                    final Map<String, String> newState = new HashMap<>();
                    componentStateDTO.getClusterState().getState().forEach(stateEntryDTO -> {
                        newState.put(stateEntryDTO.getKey(), stateEntryDTO.getValue());
                    });

                    final Map<String, String> currentState = manager.getState(Scope.CLUSTER).toMap();

                    if (hasExactlyOneKeyRemoved(currentState, newState)) {
                        manager.setState(newState, Scope.CLUSTER);
                    } else {
                        throw new IllegalStateException(String.format("Unable to remove key in the state of the specified component %s. Only one key can be removed.", componentId));
                    }

                    // we clear local anyway
                    manager.clear(Scope.LOCAL);
                } else {
                    throw new IllegalStateException(String.format("Removing specific keys for component %s with cluster state is not supported.", componentId));
                }
            } else {
                // clear both state's at the same time
                manager.clear(Scope.CLUSTER);
                manager.clear(Scope.LOCAL);
            }
        } catch (final IOException ioe) {
            throw new IllegalStateException(String.format("Unable to clear the state for the specified component %s: %s", componentId, ioe), ioe);
        }
    }

    public boolean hasExactlyOneKeyRemoved(Map<String, String> currentState, Map<String, String> newState) {
        // Check if newState has exactly one less key
        if (currentState.size() - newState.size() != 1) {
            return false;
        }

        // Check that newState is a subset of currentState
        return newState.entrySet()
                .stream()
                .allMatch(entry -> entry.getValue().equals(currentState.get(entry.getKey())));
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
