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
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.dao.ComponentStateDAO;

import java.io.IOException;

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

    private void clearState(final String componentId) {
        try {
            final StateManager manager = stateManagerProvider.getStateManager(componentId);
            if (manager == null) {
                throw new ResourceNotFoundException(String.format("State for the specified component %s could not be found.", componentId));
            }

            // clear both state's at the same time
            manager.clear(Scope.CLUSTER);
            manager.clear(Scope.LOCAL);
        } catch (final IOException ioe) {
            throw new IllegalStateException(String.format("Unable to clear the state for the specified component %s: %s", componentId, ioe), ioe);
        }
    }

    @Override
    public StateMap getState(ProcessorNode processor, Scope scope) {
        return getState(processor.getIdentifier(), scope);
    }

    @Override
    public void clearState(ProcessorNode processor) {
        clearState(processor.getIdentifier());
    }

    @Override
    public StateMap getState(ControllerServiceNode controllerService, Scope scope) {
        return getState(controllerService.getIdentifier(), scope);
    }

    @Override
    public void clearState(ControllerServiceNode controllerService) {
        clearState(controllerService.getIdentifier());
    }

    @Override
    public StateMap getState(ReportingTaskNode reportingTask, Scope scope) {
        return getState(reportingTask.getIdentifier(), scope);
    }

    @Override
    public void clearState(ReportingTaskNode reportingTask) {
        clearState(reportingTask.getIdentifier());
    }

    /* setters */

    public void setStateManagerProvider(StateManagerProvider stateManagerProvider) {
        this.stateManagerProvider = stateManagerProvider;
    }
}
