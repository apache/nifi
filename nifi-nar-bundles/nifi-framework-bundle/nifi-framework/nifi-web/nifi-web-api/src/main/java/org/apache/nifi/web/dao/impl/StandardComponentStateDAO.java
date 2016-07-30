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

import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.state.ExternalStateManager;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.dao.ComponentStateDAO;

import java.io.IOException;

public class StandardComponentStateDAO implements ComponentStateDAO {

    private StateManagerProvider stateManagerProvider;

    @Override
    public StateMap getState(final ConfigurableComponent component, final Scope scope) {
        switch (scope) {
            case EXTERNAL:
                return getExternalState(component);
            default:
                return getState(component.getIdentifier(), scope);
        }
    }

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

    private StateMap getExternalState(final ConfigurableComponent component) {
        if (component instanceof ExternalStateManager) {
            try {
                return ((ExternalStateManager)component).getExternalState();
            } catch (final IOException ioe) {
                throw new IllegalStateException(String.format("Unable to get the external state for the specified component %s: %s",
                        component.getIdentifier(), ioe), ioe);
            }
        }
        return null;
    }

    @Override
    public void clearState(final ConfigurableComponent component, final Scope scope) {
        switch (scope) {
            case EXTERNAL:
                clearExternalState(component);
                break;
            default:
                clearState(component.getIdentifier(), scope);
                break;
        }
    }

    private void clearState(final String componentId, final Scope scope) {
        try {
            final StateManager manager = stateManagerProvider.getStateManager(componentId);
            if (manager == null) {
                throw new ResourceNotFoundException(String.format("State for the specified component %s could not be found.", componentId));
            }
            manager.clear(scope);

        } catch (final IOException ioe) {
            throw new IllegalStateException(String.format("Unable to clear the state for the specified component %s: %s", componentId, ioe), ioe);
        }
    }

    private void clearExternalState(final ConfigurableComponent component) {
        if (component instanceof ExternalStateManager) {
            try {
                ((ExternalStateManager)component).clearExternalState();
            } catch (final IOException ioe) {
                throw new IllegalStateException(String.format("Unable to clear the external state for the specified component %s: %s",
                        component.getIdentifier(), ioe), ioe);
            }
        }
    }

    /* setters */

    public void setStateManagerProvider(StateManagerProvider stateManagerProvider) {
        this.stateManagerProvider = stateManagerProvider;
    }
}
