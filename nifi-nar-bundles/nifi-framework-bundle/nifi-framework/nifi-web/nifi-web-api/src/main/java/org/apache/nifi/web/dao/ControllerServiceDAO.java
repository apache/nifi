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
package org.apache.nifi.web.dao;

import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.controller.ConfiguredComponent;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;

import java.util.Set;

public interface ControllerServiceDAO {

    /**
     * @param controllerServiceId service id
     * @return Determines if the specified controller service exists
     */
    boolean hasControllerService(String controllerServiceId);

    /**
     * Determines whether this controller service can be created.
     *
     * @param controllerServiceDTO service
     */
    void verifyCreate(ControllerServiceDTO controllerServiceDTO);

    /**
     * Creates a controller service.
     *
     * @param controllerServiceDTO The controller service DTO
     * @return The controller service
     */
    ControllerServiceNode createControllerService(ControllerServiceDTO controllerServiceDTO);

    /**
     * Gets the specified controller service.
     *
     * @param controllerServiceId The controller service id
     * @return The controller service
     */
    ControllerServiceNode getControllerService(String controllerServiceId);

    /**
     * Gets all of the controller services for the group with the given ID or all
     * controller-level services if the group id is null
     *
     * @return The controller services
     */
    Set<ControllerServiceNode> getControllerServices(String groupId, boolean includeAncestorGroups, boolean includeDescendantGroups);

    /**
     * Updates the specified controller service.
     *
     * @param controllerServiceDTO The controller service DTO
     * @return The controller service
     */
    ControllerServiceNode updateControllerService(ControllerServiceDTO controllerServiceDTO);

    /**
     * Updates the referencing components for the specified controller service.
     *
     * @param controllerServiceId service id
     * @param scheduledState scheduled state
     * @param controllerServiceState the value of state
     * @return the set of all components that were modified as a result of this action
     */
    Set<ConfiguredComponent> updateControllerServiceReferencingComponents(String controllerServiceId, ScheduledState scheduledState, ControllerServiceState controllerServiceState);

    /**
     * Determines whether this controller service can be updated.
     *
     * @param controllerServiceDTO service
     */
    void verifyUpdate(ControllerServiceDTO controllerServiceDTO);

    /**
     * Determines whether the referencing component of the specified controller service can be updated.
     *
     * @param controllerServiceId service id
     * @param scheduledState scheduled state
     * @param controllerServiceState service state
     */
    void verifyUpdateReferencingComponents(String controllerServiceId, ScheduledState scheduledState, ControllerServiceState controllerServiceState);

    /**
     * Determines whether this controller service can be removed.
     *
     * @param controllerServiceId service id
     */
    void verifyDelete(String controllerServiceId);

    /**
     * Deletes the specified controller service.
     *
     * @param controllerServiceId The controller service id
     */
    void deleteControllerService(String controllerServiceId);

    /**
     * Gets the specified controller service.
     *
     * @param controllerServiceId controller service id
     * @return state map
     */
    StateMap getState(String controllerServiceId, Scope scope);

    /**
     * Verifies the controller service can clear state.
     *
     * @param controllerServiceId controller service id
     */
    void verifyClearState(String controllerServiceId);

    /**
     * Clears the state of the specified controller service.
     *
     * @param controllerServiceId controller service id
     */
    void clearState(String controllerServiceId);
}
