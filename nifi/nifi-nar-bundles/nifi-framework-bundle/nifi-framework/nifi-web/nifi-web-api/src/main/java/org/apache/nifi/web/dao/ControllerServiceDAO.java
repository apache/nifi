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

import java.util.Set;
import org.apache.nifi.controller.ScheduledState;

import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceReference;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;

/**
 *
 */
public interface ControllerServiceDAO {

    /**
     * Determines if the specified controller service exists.
     *
     * @param controllerServiceId
     * @return
     */
    boolean hasControllerService(String controllerServiceId);

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
     * Gets all of the controller services.
     *
     * @return The controller services
     */
    Set<ControllerServiceNode> getControllerServices();

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
     * @param controllerServiceId
     * @param scheduledState
     * @param controllerServiceState the value of state 
     * @return the org.apache.nifi.controller.service.ControllerServiceReference 
     */
    ControllerServiceReference updateControllerServiceReferencingComponents(String controllerServiceId, ScheduledState scheduledState, ControllerServiceState controllerServiceState);
    
    /**
     * Determines whether this controller service can be updated.
     *
     * @param controllerServiceDTO
     */
    void verifyUpdate(ControllerServiceDTO controllerServiceDTO);
    
    /**
     * Determines whether the referencing component of the specified controller service can be updated.
     * 
     * @param controllerServiceId
     * @param scheduledState
     * @param controllerServiceState 
     */
    void verifyUpdateReferencingComponents(String controllerServiceId, ScheduledState scheduledState, ControllerServiceState controllerServiceState);
    
    /**
     * Determines whether this controller service can be removed.
     *
     * @param controllerServiceId
     */
    void verifyDelete(String controllerServiceId);

    /**
     * Deletes the specified controller service.
     *
     * @param controllerServiceId The controller service id
     */
    void deleteControllerService(String controllerServiceId);
}
