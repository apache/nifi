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

import java.util.Set;

import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.Position;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.api.dto.FunnelDTO;
import org.apache.nifi.web.dao.FunnelDAO;

public class StandardFunnelDAO extends ComponentDAO implements FunnelDAO {

    private FlowController flowController;

    /**
     * Locates the specified funnel.
     *
     * @param groupId
     * @param funnelId
     * @return
     */
    private Funnel locateFunnel(String groupId, String funnelId) {
        return locateFunnel(locateProcessGroup(flowController, groupId), funnelId);
    }

    /**
     * Locates the specified funnel.
     *
     * @param group
     * @param funnelId
     * @return
     */
    private Funnel locateFunnel(ProcessGroup group, String funnelId) {
        // get the funnel
        Funnel funnel = group.getFunnel(funnelId);

        // ensure the funnel exists
        if (funnel == null) {
            throw new ResourceNotFoundException(String.format("Unable to locate funnel with id '%s'.", funnelId));
        }

        return funnel;
    }

    /**
     * Creates a funnel.
     *
     * @param funnelDTO The funnel DTO
     * @return The funnel
     */
    @Override
    public Funnel createFunnel(String groupId, FunnelDTO funnelDTO) {
        if (funnelDTO.getParentGroupId() != null && !flowController.areGroupsSame(groupId, funnelDTO.getParentGroupId())) {
            throw new IllegalArgumentException("Cannot specify a different Parent Group ID than the Group to which the Funnel is being added.");
        }

        // get the desired group
        ProcessGroup group = locateProcessGroup(flowController, groupId);

        // create the funnel
        Funnel funnel = flowController.createFunnel(funnelDTO.getId());
        if (funnelDTO.getPosition() != null) {
            funnel.setPosition(new Position(funnelDTO.getPosition().getX(), funnelDTO.getPosition().getY()));
        }

        // add the funnel
        group.addFunnel(funnel);
        group.startFunnel(funnel);
        return funnel;
    }

    /**
     * Gets the specified funnel.
     *
     * @param funnelId The funnel id
     * @return The funnel
     */
    @Override
    public Funnel getFunnel(String groupId, String funnelId) {
        return locateFunnel(groupId, funnelId);
    }

    /**
     * Determines if the specified funnel exists.
     *
     * @param funnelId
     * @return
     */
    @Override
    public boolean hasFunnel(String groupId, String funnelId) {
        ProcessGroup group;
        try {
            group = flowController.getGroup(groupId);
        } catch (NullPointerException | IllegalArgumentException e) {
            return false;
        }

        return group.getFunnel(funnelId) != null;
    }

    /**
     * Gets all of the funnels.
     *
     * @return The funnels
     */
    @Override
    public Set<Funnel> getFunnels(String groupId) {
        ProcessGroup group = locateProcessGroup(flowController, groupId);
        return group.getFunnels();
    }

    /**
     * Updates the specified funnel.
     *
     * @param funnelDTO The funnel DTO
     * @return The funnel
     */
    @Override
    public Funnel updateFunnel(String groupId, FunnelDTO funnelDTO) {
        ProcessGroup group = locateProcessGroup(flowController, groupId);

        // get the funnel being updated
        Funnel funnel = locateFunnel(group, funnelDTO.getId());

        // update the label state
        if (isNotNull(funnelDTO.getPosition())) {
            if (funnelDTO.getPosition() != null) {
                funnel.setPosition(new Position(funnelDTO.getPosition().getX(), funnelDTO.getPosition().getY()));
            }
        }

        return funnel;
    }

    @Override
    public void verifyDelete(String groupId, String funnelId) {
        ProcessGroup group = locateProcessGroup(flowController, groupId);
        Funnel funnel = locateFunnel(group, funnelId);
        funnel.verifyCanDelete();
    }

    /**
     * Deletes the specified funnel.
     *
     * @param funnelId The funnel id
     */
    @Override
    public void deleteFunnel(String groupId, String funnelId) {
        ProcessGroup group = locateProcessGroup(flowController, groupId);

        // get the funnel
        Funnel funnel = locateFunnel(group, funnelId);

        // remove the funnel
        group.removeFunnel(funnel);
    }

    /* setters */
    public void setFlowController(FlowController flowController) {
        this.flowController = flowController;
    }
}
