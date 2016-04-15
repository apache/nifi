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

import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.connectable.Position;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.api.dto.FunnelDTO;
import org.apache.nifi.web.dao.FunnelDAO;

import java.util.Set;

public class StandardFunnelDAO extends ComponentDAO implements FunnelDAO {

    private FlowController flowController;

    private Funnel locateFunnel(final String funnelId) {
        final ProcessGroup rootGroup = flowController.getGroup(flowController.getRootGroupId());
        final Funnel funnel = rootGroup.findFunnel(funnelId);

        if (funnel == null) {
            throw new ResourceNotFoundException(String.format("Unable to find funnel with id '%s'.", funnelId));
        } else {
            return funnel;
        }
    }

    @Override
    public boolean hasFunnel(String funnelId) {
        final ProcessGroup rootGroup = flowController.getGroup(flowController.getRootGroupId());
        return rootGroup.findFunnel(funnelId) != null;
    }

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

    @Override
    public Funnel getFunnel(String funnelId) {
        return locateFunnel(funnelId);
    }

    @Override
    public Set<Funnel> getFunnels(String groupId) {
        ProcessGroup group = locateProcessGroup(flowController, groupId);
        return group.getFunnels();
    }

    @Override
    public Funnel updateFunnel(FunnelDTO funnelDTO) {
        // get the funnel being updated
        Funnel funnel = locateFunnel(funnelDTO.getId());

        // update the label state
        if (isNotNull(funnelDTO.getPosition())) {
            if (funnelDTO.getPosition() != null) {
                funnel.setPosition(new Position(funnelDTO.getPosition().getX(), funnelDTO.getPosition().getY()));
            }
        }

        return funnel;
    }

    @Override
    public void verifyDelete(String funnelId) {
        Funnel funnel = locateFunnel(funnelId);
        funnel.verifyCanDelete();
    }

    @Override
    public void deleteFunnel(String funnelId) {
        // get the funnel
        Funnel funnel = locateFunnel(funnelId);

        // remove the funnel
        funnel.getProcessGroup().removeFunnel(funnel);
    }

    /* setters */
    public void setFlowController(FlowController flowController) {
        this.flowController = flowController;
    }
}
