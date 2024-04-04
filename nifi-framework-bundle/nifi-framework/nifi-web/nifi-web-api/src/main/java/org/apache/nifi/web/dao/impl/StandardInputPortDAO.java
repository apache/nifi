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

import org.apache.nifi.connectable.Port;
import org.apache.nifi.connectable.Position;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.remote.PublicPort;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.dao.PortDAO;

import java.util.Set;

public class StandardInputPortDAO extends AbstractPortDAO implements PortDAO {

    protected Port locatePort(final String portId) {
        final ProcessGroup rootGroup = flowController.getFlowManager().getRootGroup();
        Port port = rootGroup.findInputPort(portId);

        if (port == null) {
            port = rootGroup.findOutputPort(portId);
        }

        if (port == null) {
            throw new ResourceNotFoundException(String.format("Unable to find port with id '%s'.", portId));
        } else {
            return port;
        }
    }

    @Override
    public boolean hasPort(String portId) {
        final ProcessGroup rootGroup = flowController.getFlowManager().getRootGroup();
        return rootGroup.findInputPort(portId) != null || rootGroup.findOutputPort(portId) != null;
    }

    @Override
    public Port createPort(String groupId, PortDTO portDTO) {
        if (isNotNull(portDTO.getParentGroupId()) && !flowController.getFlowManager().areGroupsSame(groupId, portDTO.getParentGroupId())) {
            throw new IllegalArgumentException("Cannot specify a different Parent Group ID than the Group to which the InputPort is being added.");
        }

        // ensure the name has been specified
        if (portDTO.getName() == null) {
            throw new IllegalArgumentException("Port name must be specified.");
        }

        // get the desired group
        ProcessGroup group = locateProcessGroup(flowController, groupId);

        // determine if this is the root group
        Port port;
        if (group.getParent() == null || Boolean.TRUE.equals(portDTO.getAllowRemoteAccess())) {
            port = flowController.getFlowManager().createPublicInputPort(portDTO.getId(), portDTO.getName());
        } else {
            port = flowController.getFlowManager().createLocalInputPort(portDTO.getId(), portDTO.getName());
        }

        // Unique public port check among all groups.
        if (port instanceof PublicPort) {
            verifyPublicPortUniqueness(port.getIdentifier(), port.getName());
        }

        // ensure we can perform the update before we add the port to the flow
        verifyUpdate(port, portDTO);

        // configure
        if (portDTO.getPosition() != null) {
            port.setPosition(new Position(portDTO.getPosition().getX(), portDTO.getPosition().getY()));
        }
        port.setComments(portDTO.getComments());

        // add the port
        group.addInputPort(port);
        return port;
    }

    @Override
    public Port getPort(String portId) {
        return locatePort(portId);
    }

    @Override
    public Set<Port> getPorts(String groupId) {
        ProcessGroup group = locateProcessGroup(flowController, groupId);
        return group.getInputPorts();
    }

    @Override
    protected Set<Port> getPublicPorts() {
        return flowController.getFlowManager().getPublicInputPorts();
    }

    @Override
    protected void handleStateTransition(final Port port, final ScheduledState proposedScheduledState) throws IllegalStateException {
        final ProcessGroup processGroup = port.getProcessGroup();
        switch (proposedScheduledState) {
            case RUNNING:
                processGroup.startInputPort(port);
                break;
            case STOPPED:
                switch (port.getScheduledState()) {
                    case RUNNING:
                        processGroup.stopInputPort(port);
                        break;
                    case DISABLED:
                        processGroup.enableInputPort(port);
                        break;
                }
                break;
            case DISABLED:
                processGroup.disableInputPort(port);
                break;
        }
    }

    @Override
    public void deletePort(final String portId) {
        final Port inputPort = locatePort(portId);
        inputPort.getProcessGroup().removeInputPort(inputPort);
    }

}
