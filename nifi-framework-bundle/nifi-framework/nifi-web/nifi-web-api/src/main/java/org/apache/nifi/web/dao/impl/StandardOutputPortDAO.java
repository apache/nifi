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

import org.apache.nifi.components.connector.ConnectorNode;
import org.apache.nifi.components.connector.ConnectorState;
import org.apache.nifi.components.connector.ConnectorSyncMode;
import org.apache.nifi.components.connector.FrameworkFlowContext;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.connectable.Position;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.remote.PublicPort;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.dao.PortDAO;
import org.springframework.stereotype.Repository;

import java.util.Set;

@Repository
public class StandardOutputPortDAO extends AbstractPortDAO implements PortDAO {

    @Override
    protected Port locatePort(final String portId, final boolean includeConnectorManaged) {
        final ProcessGroup rootGroup = flowController.getFlowManager().getRootGroup();
        Port port = rootGroup.findOutputPort(portId);
        if (port != null) {
            return port;
        }

        for (final ConnectorNode connector : flowController.getConnectorRepository().getConnectors(ConnectorSyncMode.LOCAL_ONLY)) {
            final FrameworkFlowContext flowContext = connector.getActiveFlowContext();
            if (flowContext == null) {
                continue;
            }

            port = flowContext.getManagedProcessGroup().findOutputPort(portId);
            if (port != null) {
                if (!includeConnectorManaged) {
                    verifyAccessibleForComponentOperation(port.getProcessGroup(), portId);
                }
                return port;
            }
        }

        throw new ResourceNotFoundException(String.format("Unable to find port with id '%s'.", portId));
    }

    @Override
    public boolean hasPort(String portId) {
        final ProcessGroup rootGroup = flowController.getFlowManager().getRootGroup();
        if (rootGroup.findOutputPort(portId) != null) {
            return true;
        }

        for (final ConnectorNode connector : flowController.getConnectorRepository().getConnectors(ConnectorSyncMode.LOCAL_ONLY)) {
            if (connector.getCurrentState() != ConnectorState.TROUBLESHOOTING) {
                continue;
            }

            final FrameworkFlowContext flowContext = connector.getActiveFlowContext();
            if (flowContext != null && flowContext.getManagedProcessGroup().findOutputPort(portId) != null) {
                return true;
            }
        }

        return false;
    }

    @Override
    public Port createPort(String groupId, PortDTO portDTO) {
        if (isNotNull(portDTO.getParentGroupId()) && !flowController.getFlowManager().areGroupsSame(groupId, portDTO.getParentGroupId())) {
            throw new IllegalArgumentException("Cannot specify a different Parent Group ID than the Group to which the OutputPort is being added.");
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
            port = flowController.getFlowManager().createPublicOutputPort(portDTO.getId(), portDTO.getName());
        } else {
            port = flowController.getFlowManager().createLocalOutputPort(portDTO.getId(), portDTO.getName());
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
        group.addOutputPort(port);
        return port;
    }

    @Override
    public Set<Port> getPorts(String groupId) {
        ProcessGroup group = locateProcessGroup(flowController, groupId);
        return group.getOutputPorts();
    }

    @Override
    protected Set<Port> getPublicPorts() {
        return flowController.getFlowManager().getPublicOutputPorts();
    }

    @Override
    protected void handleStateTransition(final Port port, final ScheduledState proposedScheduledState) throws IllegalStateException {
        final ProcessGroup processGroup = port.getProcessGroup();
        switch (proposedScheduledState) {
            case RUNNING:
                processGroup.startOutputPort(port);
                break;
            case STOPPED:
                switch (port.getScheduledState()) {
                    case RUNNING:
                        processGroup.stopOutputPort(port);
                        break;
                    case DISABLED:
                        processGroup.enableOutputPort(port);
                        break;
                }
                break;
            case DISABLED:
                processGroup.disableOutputPort(port);
                break;
        }
    }

    @Override
    public void deletePort(String portId) {
        Port outputPort = locatePort(portId);
        outputPort.getProcessGroup().removeOutputPort(outputPort);
    }

}
