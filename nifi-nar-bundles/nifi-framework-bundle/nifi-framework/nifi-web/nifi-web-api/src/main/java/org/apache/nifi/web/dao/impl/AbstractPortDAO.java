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
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.exception.ValidationException;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.remote.PublicPort;
import org.apache.nifi.web.NiFiCoreException;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.dao.PortDAO;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public abstract class AbstractPortDAO extends ComponentDAO implements PortDAO {

    protected FlowController flowController;

    protected abstract Port locatePort(final String portId);

    @Override
    public void verifyUpdate(PortDTO portDTO) {
        final Port port = locatePort(portDTO.getId());
        verifyUpdate(port, portDTO);
    }


    protected void verifyUpdate(final Port port, final PortDTO portDTO) {
        if (isNotNull(portDTO.getState())) {
            final ScheduledState purposedScheduledState = ScheduledState.valueOf(portDTO.getState());

            // only attempt an action if it is changing
            if (!purposedScheduledState.equals(port.getScheduledState())) {
                // perform the appropriate action
                switch (purposedScheduledState) {
                    case RUNNING:
                        port.verifyCanStart();
                        break;
                    case STOPPED:
                        switch (port.getScheduledState()) {
                            case RUNNING:
                                port.verifyCanStop();
                                break;
                            case DISABLED:
                                port.verifyCanEnable();
                                break;
                        }
                        break;
                    case DISABLED:
                        port.verifyCanDisable();
                        break;
                }
            }
        }

        // see what's be modified
        if (isAnyNotNull(portDTO.getUserAccessControl(),
            portDTO.getGroupAccessControl(),
            portDTO.getConcurrentlySchedulableTaskCount(),
            portDTO.getName(),
            portDTO.getComments(),
            portDTO.getAllowRemoteAccess())) {

            // validate the request
            final List<String> requestValidation = validateProposedConfiguration(port, portDTO);

            // ensure there was no validation errors
            if (!requestValidation.isEmpty()) {
                throw new ValidationException(requestValidation);
            }

            // ensure the port can be updated
            port.verifyCanUpdate();
        }

    }

    private List<String> validateProposedConfiguration(final Port port, final PortDTO portDTO) {
        List<String> validationErrors = new ArrayList<>();

        if (isNotNull(portDTO.getName()) && portDTO.getName().trim().isEmpty()) {
            validationErrors.add("The name of the port must be specified.");
        }
        if (isNotNull(portDTO.getConcurrentlySchedulableTaskCount()) && portDTO.getConcurrentlySchedulableTaskCount() <= 0) {
            validationErrors.add("Concurrent tasks must be a positive integer.");
        }

        // Although StandardProcessGroup.addIn/OutputPort has the similar validation,
        // this validation is necessary to prevent a port becomes public with an existing port name.
        if (port instanceof PublicPort) {
            final String portName = isNotNull(portDTO.getName()) ? portDTO.getName() : port.getName();
            // If there is any port with the same name, but different identifier, throw an error.
            if (getPublicPorts().stream()
                .anyMatch(p -> portName.equals(p.getName()) && !port.getIdentifier().equals(p.getIdentifier()))) {
                throw new IllegalStateException("Public port name must be unique throughout the flow.");
            }
        }

        return validationErrors;
    }

    @Override
    public void verifyPublicPortUniqueness(final String portId, final String portName) {
        for (Port port : getPublicPorts()) {
            if (portId.equals(port.getIdentifier())) {
                throw new IllegalStateException("Public port identifier must be unique throughout the flow.");
            } else if(portName.equals(port.getName())) {
                throw new IllegalStateException("Public port name must be unique throughout the flow.");
            }
        }
    }

    protected abstract Set<Port> getPublicPorts();

    protected abstract void handleStateTransition(final Port port, final ScheduledState proposedScheduledState) throws IllegalStateException;

    @Override
    public Port updatePort(PortDTO portDTO) {
        final Port port = locatePort(portDTO.getId());
        final ProcessGroup processGroup = port.getProcessGroup();

        // ensure we can do this update
        verifyUpdate(port, portDTO);

        // handle state transition
        if (isNotNull(portDTO.getState())) {
            final ScheduledState proposedScheduledState = ScheduledState.valueOf(portDTO.getState());

            // only attempt an action if it is changing
            if (!proposedScheduledState.equals(port.getScheduledState())) {
                try {
                    handleStateTransition(port, proposedScheduledState);
                } catch (IllegalStateException ise) {
                    throw new NiFiCoreException(ise.getMessage(), ise);
                }
            }
        }

        if (port instanceof PublicPort) {
            final PublicPort publicPort = (PublicPort) port;
            if (isNotNull(portDTO.getGroupAccessControl())) {
                publicPort.setGroupAccessControl(portDTO.getGroupAccessControl());
            }
            if (isNotNull(portDTO.getUserAccessControl())) {
                publicPort.setUserAccessControl(portDTO.getUserAccessControl());
            }
        }

        // update the port
        final String name = portDTO.getName();
        final String comments = portDTO.getComments();
        final Integer concurrentTasks = portDTO.getConcurrentlySchedulableTaskCount();
        if (isNotNull(portDTO.getPosition())) {
            port.setPosition(new Position(portDTO.getPosition().getX(), portDTO.getPosition().getY()));
        }
        if (isNotNull(name)) {
            port.setName(name);
        }
        if (isNotNull(comments)) {
            port.setComments(comments);
        }
        if (isNotNull(concurrentTasks)) {
            port.setMaxConcurrentTasks(concurrentTasks);
        }

        processGroup.onComponentModified();
        return port;
    }

    @Override
    public void verifyDelete(final String portId) {
        final Port inputPort = locatePort(portId);
        inputPort.verifyCanDelete();
    }

    /* setters */
    public void setFlowController(FlowController flowController) {
        this.flowController = flowController;
    }

}
