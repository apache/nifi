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
import org.apache.nifi.remote.RootGroupPort;
import org.apache.nifi.web.NiFiCoreException;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.dao.PortDAO;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class StandardOutputPortDAO extends ComponentDAO implements PortDAO {

    private FlowController flowController;

    private Port locatePort(final String portId) {
        final ProcessGroup rootGroup = flowController.getGroup(flowController.getRootGroupId());
        final Port port = rootGroup.findOutputPort(portId);

        if (port == null) {
            throw new ResourceNotFoundException(String.format("Unable to find port with id '%s'.", portId));
        } else {
            return port;
        }
    }

    @Override
    public boolean hasPort(String portId) {
        final ProcessGroup rootGroup = flowController.getGroup(flowController.getRootGroupId());
        return rootGroup.findOutputPort(portId) != null;
    }

    @Override
    public Port createPort(String groupId, PortDTO portDTO) {
        if (isNotNull(portDTO.getParentGroupId()) && !flowController.areGroupsSame(groupId, portDTO.getParentGroupId())) {
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
        if (group.getParent() == null) {
            port = flowController.createRemoteOutputPort(portDTO.getId(), portDTO.getName());
        } else {
            port = flowController.createLocalOutputPort(portDTO.getId(), portDTO.getName());
        }

        // ensure we can perform the update before we add the processor to the flow
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
    public Port getPort(String portId) {
        return locatePort(portId);
    }

    @Override
    public Set<Port> getPorts(String groupId) {
        ProcessGroup group = locateProcessGroup(flowController, groupId);
        return group.getOutputPorts();
    }

    @Override
    public void verifyUpdate(PortDTO portDTO) {
        final Port outputPort = locatePort(portDTO.getId());
        verifyUpdate(outputPort, portDTO);
    }

    private void verifyUpdate(final Port outputPort, final PortDTO portDTO) {
        if (isNotNull(portDTO.getState())) {
            final ScheduledState purposedScheduledState = ScheduledState.valueOf(portDTO.getState());

            // only attempt an action if it is changing
            if (!purposedScheduledState.equals(outputPort.getScheduledState())) {
                // perform the appropriate action
                switch (purposedScheduledState) {
                    case RUNNING:
                        outputPort.verifyCanStart();
                        break;
                    case STOPPED:
                        switch (outputPort.getScheduledState()) {
                            case RUNNING:
                                outputPort.verifyCanStop();
                                break;
                            case DISABLED:
                                outputPort.verifyCanEnable();
                                break;
                        }
                        break;
                    case DISABLED:
                        outputPort.verifyCanDisable();
                        break;
                }
            }
        }

        // see what's be modified
        if (isAnyNotNull(portDTO.getUserAccessControl(),
                portDTO.getGroupAccessControl(),
                portDTO.getConcurrentlySchedulableTaskCount(),
                portDTO.getName(),
                portDTO.getComments())) {

            // validate the request
            final List<String> requestValidation = validateProposedConfiguration(portDTO);

            // ensure there was no validation errors
            if (!requestValidation.isEmpty()) {
                throw new ValidationException(requestValidation);
            }

            // ensure the port can be updated
            outputPort.verifyCanUpdate();
        }
    }

    private List<String> validateProposedConfiguration(PortDTO portDTO) {
        List<String> validationErrors = new ArrayList<>();

        if (isNotNull(portDTO.getName()) && portDTO.getName().trim().isEmpty()) {
            validationErrors.add("Port name cannot be blank.");
        }
        if (isNotNull(portDTO.getConcurrentlySchedulableTaskCount()) && portDTO.getConcurrentlySchedulableTaskCount() <= 0) {
            validationErrors.add("Concurrent tasks must be a positive integer.");
        }

        return validationErrors;
    }

    @Override
    public Port updatePort(PortDTO portDTO) {
        Port outputPort = locatePort(portDTO.getId());

        // ensure we can do this update
        verifyUpdate(outputPort, portDTO);

        // handle state transition
        if (portDTO.getState() != null) {
            final ScheduledState purposedScheduledState = ScheduledState.valueOf(portDTO.getState());

            // only attempt an action if it is changing
            if (!purposedScheduledState.equals(outputPort.getScheduledState())) {
                try {
                    // perform the appropriate action
                    switch (purposedScheduledState) {
                        case RUNNING:
                            outputPort.getProcessGroup().startOutputPort(outputPort);
                            break;
                        case STOPPED:
                            switch (outputPort.getScheduledState()) {
                                case RUNNING:
                                    outputPort.getProcessGroup().stopOutputPort(outputPort);
                                    break;
                                case DISABLED:
                                    outputPort.getProcessGroup().enableOutputPort(outputPort);
                                    break;
                            }
                            break;
                        case DISABLED:
                            outputPort.getProcessGroup().disableOutputPort(outputPort);
                            break;
                    }
                } catch (IllegalStateException ise) {
                    throw new NiFiCoreException(ise.getMessage(), ise);
                }
            }
        }

        if (outputPort instanceof RootGroupPort) {
            final RootGroupPort rootPort = (RootGroupPort) outputPort;
            if (isNotNull(portDTO.getGroupAccessControl())) {
                rootPort.setGroupAccessControl(portDTO.getGroupAccessControl());
            }
            if (isNotNull(portDTO.getUserAccessControl())) {
                rootPort.setUserAccessControl(portDTO.getUserAccessControl());
            }
        }

        // perform the configuration
        final String name = portDTO.getName();
        final String comments = portDTO.getComments();
        final Integer concurrentTasks = portDTO.getConcurrentlySchedulableTaskCount();
        if (isNotNull(portDTO.getPosition())) {
            outputPort.setPosition(new Position(portDTO.getPosition().getX(), portDTO.getPosition().getY()));
        }
        if (isNotNull(name)) {
            outputPort.setName(name);
        }
        if (isNotNull(comments)) {
            outputPort.setComments(comments);
        }
        if (isNotNull(concurrentTasks)) {
            outputPort.setMaxConcurrentTasks(concurrentTasks);
        }

        return outputPort;
    }

    @Override
    public void verifyDelete(final String portId) {
        final Port outputPort = locatePort(portId);
        outputPort.verifyCanDelete();
    }

    @Override
    public void deletePort(String portId) {
        Port outputPort = locatePort(portId);
        outputPort.getProcessGroup().removeOutputPort(outputPort);
    }

    /* setters */
    public void setFlowController(FlowController flowController) {
        this.flowController = flowController;
    }
}
