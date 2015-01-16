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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

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

public class StandardInputPortDAO extends ComponentDAO implements PortDAO {

    private FlowController flowController;

    private Port locatePort(String groupId, String portId) {
        return locatePort(locateProcessGroup(flowController, groupId), portId);
    }

    private Port locatePort(ProcessGroup group, String portId) {
        Port port = group.getInputPort(portId);

        // ensure the port exists
        if (port == null) {
            throw new ResourceNotFoundException(String.format("Unable to locate an input port with id '%s'.", portId));
        }

        return port;
    }

    /**
     * Creates a port.
     *
     * @param portDTO The port DTO
     * @return The port
     */
    @Override
    public Port createPort(String groupId, PortDTO portDTO) {
        if (isNotNull(portDTO.getParentGroupId()) && !flowController.areGroupsSame(groupId, portDTO.getParentGroupId())) {
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
        if (group.getParent() == null) {
            port = flowController.createRemoteInputPort(portDTO.getId(), portDTO.getName());
        } else {
            port = flowController.createLocalInputPort(portDTO.getId(), portDTO.getName());
        }

        // ensure we can perform the update before we add the processor to the flow
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

    /**
     * Gets the specified port.
     *
     * @param portId The port id
     * @return The port
     */
    @Override
    public Port getPort(String groupId, String portId) {
        return locatePort(groupId, portId);
    }

    /**
     * Determines if the specified port exists.
     *
     * @param portId
     * @return
     */
    @Override
    public boolean hasPort(String groupId, String portId) {
        ProcessGroup group = flowController.getGroup(groupId);

        if (group == null) {
            return false;
        }

        return group.getInputPort(portId) != null;
    }

    /**
     * Gets all of the ports.
     *
     * @return The ports
     */
    @Override
    public Set<Port> getPorts(String groupId) {
        ProcessGroup group = locateProcessGroup(flowController, groupId);
        return group.getInputPorts();
    }

    @Override
    public void verifyUpdate(String groupId, PortDTO portDTO) {
        final ProcessGroup group = locateProcessGroup(flowController, groupId);
        final Port inputPort = locatePort(group, portDTO.getId());
        verifyUpdate(inputPort, portDTO);
    }

    private void verifyUpdate(final Port inputPort, final PortDTO portDTO) {
        if (isNotNull(portDTO.getState())) {
            final ScheduledState purposedScheduledState = ScheduledState.valueOf(portDTO.getState());

            // only attempt an action if it is changing
            if (!purposedScheduledState.equals(inputPort.getScheduledState())) {
                // perform the appropriate action
                switch (purposedScheduledState) {
                    case RUNNING:
                        inputPort.verifyCanStart();
                        break;
                    case STOPPED:
                        switch (inputPort.getScheduledState()) {
                            case RUNNING:
                                inputPort.verifyCanStop();
                                break;
                            case DISABLED:
                                inputPort.verifyCanEnable();
                                break;
                        }
                        break;
                    case DISABLED:
                        inputPort.verifyCanDisable();
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
            inputPort.verifyCanUpdate();
        }
    }

    /**
     * Validates the proposed processor configuration.
     *
     * @param processorNode
     * @param config
     * @return
     */
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

    /**
     * Updates the specified port.
     *
     * @param portDTO The port DTO
     * @return The port
     */
    @Override
    public Port updatePort(String groupId, PortDTO portDTO) {
        ProcessGroup group = locateProcessGroup(flowController, groupId);
        Port inputPort = locatePort(group, portDTO.getId());

        // ensure we can do this update
        verifyUpdate(inputPort, portDTO);

        // handle state transition
        if (isNotNull(portDTO.getState())) {
            final ScheduledState purposedScheduledState = ScheduledState.valueOf(portDTO.getState());

            // only attempt an action if it is changing
            if (!purposedScheduledState.equals(inputPort.getScheduledState())) {
                try {
                    // perform the appropriate action
                    switch (purposedScheduledState) {
                        case RUNNING:
                            group.startInputPort(inputPort);
                            break;
                        case STOPPED:
                            switch (inputPort.getScheduledState()) {
                                case RUNNING:
                                    group.stopInputPort(inputPort);
                                    break;
                                case DISABLED:
                                    group.enableInputPort(inputPort);
                                    break;
                            }
                            break;
                        case DISABLED:
                            group.disableInputPort(inputPort);
                            break;
                    }
                } catch (IllegalStateException ise) {
                    throw new NiFiCoreException(ise.getMessage(), ise);
                }
            }
        }

        if (inputPort instanceof RootGroupPort) {
            final RootGroupPort rootPort = (RootGroupPort) inputPort;
            if (isNotNull(portDTO.getGroupAccessControl())) {
                rootPort.setGroupAccessControl(portDTO.getGroupAccessControl());
            }
            if (isNotNull(portDTO.getUserAccessControl())) {
                rootPort.setUserAccessControl(portDTO.getUserAccessControl());
            }
        }

        // update the port
        final String name = portDTO.getName();
        final String comments = portDTO.getComments();
        final Integer concurrentTasks = portDTO.getConcurrentlySchedulableTaskCount();
        if (isNotNull(portDTO.getPosition())) {
            inputPort.setPosition(new Position(portDTO.getPosition().getX(), portDTO.getPosition().getY()));
        }
        if (isNotNull(name)) {
            inputPort.setName(name);
        }
        if (isNotNull(comments)) {
            inputPort.setComments(comments);
        }
        if (isNotNull(concurrentTasks)) {
            inputPort.setMaxConcurrentTasks(concurrentTasks);
        }

        return inputPort;
    }

    @Override
    public void verifyDelete(final String groupId, final String portId) {
        final ProcessGroup group = locateProcessGroup(flowController, groupId);
        final Port inputPort = locatePort(group, portId);
        inputPort.verifyCanDelete();
    }

    /**
     * Deletes the specified port.
     *
     * @param portId The port id
     */
    @Override
    public void deletePort(final String groupId, final String portId) {
        final ProcessGroup group = locateProcessGroup(flowController, groupId);
        final Port inputPort = locatePort(group, portId);
        group.removeInputPort(inputPort);
    }

    /* setters */
    public void setFlowController(FlowController flowController) {
        this.flowController = flowController;
    }
}
