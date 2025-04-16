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
package org.apache.nifi.audit;

import org.apache.nifi.action.Action;
import org.apache.nifi.action.Component;
import org.apache.nifi.action.FlowChangeAction;
import org.apache.nifi.action.Operation;
import org.apache.nifi.action.details.ActionDetails;
import org.apache.nifi.action.details.FlowChangeConfigureDetails;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.remote.PublicPort;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.dao.PortDAO;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;

@Service
@Aspect
public class PortAuditor extends NiFiAuditor {

    private static final Logger logger = LoggerFactory.getLogger(PortAuditor.class);

    /**
     * Audits the creation of a port.
     *
     * @param proceedingJoinPoint join point
     * @return port
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.PortDAO+) && "
            + "execution(org.apache.nifi.connectable.Port createPort(java.lang.String, org.apache.nifi.web.api.dto.PortDTO))")
    public Port createPortAdvice(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        // perform the underlying operation
        Port port = (Port) proceedingJoinPoint.proceed();

        // audit the port creation
        final Action action = generateAuditRecord(port, Operation.Add);

        // save the actions
        if (action != null) {
            saveAction(action, logger);
        }

        return port;
    }

    /**
     * Audits the update of a port.
     *
     * @param proceedingJoinPoint join point
     * @param portDTO port dto
     * @param portDAO port dao
     * @return port
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.PortDAO+) && "
            + "execution(org.apache.nifi.connectable.Port updatePort(org.apache.nifi.web.api.dto.PortDTO)) && "
            + "args(portDTO) && "
            + "target(portDAO)")
    public Port updatePortAdvice(ProceedingJoinPoint proceedingJoinPoint, PortDTO portDTO, PortDAO portDAO) throws Throwable {
        final Port port = portDAO.getPort(portDTO.getId());
        final ScheduledState scheduledState = port.getScheduledState();
        final String name = port.getName();
        final String comments = port.getComments();
        final int maxConcurrentTasks = port.getMaxConcurrentTasks();

        boolean isPublicPort = port instanceof PublicPort;

        // perform the underlying operation
        final Port updatedPort = (Port) proceedingJoinPoint.proceed();

        if (isAuditable()) {
            Collection<ActionDetails> configurationDetails = new ArrayList<>();

            // see if the name has changed
            if (name != null && portDTO.getName() != null && !name.equals(updatedPort.getName())) {
                // create the config details
                FlowChangeConfigureDetails configDetails = new FlowChangeConfigureDetails();
                configDetails.setName("Name");
                configDetails.setValue(updatedPort.getName());
                configDetails.setPreviousValue(name);

                configurationDetails.add(configDetails);
            }

            // see if the comments has changed
            if (comments != null && portDTO.getComments() != null && !comments.equals(updatedPort.getComments())) {
                // create the config details
                FlowChangeConfigureDetails configDetails = new FlowChangeConfigureDetails();
                configDetails.setName("Comments");
                configDetails.setValue(updatedPort.getComments());
                configDetails.setPreviousValue(comments);

                configurationDetails.add(configDetails);
            }

            // if this is a root group port, consider concurrent tasks
            if (isPublicPort) {
                if (portDTO.getConcurrentlySchedulableTaskCount() != null && updatedPort.getMaxConcurrentTasks() != maxConcurrentTasks) {
                    // create the config details
                    FlowChangeConfigureDetails configDetails = new FlowChangeConfigureDetails();
                    configDetails.setName("Concurrent Tasks");
                    configDetails.setValue(String.valueOf(updatedPort.getMaxConcurrentTasks()));
                    configDetails.setPreviousValue(String.valueOf(maxConcurrentTasks));

                    configurationDetails.add(configDetails);
                }
            }

            final Collection<Action> actions = new ArrayList<>();

            // determine the type of port
            Component componentType = Component.OutputPort;
            if (ConnectableType.INPUT_PORT == updatedPort.getConnectableType()) {
                componentType = Component.InputPort;
            }

            // add each configuration detail
            if (!configurationDetails.isEmpty()) {
                // create the timestamp for the update
                Date timestamp = new Date();

                // create the actions
                for (ActionDetails detail : configurationDetails) {
                    // create the port action for updating the name
                    FlowChangeAction portAction = createFlowChangeAction();
                    portAction.setOperation(Operation.Configure);
                    portAction.setTimestamp(timestamp);
                    portAction.setSourceId(updatedPort.getIdentifier());
                    portAction.setSourceName(updatedPort.getName());
                    portAction.setSourceType(componentType);
                    portAction.setActionDetails(detail);

                    actions.add(portAction);
                }
            }

            // determine the new executing state
            final ScheduledState updatedScheduledState = updatedPort.getScheduledState();

            // determine if the running state has changed
            if (scheduledState != updatedScheduledState) {
                // create a processor action
                FlowChangeAction processorAction = createFlowChangeAction();
                processorAction.setSourceId(updatedPort.getIdentifier());
                processorAction.setSourceName(updatedPort.getName());
                processorAction.setSourceType(componentType);

                // set the operation accordingly
                if (ScheduledState.RUNNING.equals(updatedScheduledState)) {
                    processorAction.setOperation(Operation.Start);
                } else if (ScheduledState.DISABLED.equals(updatedScheduledState)) {
                    processorAction.setOperation(Operation.Disable);
                } else {
                    // state is now stopped... consider the previous state
                    if (ScheduledState.RUNNING.equals(scheduledState)) {
                        processorAction.setOperation(Operation.Stop);
                    } else if (ScheduledState.DISABLED.equals(scheduledState)) {
                        processorAction.setOperation(Operation.Enable);
                    }
                }
                actions.add(processorAction);
            }

            // ensure there are actions to record
            if (!actions.isEmpty()) {
                // save the actions
                saveActions(actions, logger);
            }
        }

        return updatedPort;
    }

    /**
     * Audits the removal of a processor via deleteProcessor().
     *
     * @param proceedingJoinPoint join point
     * @param portId port id
     * @param portDAO port dao
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.PortDAO+) && "
            + "execution(void deletePort(java.lang.String)) && "
            + "args(portId) && "
            + "target(portDAO)")
    public void removePortAdvice(ProceedingJoinPoint proceedingJoinPoint, String portId, PortDAO portDAO) throws Throwable {
        // get the port before removing it
        Port port = portDAO.getPort(portId);

        // remove the port
        proceedingJoinPoint.proceed();

        // if no exceptions were thrown, add removal actions...
        final Action action = generateAuditRecord(port, Operation.Remove);

        // save the actions
        if (action != null) {
            saveAction(action, logger);
        }

    }

    /**
     * Generates an audit record for the creation of the specified port.
     *
     * @param port port
     * @param operation operation
     * @return action
     */
    public Action generateAuditRecord(Port port, Operation operation) {
        return generateAuditRecord(port, operation, null);
    }

    /**
     * Generates an audit record for the creation of the specified port.
     *
     * @param port port
     * @param operation operation
     * @param actionDetails details
     * @return action
     */
    public Action generateAuditRecord(Port port, Operation operation, ActionDetails actionDetails) {
        FlowChangeAction action = null;

        if (isAuditable()) {
            // determine the type of port
            Component componentType = Component.OutputPort;
            if (ConnectableType.INPUT_PORT == port.getConnectableType()) {
                componentType = Component.InputPort;
            }

            // create the port action for adding this processor
            action = createFlowChangeAction();
            action.setOperation(operation);
            action.setSourceId(port.getIdentifier());
            action.setSourceName(port.getName());
            action.setSourceType(componentType);

            if (actionDetails != null) {
                action.setActionDetails(actionDetails);
            }
        }

        return action;
    }
}
