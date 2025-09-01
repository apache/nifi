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
import org.apache.nifi.action.component.details.FlowChangeExtensionDetails;
import org.apache.nifi.action.details.ActionDetails;
import org.apache.nifi.action.details.FlowChangeConfigureDetails;
import org.apache.nifi.action.details.FlowChangeMoveDetails;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.registry.flow.VersionControlInformation;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.VersionControlInformationDTO;
import org.apache.nifi.web.dao.ProcessGroupDAO;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;

/**
 * Audits process group creation/removal and configuration changes.
 */
@Service
@Aspect
public class ProcessGroupAuditor extends NiFiAuditor {

    private static final Logger logger = LoggerFactory.getLogger(ProcessGroupAuditor.class);

    /**
     * Audits the creation of process groups via createProcessGroup().
     *
     * This method only needs to be run 'after returning'. However, in Java 7 the order in which these methods are returned from Class.getDeclaredMethods (even though there is no order guaranteed)
     * seems to differ from Java 6. SpringAOP depends on this ordering to determine advice precedence. By normalizing all advice into Around advice we can alleviate this issue.
     *
     * @param proceedingJoinPoint join point
     * @return group
     * @throws java.lang.Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.ProcessGroupDAO+) && "
            + "execution(org.apache.nifi.groups.ProcessGroup createProcessGroup(String, org.apache.nifi.web.api.dto.ProcessGroupDTO))")
    public ProcessGroup createProcessGroupAdvice(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        // create the process group
        ProcessGroup processGroup = (ProcessGroup) proceedingJoinPoint.proceed();

        // if no exceptions were thrown, add the process group action...
        // audit process group creation
        final Action action = generateAuditRecord(processGroup, Operation.Add);

        // save the actions
        if (action != null) {
            saveAction(action, logger);
        }

        return processGroup;
    }

    /**
     * Audits the update of process group configuration.
     *
     * @param proceedingJoinPoint join point
     * @param processGroupDTO dto
     * @return group
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.ProcessGroupDAO+) && "
            + "execution(org.apache.nifi.groups.ProcessGroup updateProcessGroup(org.apache.nifi.web.api.dto.ProcessGroupDTO)) && "
            + "args(processGroupDTO)")
    public ProcessGroup updateProcessGroupAdvice(ProceedingJoinPoint proceedingJoinPoint, ProcessGroupDTO processGroupDTO) throws Throwable {
        ProcessGroupDAO processGroupDAO = getProcessGroupDAO();
        ProcessGroup processGroup = processGroupDAO.getProcessGroup(processGroupDTO.getId());

        String name = processGroup.getName();
        ParameterContext parameterContext = processGroup.getParameterContext();
        String comments = processGroup.getComments();

        // perform the underlying operation
        ProcessGroup updatedProcessGroup = (ProcessGroup) proceedingJoinPoint.proceed();

        if (isAuditable()) {
            Collection<ActionDetails> details = new ArrayList<>();

            // see if the name has changed
            if (name != null && updatedProcessGroup.getName() != null && !name.equals(updatedProcessGroup.getName())) {
                // create the config details
                FlowChangeConfigureDetails configDetails = new FlowChangeConfigureDetails();
                configDetails.setName("Name");
                configDetails.setValue(updatedProcessGroup.getName());
                configDetails.setPreviousValue(name);

                details.add(configDetails);
            }

            // see if the comments has changed
            if (comments != null && updatedProcessGroup.getComments() != null && !comments.equals(updatedProcessGroup.getComments())) {
                // create the config details
                FlowChangeConfigureDetails configDetails = new FlowChangeConfigureDetails();
                configDetails.setName("Comments");
                configDetails.setValue(updatedProcessGroup.getComments());
                configDetails.setPreviousValue(comments);

                details.add(configDetails);
            }

            // see if the parameter context has changed
            if (parameterContext != null && updatedProcessGroup.getParameterContext() != null) {
                if (!parameterContext.getIdentifier().equals(updatedProcessGroup.getParameterContext().getIdentifier())) {
                    // create the config details
                    FlowChangeConfigureDetails configDetails = new FlowChangeConfigureDetails();
                    configDetails.setName("Parameter Context");
                    configDetails.setValue(updatedProcessGroup.getParameterContext().getIdentifier());
                    configDetails.setPreviousValue(parameterContext.getIdentifier());

                    details.add(configDetails);
                }
            } else if (updatedProcessGroup.getParameterContext() != null) {
                // create the config details
                FlowChangeConfigureDetails configDetails = new FlowChangeConfigureDetails();
                configDetails.setName("Parameter Context");
                configDetails.setValue(updatedProcessGroup.getParameterContext().getIdentifier());
                configDetails.setPreviousValue(null);

                details.add(configDetails);
            } else if (parameterContext != null) {
                // create the config details
                FlowChangeConfigureDetails configDetails = new FlowChangeConfigureDetails();
                configDetails.setName("Parameter Context");
                configDetails.setValue(null);
                configDetails.setPreviousValue(parameterContext.getIdentifier());

                details.add(configDetails);
            }

            // hold all actions
            Collection<Action> actions = new ArrayList<>();

            // save the actions if necessary
            if (!details.isEmpty()) {
                Date timestamp = new Date();

                // create the actions
                for (ActionDetails detail : details) {
                    // determine the type of operation being performed
                    Operation operation = Operation.Configure;
                    if (detail instanceof FlowChangeMoveDetails) {
                        operation = Operation.Move;
                    }

                    // create the port action for updating the name
                    FlowChangeAction processGroupAction = createFlowChangeAction();
                    processGroupAction.setOperation(operation);
                    processGroupAction.setTimestamp(timestamp);
                    processGroupAction.setSourceId(updatedProcessGroup.getIdentifier());
                    processGroupAction.setSourceName(updatedProcessGroup.getName());
                    processGroupAction.setSourceType(Component.ProcessGroup);
                    processGroupAction.setActionDetails(detail);

                    actions.add(processGroupAction);
                }
            }

            // save actions if necessary
            if (!actions.isEmpty()) {
                saveActions(actions, logger);
            }
        }

        return updatedProcessGroup;
    }

    /**
     * Audits the update of process group configuration.
     *
     * @param proceedingJoinPoint join point
     * @param groupId group id
     * @param state scheduled state
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.ProcessGroupDAO+) && "
        + "execution(void scheduleComponents(String, org.apache.nifi.controller.ScheduledState, java.util.Set<String>)) && "
        + "args(groupId, state, componentIds)")
    public void scheduleComponentsAdvice(ProceedingJoinPoint proceedingJoinPoint, String groupId, ScheduledState state, Set<String> componentIds) throws Throwable {
        final Operation operation;

        proceedingJoinPoint.proceed();

        // determine the running state
        if (ScheduledState.RUNNING.equals(state)) {
            operation = Operation.Start;
        } else {
            operation = Operation.Stop;
        }

        saveUpdateProcessGroupAction(groupId, operation);
    }

    /**
     * Audits the update of process group configuration.
     *
     * @param proceedingJoinPoint join point
     * @param groupId group id
     * @param state scheduled state
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.ProcessGroupDAO+) && "
            + "execution(void enableComponents(String, org.apache.nifi.controller.ScheduledState, java.util.Set<String>)) && "
            + "args(groupId, state, componentIds)")
    public void enableComponentsAdvice(ProceedingJoinPoint proceedingJoinPoint, String groupId, ScheduledState state, Set<String> componentIds) throws Throwable {
        final Operation operation;

        proceedingJoinPoint.proceed();

        // determine the running state
        if (ScheduledState.DISABLED.equals(state)) {
            operation = Operation.Disable;
        } else {
            operation = Operation.Enable;
        }

        saveUpdateProcessGroupAction(groupId, operation);
        saveActions(getComponentActions(groupId, componentIds, operation), logger);
    }

    private List<Action> getComponentActions(final String groupId, final Collection<String> componentIds, final Operation operation) {
        final List<Action> actions = new ArrayList<>();
        final ProcessGroupDAO processGroupDAO = getProcessGroupDAO();
        final ProcessGroup processGroup = processGroupDAO.getProcessGroup(groupId);

        for (String componentId : componentIds) {
            final ProcessorNode processorNode = processGroup.findProcessor(componentId);
            if (processorNode != null) {
                actions.add(generateUpdateConnectableAction(processorNode, operation, Component.Processor));
                continue;
            }

            Port port = processGroup.findInputPort(componentId);
            if (port != null) {
                actions.add(generateUpdateConnectableAction(port, operation, Component.InputPort));
                continue;
            }

            port = processGroup.findOutputPort(componentId);
            if (port != null) {
                actions.add(generateUpdateConnectableAction(port, operation, Component.OutputPort));
            }
        }

        return actions;
    }

    /**
     * Audits the update of controller service state
     *
     * @param proceedingJoinPoint join point
     * @param groupId group id
     * @param state controller service state
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.ProcessGroupDAO+) && "
        + "execution(void activateControllerServices(String, org.apache.nifi.controller.service.ControllerServiceState, java.util.Collection<String>)) && "
        + "args(groupId, state, serviceIds)")
    public void activateControllerServicesAdvice(ProceedingJoinPoint proceedingJoinPoint, String groupId, ControllerServiceState state, Collection<String> serviceIds) throws Throwable {
        final List<ControllerServiceNode> controllerServiceNodes = getControllerServices(groupId, serviceIds);
        final Operation operation;

        proceedingJoinPoint.proceed();

        // determine the service state
        if (ControllerServiceState.ENABLED.equals(state)) {
            operation = Operation.Enable;
        } else {
            operation = Operation.Disable;
        }

        saveUpdateProcessGroupAction(groupId, operation);
        for (final ControllerServiceNode csNode : controllerServiceNodes) {
            saveUpdateControllerServiceAction(csNode, operation);
        }
    }

    private List<ControllerServiceNode> getControllerServices(final String groupId, final Collection<String> serviceIds) throws Throwable {
        final ProcessGroupDAO processGroupDAO = getProcessGroupDAO();
        final ProcessGroup processGroup = processGroupDAO.getProcessGroup(groupId);
        final List<ControllerServiceNode> csNodes = new ArrayList<>();
        for (String serviceId : serviceIds) {
            final ControllerServiceNode csNode = processGroup.findControllerService(serviceId, true, true);
            if (csNode != null) {
                csNodes.add(csNode);
            }
        }
        return csNodes;
    }

    @Around("within(org.apache.nifi.web.dao.ProcessGroupDAO+) && "
            + "execution(org.apache.nifi.groups.ProcessGroup updateProcessGroupFlow(..))")
    public ProcessGroup updateProcessGroupFlowAdvice(final ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        final Object[] args = proceedingJoinPoint.getArgs();
        final String groupId = (String) args[0];

        final ProcessGroupDAO processGroupDAO = getProcessGroupDAO();
        final ProcessGroup processGroup = processGroupDAO.getProcessGroup(groupId);
        final VersionControlInformation vci = processGroup.getVersionControlInformation();

        final ProcessGroup updatedProcessGroup = (ProcessGroup) proceedingJoinPoint.proceed();
        final VersionControlInformation updatedVci = updatedProcessGroup.getVersionControlInformation();

        final Operation operation;
        if (vci == null) {
            operation = Operation.StartVersionControl;
        } else {
            if (updatedVci == null) {
                operation = Operation.StopVersionControl;
            } else if (vci.getVersion() == updatedVci.getVersion()) {
                operation = Operation.RevertLocalChanges;
            } else {
                operation = Operation.ChangeVersion;
            }
        }

        saveUpdateProcessGroupAction(groupId, operation);

        return updatedProcessGroup;
    }

    @Around("within(org.apache.nifi.web.dao.ProcessGroupDAO+) && "
            + "execution(org.apache.nifi.groups.ProcessGroup updateVersionControlInformation(..))")
    public ProcessGroup updateVersionControlInformationAdvice(final ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        final VersionControlInformationDTO vciDto = (VersionControlInformationDTO) proceedingJoinPoint.getArgs()[0];

        final ProcessGroupDAO processGroupDAO = getProcessGroupDAO();
        final ProcessGroup processGroup = processGroupDAO.getProcessGroup(vciDto.getGroupId());
        final VersionControlInformation vci = processGroup.getVersionControlInformation();

        final ProcessGroup updatedProcessGroup = (ProcessGroup) proceedingJoinPoint.proceed();

        final Operation operation;
        if (vci == null) {
            operation = Operation.StartVersionControl;
        } else {
            operation = Operation.CommitLocalChanges;
        }

        saveUpdateProcessGroupAction(vciDto.getGroupId(), operation);

        return updatedProcessGroup;
    }

    @Around("within(org.apache.nifi.web.dao.ProcessGroupDAO+) && "
            + "execution(org.apache.nifi.groups.ProcessGroup disconnectVersionControl(String)) && "
            + "args(groupId)")
    public ProcessGroup disconnectVersionControlAdvice(final ProceedingJoinPoint proceedingJoinPoint, final String groupId) throws Throwable {
        final ProcessGroup updatedProcessGroup = (ProcessGroup) proceedingJoinPoint.proceed();

        saveUpdateProcessGroupAction(groupId, Operation.StopVersionControl);

        return updatedProcessGroup;
    }

    private void saveUpdateProcessGroupAction(final String groupId, final Operation operation) {
        ProcessGroupDAO processGroupDAO = getProcessGroupDAO();
        ProcessGroup processGroup = processGroupDAO.getProcessGroup(groupId);

        // if the user was starting/stopping this process group
        FlowChangeAction action = createFlowChangeAction();
        action.setSourceId(processGroup.getIdentifier());
        action.setSourceName(processGroup.getName());
        action.setSourceType(Component.ProcessGroup);
        action.setOperation(operation);

        // add this action
        saveAction(action, logger);
    }

    private Action generateUpdateConnectableAction(final Connectable connectable, final Operation operation, final Component component) {
        final FlowChangeAction action = createFlowChangeAction();
        action.setSourceId(connectable.getIdentifier());
        action.setSourceName(connectable.getName());
        action.setSourceType(component);
        action.setOperation(operation);

        if (component == Component.Processor) {
            FlowChangeExtensionDetails componentDetails = new FlowChangeExtensionDetails();
            componentDetails.setType(connectable.getComponentType());
            action.setComponentDetails(componentDetails);
        }
        return action;
    }

    private void saveUpdateControllerServiceAction(final ControllerServiceNode csNode, final Operation operation) throws Throwable {
        final FlowChangeAction action = createFlowChangeAction();
        action.setSourceId(csNode.getIdentifier());
        action.setSourceName(csNode.getName());
        action.setSourceType(Component.ControllerService);
        action.setOperation(operation);

        FlowChangeExtensionDetails serviceDetails = new FlowChangeExtensionDetails();
        serviceDetails.setType(csNode.getComponentType());
        action.setComponentDetails(serviceDetails);

        // add this action
        saveAction(action, logger);
    }

    /**
     * Audits the removal of a process group via deleteProcessGroup().
     *
     * @param proceedingJoinPoint join point
     * @param groupId group id
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.ProcessGroupDAO+) && "
            + "execution(void deleteProcessGroup(String)) && "
            + "args(groupId)")
    public void removeProcessGroupAdvice(ProceedingJoinPoint proceedingJoinPoint, String groupId) throws Throwable {
        // get the process group before removing it
        ProcessGroupDAO processGroupDAO = getProcessGroupDAO();
        ProcessGroup processGroup = processGroupDAO.getProcessGroup(groupId);

        // remove the process group
        proceedingJoinPoint.proceed();

        // if no exceptions were thrown, add removal actions...
        // audit the process group removal
        final Action action = generateAuditRecord(processGroup, Operation.Remove);

        // save the actions
        if (action != null) {
            saveAction(action, logger);
        }
    }

    /**
     * Generates an audit record for the creation of a process group.
     *
     * @param processGroup group
     * @param operation operation
     * @return action
     */
    public Action generateAuditRecord(ProcessGroup processGroup, Operation operation) {
        return generateAuditRecord(processGroup, operation, null);
    }

    /**
     * Generates an audit record for the creation of a process group.
     *
     * @param processGroup group
     * @param operation operation
     * @param actionDetails details
     * @return action
     */
    public Action generateAuditRecord(ProcessGroup processGroup, Operation operation, ActionDetails actionDetails) {
        FlowChangeAction action = null;

        if (isAuditable()) {
            // create the process group action for adding this process group
            action = createFlowChangeAction();
            action.setOperation(operation);
            action.setSourceId(processGroup.getIdentifier());
            action.setSourceName(processGroup.getName());
            action.setSourceType(Component.ProcessGroup);

            if (actionDetails != null) {
                action.setActionDetails(actionDetails);
            }
        }

        return action;
    }
}
