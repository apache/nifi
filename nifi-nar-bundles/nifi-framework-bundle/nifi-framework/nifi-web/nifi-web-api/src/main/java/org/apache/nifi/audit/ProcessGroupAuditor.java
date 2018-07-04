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
import org.apache.nifi.action.details.FlowChangeMoveDetails;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.registry.flow.VersionControlInformation;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.VariableRegistryDTO;
import org.apache.nifi.web.api.dto.VersionControlInformationDTO;
import org.apache.nifi.web.dao.ProcessGroupDAO;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Set;
import java.util.concurrent.Future;

/**
 * Audits process group creation/removal and configuration changes.
 */
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
        String comments = processGroup.getComments();

        // perform the underlying operation
        ProcessGroup updatedProcessGroup = (ProcessGroup) proceedingJoinPoint.proceed();

        // get the current user
        NiFiUser user = NiFiUserUtils.getNiFiUser();

        // ensure the user was found
        if (user != null) {
            Collection<ActionDetails> details = new ArrayList<>();

            // see if the name has changed
            if (name != null && updatedProcessGroup.getName() != null && !name.equals(updatedProcessGroup.getName())) {
                // create the config details
                FlowChangeConfigureDetails configDetails = new FlowChangeConfigureDetails();
                configDetails.setName("name");
                configDetails.setValue(updatedProcessGroup.getName());
                configDetails.setPreviousValue(name);

                details.add(configDetails);
            }

            // see if the comments has changed
            if (comments != null && updatedProcessGroup.getComments() != null && !comments.equals(updatedProcessGroup.getComments())) {
                // create the config details
                FlowChangeConfigureDetails configDetails = new FlowChangeConfigureDetails();
                configDetails.setName("comments");
                configDetails.setValue(updatedProcessGroup.getComments());
                configDetails.setPreviousValue(comments);

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
                    FlowChangeAction processGroupAction = new FlowChangeAction();
                    processGroupAction.setUserIdentity(user.getIdentity());
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
        + "execution(java.util.concurrent.Future<Void> scheduleComponents(String, org.apache.nifi.controller.ScheduledState, java.util.Set<String>)) && "
        + "args(groupId, state, componentIds)")
    public Future<Void> scheduleComponentsAdvice(ProceedingJoinPoint proceedingJoinPoint, String groupId, ScheduledState state, Set<String> componentIds) throws Throwable {
        final Operation operation;

        final Future<Void> result = (Future<Void>) proceedingJoinPoint.proceed();

        // determine the running state
        if (ScheduledState.RUNNING.equals(state)) {
            operation = Operation.Start;
        } else {
            operation = Operation.Stop;
        }

        saveUpdateAction(groupId, operation);

        return result;
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

        saveUpdateAction(groupId, operation);
    }

    /**
     * Audits the update of controller serivce state
     *
     * @param proceedingJoinPoint join point
     * @param groupId group id
     * @param state controller service state
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.ProcessGroupDAO+) && "
        + "execution(java.util.concurrent.Future<Void> activateControllerServices(String, org.apache.nifi.controller.service.ControllerServiceState, java.util.Collection<String>)) && "
        + "args(groupId, state, serviceIds)")
    public Future<Void> activateControllerServicesAdvice(ProceedingJoinPoint proceedingJoinPoint, String groupId, ControllerServiceState state, Collection<String> serviceIds) throws Throwable {
        final Operation operation;

        final Future<Void> result = (Future<Void>) proceedingJoinPoint.proceed();

        // determine the service state
        if (ControllerServiceState.ENABLED.equals(state)) {
            operation = Operation.Enable;
        } else {
            operation = Operation.Disable;
        }

        saveUpdateAction(groupId, operation);

        return result;
    }

    /**
     * Audits the update of process group variable registry.
     *
     * @param proceedingJoinPoint join point
     * @param variableRegistry variable registry
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.ProcessGroupDAO+) && "
        + "execution(org.apache.nifi.groups.ProcessGroup updateVariableRegistry(org.apache.nifi.web.api.dto.VariableRegistryDTO)) && "
        + "args(variableRegistry)")
    public ProcessGroup updateVariableRegistryAdvice(final ProceedingJoinPoint proceedingJoinPoint, final VariableRegistryDTO variableRegistry) throws Throwable {
        final ProcessGroup updatedProcessGroup = (ProcessGroup) proceedingJoinPoint.proceed();

        saveUpdateAction(variableRegistry.getProcessGroupId(), Operation.Configure);

        return updatedProcessGroup;
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

        saveUpdateAction(groupId, operation);

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

        saveUpdateAction(vciDto.getGroupId(), operation);

        return updatedProcessGroup;
    }

    @Around("within(org.apache.nifi.web.dao.ProcessGroupDAO+) && "
            + "execution(org.apache.nifi.groups.ProcessGroup disconnectVersionControl(String)) && "
            + "args(groupId)")
    public ProcessGroup disconnectVersionControlAdvice(final ProceedingJoinPoint proceedingJoinPoint, final String groupId) throws Throwable {
        final ProcessGroup updatedProcessGroup = (ProcessGroup) proceedingJoinPoint.proceed();

        saveUpdateAction(groupId, Operation.StopVersionControl);

        return updatedProcessGroup;
    }

    private void saveUpdateAction(final String groupId, final Operation operation) throws Throwable {
        NiFiUser user = NiFiUserUtils.getNiFiUser();
        ProcessGroupDAO processGroupDAO = getProcessGroupDAO();
        ProcessGroup processGroup = processGroupDAO.getProcessGroup(groupId);

        // if the user was starting/stopping this process group
        FlowChangeAction action = new FlowChangeAction();
        action.setUserIdentity(user.getIdentity());
        action.setSourceId(processGroup.getIdentifier());
        action.setSourceName(processGroup.getName());
        action.setSourceType(Component.ProcessGroup);
        action.setTimestamp(new Date());
        action.setOperation(operation);

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

        // get the current user
        NiFiUser user = NiFiUserUtils.getNiFiUser();

        // ensure the user was found
        if (user != null) {

            // create the process group action for adding this process group
            action = new FlowChangeAction();
            action.setUserIdentity(user.getIdentity());
            action.setOperation(operation);
            action.setTimestamp(new Date());
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
