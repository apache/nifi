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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import org.apache.nifi.action.Action;
import org.apache.nifi.action.Component;
import org.apache.nifi.action.Operation;
import org.apache.nifi.action.details.ActionDetails;
import org.apache.nifi.action.details.ConfigureDetails;
import org.apache.nifi.action.details.MoveDetails;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.web.security.user.NiFiUserUtils;
import org.apache.nifi.user.NiFiUser;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.dao.ProcessGroupDAO;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Audits process group creation/removal and configuration changes.
 */
@Aspect
public class ProcessGroupAuditor extends NiFiAuditor {

    private static final Logger logger = LoggerFactory.getLogger(ProcessGroupAuditor.class);

    /**
     * Audits the creation of process groups via createProcessGroup().
     *
     * This method only needs to be run 'after returning'. However, in Java 7
     * the order in which these methods are returned from
     * Class.getDeclaredMethods (even though there is no order guaranteed) seems
     * to differ from Java 6. SpringAOP depends on this ordering to determine
     * advice precedence. By normalizing all advice into Around advice we can
     * alleviate this issue.
     *
     * @param proceedingJoinPoint
     */
//    @AfterReturning(
//        pointcut="within(org.apache.nifi.web.dao.ProcessGroupDAO+) && "
//            + "execution(org.apache.nifi.web.api.dto.ProcessGroupDTO createProcessGroup(java.lang.String, org.apache.nifi.web.api.dto.ProcessGroupDTO))",
//        returning="processGroup"
//    )
    @Around("within(org.apache.nifi.web.dao.ProcessGroupDAO+) && "
            + "execution(org.apache.nifi.groups.ProcessGroup createProcessGroup(java.lang.String, org.apache.nifi.web.api.dto.ProcessGroupDTO))")
    public Object createProcessGroupAdvice(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
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
     * @param proceedingJoinPoint
     * @return
     * @throws Throwable
     */
    @Around("within(org.apache.nifi.web.dao.ProcessGroupDAO+) && "
            + "execution(org.apache.nifi.groups.ProcessGroup updateProcessGroup(org.apache.nifi.web.api.dto.ProcessGroupDTO)) && "
            + "args(processGroupDTO)")
    public Object updateProcessGroupAdvice(ProceedingJoinPoint proceedingJoinPoint, ProcessGroupDTO processGroupDTO) throws Throwable {
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
                ConfigureDetails configDetails = new ConfigureDetails();
                configDetails.setName("name");
                configDetails.setValue(updatedProcessGroup.getName());
                configDetails.setPreviousValue(name);

                details.add(configDetails);
            }

            // see if the comments has changed
            if (comments != null && updatedProcessGroup.getComments() != null && !comments.equals(updatedProcessGroup.getComments())) {
                // create the config details
                ConfigureDetails configDetails = new ConfigureDetails();
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
                    if (detail instanceof MoveDetails) {
                        operation = Operation.Move;
                    }

                    // create the port action for updating the name
                    Action processGroupAction = new Action();
                    processGroupAction.setUserDn(user.getDn());
                    processGroupAction.setUserName(user.getUserName());
                    processGroupAction.setOperation(operation);
                    processGroupAction.setTimestamp(timestamp);
                    processGroupAction.setSourceId(updatedProcessGroup.getIdentifier());
                    processGroupAction.setSourceName(updatedProcessGroup.getName());
                    processGroupAction.setSourceType(Component.ProcessGroup);
                    processGroupAction.setActionDetails(detail);

                    actions.add(processGroupAction);
                }
            }

            // if the user was starting/stopping this process group
            if (processGroupDTO.isRunning() != null) {
                // create a process group action
                Action processGroupAction = new Action();
                processGroupAction.setUserDn(user.getDn());
                processGroupAction.setUserName(user.getUserName());
                processGroupAction.setSourceId(processGroup.getIdentifier());
                processGroupAction.setSourceName(processGroup.getName());
                processGroupAction.setSourceType(Component.ProcessGroup);
                processGroupAction.setTimestamp(new Date());

                // determine the running state
                if (processGroupDTO.isRunning().booleanValue()) {
                    processGroupAction.setOperation(Operation.Start);
                } else {
                    processGroupAction.setOperation(Operation.Stop);
                }

                // add this action
                actions.add(processGroupAction);
            }

            // save actions if necessary
            if (!actions.isEmpty()) {
                saveActions(actions, logger);
            }
        }

        return updatedProcessGroup;
    }

    /**
     * Audits the removal of a process group via deleteProcessGroup().
     *
     * @param proceedingJoinPoint
     * @param groupId
     * @param processGroupDAO
     * @throws Throwable
     */
    @Around("within(org.apache.nifi.web.dao.ProcessGroupDAO+) && "
            + "execution(void deleteProcessGroup(java.lang.String)) && "
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
     * @param processGroup
     * @return
     */
    public Action generateAuditRecord(ProcessGroup processGroup, Operation operation) {
        return generateAuditRecord(processGroup, operation, null);
    }

    /**
     * Generates an audit record for the creation of a process group.
     *
     * @param processGroup
     * @return
     */
    public Action generateAuditRecord(ProcessGroup processGroup, Operation operation, ActionDetails actionDetails) {
        Action action = null;

        // get the current user
        NiFiUser user = NiFiUserUtils.getNiFiUser();

        // ensure the user was found
        if (user != null) {

            // create the process group action for adding this process group
            action = new Action();
            action.setUserDn(user.getDn());
            action.setUserName(user.getUserName());
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
