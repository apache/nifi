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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.action.Action;
import org.apache.nifi.action.Component;
import org.apache.nifi.action.FlowChangeAction;
import org.apache.nifi.action.Operation;
import org.apache.nifi.action.details.ActionDetails;
import org.apache.nifi.action.details.FlowChangeConfigureDetails;
import org.apache.nifi.authorization.Group;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.web.api.dto.UserGroupDTO;
import org.apache.nifi.web.dao.UserGroupDAO;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.Collator;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Audits user creation/removal and configuration changes.
 */
@Aspect
public class UserGroupAuditor extends NiFiAuditor {

    private static final Logger logger = LoggerFactory.getLogger(UserGroupAuditor.class);

    private static final String NAME = "Name";
    private static final String USERS = "Users";

    /**
     * Audits the creation of policies via createUser().
     *
     * This method only needs to be run 'after returning'. However, in Java 7 the order in which these methods are returned from Class.getDeclaredMethods (even though there is no order guaranteed)
     * seems to differ from Java 6. SpringAOP depends on this ordering to determine advice precedence. By normalizing all advice into Around advice we can alleviate this issue.
     *
     * @param proceedingJoinPoint join point
     * @return node
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.UserGroupDAO+) && "
            + "execution(org.apache.nifi.authorization.Group createUserGroup(org.apache.nifi.web.api.dto.UserGroupDTO))")
    public Group createUserGroupAdvice(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        // create the access user group
        Group userGroup = (Group) proceedingJoinPoint.proceed();

        // if no exceptions were thrown, add the user action...
        final Action action = generateAuditRecord(userGroup, Operation.Add);

        // save the actions
        if (action != null) {
            saveAction(action, logger);
        }

        return userGroup;
    }

    /**
     * Audits the configuration of a single user.
     *
     * @param proceedingJoinPoint join point
     * @param userGroupDTO dto
     * @param userGroupDAO dao
     * @return node
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.UserGroupDAO+) && "
            + "execution(org.apache.nifi.authorization.Group updateUserGroup(org.apache.nifi.web.api.dto.UserGroupDTO)) && "
            + "args(userGroupDTO) && "
            + "target(userGroupDAO)")
    public Group updateUserAdvice(ProceedingJoinPoint proceedingJoinPoint, UserGroupDTO userGroupDTO, UserGroupDAO userGroupDAO) throws Throwable {
        // determine the initial values for each property/setting that's changing
        Group user = userGroupDAO.getUserGroup(userGroupDTO.getId());
        final Map<String, String> values = extractConfiguredPropertyValues(user, userGroupDTO);

        // update the user state
        final Group updatedUserGroup = (Group) proceedingJoinPoint.proceed();

        // if no exceptions were thrown, add the user group action...
        user = userGroupDAO.getUserGroup(updatedUserGroup.getIdentifier());

        // get the current user
        NiFiUser niFiUser = NiFiUserUtils.getNiFiUser();

        // ensure the user was found
        if (niFiUser != null) {
            // determine the updated values
            Map<String, String> updatedValues = extractConfiguredPropertyValues(user, userGroupDTO);

            // create a user action
            Date actionTimestamp = new Date();
            Collection<Action> actions = new ArrayList<>();

            // go through each updated value
            for (String property : updatedValues.keySet()) {
                String newValue = updatedValues.get(property);
                String oldValue = values.get(property);
                Operation operation = null;

                // determine the type of operation
                if (oldValue == null || newValue == null || !newValue.equals(oldValue)) {
                    operation = Operation.Configure;
                }

                // create a configuration action accordingly
                if (operation != null) {
                    final FlowChangeConfigureDetails actionDetails = new FlowChangeConfigureDetails();
                    actionDetails.setName(property);
                    actionDetails.setValue(newValue);
                    actionDetails.setPreviousValue(oldValue);

                    // create a configuration action
                    FlowChangeAction configurationAction = new FlowChangeAction();
                    configurationAction.setUserIdentity(niFiUser.getIdentity());
                    configurationAction.setOperation(operation);
                    configurationAction.setTimestamp(actionTimestamp);
                    configurationAction.setSourceId(user.getIdentifier());
                    configurationAction.setSourceName(user.getName());
                    configurationAction.setSourceType(Component.UserGroup);
                    configurationAction.setActionDetails(actionDetails);
                    actions.add(configurationAction);
                }
            }

            // ensure there are actions to record
            if (!actions.isEmpty()) {
                // save the actions
                saveActions(actions, logger);
            }
        }

        return updatedUserGroup;
    }

    /**
     * Audits the removal of a user via deleteUser().
     *
     * @param proceedingJoinPoint join point
     * @param userGroupId user id
     * @param userGroupDAO dao
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.UserGroupDAO+) && "
            + "execution(org.apache.nifi.authorization.Group deleteUserGroup(java.lang.String)) && "
            + "args(userGroupId) && "
            + "target(userGroupDAO)")
    public Group removeUserAdvice(ProceedingJoinPoint proceedingJoinPoint, String userGroupId, UserGroupDAO userGroupDAO) throws Throwable {
        // get the user group before removing it
        Group userGroup = userGroupDAO.getUserGroup(userGroupId);

        // remove the user group
        final Group removedUserGroup = (Group) proceedingJoinPoint.proceed();

        // if no exceptions were thrown, add removal actions...
        // audit the user removal
        final Action action = generateAuditRecord(userGroup, Operation.Remove);

        // save the actions
        if (action != null) {
            saveAction(action, logger);
        }

        return removedUserGroup;
    }

    /**
     * Generates an audit record for the creation of a user group.
     *
     * @param userGroup userGroup
     * @param operation operation
     * @return action
     */
    public Action generateAuditRecord(Group userGroup, Operation operation) {
        return generateAuditRecord(userGroup, operation, null);
    }

    /**
     * Generates an audit record for the creation of a user group.
     *
     * @param userGroup userGroup
     * @param operation operation
     * @param actionDetails details
     * @return action
     */
    public Action generateAuditRecord(Group userGroup, Operation operation, ActionDetails actionDetails) {
        FlowChangeAction action = null;

        // get the current user
        NiFiUser niFiUser = NiFiUserUtils.getNiFiUser();

        // ensure the user was found
        if (niFiUser != null) {
            // create the user action for adding this user
            action = new FlowChangeAction();
            action.setUserIdentity(niFiUser.getIdentity());
            action.setOperation(operation);
            action.setTimestamp(new Date());
            action.setSourceId(userGroup.getIdentifier());
            action.setSourceName(userGroup.getName());
            action.setSourceType(Component.UserGroup);

            if (actionDetails != null) {
                action.setActionDetails(actionDetails);
            }
        }

        return action;
    }

    /**
     * Extracts the values for the configured properties from the specified user group.
     */
    private Map<String, String> extractConfiguredPropertyValues(Group group, UserGroupDTO userGroupDTO) {
        Map<String, String> values = new HashMap<>();

        if (userGroupDTO.getIdentity() != null) {
            values.put(NAME, group.getName());
        }
        if (userGroupDTO.getUsers() != null) {
            // get each of the auto terminated relationship names
            final List<String> currentUsers = new ArrayList<>(group.getUsers());

            // sort them and include in the configuration
            Collections.sort(currentUsers, Collator.getInstance(Locale.US));
            values.put(USERS, StringUtils.join(currentUsers, ", "));
        }

        return values;
    }

}
