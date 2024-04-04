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
import org.apache.nifi.authorization.User;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.web.api.dto.UserDTO;
import org.apache.nifi.web.dao.UserDAO;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Audits user creation/removal and configuration changes.
 */
@Aspect
public class UserAuditor extends NiFiAuditor {

    private static final Logger logger = LoggerFactory.getLogger(UserAuditor.class);

    private static final String IDENTITY = "Identity";

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
    @Around("within(org.apache.nifi.web.dao.UserDAO+) && "
            + "execution(org.apache.nifi.authorization.User createUser(org.apache.nifi.web.api.dto.UserDTO))")
    public User createUserAdvice(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        // create the access user
        User user = (User) proceedingJoinPoint.proceed();

        // if no exceptions were thrown, add the user action...
        final Action action = generateAuditRecord(user, Operation.Add);

        // save the actions
        if (action != null) {
            saveAction(action, logger);
        }

        return user;
    }

    /**
     * Audits the configuration of a single user.
     *
     * @param proceedingJoinPoint join point
     * @param userDTO dto
     * @param userDAO dao
     * @return node
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.UserDAO+) && "
            + "execution(org.apache.nifi.authorization.User updateUser(org.apache.nifi.web.api.dto.UserDTO)) && "
            + "args(userDTO) && "
            + "target(userDAO)")
    public User updateUserAdvice(ProceedingJoinPoint proceedingJoinPoint, UserDTO userDTO, UserDAO userDAO) throws Throwable {
        // determine the initial values for each property/setting that's changing
        User user = userDAO.getUser(userDTO.getId());
        final Map<String, String> values = extractConfiguredPropertyValues(user, userDTO);

        // update the user state
        final User updatedUser = (User) proceedingJoinPoint.proceed();

        // if no exceptions were thrown, add the user action...
        user = userDAO.getUser(updatedUser.getIdentifier());

        // get the current user
        NiFiUser niFiUser = NiFiUserUtils.getNiFiUser();

        // ensure the user was found
        if (niFiUser != null) {
            // determine the updated values
            Map<String, String> updatedValues = extractConfiguredPropertyValues(user, userDTO);

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
                    configurationAction.setSourceName(user.getIdentity());
                    configurationAction.setSourceType(Component.User);
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

        return updatedUser;
    }

    /**
     * Audits the removal of a user via deleteUser().
     *
     * @param proceedingJoinPoint join point
     * @param userId user id
     * @param userDAO dao
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.UserDAO+) && "
            + "execution(org.apache.nifi.authorization.User deleteUser(java.lang.String)) && "
            + "args(userId) && "
            + "target(userDAO)")
    public User removeUserAdvice(ProceedingJoinPoint proceedingJoinPoint, String userId, UserDAO userDAO) throws Throwable {
        // get the user before removing it
        User user = userDAO.getUser(userId);

        // remove the user
        final User removedUser = (User) proceedingJoinPoint.proceed();

        // if no exceptions were thrown, add removal actions...
        // audit the user removal
        final Action action = generateAuditRecord(user, Operation.Remove);

        // save the actions
        if (action != null) {
            saveAction(action, logger);
        }

        return removedUser;
    }

    /**
     * Generates an audit record for the creation of a user.
     *
     * @param user user
     * @param operation operation
     * @return action
     */
    public Action generateAuditRecord(User user, Operation operation) {
        return generateAuditRecord(user, operation, null);
    }

    /**
     * Generates an audit record for the creation of a user.
     *
     * @param user user
     * @param operation operation
     * @param actionDetails details
     * @return action
     */
    public Action generateAuditRecord(User user, Operation operation, ActionDetails actionDetails) {
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
            action.setSourceId(user.getIdentifier());
            action.setSourceName(user.getIdentity());
            action.setSourceType(Component.User);

            if (actionDetails != null) {
                action.setActionDetails(actionDetails);
            }
        }

        return action;
    }

    /**
     * Extracts the values for the configured properties from the specified user.
     */
    private Map<String, String> extractConfiguredPropertyValues(User user, UserDTO userDTO) {
        Map<String, String> values = new HashMap<>();

        if (userDTO.getIdentity() != null) {
            values.put(IDENTITY, user.getIdentity());
        }

        return values;
    }

}
