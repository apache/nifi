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
import org.apache.nifi.authorization.AccessPolicy;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.web.api.dto.AccessPolicyDTO;
import org.apache.nifi.web.dao.AccessPolicyDAO;
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
 * Audits policy creation/removal and configuration changes.
 */
@Aspect
public class AccessPolicyAuditor extends NiFiAuditor {

    private static final Logger logger = LoggerFactory.getLogger(AccessPolicyAuditor.class);

    private static final String USERS = "Users";
    private static final String USER_GROUPS = "User Groups";

    /**
     * Audits the creation of policies via createAccessPolicy().
     *
     * This method only needs to be run 'after returning'. However, in Java 7 the order in which these methods are returned from Class.getDeclaredMethods (even though there is no order guaranteed)
     * seems to differ from Java 6. SpringAOP depends on this ordering to determine advice precedence. By normalizing all advice into Around advice we can alleviate this issue.
     *
     * @param proceedingJoinPoint join point
     * @return node
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.AccessPolicyDAO+) && "
            + "execution(org.apache.nifi.authorization.AccessPolicy createAccessPolicy(org.apache.nifi.web.api.dto.AccessPolicyDTO))")
    public AccessPolicy createAccessPolicyAdvice(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        // create the access policy
        AccessPolicy policy = (AccessPolicy) proceedingJoinPoint.proceed();

        // if no exceptions were thrown, add the policy action...
        final Action action = generateAuditRecord(policy, Operation.Add);

        // save the actions
        if (action != null) {
            saveAction(action, logger);
        }

        return policy;
    }

    /**
     * Audits the configuration of a single policy.
     *
     * @param proceedingJoinPoint join point
     * @param accessPolicyDTO dto
     * @param accessPolicyDAO dao
     * @return node
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.AccessPolicyDAO+) && "
            + "execution(org.apache.nifi.authorization.AccessPolicy updateAccessPolicy(org.apache.nifi.web.api.dto.AccessPolicyDTO)) && "
            + "args(accessPolicyDTO) && "
            + "target(accessPolicyDAO)")
    public AccessPolicy updateAccessPolicyAdvice(ProceedingJoinPoint proceedingJoinPoint, AccessPolicyDTO accessPolicyDTO, AccessPolicyDAO accessPolicyDAO) throws Throwable {
        // determine the initial values for each property/setting that's changing
        AccessPolicy accessPolicy = accessPolicyDAO.getAccessPolicy(accessPolicyDTO.getId());
        final Map<String, String> values = extractConfiguredPropertyValues(accessPolicy, accessPolicyDTO);

        // update the policy state
        final AccessPolicy updatedAccessPolicy = (AccessPolicy) proceedingJoinPoint.proceed();

        // if no exceptions were thrown, add the policy action...
        accessPolicy = accessPolicyDAO.getAccessPolicy(updatedAccessPolicy.getIdentifier());

        // get the current user
        NiFiUser user = NiFiUserUtils.getNiFiUser();

        // ensure the user was found
        if (user != null) {
            // determine the updated values
            Map<String, String> updatedValues = extractConfiguredPropertyValues(accessPolicy, accessPolicyDTO);

            // create a policy action
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
                    configurationAction.setUserIdentity(user.getIdentity());
                    configurationAction.setOperation(operation);
                    configurationAction.setTimestamp(actionTimestamp);
                    configurationAction.setSourceId(accessPolicy.getIdentifier());
                    configurationAction.setSourceName(formatPolicyName(accessPolicy));
                    configurationAction.setSourceType(Component.AccessPolicy);
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

        return updatedAccessPolicy;
    }

    /**
     * Audits the removal of a policy via deleteAccessPolicy().
     *
     * @param proceedingJoinPoint join point
     * @param policyId policy id
     * @param accessPolicyDAO dao
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.AccessPolicyDAO+) && "
            + "execution(org.apache.nifi.authorization.AccessPolicy deleteAccessPolicy(java.lang.String)) && "
            + "args(policyId) && "
            + "target(accessPolicyDAO)")
    public AccessPolicy removePolicyAdvice(ProceedingJoinPoint proceedingJoinPoint, String policyId, AccessPolicyDAO accessPolicyDAO) throws Throwable {
        // get the policy before removing it
        AccessPolicy accessPolicy = accessPolicyDAO.getAccessPolicy(policyId);

        // remove the policy
        final AccessPolicy removedAccessPolicy = (AccessPolicy)proceedingJoinPoint.proceed();

        // if no exceptions were thrown, add removal actions...
        // audit the policy removal
        final Action action = generateAuditRecord(accessPolicy, Operation.Remove);

        // save the actions
        if (action != null) {
            saveAction(action, logger);
        }

        return removedAccessPolicy;
    }

    /**
     * Generates an audit record for the creation of a policy.
     *
     * @param policy policy
     * @param operation operation
     * @return action
     */
    public Action generateAuditRecord(AccessPolicy policy, Operation operation) {
        return generateAuditRecord(policy, operation, null);
    }

    /**
     * Generates an audit record for the creation of a policy.
     *
     * @param policy policy
     * @param operation operation
     * @param actionDetails details
     * @return action
     */
    public Action generateAuditRecord(AccessPolicy policy, Operation operation, ActionDetails actionDetails) {
        FlowChangeAction action = null;

        // get the current user
        NiFiUser user = NiFiUserUtils.getNiFiUser();

        // ensure the user was found
        if (user != null) {
            // create the policy action for adding this policy
            action = new FlowChangeAction();
            action.setUserIdentity(user.getIdentity());
            action.setOperation(operation);
            action.setTimestamp(new Date());
            action.setSourceId(policy.getIdentifier());
            action.setSourceName(formatPolicyName(policy));
            action.setSourceType(Component.AccessPolicy);

            if (actionDetails != null) {
                action.setActionDetails(actionDetails);
            }
        }

        return action;
    }

    /**
     * Formats the name of the specified policy.
     *
     * @param policy policy
     * @return formatted name
     */
    private String formatPolicyName(final AccessPolicy policy) {
        return policy.getAction().toString() + " " + policy.getResource();
    }

    /**
     * Extracts the values for the configured properties from the specified policy.
     */
    private Map<String, String> extractConfiguredPropertyValues(AccessPolicy policy, AccessPolicyDTO policyDTO) {
        Map<String, String> values = new HashMap<>();

        if (policyDTO.getUsers() != null) {
            // get each of the auto terminated relationship names
            final List<String> currentUsers = new ArrayList<>(policy.getUsers());

            // sort them and include in the configuration
            Collections.sort(currentUsers, Collator.getInstance(Locale.US));
            values.put(USERS, StringUtils.join(currentUsers, ", "));
        }
        if (policyDTO.getUserGroups() != null) {
            // get each of the auto terminated relationship names
            final List<String> currentUserGroups = new ArrayList<>(policy.getGroups());

            // sort them and include in the configuration
            Collections.sort(currentUserGroups, Collator.getInstance(Locale.US));
            values.put(USER_GROUPS, StringUtils.join(currentUserGroups, ", "));
        }

        return values;
    }

}
