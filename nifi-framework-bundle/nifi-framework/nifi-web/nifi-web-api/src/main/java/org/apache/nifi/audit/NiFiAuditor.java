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
import org.apache.nifi.action.FlowChangeAction;
import org.apache.nifi.action.RequestDetails;
import org.apache.nifi.action.StandardRequestDetails;
import org.apache.nifi.action.details.FlowChangeMoveDetails;
import org.apache.nifi.action.details.MoveDetails;
import org.apache.nifi.admin.service.AuditService;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.web.dao.ProcessGroupDAO;
import org.apache.nifi.web.security.NiFiWebAuthenticationDetails;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Optional;

/**
 * A NiFi audit service.
 */
public abstract class NiFiAuditor {

    protected static final String SENSITIVE_VALUE_PLACEHOLDER = "********";

    private static final String UNAUTHENTICATED_USER_IDENTITY = "UNAUTHENTICATED";

    private AuditService auditService;

    private ProcessGroupDAO processGroupDAO;

    /**
     * Records the specified action.
     *
     * @param action action
     * @param logger logger
     */
    protected void saveAction(Action action, Logger logger) {
        final Collection<Action> actions = new ArrayList<>();
        actions.add(action);
        saveActions(actions, logger);
    }

    /**
     * Records the actions.
     *
     * @param actions actions
     * @param logger logger
     */
    protected void saveActions(Collection<Action> actions, Logger logger) {
        // always save the actions regardless of cluster or stand-alone
        // all nodes in a cluster will have their own local copy without batching
        try {
            auditService.addActions(actions);
        } catch (Throwable t) {
            logger.warn("Unable to record actions: ", t);
            if (logger.isDebugEnabled()) {
                logger.warn(StringUtils.EMPTY, t);
            }
        }
    }

    protected void deletePreviousValues(String propertyName, String componentId, Logger logger) {
        try {
            auditService.deletePreviousValues(propertyName, componentId);
        } catch (Throwable t) {
            logger.warn("Unable to delete property history", t);
            if (logger.isDebugEnabled()) {
                logger.warn(StringUtils.EMPTY, t);
            }
        }
    }

    protected MoveDetails createMoveDetails(String previousGroupId, String newGroupId, Logger logger) {
        FlowChangeMoveDetails moveDetails = null;

        // get the groups in question
        ProcessGroup previousGroup = processGroupDAO.getProcessGroup(previousGroupId);
        ProcessGroup newGroup = processGroupDAO.getProcessGroup(newGroupId);

        // ensure the groups were found
        if (previousGroup != null && newGroup != null) {
            // create the move details
            moveDetails = new FlowChangeMoveDetails();
            moveDetails.setPreviousGroupId(previousGroup.getIdentifier());
            moveDetails.setPreviousGroup(previousGroup.getName());
            moveDetails.setGroupId(newGroup.getIdentifier());
            moveDetails.setGroup(newGroup.getName());
        } else {
            logger.warn("Unable to record move action because old [{}] and new [{}] groups could not be found.", previousGroupId, newGroupId);
        }

        return moveDetails;
    }

    protected String formatExtensionVersion(final String type, final BundleCoordinate bundle) {
        final String formattedType;
        if (BundleCoordinate.DEFAULT_VERSION.equals(bundle.getVersion())) {
            formattedType = type;
        } else {
            formattedType = type + " " + bundle.getVersion();
        }

        final String formattedBundle;
        if (BundleCoordinate.DEFAULT_GROUP.equals(bundle.getGroup())) {
            formattedBundle = bundle.getId();
        } else {
            formattedBundle = bundle.getGroup() + " - " + bundle.getId();
        }

        return String.format("%s from %s", formattedType, formattedBundle);
    }

    /**
     * Return auditable status based on the presence of Authentication Credentials
     *
     * @return Auditable status
     */
    protected boolean isAuditable() {
        final Optional<Object> authenticationCredentialsFound = NiFiUserUtils.getAuthenticationCredentials();
        return authenticationCredentialsFound.isPresent();
    }

    /**
     * Create Flow Change Action with current user and standard properties
     *
     * @return Flow Change Action
     */
    protected FlowChangeAction createFlowChangeAction() {
        final FlowChangeAction flowChangeAction = new FlowChangeAction();
        flowChangeAction.setTimestamp(new Date());

        final SecurityContext securityContext = SecurityContextHolder.getContext();
        final Authentication authentication = securityContext.getAuthentication();
        if (authentication == null) {
            flowChangeAction.setUserIdentity(UNAUTHENTICATED_USER_IDENTITY);
        } else {
            final String userIdentity = authentication.getName();
            flowChangeAction.setUserIdentity(userIdentity);

            final Object details = authentication.getDetails();
            if (details instanceof NiFiWebAuthenticationDetails authenticationDetails) {
                final String remoteAddress = authenticationDetails.getRemoteAddress();
                final String forwardedFor = authenticationDetails.getForwardedFor();
                final String userAgent = authenticationDetails.getUserAgent();
                final RequestDetails requestDetails = new StandardRequestDetails(remoteAddress, forwardedFor, userAgent);
                flowChangeAction.setRequestDetails(requestDetails);
            }
        }

        return flowChangeAction;
    }

    @Autowired
    public void setAuditService(AuditService auditService) {
        this.auditService = auditService;
    }

    @Autowired
    public void setProcessGroupDAO(ProcessGroupDAO processGroupDAO) {
        this.processGroupDAO = processGroupDAO;
    }

    public ProcessGroupDAO getProcessGroupDAO() {
        return processGroupDAO;
    }
}
