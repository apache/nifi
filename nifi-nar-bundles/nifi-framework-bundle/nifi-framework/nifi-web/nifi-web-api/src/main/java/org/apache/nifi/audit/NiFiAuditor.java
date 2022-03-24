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
import org.apache.nifi.action.details.FlowChangeMoveDetails;
import org.apache.nifi.action.details.MoveDetails;
import org.apache.nifi.admin.service.AuditService;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.dao.ProcessGroupDAO;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;

/**
 * A NiFi audit service.
 */
public abstract class NiFiAuditor {

    private AuditService auditService;
    private NiFiServiceFacade serviceFacade;
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
            logger.warn("Unable to record actions: " + t.getMessage());
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
            logger.warn(String.format("Unable to record move action because old (%s) and new (%s) groups could not be found.", previousGroupId, newGroupId));
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

    /* setters / getters */
    public void setAuditService(AuditService auditService) {
        this.auditService = auditService;
    }

    public void setServiceFacade(NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

    public void setProcessGroupDAO(ProcessGroupDAO processGroupDAO) {
        this.processGroupDAO = processGroupDAO;
    }

    public ProcessGroupDAO getProcessGroupDAO() {
        return processGroupDAO;
    }

}
