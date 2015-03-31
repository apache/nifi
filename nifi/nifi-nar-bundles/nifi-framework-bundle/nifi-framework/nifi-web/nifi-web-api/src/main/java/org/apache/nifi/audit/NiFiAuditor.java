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
import org.apache.nifi.action.Action;
import org.apache.nifi.action.details.MoveDetails;
import org.apache.nifi.admin.service.AuditService;
import org.apache.nifi.cluster.context.ClusterContext;
import org.apache.nifi.cluster.context.ClusterContextThreadLocal;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.dao.ProcessGroupDAO;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;

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
     * @param action
     * @param logger
     */
    protected void saveAction(Action action, Logger logger) {
        final Collection<Action> actions = new ArrayList<>();
        actions.add(action);
        saveActions(actions, logger);
    }

    /**
     * Records the actions.
     *
     * @param actions
     * @param logger
     */
    protected void saveActions(Collection<Action> actions, Logger logger) {
        ClusterContext ctx = ClusterContextThreadLocal.getContext();
        
        // if we're a connected node, then put audit actions on threadlocal to propagate back to manager
        if (ctx != null) {
            ctx.getActions().addAll(actions);
        } else {
            // if we're the cluster manager, or a disconnected node, or running standalone, then audit actions
            try {
                // record the operations
                auditService.addActions(actions);
            } catch (Throwable t) {
                logger.warn("Unable to record actions: " + t.getMessage());
                if (logger.isDebugEnabled()) {
                    logger.warn(StringUtils.EMPTY, t);
                }
            }
        }
    }

    protected MoveDetails createMoveDetails(String previousGroupId, String newGroupId, Logger logger) {
        MoveDetails moveDetails = null;

        // get the groups in question
        ProcessGroup previousGroup = processGroupDAO.getProcessGroup(previousGroupId);
        ProcessGroup newGroup = processGroupDAO.getProcessGroup(newGroupId);

        // ensure the groups were found
        if (previousGroup != null && newGroup != null) {
            // create the move details
            moveDetails = new MoveDetails();
            moveDetails.setPreviousGroupId(previousGroup.getIdentifier());
            moveDetails.setPreviousGroup(previousGroup.getName());
            moveDetails.setGroupId(newGroup.getIdentifier());
            moveDetails.setGroup(newGroup.getName());
        } else {
            logger.warn(String.format("Unable to record move action because old (%s) and new (%s) groups could not be found.", previousGroupId, newGroupId));
        }

        return moveDetails;
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
