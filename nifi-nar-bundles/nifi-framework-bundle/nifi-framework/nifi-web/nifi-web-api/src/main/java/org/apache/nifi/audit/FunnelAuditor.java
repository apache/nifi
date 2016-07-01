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
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.web.dao.FunnelDAO;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

@Aspect
public class FunnelAuditor extends NiFiAuditor {

    private static final Logger logger = LoggerFactory.getLogger(FunnelAuditor.class);

    /**
     * Audits the creation of a funnel.
     *
     * @param proceedingJoinPoint join point
     * @return funnel
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.FunnelDAO+) && "
            + "execution(org.apache.nifi.connectable.Funnel createFunnel(java.lang.String, org.apache.nifi.web.api.dto.FunnelDTO))")
    public Funnel createFunnelAdvice(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        // perform the underlying operation
        Funnel funnel = (Funnel) proceedingJoinPoint.proceed();

        // perform the audit
        final Action action = generateAuditRecord(funnel, Operation.Add);

        // save the actions
        if (action != null) {
            saveAction(action, logger);
        }

        return funnel;
    }

    /**
     * Audits the removal of a funnel.
     *
     * @param proceedingJoinPoint join point
     * @param funnelId funnel id
     * @param funnelDAO funnel dao
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.FunnelDAO+) && "
            + "execution(void deleteFunnel(java.lang.String)) && "
            + "args(funnelId) && "
            + "target(funnelDAO)")
    public void removeFunnelAdvice(ProceedingJoinPoint proceedingJoinPoint, String funnelId, FunnelDAO funnelDAO) throws Throwable {
        // get the funnel before removing it
        Funnel funnel = funnelDAO.getFunnel(funnelId);

        // remove the funnel
        proceedingJoinPoint.proceed();

        // if no exceptions were thrown, add removal actions...
        final Action action = generateAuditRecord(funnel, Operation.Remove);

        // save the actions
        if (action != null) {
            saveAction(action, logger);
        }
    }

    /**
     * Generates an audit record for the creation of the specified funnel.
     *
     * @param funnel funnel
     * @param operation operation
     * @return action
     */
    public Action generateAuditRecord(Funnel funnel, Operation operation) {
        return generateAuditRecord(funnel, operation, null);
    }

    /**
     * Generates an audit record for the creation of the specified funnel.
     *
     * @param funnel funnel
     * @param operation operation
     * @param actionDetails details
     * @return action
     */
    public Action generateAuditRecord(Funnel funnel, Operation operation, ActionDetails actionDetails) {
        FlowChangeAction action = null;

        // get the current user
        NiFiUser user = NiFiUserUtils.getNiFiUser();

        // ensure the user was found
        if (user != null) {
            // create the action for adding this funnel
            action = new FlowChangeAction();
            action.setUserIdentity(user.getIdentity());
            action.setOperation(operation);
            action.setTimestamp(new Date());
            action.setSourceId(funnel.getIdentifier());
            action.setSourceName(funnel.getName());
            action.setSourceType(Component.Funnel);

            if (actionDetails != null) {
                action.setActionDetails(actionDetails);
            }
        }

        return action;
    }
}
