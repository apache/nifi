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
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.web.api.entity.PriorityRuleEntity;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.Response;
import java.util.Date;

@Aspect
public class PriorityRulesAuditor extends NiFiAuditor {
    public static final Logger LOGGER = LoggerFactory.getLogger(PriorityRulesAuditor.class);

    @Around("within(org.apache.nifi.web.api.PriorityRuleResource+) && "
            + "execution(javax.ws.rs.core.Response addRule(org.apache.nifi.web.api.entity.PriorityRuleEntity))")
    public Response addPriorityRuleAdvice(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        Response response = (Response)proceedingJoinPoint.proceed();

        // We'll only audit something if the operation was successful
        if(response.getStatus() == 200) {
            Object argObject = proceedingJoinPoint.getArgs()[0];
            if(argObject instanceof PriorityRuleEntity) {
                PriorityRuleEntity priorityRuleEntity = (PriorityRuleEntity)argObject;

                FlowChangeConfigureDetails flowChangeConfigureDetails = new FlowChangeConfigureDetails();
                flowChangeConfigureDetails.setName("Add");
                flowChangeConfigureDetails.setValue(priorityRuleEntity.getPriorityRule().toString());

                Action action = generateAuditRecord(priorityRuleEntity, Operation.Add, flowChangeConfigureDetails);

                saveAction(action, LOGGER);
            }
        }

        return response;
    }

    @Around("within(org.apache.nifi.web.api.PriorityRuleResource+) && "
            + "execution(javax.ws.rs.core.Response editRule(org.apache.nifi.web.api.entity.PriorityRuleEntity))")
    public Response editPriorityRuleAdvice(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        Response response = (Response)proceedingJoinPoint.proceed();

        // We'll only audit something if the operation was successful
        if(response.getStatus() == 200) {
            Object argObject = proceedingJoinPoint.getArgs()[0];
            if(argObject instanceof PriorityRuleEntity) {
                PriorityRuleEntity priorityRuleEntity = (PriorityRuleEntity)argObject;
                Operation operation = priorityRuleEntity.getPriorityRule().isExpired() ? Operation.Remove : Operation.Configure;

                FlowChangeConfigureDetails flowChangeConfigureDetails = new FlowChangeConfigureDetails();
                flowChangeConfigureDetails.setName(operation.toString());
                flowChangeConfigureDetails.setValue(priorityRuleEntity.getPriorityRule().toString());

                Action action = generateAuditRecord(priorityRuleEntity, operation, flowChangeConfigureDetails);

                saveAction(action, LOGGER);
            }
        }

        return response;
    }

    public Action generateAuditRecord(PriorityRuleEntity priorityRuleEntity, Operation operation, ActionDetails actionDetails) {
        FlowChangeAction action = null;

        // get the current user
        NiFiUser user = NiFiUserUtils.getNiFiUser();

        // Ensure the user was found
        if(user != null) {
            // create the port action for adding this processor
            action = new FlowChangeAction();
            action.setUserIdentity(user.getIdentity());
            action.setOperation(operation);
            action.setTimestamp(new Date());
            action.setSourceId(priorityRuleEntity.getPriorityRule().getId());
            action.setSourceName(priorityRuleEntity.getPriorityRule().getLabel());
            action.setSourceType(Component.PriorityRule);

            if(actionDetails != null) {
                action.setActionDetails(actionDetails);
            }
        }

        return action;
    }
}
