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
import org.apache.nifi.controller.label.Label;
import org.apache.nifi.web.api.dto.LabelDTO;
import org.apache.nifi.web.dao.LabelDAO;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.Objects;

@Service
@Aspect
public class LabelAuditor extends NiFiAuditor {
    private static final Logger logger = LoggerFactory.getLogger(LabelAuditor.class);

    /**
     * Audits the creation of a Label.
     *
     * @param proceedingJoinPoint Join Point observed
     * @return Label
     * @throws Throwable Thrown on failure to proceed with target invocation
     */
    @Around("within(org.apache.nifi.web.dao.LabelDAO+) && "
            + "execution(org.apache.nifi.controller.label.Label createLabel(java.lang.String, org.apache.nifi.web.api.dto.LabelDTO))")
    public Label createLabelAdvice(final ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        final Label label = (Label) proceedingJoinPoint.proceed();
        final Action action = generateAuditRecord(label, Operation.Add);

        if (action != null) {
            saveAction(action, logger);
        }

        return label;
    }

    /**
     * Audits the configuration of a Label.
     *
     * @param proceedingJoinPoint Join Point observed
     * @param labelDTO Data Transfer Object
     * @param labelDAO Data Access Object
     * @return Label
     * @throws Throwable Thrown on failure to proceed with target invocation
     */
    @Around("within(org.apache.nifi.web.dao.LabelDAO+) && "
            + "execution(org.apache.nifi.controller.label.Label updateLabel(org.apache.nifi.web.api.dto.LabelDTO)) && "
            + "args(labelDTO) && "
            + "target(labelDAO)")
    public Label updateLabelAdvice(final ProceedingJoinPoint proceedingJoinPoint, final LabelDTO labelDTO, final LabelDAO labelDAO) throws Throwable {
        // determine the initial content of label
        final Label label = labelDAO.getLabel(labelDTO.getId());
        final String originalLabelValue = label.getValue();

        final Label updatedLabel = (Label) proceedingJoinPoint.proceed();

        // ensure the user was found
        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        if (user != null) {
            final String updatedLabelValue = updatedLabel.getValue();
            if ((originalLabelValue == null && updatedLabelValue != null)
                    || !Objects.equals(originalLabelValue, updatedLabelValue)) {
                final FlowChangeAction labelAction = new FlowChangeAction();
                labelAction.setUserIdentity(user.getIdentity());
                labelAction.setTimestamp(new Date());
                labelAction.setSourceId(label.getIdentifier());
                labelAction.setSourceType(Component.Label);
                labelAction.setOperation(Operation.Configure);
                // Source Name is a required field for the database but not applicable for a label; use UUID to create a unique name
                labelAction.setSourceName(label.getIdentifier());

                final FlowChangeConfigureDetails actionDetails = new FlowChangeConfigureDetails();
                actionDetails.setName(label.getIdentifier());
                actionDetails.setValue(updatedLabelValue);
                actionDetails.setPreviousValue(originalLabelValue);
                labelAction.setActionDetails(actionDetails);

                saveAction(labelAction, logger);
            }
        }
        return updatedLabel;
    }

    /**
     * Audits the removal of a Label.
     *
     * @param proceedingJoinPoint Join Point observed
     * @param labelId Label identifier
     * @param labelDAO Label Data Access Object
     * @throws Throwable Thrown on failure to proceed with target invocation
     */
    @Around("within(org.apache.nifi.web.dao.LabelDAO+) && "
            + "execution(void deleteLabel(java.lang.String)) && "
            + "args(labelId) && "
            + "target(labelDAO)")
    public void removeLabelAdvice(final ProceedingJoinPoint proceedingJoinPoint, final String labelId, final LabelDAO labelDAO) throws Throwable {
        // get the label before removing it
        final Label label = labelDAO.getLabel(labelId);

        // remove the label
        proceedingJoinPoint.proceed();

        // if no exceptions were thrown, add removal actions...
        final Action action = generateAuditRecord(label, Operation.Remove);

        if (action != null) {
            saveAction(action, logger);
        }
    }

    /**
     * Generates an audit record for the creation of the specified label.
     *
     * @param label Label audited
     * @param operation Operation audited
     * @return Action description
     */
    public Action generateAuditRecord(final Label label, final Operation operation) {
        return generateAuditRecord(label, operation, null);
    }

    /**
     * Generates an audit record for the creation of the specified label.
     *
     * @param label Label audited
     * @param operation Operation audited
     * @param actionDetails Action Details or null when not provided
     * @return Action description
     */
    public Action generateAuditRecord(final Label label, final Operation operation, final ActionDetails actionDetails) {
        FlowChangeAction action = null;

        final NiFiUser user = NiFiUserUtils.getNiFiUser();
        if (user != null) {
            action = new FlowChangeAction();
            action.setUserIdentity(user.getIdentity());
            action.setOperation(operation);
            action.setTimestamp(new Date());
            action.setSourceId(label.getIdentifier());
            // Labels do not have a Name; use UUID to provide a unique name
            action.setSourceName(label.getIdentifier());
            action.setSourceType(Component.Label);

            if (actionDetails != null) {
                action.setActionDetails(actionDetails);
            }
        }

        return action;
    }
}
