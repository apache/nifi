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

import java.util.Date;
import java.util.Objects;

@Aspect
public class LabelAuditor extends NiFiAuditor {
    private static final Logger logger = LoggerFactory.getLogger(LabelAuditor.class);

    private static final String LABEL_NAME = "N/A";

    /**
     * Audits the creation of a Label.
     *
     * @return label
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.LabelDAO+) && "
            + "execution(org.apache.nifi.controller.label.Label createLabel(java.lang.String, org.apache.nifi.web.api.dto.LabelDTO))")
    public Label createLabelAdvice(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        // perform the underlying operation
        Label label = (Label) proceedingJoinPoint.proceed();

        // perform the audit
        final Action action = generateAuditRecord(label, Operation.Add);

        // save the actions
        if (action != null) {
            saveAction(action, logger);
        }

        return label;
    }

    /**
     * Audits the configuration of a label.
     *
     * @param proceedingJoinPoint join point
     * @param labelDTO dto
     * @param labelDAO dao
     * @return node
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.LabelDAO+) && "
            + "execution(org.apache.nifi.controller.label.Label updateLabel(org.apache.nifi.web.api.dto.LabelDTO)) && "
            + "args(labelDTO) && "
            + "target(labelDAO)")
    public Label updateLabelAdvice(ProceedingJoinPoint proceedingJoinPoint, LabelDTO labelDTO, LabelDAO labelDAO) throws Throwable {
        // determine the initial content of label
        Label label = labelDAO.getLabel(labelDTO.getId());
        String originalLabelValue = label.getValue();

        // update the processor state
        final Label updatedLabel = (Label) proceedingJoinPoint.proceed();

        // if no exceptions were thrown, add the label action...
        label = labelDAO.getLabel(updatedLabel.getIdentifier());

        // get the current user
        NiFiUser user = NiFiUserUtils.getNiFiUser();

        // ensure the user was found
        if (user != null) {
            final String updatedLabelValue = label.getValue();
            if ((originalLabelValue == null && updatedLabelValue != null)
                    || !Objects.equals(originalLabelValue, updatedLabelValue)) {
                final FlowChangeAction labelAction = new FlowChangeAction();
                labelAction.setUserIdentity(user.getIdentity());
                labelAction.setTimestamp(new Date());
                labelAction.setSourceId(label.getIdentifier());
                labelAction.setSourceType(Component.Label);
                labelAction.setOperation(Operation.Configure);
                labelAction.setSourceName(LABEL_NAME); // Source Name is a required field for the database but not applicable for a label

                final FlowChangeConfigureDetails actionDetails = new FlowChangeConfigureDetails();
                actionDetails.setName(LABEL_NAME);
                actionDetails.setValue(updatedLabelValue);
                actionDetails.setPreviousValue(originalLabelValue);
                labelAction.setActionDetails(actionDetails);

                // save the actions
                saveAction(labelAction, logger);
            }
        }
        return updatedLabel;
    }


    /**
     * Audits the removal of a label.
     *
     * @param proceedingJoinPoint join point
     * @param labelId label id
     * @param labelDAO label dao
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.LabelDAO+) && "
            + "execution(void deleteLabel(java.lang.String)) && "
            + "args(labelId) && "
            + "target(labelDAO)")
    public void removeLabelAdvice(ProceedingJoinPoint proceedingJoinPoint, String labelId, LabelDAO labelDAO) throws Throwable {
        // get the label before removing it
        Label label = labelDAO.getLabel(labelId);

        // remove the label
        proceedingJoinPoint.proceed();

        // if no exceptions were thrown, add removal actions...
        final Action action = generateAuditRecord(label, Operation.Remove);

        // save the actions
        if (action != null) {
            saveAction(action, logger);
        }
    }

    /**
     * Generates an audit record for the creation of the specified label.
     *
     * @param label label
     * @param operation operation
     * @return action
     */
    public Action generateAuditRecord(Label label, Operation operation) {
        return generateAuditRecord(label, operation, null);
    }

    /**
     * Generates an audit record for the creation of the specified label.
     *
     * @param label label
     * @param operation operation
     * @param actionDetails details
     * @return action
     */
    public Action generateAuditRecord(Label label, Operation operation, ActionDetails actionDetails) {
        FlowChangeAction action = null;

        // get the current user
        NiFiUser user = NiFiUserUtils.getNiFiUser();

        // ensure the user was found
        if (user != null) {
            // create the action for adding this label
            action = new FlowChangeAction();
            action.setUserIdentity(user.getIdentity());
            action.setOperation(operation);
            action.setTimestamp(new Date());
            action.setSourceId(label.getIdentifier());
            action.setSourceName(LABEL_NAME);
            action.setSourceType(Component.Label);

            if (actionDetails != null) {
                action.setActionDetails(actionDetails);
            }
        }

        return action;
    }
}
