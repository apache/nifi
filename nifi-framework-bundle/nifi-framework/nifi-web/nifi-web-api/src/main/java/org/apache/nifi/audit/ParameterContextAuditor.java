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
import org.apache.nifi.action.details.FlowChangeConfigureDetails;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.web.api.dto.ParameterContextDTO;
import org.apache.nifi.web.api.dto.ParameterDTO;
import org.apache.nifi.web.dao.ParameterContextDAO;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Aspect
public class ParameterContextAuditor extends NiFiAuditor {

    private static final Logger logger = LoggerFactory.getLogger(ParameterContextAuditor.class);

    /**
     * Audits the creation of parameter contexts via createParameterContext().
     *
     * @param proceedingJoinPoint join point
     * @param parameterContextDTO dto
     * @param parameterContextDAO dao
     * @return context
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.ParameterContextDAO+) && "
            + "execution(org.apache.nifi.parameter.ParameterContext createParameterContext(org.apache.nifi.web.api.dto.ParameterContextDTO)) && "
            + "args(parameterContextDTO) && "
            + "target(parameterContextDAO)")
    public ParameterContext createParameterContextAdvice(ProceedingJoinPoint proceedingJoinPoint, ParameterContextDTO parameterContextDTO, ParameterContextDAO parameterContextDAO) throws Throwable {
        // update the processor state
        ParameterContext parameterContext = (ParameterContext) proceedingJoinPoint.proceed();

        if (isAuditable()) {
            // create a parameter context action
            Collection<Action> actions = new ArrayList<>();

            // if no exceptions were thrown, add the processor action...
            final Action createAction = generateAuditRecord(parameterContext, Operation.Add);
            actions.add(createAction);

            // determine the updated values
            Map<String, String> updatedValues = extractConfiguredParameterContextValues(parameterContext, parameterContextDTO);

            // determine the actions performed in this request
            final Date actionTimestamp = new Date(createAction.getTimestamp().getTime() + 1);
            determineActions(parameterContext, actions, actionTimestamp, updatedValues, Map.of());

            // save the actions
            saveActions(actions, logger);
        }

        return parameterContext;
    }

    /**
     * Audits the configuration of a parameter context via updateParameterContext().
     *
     * @param proceedingJoinPoint join point
     * @param parameterContextDTO dto
     * @param parameterContextDAO dao
     * @return parameter context
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.ParameterContextDAO+) && "
            + "execution(org.apache.nifi.parameter.ParameterContext updateParameterContext(org.apache.nifi.web.api.dto.ParameterContextDTO)) && "
            + "args(parameterContextDTO) && "
            + "target(parameterContextDAO)")
    public ParameterContext updateParameterContextAdvice(ProceedingJoinPoint proceedingJoinPoint, ParameterContextDTO parameterContextDTO, ParameterContextDAO parameterContextDAO) throws Throwable {
        // determine the initial values for each property/setting that's changing
        ParameterContext parameterContext = parameterContextDAO.getParameterContext(parameterContextDTO.getId());
        final Map<String, String> values = extractConfiguredParameterContextValues(parameterContext, parameterContextDTO);

        // update the processor state
        final ParameterContext updatedParameterContext = (ParameterContext) proceedingJoinPoint.proceed();

        // if no exceptions were thrown, add the processor action...
        parameterContext = parameterContextDAO.getParameterContext(updatedParameterContext.getIdentifier());

        if (isAuditable()) {
            // determine the updated values
            Map<String, String> updatedValues = extractConfiguredParameterContextValues(parameterContext, parameterContextDTO);

            // create a parameter context action
            Date actionTimestamp = new Date();
            Collection<Action> actions = new ArrayList<>();

            // determine the actions performed in this request
            determineActions(parameterContext, actions, actionTimestamp, updatedValues, values);

            // ensure there are actions to record
            if (!actions.isEmpty()) {
                // save the actions
                saveActions(actions, logger);
            }
        }

        return updatedParameterContext;
    }

    /**
     * Extract configuration changes.
     *
     * @param parameterContext the parameter context
     * @param actions actions list
     * @param actionTimestamp timestamp of the request
     * @param updatedValues the updated values
     * @param values the current values
     */
    private void determineActions(final ParameterContext parameterContext, final Collection<Action> actions,
                                   final Date actionTimestamp, final Map<String, String> updatedValues, final Map<String, String> values) {

        // go through each updated value
        for (String key : updatedValues.keySet()) {
            String newValue = updatedValues.get(key);
            String oldValue = values.get(key);
            Operation operation = null;

            // determine the type of operation
            if (oldValue == null || newValue == null || !newValue.equals(oldValue)) {
                operation = Operation.Configure;
            }

            // create a configuration action accordingly
            if (operation != null) {
                // clear the value if this property is sensitive
                final Parameter parameter = parameterContext.getParameter(key).orElse(null);
                if (parameter != null && parameter.getDescriptor().isSensitive()) {
                    if (newValue != null) {
                        newValue = SENSITIVE_VALUE_PLACEHOLDER;
                    }
                    if (oldValue != null) {
                        oldValue = SENSITIVE_VALUE_PLACEHOLDER;
                    }
                }

                final FlowChangeConfigureDetails actionDetails = new FlowChangeConfigureDetails();
                actionDetails.setName(key);
                actionDetails.setValue(newValue);
                actionDetails.setPreviousValue(oldValue);

                // create a configuration action
                final FlowChangeAction configurationAction = createFlowChangeAction();
                configurationAction.setOperation(operation);
                configurationAction.setTimestamp(actionTimestamp);
                configurationAction.setSourceId(parameterContext.getIdentifier());
                configurationAction.setSourceName(parameterContext.getName());
                configurationAction.setSourceType(Component.ParameterContext);
                configurationAction.setActionDetails(actionDetails);
                actions.add(configurationAction);
            }
        }
    }

    /**
     * Audits the removal of a parameter context via deleteParameterContext().
     *
     * @param proceedingJoinPoint join point
     * @param parameterContextId parameterContextId
     * @param parameterContextDAO dao
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.ParameterContextDAO+) && "
            + "execution(void deleteParameterContext(java.lang.String)) && "
            + "args(parameterContextId) && "
            + "target(parameterContextDAO)")
    public void removeParameterContextAdvice(ProceedingJoinPoint proceedingJoinPoint, String parameterContextId, ParameterContextDAO parameterContextDAO) throws Throwable {
        // get the parameter context before removing it
        ParameterContext parameterContext = parameterContextDAO.getParameterContext(parameterContextId);

        // remove the processor
        proceedingJoinPoint.proceed();

        // if no exceptions were thrown, add removal actions...
        // audit the processor removal
        final Action action = generateAuditRecord(parameterContext, Operation.Remove);

        // save the actions
        if (action != null) {
            saveAction(action, logger);
        }
    }

    /**
     * Generates an audit record for the creation of a ParameterContext.
     *
     * @param parameterContext parameterContext
     * @param operation operation
     * @return action
     */
    private Action generateAuditRecord(ParameterContext parameterContext, Operation operation) {
        FlowChangeAction action = null;

        if (isAuditable()) {
            // create the processor action for adding this processor
            action = createFlowChangeAction();
            action.setOperation(operation);
            action.setSourceId(parameterContext.getIdentifier());
            action.setSourceName(parameterContext.getName());
            action.setSourceType(Component.ParameterContext);
        }

        return action;
    }

    /**
     * Extracts the values for the configured fields from the specified ParameterContext.
     */
    private Map<String, String> extractConfiguredParameterContextValues(final ParameterContext parameterContext, final ParameterContextDTO parameterContextDTO) {
        final Map<String, String> values = new HashMap<>();

        if (parameterContextDTO.getDescription() != null) {
            values.put("Name", parameterContext.getName());
        }
        if (parameterContextDTO.getDescription() != null) {
            values.put("Description", parameterContext.getDescription());
        }
        if (parameterContextDTO.getParameters() != null) {
            parameterContextDTO.getParameters().forEach(parameterEntity -> {
                final ParameterDTO parameterDTO = parameterEntity.getParameter();
                final Parameter parameter = parameterContext.getParameter(parameterDTO.getName()).orElse(null);
                if (parameter == null) {
                    values.put(parameterDTO.getName(), null);
                } else {
                    values.put(parameterDTO.getName(), parameter.getValue());
                }
            });
        }
        if (!parameterContext.getInheritedParameterContexts().isEmpty()) {
            values.put("Inherited Parameter Contexts", parameterContext.getInheritedParameterContexts()
                    .stream().map(pc -> pc.getIdentifier()).collect(Collectors.joining(", ")));
        }
        if (parameterContext.getParameterProvider() != null) {
            values.put("Sensitive Parameter Provider", parameterContext.getParameterProvider().getIdentifier());
        }

        return values;
    }

}
