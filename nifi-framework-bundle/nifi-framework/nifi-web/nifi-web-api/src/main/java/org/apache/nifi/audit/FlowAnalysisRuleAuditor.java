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
import org.apache.nifi.action.component.details.FlowChangeExtensionDetails;
import org.apache.nifi.action.details.ActionDetails;
import org.apache.nifi.action.details.FlowChangeConfigureDetails;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.FlowAnalysisRuleNode;
import org.apache.nifi.flowanalysis.EnforcementPolicy;
import org.apache.nifi.flowanalysis.FlowAnalysisRuleState;
import org.apache.nifi.web.api.dto.FlowAnalysisRuleDTO;
import org.apache.nifi.web.dao.FlowAnalysisRuleDAO;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Audits flow analysis rule creation/removal and configuration changes.
 */
@Service
@Aspect
public class FlowAnalysisRuleAuditor extends NiFiAuditor {

    private static final Logger logger = LoggerFactory.getLogger(FlowAnalysisRuleAuditor.class);

    private static final String COMMENTS = "Comments";
    private static final String NAME = "Name";
    private static final String ANNOTATION_DATA = "Annotation Data";
    private static final String EXTENSION_VERSION = "Extension Version";
    private static final String ENFORCEMENT_POLICY = "Enforcement Policy";

    /**
     * Audits the creation of flow analysis rule via createFlowAnalysisRule().
     *
     * This method only needs to be run 'after returning'. However, in Java 7 the order in which these methods are returned from Class.getDeclaredMethods (even though there is no order guaranteed)
     * seems to differ from Java 6. SpringAOP depends on this ordering to determine advice precedence. By normalizing all advice into Around advice we can alleviate this issue.
     *
     * @param proceedingJoinPoint joinpoint
     * @return node
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.FlowAnalysisRuleDAO+) && "
            + "execution(org.apache.nifi.controller.FlowAnalysisRuleNode createFlowAnalysisRule(org.apache.nifi.web.api.dto.FlowAnalysisRuleDTO))")
    public FlowAnalysisRuleNode createFlowAnalysisRuleAdvice(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        // update the flow analysis rule state
        FlowAnalysisRuleNode flowAnalysisRule = (FlowAnalysisRuleNode) proceedingJoinPoint.proceed();

        // if no exceptions were thrown, add the flow analysis rule action...
        final Action action = generateAuditRecord(flowAnalysisRule, Operation.Add);

        // save the actions
        if (action != null) {
            saveAction(action, logger);
        }

        return flowAnalysisRule;
    }

    /**
     * Audits the configuration of a flow analysis rule.
     *
     * @param proceedingJoinPoint joinpoint
     * @param flowAnalysisRuleDTO dto
     * @param flowAnalysisRuleDAO dao
     * @return object
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.FlowAnalysisRuleDAO+) && "
            + "execution(org.apache.nifi.controller.FlowAnalysisRuleNode updateFlowAnalysisRule(org.apache.nifi.web.api.dto.FlowAnalysisRuleDTO)) && "
            + "args(flowAnalysisRuleDTO) && "
            + "target(flowAnalysisRuleDAO)")
    public Object updateFlowAnalysisRuleAdvice(ProceedingJoinPoint proceedingJoinPoint, FlowAnalysisRuleDTO flowAnalysisRuleDTO, FlowAnalysisRuleDAO flowAnalysisRuleDAO) throws Throwable {
        // determine the initial values for each property/setting thats changing
        FlowAnalysisRuleNode flowAnalysisRule = flowAnalysisRuleDAO.getFlowAnalysisRule(flowAnalysisRuleDTO.getId());
        final Map<String, String> values = extractConfiguredPropertyValues(flowAnalysisRule, flowAnalysisRuleDTO);
        final FlowAnalysisRuleState state = flowAnalysisRule.getState();
        final EnforcementPolicy enforcementPolicy = flowAnalysisRule.getEnforcementPolicy();

        // update the flow analysis rule state
        final FlowAnalysisRuleNode updatedFlowAnalysisRule = (FlowAnalysisRuleNode) proceedingJoinPoint.proceed();

        // if no exceptions were thrown, add the flow analysis rule action...
        flowAnalysisRule = flowAnalysisRuleDAO.getFlowAnalysisRule(updatedFlowAnalysisRule.getIdentifier());

        // get the current user
        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        // ensure the user was found
        if (user != null) {
            final Set<String> sensitiveDynamicPropertyNames = flowAnalysisRuleDTO.getSensitiveDynamicPropertyNames() == null
                    ? Collections.emptySet() : flowAnalysisRuleDTO.getSensitiveDynamicPropertyNames();

            // determine the updated values
            Map<String, String> updatedValues = extractConfiguredPropertyValues(flowAnalysisRule, flowAnalysisRuleDTO);

            // create the flow analysis rule details
            FlowChangeExtensionDetails ruleDetails = new FlowChangeExtensionDetails();
            ruleDetails.setType(flowAnalysisRule.getComponentType());

            // create a flow analysis rule action
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
                    // clear the value if this property is sensitive
                    final PropertyDescriptor propertyDescriptor = flowAnalysisRule.getPropertyDescriptor(property);
                    // Evaluate both Property Descriptor status and whether the client requested a new Sensitive Dynamic Property
                    if (propertyDescriptor != null && (propertyDescriptor.isSensitive() || sensitiveDynamicPropertyNames.contains(property))) {
                        if (newValue != null) {
                            newValue = SENSITIVE_VALUE_PLACEHOLDER;
                        }
                        if (oldValue != null) {
                            oldValue = SENSITIVE_VALUE_PLACEHOLDER;
                        }
                    } else if (ANNOTATION_DATA.equals(property)) {
                        if (newValue != null) {
                            newValue = "<annotation data not shown>";
                        }
                        if (oldValue != null) {
                            oldValue = "<annotation data not shown>";
                        }
                    }

                    final FlowChangeConfigureDetails actionDetails = new FlowChangeConfigureDetails();
                    actionDetails.setName(property);
                    actionDetails.setValue(newValue);
                    actionDetails.setPreviousValue(oldValue);

                    // create a configuration action
                    FlowChangeAction configurationAction = new FlowChangeAction();
                    configurationAction.setUserIdentity(user.getIdentity());
                    configurationAction.setOperation(operation);
                    configurationAction.setTimestamp(actionTimestamp);
                    configurationAction.setSourceId(flowAnalysisRule.getIdentifier());
                    configurationAction.setSourceName(flowAnalysisRule.getName());
                    configurationAction.setSourceType(Component.FlowAnalysisRule);
                    configurationAction.setComponentDetails(ruleDetails);
                    configurationAction.setActionDetails(actionDetails);
                    actions.add(configurationAction);
                }
            }

            final EnforcementPolicy updatedEnforcementPolicy = flowAnalysisRule.getEnforcementPolicy();
            if (enforcementPolicy != updatedEnforcementPolicy) {
                final FlowChangeConfigureDetails actionDetails = new FlowChangeConfigureDetails();
                actionDetails.setName(ENFORCEMENT_POLICY);
                actionDetails.setValue(String.valueOf(updatedEnforcementPolicy));
                actionDetails.setPreviousValue(String.valueOf(enforcementPolicy));

                final FlowChangeAction configurationAction = new FlowChangeAction();
                configurationAction.setUserIdentity(user.getIdentity());
                configurationAction.setOperation(Operation.Configure);
                configurationAction.setTimestamp(actionTimestamp);
                configurationAction.setSourceId(flowAnalysisRule.getIdentifier());
                configurationAction.setSourceName(flowAnalysisRule.getName());
                configurationAction.setSourceType(Component.FlowAnalysisRule);
                configurationAction.setComponentDetails(ruleDetails);
                configurationAction.setActionDetails(actionDetails);
                actions.add(configurationAction);
            }

            // determine the new state
            final FlowAnalysisRuleState updatedState = flowAnalysisRule.getState();

            // determine if the running state has changed and its not disabled
            if (state != updatedState) {
                // create a flow analysis rule action
                FlowChangeAction ruleAction = new FlowChangeAction();
                ruleAction.setUserIdentity(user.getIdentity());
                ruleAction.setTimestamp(new Date());
                ruleAction.setSourceId(flowAnalysisRule.getIdentifier());
                ruleAction.setSourceName(flowAnalysisRule.getName());
                ruleAction.setSourceType(Component.FlowAnalysisRule);
                ruleAction.setComponentDetails(ruleDetails);

                // set the operation accordingly
                if (FlowAnalysisRuleState.ENABLED.equals(updatedState)) {
                    ruleAction.setOperation(Operation.Enable);
                } else if (FlowAnalysisRuleState.DISABLED.equals(updatedState)) {
                    ruleAction.setOperation(Operation.Disable);
                }
                actions.add(ruleAction);
            }

            // ensure there are actions to record
            if (!actions.isEmpty()) {
                // save the actions
                saveActions(actions, logger);
            }
        }

        return updatedFlowAnalysisRule;
    }

    /**
     * Audits the removal of a flow analysis rule via deleteFlowAnalysisRule().
     *
     * @param proceedingJoinPoint join point
     * @param flowAnalysisRuleId rule id
     * @param flowAnalysisRuleDAO rule dao
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.FlowAnalysisRuleDAO+) && "
            + "execution(void deleteFlowAnalysisRule(java.lang.String)) && "
            + "args(flowAnalysisRuleId) && "
            + "target(flowAnalysisRuleDAO)")
    public void removeFlowAnalysisRuleAdvice(ProceedingJoinPoint proceedingJoinPoint, String flowAnalysisRuleId, FlowAnalysisRuleDAO flowAnalysisRuleDAO) throws Throwable {
        // get the flow analysis rule before removing it
        FlowAnalysisRuleNode flowAnalysisRule = flowAnalysisRuleDAO.getFlowAnalysisRule(flowAnalysisRuleId);

        // remove the flow analysis rule
        proceedingJoinPoint.proceed();

        // if no exceptions were thrown, add removal actions...
        // audit the flow analysis rule removal
        final Action action = generateAuditRecord(flowAnalysisRule, Operation.Remove);

        // save the actions
        if (action != null) {
            saveAction(action, logger);
        }
    }

    /**
     * Generates an audit record for the creation of a flow analysis rule.
     *
     * @param flowAnalysisRule rule
     * @param operation operation
     * @return action
     */
    public Action generateAuditRecord(FlowAnalysisRuleNode flowAnalysisRule, Operation operation) {
        return generateAuditRecord(flowAnalysisRule, operation, null);
    }

    /**
     * Generates an audit record for the creation of a flow analysis rule.
     *
     * @param flowAnalysisRule rule
     * @param operation operation
     * @param actionDetails details
     * @return action
     */
    public Action generateAuditRecord(FlowAnalysisRuleNode flowAnalysisRule, Operation operation, ActionDetails actionDetails) {
        FlowChangeAction action = null;

        // get the current user
        NiFiUser user = NiFiUserUtils.getNiFiUser();

        // ensure the user was found
        if (user != null) {
            // create the flow analysis rule details
            FlowChangeExtensionDetails ruleDetails = new FlowChangeExtensionDetails();
            ruleDetails.setType(flowAnalysisRule.getComponentType());

            // create the flow analysis rule action for adding this flow analysis rule
            action = new FlowChangeAction();
            action.setUserIdentity(user.getIdentity());
            action.setOperation(operation);
            action.setTimestamp(new Date());
            action.setSourceId(flowAnalysisRule.getIdentifier());
            action.setSourceName(flowAnalysisRule.getName());
            action.setSourceType(Component.FlowAnalysisRule);
            action.setComponentDetails(ruleDetails);

            if (actionDetails != null) {
                action.setActionDetails(actionDetails);
            }
        }

        return action;
    }

    /**
     * Extracts the values for the configured properties from the specified FlowAnalysisRule.
     *
     * @param flowAnalysisRule rule
     * @param flowAnalysisRuleDTO dto
     * @return properties of rule
     */
    private Map<String, String> extractConfiguredPropertyValues(FlowAnalysisRuleNode flowAnalysisRule, FlowAnalysisRuleDTO flowAnalysisRuleDTO) {
        Map<String, String> values = new HashMap<>();

        if (flowAnalysisRuleDTO.getName() != null) {
            values.put(NAME, flowAnalysisRule.getName());
        }
        if (flowAnalysisRuleDTO.getBundle() != null) {
            final BundleCoordinate bundle = flowAnalysisRule.getBundleCoordinate();
            values.put(EXTENSION_VERSION, formatExtensionVersion(flowAnalysisRule.getComponentType(), bundle));
        }
        if (flowAnalysisRuleDTO.getProperties() != null) {
            // for each property specified, extract its configured value
            Map<String, String> properties = flowAnalysisRuleDTO.getProperties();
            Map<PropertyDescriptor, String> configuredProperties = flowAnalysisRule.getRawPropertyValues();
            for (String propertyName : properties.keySet()) {
                // build a descriptor for getting the configured value
                PropertyDescriptor propertyDescriptor = new PropertyDescriptor.Builder().name(propertyName).build();
                String configuredPropertyValue = configuredProperties.get(propertyDescriptor);

                // if the configured value couldn't be found, use the default value from the actual descriptor
                if (configuredPropertyValue == null) {
                    propertyDescriptor = locatePropertyDescriptor(configuredProperties.keySet(), propertyDescriptor);
                    configuredPropertyValue = propertyDescriptor.getDefaultValue();
                }
                values.put(propertyName, configuredPropertyValue);
            }
        }
        if (flowAnalysisRuleDTO.getComments() != null) {
            values.put(COMMENTS, flowAnalysisRuleDTO.getComments());
        }

        return values;
    }

    /**
     * Locates the actual property descriptor for the given spec property descriptor.
     *
     * @param propertyDescriptors properties
     * @param specDescriptor example property
     * @return property
     */
    private PropertyDescriptor locatePropertyDescriptor(Set<PropertyDescriptor> propertyDescriptors, PropertyDescriptor specDescriptor) {
        for (PropertyDescriptor propertyDescriptor : propertyDescriptors) {
            if (propertyDescriptor.equals(specDescriptor)) {
                return propertyDescriptor;
            }
        }
        return specDescriptor;
    }

}
