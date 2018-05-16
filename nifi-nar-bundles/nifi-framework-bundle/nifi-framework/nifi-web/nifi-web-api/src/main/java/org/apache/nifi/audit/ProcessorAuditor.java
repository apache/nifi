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
import org.apache.nifi.action.component.details.FlowChangeExtensionDetails;
import org.apache.nifi.action.details.ActionDetails;
import org.apache.nifi.action.details.FlowChangeConfigureDetails;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.dao.ProcessorDAO;
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
import java.util.Set;

/**
 * Audits processor creation/removal and configuration changes.
 */
@Aspect
public class ProcessorAuditor extends NiFiAuditor {

    private static final Logger logger = LoggerFactory.getLogger(ProcessorAuditor.class);

    private static final String COMMENTS = "Comments";
    private static final String PENALTY_DURATION = "Penalty Duration";
    private static final String YIELD_DURATION = "Yield Duration";
    private static final String CONCURRENTLY_SCHEDULABLE_TASKS = "Concurrent Tasks";
    private static final String NAME = "Name";
    private static final String BULLETIN_LEVEL = "Bulletin Level";
    private static final String ANNOTATION_DATA = "Annotation Data";
    private static final String AUTO_TERMINATED_RELATIONSHIPS = "Auto Terminated Relationships";
    private static final String SCHEDULING_PERIOD = "Run Schedule";
    private static final String SCHEDULING_STRATEGY = "Scheduling Strategy";
    private static final String EXECUTION_NODE = "Execution Node";
    private static final String EXTENSION_VERSION = "Extension Version";

    /**
     * Audits the creation of processors via createProcessor().
     *
     * This method only needs to be run 'after returning'. However, in Java 7 the order in which these methods are returned from Class.getDeclaredMethods (even though there is no order guaranteed)
     * seems to differ from Java 6. SpringAOP depends on this ordering to determine advice precedence. By normalizing all advice into Around advice we can alleviate this issue.
     *
     * @param proceedingJoinPoint join point
     * @return node
     * @throws java.lang.Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.ProcessorDAO+) && "
            + "execution(org.apache.nifi.controller.ProcessorNode createProcessor(java.lang.String, org.apache.nifi.web.api.dto.ProcessorDTO))")
    public ProcessorNode createProcessorAdvice(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        // update the processor state
        ProcessorNode processor = (ProcessorNode) proceedingJoinPoint.proceed();

        // if no exceptions were thrown, add the processor action...
        final Action action = generateAuditRecord(processor, Operation.Add);

        // save the actions
        if (action != null) {
            saveAction(action, logger);
        }

        return processor;
    }

    /**
     * Audits the configuration of a single processor.
     *
     * @param proceedingJoinPoint join point
     * @param processorDTO dto
     * @param processorDAO dao
     * @return node
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.ProcessorDAO+) && "
            + "execution(org.apache.nifi.controller.ProcessorNode updateProcessor(org.apache.nifi.web.api.dto.ProcessorDTO)) && "
            + "args(processorDTO) && "
            + "target(processorDAO)")
    public ProcessorNode updateProcessorAdvice(ProceedingJoinPoint proceedingJoinPoint, ProcessorDTO processorDTO, ProcessorDAO processorDAO) throws Throwable {
        // determine the initial values for each property/setting that's changing
        ProcessorNode processor = processorDAO.getProcessor(processorDTO.getId());
        final Map<String, String> values = extractConfiguredPropertyValues(processor, processorDTO);
        final ScheduledState scheduledState = processor.getScheduledState();

        // update the processor state
        final ProcessorNode updatedProcessor = (ProcessorNode) proceedingJoinPoint.proceed();

        // if no exceptions were thrown, add the processor action...
        processor = processorDAO.getProcessor(updatedProcessor.getIdentifier());

        // get the current user
        NiFiUser user = NiFiUserUtils.getNiFiUser();

        // ensure the user was found
        if (user != null) {
            // determine the updated values
            Map<String, String> updatedValues = extractConfiguredPropertyValues(processor, processorDTO);

            // create the processor details
            FlowChangeExtensionDetails processorDetails = new FlowChangeExtensionDetails();
            processorDetails.setType(processor.getComponentType());

            // create a processor action
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
                    final PropertyDescriptor propertyDescriptor = processor.getProcessor().getPropertyDescriptor(property);
                    if (propertyDescriptor != null && propertyDescriptor.isSensitive()) {
                        if (newValue != null) {
                            newValue = "********";
                        }
                        if (oldValue != null) {
                            oldValue = "********";
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
                    configurationAction.setSourceId(processor.getIdentifier());
                    configurationAction.setSourceName(processor.getName());
                    configurationAction.setSourceType(Component.Processor);
                    configurationAction.setComponentDetails(processorDetails);
                    configurationAction.setActionDetails(actionDetails);
                    actions.add(configurationAction);
                }
            }

            // determine the new executing state
            final ScheduledState updatedScheduledState = processor.getScheduledState();

            // determine if the running state has changed and its not disabled
            if (scheduledState != updatedScheduledState) {
                // create a processor action
                FlowChangeAction processorAction = new FlowChangeAction();
                processorAction.setUserIdentity(user.getIdentity());
                processorAction.setTimestamp(new Date());
                processorAction.setSourceId(processor.getIdentifier());
                processorAction.setSourceName(processor.getName());
                processorAction.setSourceType(Component.Processor);
                processorAction.setComponentDetails(processorDetails);

                // set the operation accordingly
                if (ScheduledState.RUNNING.equals(updatedScheduledState)) {
                    processorAction.setOperation(Operation.Start);
                } else if (ScheduledState.DISABLED.equals(updatedScheduledState)) {
                    processorAction.setOperation(Operation.Disable);
                } else {
                    // state is now stopped... consider the previous state
                    if (ScheduledState.RUNNING.equals(scheduledState)) {
                        processorAction.setOperation(Operation.Stop);
                    } else if (ScheduledState.DISABLED.equals(scheduledState)) {
                        processorAction.setOperation(Operation.Enable);
                    }
                }
                actions.add(processorAction);
            }

            // ensure there are actions to record
            if (!actions.isEmpty()) {
                // save the actions
                saveActions(actions, logger);
            }
        }

        return updatedProcessor;
    }

    /**
     * Audits the removal of a processor via deleteProcessor().
     *
     * @param proceedingJoinPoint join point
     * @param processorId processor id
     * @param processorDAO dao
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.ProcessorDAO+) && "
            + "execution(void deleteProcessor(java.lang.String)) && "
            + "args(processorId) && "
            + "target(processorDAO)")
    public void removeProcessorAdvice(ProceedingJoinPoint proceedingJoinPoint, String processorId, ProcessorDAO processorDAO) throws Throwable {
        // get the processor before removing it
        ProcessorNode processor = processorDAO.getProcessor(processorId);

        // remove the processor
        proceedingJoinPoint.proceed();

        // if no exceptions were thrown, add removal actions...
        // audit the processor removal
        final Action action = generateAuditRecord(processor, Operation.Remove);

        // save the actions
        if (action != null) {
            saveAction(action, logger);
        }
    }

    /**
     * Generates an audit record for the creation of a processor.
     *
     * @param processor processor
     * @param operation operation
     * @return action
     */
    public Action generateAuditRecord(ProcessorNode processor, Operation operation) {
        return generateAuditRecord(processor, operation, null);
    }

    /**
     * Generates an audit record for the creation of a processor.
     *
     * @param processor processor
     * @param operation operation
     * @param actionDetails details
     * @return action
     */
    public Action generateAuditRecord(ProcessorNode processor, Operation operation, ActionDetails actionDetails) {
        FlowChangeAction action = null;

        // get the current user
        NiFiUser user = NiFiUserUtils.getNiFiUser();

        // ensure the user was found
        if (user != null) {
            // create the processor details
            FlowChangeExtensionDetails processorDetails = new FlowChangeExtensionDetails();
            processorDetails.setType(processor.getComponentType());

            // create the processor action for adding this processor
            action = new FlowChangeAction();
            action.setUserIdentity(user.getIdentity());
            action.setOperation(operation);
            action.setTimestamp(new Date());
            action.setSourceId(processor.getIdentifier());
            action.setSourceName(processor.getName());
            action.setSourceType(Component.Processor);
            action.setComponentDetails(processorDetails);

            if (actionDetails != null) {
                action.setActionDetails(actionDetails);
            }
        }

        return action;
    }

    /**
     * Extracts the values for the configured properties from the specified Processor.
     */
    private Map<String, String> extractConfiguredPropertyValues(ProcessorNode processor, ProcessorDTO processorDTO) {
        Map<String, String> values = new HashMap<>();

        if (processorDTO.getName() != null) {
            values.put(NAME, processor.getName());
        }
        if (processorDTO.getBundle() != null) {
            final BundleCoordinate bundle = processor.getBundleCoordinate();
            values.put(EXTENSION_VERSION, formatExtensionVersion(processor.getComponentType(), bundle));
        }
        if (processorDTO.getConfig() != null) {
            ProcessorConfigDTO newConfig = processorDTO.getConfig();
            if (newConfig.getConcurrentlySchedulableTaskCount() != null) {
                values.put(CONCURRENTLY_SCHEDULABLE_TASKS, String.valueOf(processor.getMaxConcurrentTasks()));
            }
            if (newConfig.getPenaltyDuration() != null) {
                values.put(PENALTY_DURATION, processor.getPenalizationPeriod());
            }
            if (newConfig.getYieldDuration() != null) {
                values.put(YIELD_DURATION, processor.getYieldPeriod());
            }
            if (newConfig.getBulletinLevel() != null) {
                values.put(BULLETIN_LEVEL, processor.getBulletinLevel().name());
            }
            if (newConfig.getAnnotationData() != null) {
                values.put(ANNOTATION_DATA, processor.getAnnotationData());
            }
            if (newConfig.getSchedulingPeriod() != null) {
                values.put(SCHEDULING_PERIOD, String.valueOf(processor.getSchedulingPeriod()));
            }
            if (newConfig.getAutoTerminatedRelationships() != null) {
                // get each of the auto terminated relationship names
                final Set<Relationship> autoTerminatedRelationships = processor.getAutoTerminatedRelationships();
                final List<String> autoTerminatedRelationshipNames = new ArrayList<>(autoTerminatedRelationships.size());
                for (final Relationship relationship : autoTerminatedRelationships) {
                    autoTerminatedRelationshipNames.add(relationship.getName());
                }

                // sort them and include in the configuration
                Collections.sort(autoTerminatedRelationshipNames, Collator.getInstance(Locale.US));
                values.put(AUTO_TERMINATED_RELATIONSHIPS, StringUtils.join(autoTerminatedRelationshipNames, ", "));
            }
            if (newConfig.getProperties() != null) {
                // for each property specified, extract its configured value
                Map<String, String> properties = newConfig.getProperties();
                Map<PropertyDescriptor, String> configuredProperties = processor.getProperties();
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
            if (newConfig.getComments() != null) {
                values.put(COMMENTS, processor.getComments());
            }
            if (newConfig.getSchedulingStrategy() != null) {
                values.put(SCHEDULING_STRATEGY, processor.getSchedulingStrategy().name());
            }
            if (newConfig.getExecutionNode() != null) {
                values.put(EXECUTION_NODE, processor.getExecutionNode().name());
            }
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
