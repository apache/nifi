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
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.web.api.dto.ReportingTaskDTO;
import org.apache.nifi.web.dao.ReportingTaskDAO;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Audits reporting creation/removal and configuration changes.
 */
@Aspect
public class ReportingTaskAuditor extends NiFiAuditor {

    private static final Logger logger = LoggerFactory.getLogger(ReportingTaskAuditor.class);

    private static final String NAME = "Name";
    private static final String ANNOTATION_DATA = "Annotation Data";
    private static final String EXTENSION_VERSION = "Extension Version";

    /**
     * Audits the creation of reporting task via createReportingTask().
     *
     * This method only needs to be run 'after returning'. However, in Java 7 the order in which these methods are returned from Class.getDeclaredMethods (even though there is no order guaranteed)
     * seems to differ from Java 6. SpringAOP depends on this ordering to determine advice precedence. By normalizing all advice into Around advice we can alleviate this issue.
     *
     * @param proceedingJoinPoint joinpoint
     * @return node
     * @throws java.lang.Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.ReportingTaskDAO+) && "
            + "execution(org.apache.nifi.controller.ReportingTaskNode createReportingTask(org.apache.nifi.web.api.dto.ReportingTaskDTO))")
    public ReportingTaskNode createReportingTaskAdvice(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        // update the reporting task state
        ReportingTaskNode reportingTask = (ReportingTaskNode) proceedingJoinPoint.proceed();

        // if no exceptions were thrown, add the reporting task action...
        final Action action = generateAuditRecord(reportingTask, Operation.Add);

        // save the actions
        if (action != null) {
            saveAction(action, logger);
        }

        return reportingTask;
    }

    /**
     * Audits the configuration of a reporting task.
     *
     * @param proceedingJoinPoint joinpoint
     * @param reportingTaskDTO dto
     * @param reportingTaskDAO dao
     * @return object
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.ReportingTaskDAO+) && "
            + "execution(org.apache.nifi.controller.ReportingTaskNode updateReportingTask(org.apache.nifi.web.api.dto.ReportingTaskDTO)) && "
            + "args(reportingTaskDTO) && "
            + "target(reportingTaskDAO)")
    public Object updateReportingTaskAdvice(ProceedingJoinPoint proceedingJoinPoint, ReportingTaskDTO reportingTaskDTO, ReportingTaskDAO reportingTaskDAO) throws Throwable {
        // determine the initial values for each property/setting thats changing
        ReportingTaskNode reportingTask = reportingTaskDAO.getReportingTask(reportingTaskDTO.getId());
        final Map<String, String> values = extractConfiguredPropertyValues(reportingTask, reportingTaskDTO);
        final ScheduledState scheduledState = reportingTask.getScheduledState();

        // update the reporting task state
        final ReportingTaskNode updatedReportingTask = (ReportingTaskNode) proceedingJoinPoint.proceed();

        // if no exceptions were thrown, add the reporting task action...
        reportingTask = reportingTaskDAO.getReportingTask(updatedReportingTask.getIdentifier());

        // get the current user
        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        // ensure the user was found
        if (user != null) {
            // determine the updated values
            Map<String, String> updatedValues = extractConfiguredPropertyValues(reportingTask, reportingTaskDTO);

            // create the reporting task details
            FlowChangeExtensionDetails taskDetails = new FlowChangeExtensionDetails();
            taskDetails.setType(reportingTask.getComponentType());

            // create a reporting task action
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
                    final PropertyDescriptor propertyDescriptor = reportingTask.getReportingTask().getPropertyDescriptor(property);
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
                    configurationAction.setSourceId(reportingTask.getIdentifier());
                    configurationAction.setSourceName(reportingTask.getName());
                    configurationAction.setSourceType(Component.ReportingTask);
                    configurationAction.setComponentDetails(taskDetails);
                    configurationAction.setActionDetails(actionDetails);
                    actions.add(configurationAction);
                }
            }

            // determine the new executing state
            final ScheduledState updatedScheduledState = reportingTask.getScheduledState();

            // determine if the running state has changed and its not disabled
            if (scheduledState != updatedScheduledState) {
                // create a reporting task action
                FlowChangeAction taskAction = new FlowChangeAction();
                taskAction.setUserIdentity(user.getIdentity());
                taskAction.setTimestamp(new Date());
                taskAction.setSourceId(reportingTask.getIdentifier());
                taskAction.setSourceName(reportingTask.getName());
                taskAction.setSourceType(Component.ReportingTask);
                taskAction.setComponentDetails(taskDetails);

                // set the operation accordingly
                if (ScheduledState.RUNNING.equals(updatedScheduledState)) {
                    taskAction.setOperation(Operation.Start);
                } else if (ScheduledState.DISABLED.equals(updatedScheduledState)) {
                    taskAction.setOperation(Operation.Disable);
                } else {
                    // state is now stopped... consider the previous state
                    if (ScheduledState.RUNNING.equals(scheduledState)) {
                        taskAction.setOperation(Operation.Stop);
                    } else if (ScheduledState.DISABLED.equals(scheduledState)) {
                        taskAction.setOperation(Operation.Enable);
                    }
                }
                actions.add(taskAction);
            }

            // ensure there are actions to record
            if (!actions.isEmpty()) {
                // save the actions
                saveActions(actions, logger);
            }
        }

        return updatedReportingTask;
    }

    /**
     * Audits the removal of a reporting task via deleteReportingTask().
     *
     * @param proceedingJoinPoint join point
     * @param reportingTaskId task id
     * @param reportingTaskDAO task dao
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.ReportingTaskDAO+) && "
            + "execution(void deleteReportingTask(java.lang.String)) && "
            + "args(reportingTaskId) && "
            + "target(reportingTaskDAO)")
    public void removeReportingTaskAdvice(ProceedingJoinPoint proceedingJoinPoint, String reportingTaskId, ReportingTaskDAO reportingTaskDAO) throws Throwable {
        // get the reporting task before removing it
        ReportingTaskNode reportingTask = reportingTaskDAO.getReportingTask(reportingTaskId);

        // remove the reporting task
        proceedingJoinPoint.proceed();

        // if no exceptions were thrown, add removal actions...
        // audit the reporting task removal
        final Action action = generateAuditRecord(reportingTask, Operation.Remove);

        // save the actions
        if (action != null) {
            saveAction(action, logger);
        }
    }

    /**
     * Generates an audit record for the creation of a reporting task.
     *
     * @param reportingTask task
     * @param operation operation
     * @return action
     */
    public Action generateAuditRecord(ReportingTaskNode reportingTask, Operation operation) {
        return generateAuditRecord(reportingTask, operation, null);
    }

    /**
     * Generates an audit record for the creation of a reporting task.
     *
     * @param reportingTask task
     * @param operation operation
     * @param actionDetails details
     * @return action
     */
    public Action generateAuditRecord(ReportingTaskNode reportingTask, Operation operation, ActionDetails actionDetails) {
        FlowChangeAction action = null;

        // get the current user
        NiFiUser user = NiFiUserUtils.getNiFiUser();

        // ensure the user was found
        if (user != null) {
            // create the reporting task details
            FlowChangeExtensionDetails taskDetails = new FlowChangeExtensionDetails();
            taskDetails.setType(reportingTask.getComponentType());

            // create the reporting task action for adding this reporting task
            action = new FlowChangeAction();
            action.setUserIdentity(user.getIdentity());
            action.setOperation(operation);
            action.setTimestamp(new Date());
            action.setSourceId(reportingTask.getIdentifier());
            action.setSourceName(reportingTask.getName());
            action.setSourceType(Component.ReportingTask);
            action.setComponentDetails(taskDetails);

            if (actionDetails != null) {
                action.setActionDetails(actionDetails);
            }
        }

        return action;
    }

    /**
     * Extracts the values for the configured properties from the specified ReportingTask.
     *
     * @param reportingTask task
     * @param reportingTaskDTO dto
     * @return properties of task
     */
    private Map<String, String> extractConfiguredPropertyValues(ReportingTaskNode reportingTask, ReportingTaskDTO reportingTaskDTO) {
        Map<String, String> values = new HashMap<>();

        if (reportingTaskDTO.getName() != null) {
            values.put(NAME, reportingTask.getName());
        }
        if (reportingTaskDTO.getAnnotationData() != null) {
            values.put(ANNOTATION_DATA, reportingTask.getAnnotationData());
        }
        if (reportingTaskDTO.getBundle() != null) {
            final BundleCoordinate bundle = reportingTask.getBundleCoordinate();
            values.put(EXTENSION_VERSION, formatExtensionVersion(reportingTask.getComponentType(), bundle));
        }
        if (reportingTaskDTO.getProperties() != null) {
            // for each property specified, extract its configured value
            Map<String, String> properties = reportingTaskDTO.getProperties();
            Map<PropertyDescriptor, String> configuredProperties = reportingTask.getProperties();
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
