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
import org.apache.nifi.controller.ConfiguredComponent;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceReference;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.reporting.ReportingTask;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.dao.ControllerServiceDAO;
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
 * Audits controller service creation/removal and configuration changes.
 */
@Aspect
public class ControllerServiceAuditor extends NiFiAuditor {

    private static final Logger logger = LoggerFactory.getLogger(ControllerServiceAuditor.class);

    private static final String COMMENTS = "Comments";
    private static final String NAME = "Name";
    private static final String ANNOTATION_DATA = "Annotation Data";
    private static final String EXTENSION_VERSION = "Extension Version";

    /**
     * Audits the creation of controller service via createControllerService().
     *
     * This method only needs to be run 'after returning'. However, in Java 7 the order in which these methods are returned from Class.getDeclaredMethods (even though there is no order guaranteed)
     * seems to differ from Java 6. SpringAOP depends on this ordering to determine advice precedence. By normalizing all advice into Around advice we can alleviate this issue.
     *
     * @param proceedingJoinPoint join point
     * @return node
     * @throws java.lang.Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.ControllerServiceDAO+) && "
            + "execution(org.apache.nifi.controller.service.ControllerServiceNode createControllerService(org.apache.nifi.web.api.dto.ControllerServiceDTO))")
    public ControllerServiceNode createControllerServiceAdvice(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        // update the controller service state
        ControllerServiceNode controllerService = (ControllerServiceNode) proceedingJoinPoint.proceed();

        // if no exceptions were thrown, add the controller service action...
        final Action action = generateAuditRecord(controllerService, Operation.Add);

        // save the actions
        if (action != null) {
            saveAction(action, logger);
        }

        return controllerService;
    }

    /**
     * Audits the configuration of a single controller service.
     *
     * @param proceedingJoinPoint join point
     * @param controllerServiceDTO dto
     * @param controllerServiceDAO dao
     * @return object
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.ControllerServiceDAO+) && "
            + "execution(org.apache.nifi.controller.service.ControllerServiceNode updateControllerService(org.apache.nifi.web.api.dto.ControllerServiceDTO)) && "
            + "args(controllerServiceDTO) && "
            + "target(controllerServiceDAO)")
    public Object updateControllerServiceAdvice(ProceedingJoinPoint proceedingJoinPoint, ControllerServiceDTO controllerServiceDTO, ControllerServiceDAO controllerServiceDAO) throws Throwable {
        // determine the initial values for each property/setting that's changing
        ControllerServiceNode controllerService = controllerServiceDAO.getControllerService(controllerServiceDTO.getId());
        final Map<String, String> values = extractConfiguredPropertyValues(controllerService, controllerServiceDTO);
        final boolean isDisabled = isDisabled(controllerService);

        // update the controller service state
        final ControllerServiceNode updatedControllerService = (ControllerServiceNode) proceedingJoinPoint.proceed();

        // if no exceptions were thrown, add the controller service action...
        controllerService = controllerServiceDAO.getControllerService(updatedControllerService.getIdentifier());

        // get the current user
        NiFiUser user = NiFiUserUtils.getNiFiUser();

        // ensure the user was found
        if (user != null) {
            // determine the updated values
            Map<String, String> updatedValues = extractConfiguredPropertyValues(controllerService, controllerServiceDTO);

            // create the controller service details
            FlowChangeExtensionDetails serviceDetails = new FlowChangeExtensionDetails();
            serviceDetails.setType(controllerService.getComponentType());

            // create a controller service action
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
                    final PropertyDescriptor propertyDescriptor = controllerService.getControllerServiceImplementation().getPropertyDescriptor(property);
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
                    configurationAction.setSourceId(controllerService.getIdentifier());
                    configurationAction.setSourceName(controllerService.getName());
                    configurationAction.setSourceType(Component.ControllerService);
                    configurationAction.setComponentDetails(serviceDetails);
                    configurationAction.setActionDetails(actionDetails);
                    actions.add(configurationAction);
                }
            }

            // determine the new executing state
            final boolean updateIsDisabled = isDisabled(updatedControllerService);

            // determine if the running state has changed and its not disabled
            if (isDisabled != updateIsDisabled) {
                // create a controller service action
                FlowChangeAction serviceAction = new FlowChangeAction();
                serviceAction.setUserIdentity(user.getIdentity());
                serviceAction.setTimestamp(new Date());
                serviceAction.setSourceId(controllerService.getIdentifier());
                serviceAction.setSourceName(controllerService.getName());
                serviceAction.setSourceType(Component.ControllerService);
                serviceAction.setComponentDetails(serviceDetails);

                // set the operation accordingly
                if (updateIsDisabled) {
                    serviceAction.setOperation(Operation.Disable);
                } else {
                    serviceAction.setOperation(Operation.Enable);
                }
                actions.add(serviceAction);
            }

            // ensure there are actions to record
            if (!actions.isEmpty()) {
                // save the actions
                saveActions(actions, logger);
            }
        }

        return updatedControllerService;
    }

    /**
     * Audits the update of a component referencing a controller service.
     *
     * @param proceedingJoinPoint join point
     * @return object
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.ControllerServiceDAO+) && "
            + "execution(org.apache.nifi.controller.service.ControllerServiceReference "
            + "updateControllerServiceReferencingComponents(java.lang.String, org.apache.nifi.controller.ScheduledState, "
            + "org.apache.nifi.controller.service.ControllerServiceState))")
    public Object updateControllerServiceReferenceAdvice(ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        // update the controller service references
        final ControllerServiceReference controllerServiceReference = (ControllerServiceReference) proceedingJoinPoint.proceed();

        // get the current user
        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        if (user != null) {
            final Collection<Action> actions = new ArrayList<>();
            final Collection<String> visitedServices = new ArrayList<>();
            visitedServices.add(controllerServiceReference.getReferencedComponent().getIdentifier());

            // get all applicable actions
            getUpdateActionsForReferencingComponents(user, actions, visitedServices, controllerServiceReference.getReferencingComponents());

            // ensure there are actions to record
            if (!actions.isEmpty()) {
                // save the actions
                saveActions(actions, logger);
            }
        }

        return controllerServiceReference;
    }

    /**
     * Gets the update actions for all specified referencing components.
     *
     * @param user user
     * @param actions actions
     * @param visitedServices services
     * @param referencingComponents components
     */
    private void getUpdateActionsForReferencingComponents(
            final NiFiUser user, final Collection<Action> actions, final Collection<String> visitedServices, final Set<ConfiguredComponent> referencingComponents) {
        // consider each component updates
        for (final ConfiguredComponent component : referencingComponents) {
            if (component instanceof ProcessorNode) {
                final ProcessorNode processor = ((ProcessorNode) component);

                // create the processor details
                FlowChangeExtensionDetails processorDetails = new FlowChangeExtensionDetails();
                processorDetails.setType(processor.getComponentType());

                // create a processor action
                FlowChangeAction processorAction = new FlowChangeAction();
                processorAction.setUserIdentity(user.getIdentity());
                processorAction.setTimestamp(new Date());
                processorAction.setSourceId(processor.getIdentifier());
                processorAction.setSourceName(processor.getName());
                processorAction.setSourceType(Component.Processor);
                processorAction.setComponentDetails(processorDetails);
                processorAction.setOperation(ScheduledState.RUNNING.equals(processor.getScheduledState()) ? Operation.Start : Operation.Stop);
                actions.add(processorAction);
            } else if (component instanceof ReportingTask) {
                final ReportingTaskNode reportingTask = ((ReportingTaskNode) component);

                // create the reporting task details
                FlowChangeExtensionDetails taskDetails = new FlowChangeExtensionDetails();
                taskDetails.setType(reportingTask.getComponentType());

                // create a reporting task action
                FlowChangeAction reportingTaskAction = new FlowChangeAction();
                reportingTaskAction.setUserIdentity(user.getIdentity());
                reportingTaskAction.setTimestamp(new Date());
                reportingTaskAction.setSourceId(reportingTask.getIdentifier());
                reportingTaskAction.setSourceName(reportingTask.getName());
                reportingTaskAction.setSourceType(Component.ReportingTask);
                reportingTaskAction.setComponentDetails(taskDetails);
                reportingTaskAction.setOperation(ScheduledState.RUNNING.equals(reportingTask.getScheduledState()) ? Operation.Start : Operation.Stop);
                actions.add(reportingTaskAction);
            } else if (component instanceof ControllerServiceNode) {
                final ControllerServiceNode controllerService = ((ControllerServiceNode) component);

                // create the controller service details
                FlowChangeExtensionDetails serviceDetails = new FlowChangeExtensionDetails();
                serviceDetails.setType(controllerService.getComponentType());

                // create a controller service action
                FlowChangeAction serviceAction = new FlowChangeAction();
                serviceAction.setUserIdentity(user.getIdentity());
                serviceAction.setTimestamp(new Date());
                serviceAction.setSourceId(controllerService.getIdentifier());
                serviceAction.setSourceName(controllerService.getName());
                serviceAction.setSourceType(Component.ControllerService);
                serviceAction.setComponentDetails(serviceDetails);
                serviceAction.setOperation(isDisabled(controllerService) ? Operation.Disable : Operation.Enable);
                actions.add(serviceAction);

                // need to consider components referencing this controller service (transitive)
                if (!visitedServices.contains(controllerService.getIdentifier())) {
                    getUpdateActionsForReferencingComponents(user, actions, visitedServices, controllerService.getReferences().getReferencingComponents());
                }
            }
        }
    }

    /**
     * Audits the removal of a controller service via deleteControllerService().
     *
     * @param proceedingJoinPoint join point
     * @param controllerServiceId id
     * @param controllerServiceDAO dao
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.ControllerServiceDAO+) && "
            + "execution(void deleteControllerService(java.lang.String)) && "
            + "args(controllerServiceId) && "
            + "target(controllerServiceDAO)")
    public void removeControllerServiceAdvice(ProceedingJoinPoint proceedingJoinPoint, String controllerServiceId, ControllerServiceDAO controllerServiceDAO) throws Throwable {
        // get the controller service before removing it
        ControllerServiceNode controllerService = controllerServiceDAO.getControllerService(controllerServiceId);

        // remove the controller service
        proceedingJoinPoint.proceed();

        // if no exceptions were thrown, add removal actions...
        // audit the controller service removal
        final Action action = generateAuditRecord(controllerService, Operation.Remove);

        // save the actions
        if (action != null) {
            saveAction(action, logger);
        }
    }

    /**
     * Generates an audit record for the creation of a controller service.
     *
     * @param controllerService service
     * @param operation operation
     * @return action
     */
    private Action generateAuditRecord(ControllerServiceNode controllerService, Operation operation) {
        return generateAuditRecord(controllerService, operation, null);
    }

    /**
     * Generates an audit record for the creation of a controller service.
     *
     * @param controllerService service
     * @param operation operation
     * @param actionDetails details
     * @return action
     */
    private Action generateAuditRecord(ControllerServiceNode controllerService, Operation operation, ActionDetails actionDetails) {
        FlowChangeAction action = null;

        // get the current user
        NiFiUser user = NiFiUserUtils.getNiFiUser();

        // ensure the user was found
        if (user != null) {
            // create the controller service details
            FlowChangeExtensionDetails serviceDetails = new FlowChangeExtensionDetails();
            serviceDetails.setType(controllerService.getComponentType());

            // create the controller service action for adding this controller service
            action = new FlowChangeAction();
            action.setUserIdentity(user.getIdentity());
            action.setOperation(operation);
            action.setTimestamp(new Date());
            action.setSourceId(controllerService.getIdentifier());
            action.setSourceName(controllerService.getName());
            action.setSourceType(Component.ControllerService);
            action.setComponentDetails(serviceDetails);

            if (actionDetails != null) {
                action.setActionDetails(actionDetails);
            }
        }

        return action;
    }

    /**
     * Extracts the values for the configured properties from the specified ControllerService.
     *
     * @param controllerService service
     * @param controllerServiceDTO dto
     * @return properties
     */
    private Map<String, String> extractConfiguredPropertyValues(ControllerServiceNode controllerService, ControllerServiceDTO controllerServiceDTO) {
        Map<String, String> values = new HashMap<>();

        if (controllerServiceDTO.getName() != null) {
            values.put(NAME, controllerService.getName());
        }
        if (controllerServiceDTO.getAnnotationData() != null) {
            values.put(ANNOTATION_DATA, controllerService.getAnnotationData());
        }
        if (controllerServiceDTO.getBundle() != null) {
            final BundleCoordinate bundle = controllerService.getBundleCoordinate();
            values.put(EXTENSION_VERSION, formatExtensionVersion(controllerService.getComponentType(), bundle));
        }
        if (controllerServiceDTO.getProperties() != null) {
            // for each property specified, extract its configured value
            Map<String, String> properties = controllerServiceDTO.getProperties();
            Map<PropertyDescriptor, String> configuredProperties = controllerService.getProperties();
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
        if (controllerServiceDTO.getComments() != null) {
            values.put(COMMENTS, controllerService.getComments());
        }

        return values;
    }

    /**
     * Locates the actual property descriptor for the given spec property descriptor.
     *
     * @param propertyDescriptors descriptors
     * @param specDescriptor example descriptor
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

    /**
     * Returns whether the specified controller service is disabled (or disabling).
     *
     * @param controllerService service
     * @return whether the specified controller service is disabled (or disabling)
     */
    private boolean isDisabled(final ControllerServiceNode controllerService) {
        return ControllerServiceState.DISABLED.equals(controllerService.getState()) || ControllerServiceState.DISABLING.equals(controllerService.getState());
    }
}
