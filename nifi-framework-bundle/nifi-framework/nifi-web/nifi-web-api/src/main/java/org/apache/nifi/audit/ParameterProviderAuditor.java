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
import org.apache.nifi.controller.ParameterProviderNode;
import org.apache.nifi.web.api.dto.ParameterProviderDTO;
import org.apache.nifi.web.dao.ParameterProviderDAO;
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
import java.util.Set;

/**
 * Audits parameter provider creation/removal and configuration changes.
 */
@Service
@Aspect
public class ParameterProviderAuditor extends NiFiAuditor {

    private static final Logger logger = LoggerFactory.getLogger(ParameterProviderAuditor.class);

    private static final String NAME = "Name";
    private static final String ANNOTATION_DATA = "Annotation Data";
    private static final String EXTENSION_VERSION = "Extension Version";

    /**
     * Audits the creation of parameter provider via createParameterProvider().
     *
     * This method only needs to be run 'after returning'. However, in Java 7 the order in which these methods are returned from Class.getDeclaredMethods (even though there is no order guaranteed)
     * seems to differ from Java 6. SpringAOP depends on this ordering to determine advice precedence. By normalizing all advice into Around advice we can alleviate this issue.
     *
     * @param proceedingJoinPoint joinpoint
     * @return node
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.ParameterProviderDAO+) && "
            + "execution(org.apache.nifi.controller.ParameterProviderNode createParameterProvider(org.apache.nifi.web.api.dto.ParameterProviderDTO))")
    public ParameterProviderNode createParameterProviderAdvice(final ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        // update the parameter provider state
        final ParameterProviderNode parameterProvider = (ParameterProviderNode) proceedingJoinPoint.proceed();

        // if no exceptions were thrown, add the parameter provider action...
        final Action action = generateAuditRecord(parameterProvider, Operation.Add);

        // save the actions
        if (action != null) {
            saveAction(action, logger);
        }

        return parameterProvider;
    }

    /**
     * Audits the configuration of a parameter provider.
     *
     * @param proceedingJoinPoint joinpoint
     * @param parameterProviderDTO dto
     * @param parameterProviderDAO dao
     * @return object
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.ParameterProviderDAO+) && "
            + "execution(org.apache.nifi.controller.ParameterProviderNode updateParameterProvider(org.apache.nifi.web.api.dto.ParameterProviderDTO)) && "
            + "args(parameterProviderDTO) && "
            + "target(parameterProviderDAO)")
    public Object updateParameterProviderAdvice(final ProceedingJoinPoint proceedingJoinPoint, final ParameterProviderDTO parameterProviderDTO, final ParameterProviderDAO parameterProviderDAO)
            throws Throwable {
        // determine the initial values for each property/setting thats changing
        ParameterProviderNode parameterProvider = parameterProviderDAO.getParameterProvider(parameterProviderDTO.getId());
        final Map<String, String> values = extractConfiguredPropertyValues(parameterProvider, parameterProviderDTO);

        // update the parameter provider state
        final ParameterProviderNode updatedParameterProvider = (ParameterProviderNode) proceedingJoinPoint.proceed();

        // if no exceptions were thrown, add the parameter provider action...
        parameterProvider = parameterProviderDAO.getParameterProvider(updatedParameterProvider.getIdentifier());

        // get the current user
        final NiFiUser user = NiFiUserUtils.getNiFiUser();

        // ensure the user was found
        if (user != null) {
            // determine the updated values
            final Map<String, String> updatedValues = extractConfiguredPropertyValues(parameterProvider, parameterProviderDTO);

            // create the parameter provider details
            final FlowChangeExtensionDetails providerDetails = new FlowChangeExtensionDetails();
            providerDetails.setType(parameterProvider.getComponentType());

            // create a parameter provider action
            final Date actionTimestamp = new Date();
            final Collection<Action> actions = new ArrayList<>();

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
                    final PropertyDescriptor propertyDescriptor = parameterProvider.getParameterProvider().getPropertyDescriptor(property);
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
                    final FlowChangeAction configurationAction = new FlowChangeAction();
                    configurationAction.setUserIdentity(user.getIdentity());
                    configurationAction.setOperation(operation);
                    configurationAction.setTimestamp(actionTimestamp);
                    configurationAction.setSourceId(parameterProvider.getIdentifier());
                    configurationAction.setSourceName(parameterProvider.getName());
                    configurationAction.setSourceType(Component.ParameterProvider);
                    configurationAction.setComponentDetails(providerDetails);
                    configurationAction.setActionDetails(actionDetails);
                    actions.add(configurationAction);
                }
            }

            // ensure there are actions to record
            if (!actions.isEmpty()) {
                // save the actions
                saveActions(actions, logger);
            }
        }

        return updatedParameterProvider;
    }

    /**
     * Audits the removal of a parameter provider via deleteParameterProvider().
     *
     * @param proceedingJoinPoint join point
     * @param parameterProviderId task id
     * @param parameterProviderDAO task dao
     * @throws Throwable ex
     */
    @Around("within(org.apache.nifi.web.dao.ParameterProviderDAO+) && "
            + "execution(void deleteParameterProvider(java.lang.String)) && "
            + "args(parameterProviderId) && "
            + "target(parameterProviderDAO)")
    public void removeParameterProviderAdvice(final ProceedingJoinPoint proceedingJoinPoint, final String parameterProviderId, final ParameterProviderDAO parameterProviderDAO) throws Throwable {
        // get the parameter provider before removing it
        final ParameterProviderNode parameterProvider = parameterProviderDAO.getParameterProvider(parameterProviderId);

        // remove the parameter provider
        proceedingJoinPoint.proceed();

        // if no exceptions were thrown, add removal actions...
        // audit the parameter provider removal
        final Action action = generateAuditRecord(parameterProvider, Operation.Remove);

        // save the actions
        if (action != null) {
            saveAction(action, logger);
        }
    }

    /**
     * Generates an audit record for the creation of a parameter provider.
     *
     * @param parameterProvider task
     * @param operation operation
     * @return action
     */
    public Action generateAuditRecord(ParameterProviderNode parameterProvider, Operation operation) {
        return generateAuditRecord(parameterProvider, operation, null);
    }

    /**
     * Generates an audit record for the creation of a parameter provider.
     *
     * @param parameterProvider parameter provider
     * @param operation operation
     * @param actionDetails details
     * @return action
     */
    public Action generateAuditRecord(ParameterProviderNode parameterProvider, Operation operation, ActionDetails actionDetails) {
        FlowChangeAction action = null;

        // get the current user
        NiFiUser user = NiFiUserUtils.getNiFiUser();

        // ensure the user was found
        if (user != null) {
            // create the parameter provider details
            FlowChangeExtensionDetails taskDetails = new FlowChangeExtensionDetails();
            taskDetails.setType(parameterProvider.getComponentType());

            // create the parameter provider action for adding this parameter provider
            action = new FlowChangeAction();
            action.setUserIdentity(user.getIdentity());
            action.setOperation(operation);
            action.setTimestamp(new Date());
            action.setSourceId(parameterProvider.getIdentifier());
            action.setSourceName(parameterProvider.getName());
            action.setSourceType(Component.ParameterProvider);
            action.setComponentDetails(taskDetails);

            if (actionDetails != null) {
                action.setActionDetails(actionDetails);
            }
        }

        return action;
    }

    /**
     * Extracts the values for the configured properties from the specified ParameterProvider.
     *
     * @param parameterProvider task
     * @param parameterProviderDTO dto
     * @return properties of task
     */
    private Map<String, String> extractConfiguredPropertyValues(final ParameterProviderNode parameterProvider, final ParameterProviderDTO parameterProviderDTO) {
        final Map<String, String> values = new HashMap<>();

        if (parameterProviderDTO.getName() != null) {
            values.put(NAME, parameterProvider.getName());
        }
        if (parameterProviderDTO.getAnnotationData() != null) {
            values.put(ANNOTATION_DATA, parameterProvider.getAnnotationData());
        }
        if (parameterProviderDTO.getBundle() != null) {
            final BundleCoordinate bundle = parameterProvider.getBundleCoordinate();
            values.put(EXTENSION_VERSION, formatExtensionVersion(parameterProvider.getComponentType(), bundle));
        }
        if (parameterProviderDTO.getProperties() != null) {
            // for each property specified, extract its configured value
            Map<String, String> properties = parameterProviderDTO.getProperties();
            Map<PropertyDescriptor, String> configuredProperties = parameterProvider.getRawPropertyValues();
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
    private PropertyDescriptor locatePropertyDescriptor(final Set<PropertyDescriptor> propertyDescriptors, final PropertyDescriptor specDescriptor) {
        for (final PropertyDescriptor propertyDescriptor : propertyDescriptors) {
            if (propertyDescriptor.equals(specDescriptor)) {
                return propertyDescriptor;
            }
        }
        return specDescriptor;
    }

}
