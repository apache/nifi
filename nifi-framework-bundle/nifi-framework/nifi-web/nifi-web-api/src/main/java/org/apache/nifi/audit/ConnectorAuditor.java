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
import org.apache.nifi.components.connector.AssetReference;
import org.apache.nifi.components.connector.ConnectorConfiguration;
import org.apache.nifi.components.connector.ConnectorNode;
import org.apache.nifi.components.connector.ConnectorState;
import org.apache.nifi.components.connector.ConnectorValueReference;
import org.apache.nifi.components.connector.NamedStepConfiguration;
import org.apache.nifi.components.connector.SecretReference;
import org.apache.nifi.components.connector.StepConfiguration;
import org.apache.nifi.components.connector.StringLiteralValue;
import org.apache.nifi.web.api.dto.ConfigurationStepConfigurationDTO;
import org.apache.nifi.web.api.dto.ConnectorValueReferenceDTO;
import org.apache.nifi.web.api.dto.PropertyGroupConfigurationDTO;
import org.apache.nifi.web.dao.ConnectorDAO;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Audits connector creation/removal and configuration changes.
 */
@Service
@Aspect
public class ConnectorAuditor extends NiFiAuditor {

    private static final Logger logger = LoggerFactory.getLogger(ConnectorAuditor.class);

    /**
     * Audits the creation of connectors via createConnector().
     *
     * @param proceedingJoinPoint join point
     * @return connector node
     * @throws Throwable if an error occurs
     */
    @Around("within(org.apache.nifi.web.dao.ConnectorDAO+) && "
            + "execution(org.apache.nifi.components.connector.ConnectorNode createConnector(..))")
    public ConnectorNode createConnectorAdvice(final ProceedingJoinPoint proceedingJoinPoint) throws Throwable {
        final ConnectorNode connector = (ConnectorNode) proceedingJoinPoint.proceed();

        final Action action = generateAuditRecord(connector, Operation.Add);
        if (action != null) {
            saveAction(action, logger);
        }

        return connector;
    }

    /**
     * Audits the removal of a connector via deleteConnector().
     *
     * @param proceedingJoinPoint join point
     * @param connectorId connector id
     * @param connectorDAO connector dao
     * @throws Throwable if an error occurs
     */
    @Around("within(org.apache.nifi.web.dao.ConnectorDAO+) && "
            + "execution(void deleteConnector(java.lang.String)) && "
            + "args(connectorId) && "
            + "target(connectorDAO)")
    public void removeConnectorAdvice(final ProceedingJoinPoint proceedingJoinPoint, final String connectorId, final ConnectorDAO connectorDAO) throws Throwable {
        final ConnectorNode connector = connectorDAO.getConnector(connectorId);

        proceedingJoinPoint.proceed();

        final Action action = generateAuditRecord(connector, Operation.Remove);
        if (action != null) {
            saveAction(action, logger);
        }
    }

    /**
     * Audits the starting of a connector via startConnector().
     *
     * @param proceedingJoinPoint join point
     * @param connectorId connector id
     * @param connectorDAO connector dao
     * @throws Throwable if an error occurs
     */
    @Around("within(org.apache.nifi.web.dao.ConnectorDAO+) && "
            + "execution(void startConnector(java.lang.String)) && "
            + "args(connectorId) && "
            + "target(connectorDAO)")
    public void startConnectorAdvice(final ProceedingJoinPoint proceedingJoinPoint, final String connectorId, final ConnectorDAO connectorDAO) throws Throwable {
        final ConnectorNode connector = connectorDAO.getConnector(connectorId);
        final ConnectorState previousState = connector.getCurrentState();

        proceedingJoinPoint.proceed();

        if (isAuditable() && previousState != ConnectorState.RUNNING && previousState != ConnectorState.STARTING) {
            final Action action = generateAuditRecord(connector, Operation.Start);
            if (action != null) {
                saveAction(action, logger);
            }
        }
    }

    /**
     * Audits the stopping of a connector via stopConnector().
     *
     * @param proceedingJoinPoint join point
     * @param connectorId connector id
     * @param connectorDAO connector dao
     * @throws Throwable if an error occurs
     */
    @Around("within(org.apache.nifi.web.dao.ConnectorDAO+) && "
            + "execution(void stopConnector(java.lang.String)) && "
            + "args(connectorId) && "
            + "target(connectorDAO)")
    public void stopConnectorAdvice(final ProceedingJoinPoint proceedingJoinPoint, final String connectorId, final ConnectorDAO connectorDAO) throws Throwable {
        final ConnectorNode connector = connectorDAO.getConnector(connectorId);
        final ConnectorState previousState = connector.getCurrentState();

        proceedingJoinPoint.proceed();

        if (isAuditable() && previousState != ConnectorState.STOPPED && previousState != ConnectorState.STOPPING) {
            final Action action = generateAuditRecord(connector, Operation.Stop);
            if (action != null) {
                saveAction(action, logger);
            }
        }
    }

    /**
     * Audits the enabling of a connector via enableConnector().
     *
     * @param proceedingJoinPoint join point
     * @param connectorId connector id
     * @param connectorDAO connector dao
     * @throws Throwable if an error occurs
     */
    @Around("within(org.apache.nifi.web.dao.ConnectorDAO+) && "
            + "execution(void enableConnector(java.lang.String)) && "
            + "args(connectorId) && "
            + "target(connectorDAO)")
    public void enableConnectorAdvice(final ProceedingJoinPoint proceedingJoinPoint, final String connectorId, final ConnectorDAO connectorDAO) throws Throwable {
        final ConnectorNode connector = connectorDAO.getConnector(connectorId);
        final ConnectorState previousState = connector.getCurrentState();

        proceedingJoinPoint.proceed();

        if (isAuditable() && previousState == ConnectorState.DISABLED) {
            final Action action = generateAuditRecord(connector, Operation.Enable);
            if (action != null) {
                saveAction(action, logger);
            }
        }
    }

    /**
     * Audits the disabling of a connector via disableConnector().
     *
     * @param proceedingJoinPoint join point
     * @param connectorId connector id
     * @param connectorDAO connector dao
     * @throws Throwable if an error occurs
     */
    @Around("within(org.apache.nifi.web.dao.ConnectorDAO+) && "
            + "execution(void disableConnector(java.lang.String)) && "
            + "args(connectorId) && "
            + "target(connectorDAO)")
    public void disableConnectorAdvice(final ProceedingJoinPoint proceedingJoinPoint, final String connectorId, final ConnectorDAO connectorDAO) throws Throwable {
        final ConnectorNode connector = connectorDAO.getConnector(connectorId);
        final ConnectorState previousState = connector.getCurrentState();

        proceedingJoinPoint.proceed();

        if (isAuditable() && previousState != ConnectorState.DISABLED) {
            final Action action = generateAuditRecord(connector, Operation.Disable);
            if (action != null) {
                saveAction(action, logger);
            }
        }
    }

    /**
     * Audits configuration step updates via updateConnectorConfigurationStep().
     *
     * @param proceedingJoinPoint join point
     * @param connectorId connector id
     * @param configurationStepName name of the configuration step
     * @param configurationStepConfiguration the configuration step configuration DTO
     * @param connectorDAO connector dao
     * @throws Throwable if an error occurs
     */
    @Around("within(org.apache.nifi.web.dao.ConnectorDAO+) && "
            + "execution(void updateConnectorConfigurationStep(java.lang.String, java.lang.String, org.apache.nifi.web.api.dto.ConfigurationStepConfigurationDTO)) && "
            + "args(connectorId, configurationStepName, configurationStepConfiguration) && "
            + "target(connectorDAO)")
    public void updateConfigurationStepAdvice(final ProceedingJoinPoint proceedingJoinPoint, final String connectorId, final String configurationStepName,
                                              final ConfigurationStepConfigurationDTO configurationStepConfiguration, final ConnectorDAO connectorDAO) throws Throwable {
        final ConnectorNode connector = connectorDAO.getConnector(connectorId);

        // Capture the current property values before the update (flat map: property name -> value)
        final Map<String, String> previousValues = extractCurrentPropertyValues(connector, configurationStepName);

        proceedingJoinPoint.proceed();

        if (isAuditable()) {
            // Generate audit actions for each changed property, using the DTO for group structure
            final List<Action> actions = generateConfigurationChangeActions(connector, configurationStepName, previousValues, configurationStepConfiguration);
            if (!actions.isEmpty()) {
                saveActions(actions, logger);
            }
        }
    }

    /**
     * Extracts the current property values for a specific configuration step from the connector's working flow context.
     *
     * @param connector the connector node
     * @param configurationStepName the name of the configuration step
     * @return a map of property name to property value
     */
    private Map<String, String> extractCurrentPropertyValues(final ConnectorNode connector, final String configurationStepName) {
        final Map<String, String> result = new HashMap<>();

        final ConnectorConfiguration configuration = connector.getWorkingFlowContext().getConfigurationContext().toConnectorConfiguration();
        for (final NamedStepConfiguration namedStepConfig : configuration.getNamedStepConfigurations()) {
            if (Objects.equals(namedStepConfig.stepName(), configurationStepName)) {
                final StepConfiguration stepConfig = namedStepConfig.configuration();
                for (final Map.Entry<String, ConnectorValueReference> entry : stepConfig.getPropertyValues().entrySet()) {
                    result.put(entry.getKey(), formatValueReference(entry.getValue()));
                }
                break;
            }
        }

        return result;
    }

    /**
     * Formats a ConnectorValueReference as a string for audit logging.
     *
     * @param valueRef the value reference
     * @return the formatted string representation
     */
    private String formatValueReference(final ConnectorValueReference valueRef) {
        if (valueRef == null) {
            return null;
        }

        return switch (valueRef) {
            case StringLiteralValue stringLiteral -> stringLiteral.getValue();
            case AssetReference assetRef -> "asset:" + assetRef.getAssetIdentifier();
            case SecretReference secretRef -> "secret:" + secretRef.getProviderId() + "/" + secretRef.getProviderName() + "/" + secretRef.getSecretName();
        };
    }

    /**
     * Formats a ConnectorValueReferenceDTO as a string for audit logging.
     *
     * @param valueRefDto the value reference DTO
     * @return the formatted string representation
     */
    private String formatValueReferenceDto(final ConnectorValueReferenceDTO valueRefDto) {
        if (valueRefDto == null) {
            return null;
        }

        final String valueType = valueRefDto.getValueType();
        if (valueType == null || "STRING_LITERAL".equals(valueType)) {
            return valueRefDto.getValue();
        } else if ("ASSET_REFERENCE".equals(valueType)) {
            return "asset:" + valueRefDto.getAssetIdentifier();
        } else if ("SECRET_REFERENCE".equals(valueType)) {
            return "secret:" + valueRefDto.getSecretProviderId() + "/" + valueRefDto.getSecretProviderName() + "/" + valueRefDto.getSecretName();
        }

        return valueRefDto.getValue();
    }

    /**
     * Generates audit actions for configuration changes by comparing previous values with the new values from the DTO.
     *
     * @param connector the connector node
     * @param configurationStepName the configuration step name
     * @param previousValues the previous property values (flat map: property name -> value)
     * @param configurationStepDto the configuration step DTO containing new values with group structure
     * @return list of actions for changed properties
     */
    private List<Action> generateConfigurationChangeActions(final ConnectorNode connector, final String configurationStepName,
                                                            final Map<String, String> previousValues, final ConfigurationStepConfigurationDTO configurationStepDto) {
        final List<Action> actions = new ArrayList<>();
        final Date timestamp = new Date();

        final FlowChangeExtensionDetails connectorDetails = new FlowChangeExtensionDetails();
        connectorDetails.setType(connector.getComponentType());

        if (configurationStepDto.getPropertyGroupConfigurations() == null) {
            return actions;
        }

        // Iterate through all property groups in the DTO to preserve group names for audit logging
        for (final PropertyGroupConfigurationDTO groupDto : configurationStepDto.getPropertyGroupConfigurations()) {
            final String groupName = groupDto.getPropertyGroupName();

            if (groupDto.getPropertyValues() == null) {
                continue;
            }

            // Check each property in this group
            for (final Map.Entry<String, ConnectorValueReferenceDTO> propertyEntry : groupDto.getPropertyValues().entrySet()) {
                final String propertyName = propertyEntry.getKey();
                final String newValue = formatValueReferenceDto(propertyEntry.getValue());
                final String previousValue = previousValues.get(propertyName);

                // Only create an action if the value changed
                if (!Objects.equals(previousValue, newValue)) {
                    final String fullPropertyName = formatPropertyName(configurationStepName, groupName, propertyName);

                    final FlowChangeConfigureDetails actionDetails = new FlowChangeConfigureDetails();
                    actionDetails.setName(fullPropertyName);
                    actionDetails.setPreviousValue(previousValue);
                    actionDetails.setValue(newValue);

                    final FlowChangeAction configurationAction = createFlowChangeAction();
                    configurationAction.setOperation(Operation.Configure);
                    configurationAction.setTimestamp(timestamp);
                    configurationAction.setSourceId(connector.getIdentifier());
                    configurationAction.setSourceName(connector.getName());
                    configurationAction.setSourceType(Component.Connector);
                    configurationAction.setComponentDetails(connectorDetails);
                    configurationAction.setActionDetails(actionDetails);

                    actions.add(configurationAction);
                }
            }
        }

        return actions;
    }

    /**
     * Formats the property name for audit logging, including step and group context.
     *
     * @param stepName the configuration step name
     * @param groupName the property group name
     * @param propertyName the property name
     * @return the formatted property name
     */
    private String formatPropertyName(final String stepName, final String groupName, final String propertyName) {
        if (groupName == null || groupName.isEmpty()) {
            return stepName + " / " + propertyName;
        }
        return stepName + " / " + groupName + " / " + propertyName;
    }

    /**
     * Audits application of connector updates via applyConnectorUpdate().
     *
     * @param proceedingJoinPoint join point
     * @param connectorId connector id
     * @param connectorDAO connector dao
     * @throws Throwable if an error occurs
     */
    @Around("within(org.apache.nifi.web.dao.ConnectorDAO+) && "
            + "execution(void applyConnectorUpdate(java.lang.String)) && "
            + "args(connectorId) && "
            + "target(connectorDAO)")
    public void applyConnectorUpdateAdvice(final ProceedingJoinPoint proceedingJoinPoint, final String connectorId, final ConnectorDAO connectorDAO) throws Throwable {
        final ConnectorNode connector = connectorDAO.getConnector(connectorId);

        proceedingJoinPoint.proceed();

        if (isAuditable()) {
            final FlowChangeExtensionDetails connectorDetails = new FlowChangeExtensionDetails();
            connectorDetails.setType(connector.getComponentType());

            final FlowChangeConfigureDetails actionDetails = new FlowChangeConfigureDetails();
            actionDetails.setName("Applied Update");
            actionDetails.setValue("true");
            actionDetails.setPreviousValue(null);

            final FlowChangeAction configurationAction = createFlowChangeAction();
            configurationAction.setOperation(Operation.Configure);
            configurationAction.setTimestamp(new Date());
            configurationAction.setSourceId(connector.getIdentifier());
            configurationAction.setSourceName(connector.getName());
            configurationAction.setSourceType(Component.Connector);
            configurationAction.setComponentDetails(connectorDetails);
            configurationAction.setActionDetails(actionDetails);

            saveAction(configurationAction, logger);
        }
    }

    /**
     * Generates an audit record for a connector.
     *
     * @param connector the connector
     * @param operation the operation
     * @return the action
     */
    public Action generateAuditRecord(final ConnectorNode connector, final Operation operation) {
        return generateAuditRecord(connector, operation, null);
    }

    /**
     * Generates an audit record for a connector.
     *
     * @param connector the connector
     * @param operation the operation
     * @param actionDetails the action details
     * @return the action
     */
    public Action generateAuditRecord(final ConnectorNode connector, final Operation operation, final ActionDetails actionDetails) {
        FlowChangeAction action = null;

        if (isAuditable()) {
            final FlowChangeExtensionDetails connectorDetails = new FlowChangeExtensionDetails();
            connectorDetails.setType(connector.getComponentType());

            action = createFlowChangeAction();
            action.setOperation(operation);
            action.setSourceId(connector.getIdentifier());
            action.setSourceName(connector.getName());
            action.setSourceType(Component.Connector);
            action.setComponentDetails(connectorDetails);

            if (actionDetails != null) {
                action.setActionDetails(actionDetails);
            }
        }

        return action;
    }
}

