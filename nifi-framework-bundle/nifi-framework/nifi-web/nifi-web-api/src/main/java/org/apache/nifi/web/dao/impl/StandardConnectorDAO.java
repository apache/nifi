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
package org.apache.nifi.web.dao.impl;

import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.connector.AssetReference;
import org.apache.nifi.components.connector.ConnectorNode;
import org.apache.nifi.components.connector.ConnectorRepository;
import org.apache.nifi.components.connector.ConnectorValueReference;
import org.apache.nifi.components.connector.ConnectorValueType;
import org.apache.nifi.components.connector.PropertyGroupConfiguration;
import org.apache.nifi.components.connector.SecretReference;
import org.apache.nifi.components.connector.StringLiteralValue;
import org.apache.nifi.web.api.dto.ConfigurationStepConfigurationDTO;
import org.apache.nifi.web.api.dto.ConnectorValueReferenceDTO;
import org.apache.nifi.web.api.dto.PropertyGroupConfigurationDTO;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.web.NiFiCoreException;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.dao.ConnectorDAO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Repository
public class StandardConnectorDAO implements ConnectorDAO {

    private FlowController flowController;

    @Autowired
    public void setFlowController(final FlowController flowController) {
        this.flowController = flowController;
    }

    private FlowManager getFlowManager() {
        return flowController.getFlowManager();
    }

    private ConnectorRepository getConnectorRepository() {
        return flowController.getConnectorRepository();
    }

    @Override
    public boolean hasConnector(final String id) {
        return getConnectorRepository().getConnector(id) != null;
    }

    @Override
    public ConnectorNode getConnector(final String id) {
        final ConnectorNode connector = getConnectorRepository().getConnector(id);
        if (connector == null) {
            throw new ResourceNotFoundException("Could not find Connector with ID " + id);
        }
        return connector;
    }

    @Override
    public List<ConnectorNode> getConnectors() {
        return getConnectorRepository().getConnectors();
    }

    @Override
    public ConnectorNode createConnector(final String type, final String id, final BundleCoordinate bundleCoordinate, final boolean firstTimeAdded, final boolean registerLogObserver) {
        final FlowManager flowManager = getFlowManager();
        final ConnectorNode connector = flowManager.createConnector(type, id, bundleCoordinate, firstTimeAdded, registerLogObserver);
        getConnectorRepository().addConnector(connector);
        return connector;
    }

    @Override
    public void deleteConnector(final String id) {
        getConnectorRepository().removeConnector(id);
    }

    @Override
    public void startConnector(final String id) {
        final ConnectorNode connector = getConnector(id);
        getConnectorRepository().startConnector(connector);
    }

    @Override
    public void stopConnector(final String id) {
        final ConnectorNode connector = getConnector(id);
        getConnectorRepository().stopConnector(connector);
    }

    @Override
    public void enableConnector(final String id) {
        final ConnectorNode connector = getConnector(id);
        connector.enable();
    }

    @Override
    public void disableConnector(final String id) {
        final ConnectorNode connector = getConnector(id);
        connector.disable();
    }

    @Override
    public void updateConnectorConfigurationStep(final String id, final String configurationStepName, final ConfigurationStepConfigurationDTO configurationStepDto) {
        final ConnectorNode connector = getConnector(id);

        // Convert DTO to domain object - extract the property groups from the configuration step
        final List<PropertyGroupConfiguration> propertyGroups = new ArrayList<>();
        if (configurationStepDto.getPropertyGroupConfigurations() != null) {
            propertyGroups.addAll(configurationStepDto.getPropertyGroupConfigurations().stream()
                    .map(this::convertToPropertyGroupConfiguration)
                    .toList());
        }

        // Update the connector configuration through the repository
        try {
            getConnectorRepository().configureConnector(connector, configurationStepName, propertyGroups);
        } catch (final Exception e) {
            throw new IllegalStateException("Failed to update connector configuration: " + e, e);
        }
    }

    private PropertyGroupConfiguration convertToPropertyGroupConfiguration(final PropertyGroupConfigurationDTO dto) {
        final Map<String, ConnectorValueReference> propertyValues = new HashMap<>();
        if (dto.getPropertyValues() != null) {
            for (final Map.Entry<String, ConnectorValueReferenceDTO> entry : dto.getPropertyValues().entrySet()) {
                propertyValues.put(entry.getKey(), convertToConnectorValueReference(entry.getValue()));
            }
        }
        return new PropertyGroupConfiguration(dto.getPropertyGroupName(), propertyValues);
    }

    private ConnectorValueReference convertToConnectorValueReference(final ConnectorValueReferenceDTO dto) {
        if (dto == null) {
            return null;
        }
        final ConnectorValueType valueType = dto.getValueType() != null ? ConnectorValueType.valueOf(dto.getValueType()) : ConnectorValueType.STRING_LITERAL;
        return switch (valueType) {
            case STRING_LITERAL -> new StringLiteralValue(dto.getValue());
            case ASSET_REFERENCE -> new AssetReference(dto.getAssetIdentifier());
            case SECRET_REFERENCE -> new SecretReference(dto.getSecretProviderId(), dto.getSecretProviderName(), dto.getSecretName());
        };
    }

    @Override
    public void applyConnectorUpdate(final String id) {
        final ConnectorNode connector = getConnector(id);
        try {
            getConnectorRepository().applyUpdate(connector);
        } catch (final Exception e) {
            throw new NiFiCoreException("Failed to apply connector update: " + e, e);
        }
    }

    @Override
    public void verifyCanVerifyConfigurationStep(final String id, final String configurationStepName) {
        // Verify that the connector exists
        getConnector(id);
    }

    @Override
    public List<ConfigVerificationResult> verifyConfigurationStep(final String id, final String configurationStepName, final List<PropertyGroupConfigurationDTO> propertyGroupConfigurationDtos) {
        final ConnectorNode connector = getConnector(id);

        final List<PropertyGroupConfiguration> propertyGroupConfigurations = new ArrayList<>();
        if (propertyGroupConfigurationDtos != null) {
            for (final PropertyGroupConfigurationDTO dto : propertyGroupConfigurationDtos) {
                propertyGroupConfigurations.add(convertToPropertyGroupConfiguration(dto));
            }
        }

        return connector.verifyConfigurationStep(configurationStepName, propertyGroupConfigurations);
    }

    @Override
    public List<AllowableValue> fetchAllowableValues(final String id, final String stepName, final String groupName, final String propertyName, final String filter) {
        final ConnectorNode connector = getConnector(id);
        if (filter == null || filter.isEmpty()) {
            return connector.fetchAllowableValues(stepName, groupName, propertyName);
        } else {
            return connector.fetchAllowableValues(stepName, groupName, propertyName, filter);
        }
    }
}


