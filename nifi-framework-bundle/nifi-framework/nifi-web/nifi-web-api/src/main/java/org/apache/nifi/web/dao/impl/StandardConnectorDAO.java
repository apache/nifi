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

import org.apache.nifi.asset.Asset;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.connector.AssetReference;
import org.apache.nifi.components.connector.ConnectorAssetRepository;
import org.apache.nifi.components.connector.ConnectorNode;
import org.apache.nifi.components.connector.ConnectorRepository;
import org.apache.nifi.components.connector.ConnectorUpdateContext;
import org.apache.nifi.components.connector.ConnectorValueReference;
import org.apache.nifi.components.connector.ConnectorValueType;
import org.apache.nifi.components.connector.SecretReference;
import org.apache.nifi.components.connector.StepConfiguration;
import org.apache.nifi.components.connector.StringLiteralValue;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.web.NiFiCoreException;
import org.apache.nifi.web.ResourceNotFoundException;
import org.apache.nifi.web.api.dto.AssetReferenceDTO;
import org.apache.nifi.web.api.dto.ConfigurationStepConfigurationDTO;
import org.apache.nifi.web.api.dto.ConnectorDTO;
import org.apache.nifi.web.api.dto.ConnectorValueReferenceDTO;
import org.apache.nifi.web.api.dto.PropertyGroupConfigurationDTO;
import org.apache.nifi.web.dao.ConnectorDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Repository
public class StandardConnectorDAO implements ConnectorDAO {

    private static final Logger logger = LoggerFactory.getLogger(StandardConnectorDAO.class);

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

    private ConnectorAssetRepository getConnectorAssetRepository() {
        return flowController.getConnectorRepository().getAssetRepository();
    }

    @Override
    public void verifyCreate(final ConnectorDTO connectorDTO) {
        final String id = connectorDTO.getId();
        if (id != null && hasConnector(id)) {
            throw new IllegalStateException("A Connector already exists with ID %s".formatted(id));
        }
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
        getConnectorAssetRepository().deleteAssets(id);
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
    public void drainFlowFiles(final String id) {
        final ConnectorNode connector = getConnector(id);
        connector.drainFlowFiles();
    }

    @Override
    public void cancelDrainFlowFiles(final String id) {
        final ConnectorNode connector = getConnector(id);
        connector.cancelDrainFlowFiles();
    }

    @Override
    public void verifyCancelDrainFlowFile(final String id) {
        final ConnectorNode connector = getConnector(id);
        connector.verifyCancelDrainFlowFiles();
    }

    @Override
    public void updateConnectorConfigurationStep(final String id, final String configurationStepName, final ConfigurationStepConfigurationDTO configurationStepDto) {
        final ConnectorNode connector = getConnector(id);

        // Convert DTO to domain object - flatten all property groups into a single StepConfiguration
        final StepConfiguration stepConfiguration = convertToStepConfiguration(configurationStepDto);

        // Update the connector configuration through the repository
        try {
            getConnectorRepository().configureConnector(connector, configurationStepName, stepConfiguration);
        } catch (final Exception e) {
            throw new IllegalStateException("Failed to update connector configuration: " + e, e);
        }
    }

    private StepConfiguration convertToStepConfiguration(final ConfigurationStepConfigurationDTO dto) {
        final Map<String, ConnectorValueReference> propertyValues = new HashMap<>();
        if (dto.getPropertyGroupConfigurations() != null) {
            for (final PropertyGroupConfigurationDTO groupDto : dto.getPropertyGroupConfigurations()) {
                if (groupDto.getPropertyValues() != null) {
                    for (final Map.Entry<String, ConnectorValueReferenceDTO> entry : groupDto.getPropertyValues().entrySet()) {
                        propertyValues.put(entry.getKey(), convertToConnectorValueReference(entry.getValue()));
                    }
                }
            }
        }
        return new StepConfiguration(propertyValues);
    }

    private ConnectorValueReference convertToConnectorValueReference(final ConnectorValueReferenceDTO dto) {
        if (dto == null) {
            return null;
        }
        final ConnectorValueType valueType = dto.getValueType() != null ? ConnectorValueType.valueOf(dto.getValueType()) : ConnectorValueType.STRING_LITERAL;
        return switch (valueType) {
            case STRING_LITERAL -> new StringLiteralValue(dto.getValue());
            case ASSET_REFERENCE -> new AssetReference(convertToAssetIdentifiers(dto.getAssetReferences()));
            case SECRET_REFERENCE -> new SecretReference(dto.getSecretProviderId(), dto.getSecretProviderName(), dto.getSecretName(), dto.getFullyQualifiedSecretName());
        };
    }

    private Set<String> convertToAssetIdentifiers(final List<AssetReferenceDTO> assetReferenceDTOs) {
        if (assetReferenceDTOs == null || assetReferenceDTOs.isEmpty()) {
            return Collections.emptySet();
        }
        return assetReferenceDTOs.stream().map(AssetReferenceDTO::getId).collect(Collectors.toSet());
    }

    @Override
    public void applyConnectorUpdate(final String id, final ConnectorUpdateContext updateContext) {
        final ConnectorNode connector = getConnector(id);
        try {
            getConnectorRepository().applyUpdate(connector, updateContext);
        } catch (final Exception e) {
            throw new NiFiCoreException("Failed to apply connector update: " + e, e);
        }
    }

    @Override
    public void discardWorkingConfiguration(final String id) {
        final ConnectorNode connector = getConnector(id);
        getConnectorRepository().discardWorkingConfiguration(connector);
    }

    @Override
    public void verifyCanVerifyConfigurationStep(final String id, final String configurationStepName) {
        // Verify that the connector exists
        getConnector(id);
    }

    @Override
    public List<ConfigVerificationResult> verifyConfigurationStep(final String id, final String configurationStepName, final ConfigurationStepConfigurationDTO configurationStepDto) {
        final ConnectorNode connector = getConnector(id);
        final StepConfiguration stepConfiguration = convertToStepConfiguration(configurationStepDto);
        return connector.verifyConfigurationStep(configurationStepName, stepConfiguration);
    }

    @Override
    public List<AllowableValue> fetchAllowableValues(final String id, final String stepName, final String propertyName, final String filter) {
        final ConnectorNode connector = getConnector(id);
        if (filter == null || filter.isEmpty()) {
            return connector.fetchAllowableValues(stepName, propertyName);
        } else {
            return connector.fetchAllowableValues(stepName, propertyName, filter);
        }
    }

    @Override
    public void verifyCreateAsset(final String id) {
        getConnector(id);
    }

    @Override
    public Asset createAsset(final String id, final String assetId, final String assetName, final InputStream content) throws IOException {
        final ConnectorAssetRepository assetRepository = getConnectorAssetRepository();
        return assetRepository.storeAsset(id, assetId, assetName, content);
    }

    @Override
    public List<Asset> getAssets(final String id) {
        final ConnectorAssetRepository assetRepository = getConnectorAssetRepository();
        return assetRepository.getAssets(id);
    }

    @Override
    public Optional<Asset> getAsset(final String assetId) {
        final ConnectorAssetRepository assetRepository = getConnectorAssetRepository();
        return assetRepository.getAsset(assetId);
    }
}


