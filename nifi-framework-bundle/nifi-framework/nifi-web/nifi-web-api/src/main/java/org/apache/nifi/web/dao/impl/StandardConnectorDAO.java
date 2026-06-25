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

import jakarta.annotation.PostConstruct;
import org.apache.nifi.asset.Asset;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.components.connector.AssetReference;
import org.apache.nifi.components.connector.ConnectorFlowSnapshotProvider;
import org.apache.nifi.components.connector.ConnectorMigrationManager;
import org.apache.nifi.components.connector.ConnectorMigrationSource;
import org.apache.nifi.components.connector.ConnectorNode;
import org.apache.nifi.components.connector.ConnectorRepository;
import org.apache.nifi.components.connector.ConnectorSyncMode;
import org.apache.nifi.components.connector.ConnectorUpdateContext;
import org.apache.nifi.components.connector.ConnectorValueReference;
import org.apache.nifi.components.connector.ConnectorValueType;
import org.apache.nifi.components.connector.FlowUpdateException;
import org.apache.nifi.components.connector.SecretReference;
import org.apache.nifi.components.connector.StandardConnectorMigrationManager;
import org.apache.nifi.components.connector.StepConfiguration;
import org.apache.nifi.components.connector.StringLiteralValue;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.web.NiFiCoreException;
import org.apache.nifi.web.NiFiServiceFacade;
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
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

@Repository
public class StandardConnectorDAO implements ConnectorDAO {

    private static final Logger logger = LoggerFactory.getLogger(StandardConnectorDAO.class);

    private FlowController flowController;
    private NiFiServiceFacade serviceFacade;
    private ConnectorMigrationManager connectorMigrationManager;

    @Autowired
    public void setFlowController(final FlowController flowController) {
        this.flowController = flowController;
    }

    /**
     * Injected with {@link Lazy @Lazy} to break the circular dependency between this DAO and
     * {@link NiFiServiceFacade}. The service facade is used solely as a {@link ConnectorFlowSnapshotProvider}
     * for the migration manager constructed in {@link #initialize()}; the proxy is invoked lazily at
     * migration time, so the real service-facade bean is not required at DAO construction.
     */
    @Autowired
    public void setServiceFacade(@Lazy final NiFiServiceFacade serviceFacade) {
        this.serviceFacade = serviceFacade;
    }

    @PostConstruct
    public void initialize() {
        final ConnectorFlowSnapshotProvider snapshotProvider = serviceFacade::getCurrentFlowSnapshotByGroupId;
        this.connectorMigrationManager = new StandardConnectorMigrationManager(flowController, snapshotProvider);
    }

    private FlowManager getFlowManager() {
        return flowController.getFlowManager();
    }

    private ConnectorRepository getConnectorRepository() {
        return flowController.getConnectorRepository();
    }

    @Override
    public void verifyCreate(final ConnectorDTO connectorDTO) {
        final String id = connectorDTO.getId();
        if (id != null) {
            getConnectorRepository().verifyCreate(id);
        }
    }

    @Override
    public boolean hasConnector(final String id) {
        return getConnectorRepository().getConnector(id, ConnectorSyncMode.LOCAL_ONLY) != null;
    }

    @Override
    public ConnectorNode getConnector(final String id) {
        // Public read returned to clients; must reflect the latest configuration from the provider.
        return requireConnector(id, ConnectorSyncMode.SYNC_WITH_PROVIDER);
    }

    @Override
    public ConnectorNode getConnector(final String id, final ConnectorSyncMode syncMode) {
        return requireConnector(id, syncMode);
    }

    @Override
    public List<ConnectorNode> getConnectors() {
        // Public read returned to clients; must reflect the latest configuration from the provider.
        return getConnectorRepository().getConnectors(ConnectorSyncMode.SYNC_WITH_PROVIDER);
    }

    @Override
    public ConnectorNode createConnector(final String type, final String id, final BundleCoordinate bundleCoordinate, final boolean firstTimeAdded, final boolean registerLogObserver) {
        final FlowManager flowManager = getFlowManager();
        final ConnectorNode connector = flowManager.createConnector(type, id, bundleCoordinate, firstTimeAdded, registerLogObserver);
        return connector;
    }

    @Override
    public void updateConnector(final ConnectorDTO connectorDTO) {
        final ConnectorNode connector = requireConnector(connectorDTO.getId(), ConnectorSyncMode.LOCAL_ONLY);
        if (connectorDTO.getName() != null) {
            getConnectorRepository().updateConnector(connector, connectorDTO.getName());
        }
    }

    @Override
    public void verifyDelete(final String id) {
        final ConnectorNode connector = requireConnector(id, ConnectorSyncMode.LOCAL_ONLY);
        getConnectorRepository().verifyDelete(connector);
    }

    @Override
    public void deleteConnector(final String id) {
        getConnectorRepository().deleteAssets(id);
        getConnectorRepository().removeConnector(id);
    }

    @Override
    public void startConnector(final String id) {
        final ConnectorNode connector = requireConnector(id, ConnectorSyncMode.LOCAL_ONLY);
        getConnectorRepository().startConnector(connector);
    }

    @Override
    public void stopConnector(final String id) {
        final ConnectorNode connector = requireConnector(id, ConnectorSyncMode.LOCAL_ONLY);
        getConnectorRepository().stopConnector(connector);
    }

    @Override
    public void drainFlowFiles(final String id) {
        final ConnectorNode connector = requireConnector(id, ConnectorSyncMode.LOCAL_ONLY);
        connector.drainFlowFiles();
    }

    @Override
    public void cancelDrainFlowFiles(final String id) {
        final ConnectorNode connector = requireConnector(id, ConnectorSyncMode.LOCAL_ONLY);
        connector.cancelDrainFlowFiles();
    }

    @Override
    public void verifyCancelDrainFlowFile(final String id) {
        final ConnectorNode connector = requireConnector(id, ConnectorSyncMode.LOCAL_ONLY);
        connector.verifyCancelDrainFlowFiles();
    }

    @Override
    public void verifyEnterTroubleshooting(final String id) {
        final ConnectorNode connector = requireConnector(id, ConnectorSyncMode.LOCAL_ONLY);
        getConnectorRepository().verifyEnterTroubleshooting(connector);
    }

    @Override
    public void enterTroubleshooting(final String id) {
        final ConnectorNode connector = requireConnector(id, ConnectorSyncMode.LOCAL_ONLY);
        getConnectorRepository().enterTroubleshooting(connector);
    }

    @Override
    public void verifyEndTroubleshooting(final String id) {
        final ConnectorNode connector = requireConnector(id, ConnectorSyncMode.LOCAL_ONLY);
        getConnectorRepository().verifyEndTroubleshooting(connector);
    }

    @Override
    public void endTroubleshooting(final String id) {
        final ConnectorNode connector = requireConnector(id, ConnectorSyncMode.LOCAL_ONLY);
        try {
            getConnectorRepository().endTroubleshooting(connector);
        } catch (final FlowUpdateException e) {
            throw new IllegalStateException("Failed to exit troubleshooting mode for Connector " + id + ": " + e.getMessage(), e);
        }
    }

    @Override
    public void verifyPurgeFlowFiles(final String id) {
        final ConnectorNode connector = requireConnector(id, ConnectorSyncMode.LOCAL_ONLY);
        connector.verifyCanPurgeFlowFiles();
    }

    @Override
    public void purgeFlowFiles(final String id, final String requestor) {
        final ConnectorNode connector = requireConnector(id, ConnectorSyncMode.LOCAL_ONLY);
        try {
            connector.purgeFlowFiles(requestor).get();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Thread was interrupted while purging FlowFiles for Connector " + id, e);
        } catch (final ExecutionException e) {
            throw new IllegalStateException("Failed to purge FlowFiles for Connector " + id, e.getCause());
        }
    }

    @Override
    public void updateConnectorConfigurationStep(final String id, final String configurationStepName, final ConfigurationStepConfigurationDTO configurationStepDto) {
        // ConnectorRepository.configureConnector consults the provider directly when merging the step,
        // so a sync at the lookup would be redundant.
        final ConnectorNode connector = requireConnector(id, ConnectorSyncMode.LOCAL_ONLY);

        final StepConfiguration stepConfiguration = convertToStepConfiguration(configurationStepDto);

        try {
            getConnectorRepository().configureConnector(connector, configurationStepName, stepConfiguration);
        } catch (final Exception e) {
            throw new IllegalStateException("Failed to update connector configuration: " + e, e);
        }
    }

    private ConnectorNode requireConnector(final String id, final ConnectorSyncMode syncMode) {
        final ConnectorNode connector = getConnectorRepository().getConnector(id, syncMode);
        if (connector == null) {
            throw new ResourceNotFoundException("Could not find Connector with ID " + id);
        }
        return connector;
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
        // ConnectorRepository.applyUpdate calls syncAssetsFromProvider internally, which refreshes
        // the connector from the provider; a sync at the lookup would be redundant.
        final ConnectorNode connector = requireConnector(id, ConnectorSyncMode.LOCAL_ONLY);
        try {
            getConnectorRepository().applyUpdate(connector, updateContext);
        } catch (final Exception e) {
            throw new NiFiCoreException("Failed to apply connector update: " + e, e);
        }
    }

    @Override
    public void discardWorkingConfiguration(final String id) {
        // The working configuration is being thrown away; reading the latest from the provider first serves no purpose.
        final ConnectorNode connector = requireConnector(id, ConnectorSyncMode.LOCAL_ONLY);
        getConnectorRepository().discardWorkingConfiguration(connector);
    }

    @Override
    public void verifyCanVerifyConfigurationStep(final String id, final String configurationStepName) {
        requireConnector(id, ConnectorSyncMode.LOCAL_ONLY);
    }

    @Override
    public List<ConfigVerificationResult> verifyConfigurationStep(final String id, final String configurationStepName, final ConfigurationStepConfigurationDTO configurationStepDto) {
        // syncAssetsFromProvider below performs the provider sync, so the lookup itself does not need to.
        final ConnectorNode connector = requireConnector(id, ConnectorSyncMode.LOCAL_ONLY);
        getConnectorRepository().syncAssetsFromProvider(connector);
        final StepConfiguration stepConfiguration = convertToStepConfiguration(configurationStepDto);
        return connector.verifyConfigurationStep(configurationStepName, stepConfiguration);
    }

    @Override
    public List<DescribedValue> fetchAllowableValues(final String id, final String stepName, final String propertyName, final String filter) {
        final ConnectorNode connector = requireConnector(id, ConnectorSyncMode.LOCAL_ONLY);
        if (filter == null || filter.isEmpty()) {
            return connector.fetchAllowableValues(stepName, propertyName);
        } else {
            return connector.fetchAllowableValues(stepName, propertyName, filter);
        }
    }

    @Override
    public void verifyCreateAsset(final String id) {
        requireConnector(id, ConnectorSyncMode.LOCAL_ONLY);
    }

    @Override
    public Asset createAsset(final String id, final String assetId, final String assetName, final InputStream content) throws IOException {
        return getConnectorRepository().storeAsset(id, assetId, assetName, content);
    }

    @Override
    public List<Asset> getAssets(final String id) {
        return getConnectorRepository().getAssets(id);
    }

    @Override
    public Optional<Asset> getAsset(final String assetId) {
        return getConnectorRepository().getAsset(assetId);
    }

    @Override
    public List<ConnectorMigrationSource> getMigrationSources(final String id) {
        requireConnector(id, ConnectorSyncMode.LOCAL_ONLY);
        return connectorMigrationManager.listMigrationSources(id);
    }

    @Override
    public void verifyCanMigrateFromVersionedFlow(final String connectorId, final String processGroupId) {
        requireConnector(connectorId, ConnectorSyncMode.LOCAL_ONLY);
        connectorMigrationManager.verifyEligibility(connectorId, processGroupId);
    }

    @Override
    public void verifyConnectorReadyForMigration(final String connectorId) {
        requireConnector(connectorId, ConnectorSyncMode.LOCAL_ONLY);
        connectorMigrationManager.verifyConnectorReadyForMigration(connectorId);
    }

    @Override
    public void migrateFromVersionedFlow(final String connectorId, final String processGroupId, final VersionedExternalFlow sourceFlow,
            final BooleanSupplier cancellationCheck) {
        requireConnector(connectorId, ConnectorSyncMode.LOCAL_ONLY);
        try {
            connectorMigrationManager.migrateFromVersionedFlow(connectorId, processGroupId, sourceFlow, cancellationCheck);
        } catch (final Exception e) {
            throw new NiFiCoreException("Failed to migrate Connector from Versioned Flow: " + e.getMessage(), e);
        }
    }
}


