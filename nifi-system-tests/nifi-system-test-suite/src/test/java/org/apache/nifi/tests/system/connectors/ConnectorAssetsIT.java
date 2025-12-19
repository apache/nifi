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

package org.apache.nifi.tests.system.connectors;

import org.apache.nifi.tests.system.NiFiSystemIT;
import org.apache.nifi.toolkit.client.ConnectorClient;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.web.api.dto.AssetReferenceDTO;
import org.apache.nifi.web.api.dto.ConfigurationStepConfigurationDTO;
import org.apache.nifi.web.api.dto.ConnectorConfigurationDTO;
import org.apache.nifi.web.api.dto.ConnectorValueReferenceDTO;
import org.apache.nifi.web.api.dto.PropertyGroupConfigurationDTO;
import org.apache.nifi.web.api.entity.AssetEntity;
import org.apache.nifi.web.api.entity.AssetsEntity;
import org.apache.nifi.web.api.entity.ConfigurationStepEntity;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ConnectorAssetsIT extends NiFiSystemIT {

    @Test
    public void testCreateConnectorAndUploadAsset() throws NiFiClientException, IOException, InterruptedException {
        // Create a Connector instance using the AssetConnector test extension
        final ConnectorEntity connector = getClientUtil().createConnector("AssetConnector");
        assertNotNull(connector);
        assertNotNull(connector.getId());

        final String connectorId = connector.getId();

        // Upload an Asset to the Connector
        final File assetFile = new File("src/test/resources/sample-assets/helloworld.txt");
        final ConnectorClient connectorClient = getNifiClient().getConnectorClient();
        final AssetEntity assetEntity = connectorClient.createAsset(connectorId, assetFile.getName(), assetFile);

        assertNotNull(assetEntity);
        assertNotNull(assetEntity.getAsset());
        assertNotNull(assetEntity.getAsset().getId());
        assertEquals(assetFile.getName(), assetEntity.getAsset().getName());

        // List the Connector's Assets and verify that the uploaded Asset is present
        final AssetsEntity assetsEntity = connectorClient.getAssets(connectorId);
        assertNotNull(assetsEntity);
        assertNotNull(assetsEntity.getAssets());
        assertFalse(assetsEntity.getAssets().isEmpty());

        final String uploadedAssetId = assetEntity.getAsset().getId();
        final boolean assetFound = assetsEntity.getAssets().stream()
                .filter(a -> a.getAsset() != null)
                .anyMatch(a -> uploadedAssetId.equals(a.getAsset().getId()));

        assertTrue(assetFound);

        // Update the configuration step to reference the uploaded Asset in the Test Asset property
        final String configurationStepName = "Asset Configuration";
        final String propertyGroupName = "Asset Configuration";
        final String propertyName = "Test Asset";

        final ConfigurationStepEntity configurationStepEntity = connectorClient.getConfigurationStep(connectorId, configurationStepName);
        assertNotNull(configurationStepEntity);
        assertNotNull(configurationStepEntity.getConfigurationStep());

        final ConfigurationStepConfigurationDTO configurationStepConfiguration = configurationStepEntity.getConfigurationStep();
        assertNotNull(configurationStepConfiguration.getPropertyGroupConfigurations());
        assertFalse(configurationStepConfiguration.getPropertyGroupConfigurations().isEmpty());

        PropertyGroupConfigurationDTO assetConfigurationGroup = null;
        for (final PropertyGroupConfigurationDTO group : configurationStepConfiguration.getPropertyGroupConfigurations()) {
            if (propertyGroupName.equals(group.getPropertyGroupName())) {
                assetConfigurationGroup = group;
                break;
            }
        }
        assertNotNull(assetConfigurationGroup);

        Map<String, ConnectorValueReferenceDTO> propertyValues = assetConfigurationGroup.getPropertyValues();
        if (propertyValues == null) {
            propertyValues = new HashMap<>();
            assetConfigurationGroup.setPropertyValues(propertyValues);
        }

        final ConnectorValueReferenceDTO assetValueReference = new ConnectorValueReferenceDTO();
        assetValueReference.setValueType("ASSET_REFERENCE");

        final AssetReferenceDTO assetReferenceDTO = new AssetReferenceDTO(uploadedAssetId);
        assetValueReference.setAssetReferences(List.of(assetReferenceDTO));

        propertyValues.put(propertyName, assetValueReference);

        final ConfigurationStepEntity updatedConfigurationStepEntity = connectorClient.updateConfigurationStep(configurationStepEntity);
        assertNotNull(updatedConfigurationStepEntity);

        // Retrieve the Connector configuration and verify that the Test Asset property has the Asset reference set
        final ConnectorEntity connectorWithConfiguration = connectorClient.getConnector(connectorId);
        assertNotNull(connectorWithConfiguration);
        assertNotNull(connectorWithConfiguration.getComponent());

        final ConnectorConfigurationDTO workingConfiguration = connectorWithConfiguration.getComponent().getWorkingConfiguration();
        assertNotNull(workingConfiguration);
        assertNotNull(workingConfiguration.getConfigurationStepConfigurations());
        assertFalse(workingConfiguration.getConfigurationStepConfigurations().isEmpty());

        ConfigurationStepConfigurationDTO configuredStep = null;
        for (final ConfigurationStepConfigurationDTO stepConfiguration : workingConfiguration.getConfigurationStepConfigurations()) {
            if (configurationStepName.equals(stepConfiguration.getConfigurationStepName())) {
                configuredStep = stepConfiguration;
                break;
            }
        }
        assertNotNull(configuredStep);
        assertNotNull(configuredStep.getPropertyGroupConfigurations());
        assertFalse(configuredStep.getPropertyGroupConfigurations().isEmpty());

        PropertyGroupConfigurationDTO configuredAssetGroup = null;
        for (final PropertyGroupConfigurationDTO group : configuredStep.getPropertyGroupConfigurations()) {
            if (propertyGroupName.equals(group.getPropertyGroupName())) {
                configuredAssetGroup = group;
                break;
            }
        }
        assertNotNull(configuredAssetGroup);
        assertNotNull(configuredAssetGroup.getPropertyValues());

        final ConnectorValueReferenceDTO configuredAssetValue = configuredAssetGroup.getPropertyValues().get(propertyName);
        assertNotNull(configuredAssetValue);
        assertEquals("ASSET_REFERENCE", configuredAssetValue.getValueType());
        assertNotNull(configuredAssetValue.getAssetReferences());
        assertFalse(configuredAssetValue.getAssetReferences().isEmpty());
        assertEquals(uploadedAssetId, configuredAssetValue.getAssetReferences().getFirst().getId());

        // Update the configuration step again to remove the Asset reference
        final ConfigurationStepEntity configurationStepEntityWithoutAsset = connectorClient.getConfigurationStep(connectorId, configurationStepName);
        assertNotNull(configurationStepEntityWithoutAsset);
        assertNotNull(configurationStepEntityWithoutAsset.getConfigurationStep());

        final ConfigurationStepConfigurationDTO configurationStepConfigurationWithoutAsset = configurationStepEntityWithoutAsset.getConfigurationStep();
        assertNotNull(configurationStepConfigurationWithoutAsset.getPropertyGroupConfigurations());

        PropertyGroupConfigurationDTO groupWithoutAsset = null;
        for (final PropertyGroupConfigurationDTO group : configurationStepConfigurationWithoutAsset.getPropertyGroupConfigurations()) {
            if (propertyGroupName.equals(group.getPropertyGroupName())) {
                groupWithoutAsset = group;
                break;
            }
        }
        assertNotNull(groupWithoutAsset);

        configuredAssetValue.setAssetReferences(null);
        final Map<String, ConnectorValueReferenceDTO> propertyValuesWithoutAsset = groupWithoutAsset.getPropertyValues();
        propertyValuesWithoutAsset.put(propertyName, configuredAssetValue);

        final ConfigurationStepEntity configurationStepEntityAfterRemoval = connectorClient.updateConfigurationStep(configurationStepEntityWithoutAsset);
        assertNotNull(configurationStepEntityAfterRemoval);

        // Apply the connector update so that unreferenced assets are cleaned up based on the active configuration
        final ConnectorEntity connectorBeforeApply = connectorClient.getConnector(connectorId);
        assertNotNull(connectorBeforeApply);
        connectorBeforeApply.setDisconnectedNodeAcknowledged(true);

        final ConnectorEntity connectorAfterApply = connectorClient.applyUpdate(connectorBeforeApply);
        assertNotNull(connectorAfterApply);

        // Verify that the Asset has been removed from the Connector's Assets list
        final AssetsEntity assetsAfterRemoval = connectorClient.getAssets(connectorId);
        assertNotNull(assetsAfterRemoval);

        final boolean assetStillPresent = assetsAfterRemoval.getAssets() != null && assetsAfterRemoval.getAssets().stream()
                .filter(a -> a.getAsset() != null)
                .anyMatch(a -> uploadedAssetId.equals(a.getAsset().getId()));

        assertFalse(assetStillPresent);

        // Wait for Connector to stop before attempting to delete it.
        getClientUtil().waitForConnectorStopped(connectorAfterApply.getId());
        connectorClient.deleteConnector(connectorAfterApply);
    }
}


