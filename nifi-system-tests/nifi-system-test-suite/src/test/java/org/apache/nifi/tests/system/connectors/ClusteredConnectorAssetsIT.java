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

import org.apache.nifi.components.connector.ConnectorState;
import org.apache.nifi.tests.system.NiFiInstanceFactory;
import org.apache.nifi.toolkit.client.ConnectorClient;
import org.apache.nifi.toolkit.client.NiFiClientException;
import org.apache.nifi.util.file.FileUtils;
import org.apache.nifi.web.api.dto.AssetReferenceDTO;
import org.apache.nifi.web.api.dto.ConnectorValueReferenceDTO;
import org.apache.nifi.web.api.entity.AssetEntity;
import org.apache.nifi.web.api.entity.AssetsEntity;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Clustered variants of the connector system tests that verify connector assets
 * are synchronized across all cluster nodes.
 *
 * The structure of these tests is based on the clustered parameter context
 * asset synchronization tests in {@code ClusteredParameterContextIT}.
 */
public class ClusteredConnectorAssetsIT extends ConnectorAssetsIT {

    @Override
    public NiFiInstanceFactory getInstanceFactory() {
        return createTwoNodeInstanceFactory();
    }

    @Test
    public void testSynchronizeConnectorAssets() throws NiFiClientException, IOException, InterruptedException {
        waitForAllNodesConnected();

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

        final String uploadedAssetId = assetEntity.getAsset().getId();

        // List the Connector's Assets and verify that the uploaded Asset is present
        final AssetsEntity assetsEntity = connectorClient.getAssets(connectorId);
        assertNotNull(assetsEntity);
        assertNotNull(assetsEntity.getAssets());
        assertFalse(assetsEntity.getAssets().isEmpty());

        final boolean assetFound = assetsEntity.getAssets().stream()
                .filter(a -> a.getAsset() != null)
                .anyMatch(a -> uploadedAssetId.equals(a.getAsset().getId()));

        assertTrue(assetFound);

        // Check that the asset exists in the connector_assets directory for each node
        final File node1Dir = getNiFiInstance().getNodeInstance(1).getInstanceDirectory();
        final File node1AssetsDir = new File(node1Dir, "connector_assets");
        final File node1ConnectorDir = new File(node1AssetsDir, connectorId);
        assertTrue(node1ConnectorDir.exists());

        final File node2Dir = getNiFiInstance().getNodeInstance(2).getInstanceDirectory();
        final File node2AssetsDir = new File(node2Dir, "connector_assets");
        final File node2ConnectorDir = new File(node2AssetsDir, connectorId);
        assertTrue(node2ConnectorDir.exists());

        final File[] node1AssetIdDirs = node1ConnectorDir.listFiles();
        assertNotNull(node1AssetIdDirs);
        assertEquals(1, node1AssetIdDirs.length);

        final File[] node2AssetIdDirs = node2ConnectorDir.listFiles();
        assertNotNull(node2AssetIdDirs);
        assertEquals(1, node2AssetIdDirs.length);

        // Configure the connector's "Test Asset" property to reference the uploaded asset
        final ConnectorValueReferenceDTO assetValueReference = new ConnectorValueReferenceDTO();
        assetValueReference.setValueType("ASSET_REFERENCE");
        assetValueReference.setAssetReferences(List.of(new AssetReferenceDTO(uploadedAssetId)));
        getClientUtil().configureConnectorWithReferences(connectorId, "Asset Configuration", Map.of("Test Asset", assetValueReference));

        // Apply the updates to the connector
        getClientUtil().applyConnectorUpdate(connector);

        // Start the connector before disconnecting node 2
        getClientUtil().startConnector(connectorId);

        // Stop node 2 and delete its connector_assets directory
        disconnectNode(2);
        getNiFiInstance().getNodeInstance(2).stop();

        FileUtils.deleteFilesInDir(node2AssetsDir, (dir, name) -> true, null, true, true);
        assertTrue(node2AssetsDir.delete());
        assertFalse(node2AssetsDir.exists());

        // Start node 2 again and wait for it to rejoin the cluster
        getNiFiInstance().getNodeInstance(2).start(true);
        waitForAllNodesConnected();

        // Verify that the connector state is RUNNING after node 2 rejoins
        getClientUtil().waitForConnectorState(connectorId, ConnectorState.RUNNING);
        getClientUtil().waitForValidConnector(connectorId);

        // Verify node 2 connector assets directory is recreated and contains the expected asset
        assertTrue(node2AssetsDir.exists());
        assertTrue(node2ConnectorDir.exists());

        final File[] node2AssetIdDirsAfterRestart = node2ConnectorDir.listFiles();
        assertNotNull(node2AssetIdDirsAfterRestart);
        assertEquals(1, node2AssetIdDirsAfterRestart.length);
        assertEquals(uploadedAssetId, node2AssetIdDirsAfterRestart[0].getName());

        // Verify that the Asset is still returned from the Connector's Assets list
        final AssetsEntity assetsAfterRestart = connectorClient.getAssets(connectorId);
        assertNotNull(assetsAfterRestart);
        assertNotNull(assetsAfterRestart.getAssets());

        final boolean assetStillPresent = assetsAfterRestart.getAssets().stream()
                .filter(a -> a.getAsset() != null)
                .anyMatch(a -> uploadedAssetId.equals(a.getAsset().getId()));

        assertTrue(assetStillPresent);
    }
}
