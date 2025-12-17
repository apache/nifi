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

package org.apache.nifi.asset;

import org.apache.nifi.controller.NodeTypeProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class StandardConnectorAssetManagerTest {

    @Test
    void testInitializeRecoversExistingAssets(@TempDir final Path tempDir) throws Exception {
        final Path storageDirectory = tempDir.resolve("assets");
        final String connectorIdentifier = "connector-1";
        final String assetIdentifier = "asset-1";
        final String assetName = "asset.txt";

        final Path assetDirectory = storageDirectory.resolve(connectorIdentifier).resolve(assetIdentifier);
        Files.createDirectories(assetDirectory);
        final Path assetPath = assetDirectory.resolve(assetName);
        Files.writeString(assetPath, "existing-content", StandardCharsets.UTF_8);

        final StandardConnectorAssetManager manager = new StandardConnectorAssetManager();
        final AssetManagerInitializationContext context = createInitializationContext(storageDirectory.toFile());
        manager.initialize(context);

        final List<Asset> assets = manager.getAssets(connectorIdentifier);
        assertEquals(1, assets.size());

        final Asset recoveredAsset = assets.getFirst();
        assertEquals(assetName, recoveredAsset.getName());
        assertEquals(connectorIdentifier, recoveredAsset.getOwnerIdentifier());
        assertTrue(recoveredAsset.getDigest().isPresent());
        assertEquals(assetPath.toFile().getAbsolutePath(), recoveredAsset.getFile().getAbsolutePath());
    }

    @Test
    void testCreateAssetAndGetAsset(@TempDir final Path tempDir) throws Exception {
        final Path storageDirectory = tempDir.resolve("assets");
        final StandardConnectorAssetManager manager = new StandardConnectorAssetManager();
        final AssetManagerInitializationContext context = createInitializationContext(storageDirectory.toFile());
        manager.initialize(context);

        assertTrue(Files.isDirectory(storageDirectory));

        final String connectorIdentifier = "connector-1";
        final String assetName = "created-asset.txt";
        final byte[] contents = "created-content".getBytes(StandardCharsets.UTF_8);
        final InputStream inputStream = new ByteArrayInputStream(contents);

        final Asset createdAsset = manager.createAsset(connectorIdentifier, assetName, inputStream);
        assertNotNull(createdAsset);
        assertEquals(connectorIdentifier, createdAsset.getOwnerIdentifier());
        assertEquals(assetName, createdAsset.getName());
        assertTrue(createdAsset.getDigest().isPresent());
        assertTrue(createdAsset.getFile().exists());

        final Optional<Asset> retrievedByIdentifier = manager.getAsset(createdAsset.getIdentifier());
        assertTrue(retrievedByIdentifier.isPresent());
        assertEquals(createdAsset.getIdentifier(), retrievedByIdentifier.get().getIdentifier());

        final List<Asset> connectorAssets = manager.getAssets(connectorIdentifier);
        assertEquals(1, connectorAssets.size());
        assertEquals(createdAsset.getIdentifier(), connectorAssets.get(0).getIdentifier());
    }

    @Test
    void testCreateMissingAssetDoesNotCreateFile(@TempDir final Path tempDir) {
        final Path storageDirectory = tempDir.resolve("assets");
        final StandardConnectorAssetManager manager = new StandardConnectorAssetManager();
        final AssetManagerInitializationContext context = createInitializationContext(storageDirectory.toFile());
        manager.initialize(context);

        final String connectorIdentifier = "connector-2";
        final String assetName = "missing-asset.txt";

        final Asset missingAsset = manager.createMissingAsset(connectorIdentifier, assetName);
        assertNotNull(missingAsset);
        assertEquals(connectorIdentifier, missingAsset.getOwnerIdentifier());
        assertEquals(assetName, missingAsset.getName());
        assertFalse(missingAsset.getDigest().isPresent());
        assertFalse(missingAsset.getFile().exists());

        final Optional<Asset> retrieved = manager.getAsset(missingAsset.getIdentifier());
        assertTrue(retrieved.isPresent());
        assertEquals(missingAsset.getIdentifier(), retrieved.get().getIdentifier());

        final List<Asset> connectorAssets = manager.getAssets(connectorIdentifier);
        assertEquals(1, connectorAssets.size());
        assertEquals(missingAsset.getIdentifier(), connectorAssets.getFirst().getIdentifier());
    }

    @Test
    void testDeleteAssetRemovesFilesAndDirectories(@TempDir final Path tempDir) throws Exception {
        final Path storageDirectory = tempDir.resolve("assets");
        final StandardConnectorAssetManager manager = new StandardConnectorAssetManager();
        final AssetManagerInitializationContext context = createInitializationContext(storageDirectory.toFile());
        manager.initialize(context);

        final String connectorIdentifier = "connector-3";
        final String assetName = "deletable-asset.txt";
        final byte[] contents = "deletable-content".getBytes(StandardCharsets.UTF_8);
        final InputStream inputStream = new ByteArrayInputStream(contents);

        final Asset asset = manager.createAsset(connectorIdentifier, assetName, inputStream);
        final File assetFile = asset.getFile();
        final File assetDirectory = assetFile.getParentFile();
        final File connectorDirectory = assetDirectory.getParentFile();

        assertTrue(assetFile.exists());
        assertTrue(assetDirectory.exists());
        assertTrue(connectorDirectory.exists());

        final Optional<Asset> deleted = manager.deleteAsset(asset.getIdentifier());
        assertTrue(deleted.isPresent());
        assertFalse(assetFile.exists());
        assertFalse(assetDirectory.exists());
        assertFalse(connectorDirectory.exists());

        assertTrue(manager.getAsset(asset.getIdentifier()).isEmpty());
        assertTrue(manager.getAssets(connectorIdentifier).isEmpty());
    }

    @Test
    void testDeleteNonExistentAssetReturnsEmpty(@TempDir final Path tempDir) {
        final Path storageDirectory = tempDir.resolve("assets");
        final StandardConnectorAssetManager manager = new StandardConnectorAssetManager();
        final AssetManagerInitializationContext context = createInitializationContext(storageDirectory.toFile());
        manager.initialize(context);

        final Optional<Asset> deleted = manager.deleteAsset("non-existent-id");
        assertTrue(deleted.isEmpty());
    }

    private AssetManagerInitializationContext createInitializationContext(final File storageDirectory) {
        final Map<String, String> properties = new HashMap<>();
        properties.put(StandardConnectorAssetManager.ASSET_STORAGE_LOCATION_PROPERTY, storageDirectory.getAbsolutePath());

        final AssetReferenceLookup assetReferenceLookup = mock(AssetReferenceLookup.class);
        final NodeTypeProvider nodeTypeProvider = mock(NodeTypeProvider.class);

        return new StandardAssetManagerInitializationContext(assetReferenceLookup, properties, nodeTypeProvider);
    }
}


