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

package org.apache.nifi.mock.connector.server;

import org.apache.nifi.asset.Asset;
import org.apache.nifi.asset.AssetManagerInitializationContext;
import org.apache.nifi.asset.AssetReferenceLookup;
import org.apache.nifi.controller.NodeTypeProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MockConnectorAssetManagerTest {

    private static final String CONNECTOR_ID = "test-connector";
    private static final String ASSET_NAME = "test-asset.txt";
    private static final String ASSET_CONTENTS = "test-contents";

    @TempDir
    private Path tempDir;

    private MockConnectorAssetManager assetManager;

    @BeforeEach
    void setUp() {
        assetManager = new MockConnectorAssetManager();
        final AssetManagerInitializationContext context = createInitializationContext(tempDir);
        assetManager.initialize(context);
    }

    @Test
    void testCreateAsset() throws IOException {
        final InputStream contents = new ByteArrayInputStream(ASSET_CONTENTS.getBytes(StandardCharsets.UTF_8));

        final Asset asset = assetManager.createAsset(CONNECTOR_ID, ASSET_NAME, contents);

        assertNotNull(asset);
        assertNotNull(asset.getIdentifier());
        assertEquals(CONNECTOR_ID, asset.getOwnerIdentifier());
        assertEquals(ASSET_NAME, asset.getName());
        assertTrue(asset.getFile().exists());
        assertEquals(ASSET_CONTENTS, Files.readString(asset.getFile().toPath(), StandardCharsets.UTF_8));
    }

    @Test
    void testCreateAssetGeneratesUniqueIdentifiers() throws IOException {
        final InputStream contents1 = new ByteArrayInputStream(ASSET_CONTENTS.getBytes(StandardCharsets.UTF_8));
        final InputStream contents2 = new ByteArrayInputStream(ASSET_CONTENTS.getBytes(StandardCharsets.UTF_8));

        final Asset asset1 = assetManager.createAsset(CONNECTOR_ID, ASSET_NAME, contents1);
        final Asset asset2 = assetManager.createAsset(CONNECTOR_ID, ASSET_NAME, contents2);

        assertNotEquals(asset1.getIdentifier(), asset2.getIdentifier());
    }

    @Test
    void testSaveAsset() throws IOException {
        final String assetId = "specific-asset-id";
        final InputStream contents = new ByteArrayInputStream(ASSET_CONTENTS.getBytes(StandardCharsets.UTF_8));

        final Asset asset = assetManager.saveAsset(CONNECTOR_ID, assetId, ASSET_NAME, contents);

        assertNotNull(asset);
        assertEquals(assetId, asset.getIdentifier());
        assertEquals(CONNECTOR_ID, asset.getOwnerIdentifier());
        assertEquals(ASSET_NAME, asset.getName());
        assertTrue(asset.getFile().exists());
        assertEquals(ASSET_CONTENTS, Files.readString(asset.getFile().toPath(), StandardCharsets.UTF_8));
    }

    @Test
    void testSaveAssetOverwritesExisting() throws IOException {
        final String assetId = "overwrite-asset-id";
        final String originalContents = "original";
        final String updatedContents = "updated";

        final InputStream originalStream = new ByteArrayInputStream(originalContents.getBytes(StandardCharsets.UTF_8));
        final Asset originalAsset = assetManager.saveAsset(CONNECTOR_ID, assetId, ASSET_NAME, originalStream);
        assertEquals(originalContents, Files.readString(originalAsset.getFile().toPath(), StandardCharsets.UTF_8));

        final InputStream updatedStream = new ByteArrayInputStream(updatedContents.getBytes(StandardCharsets.UTF_8));
        final Asset updatedAsset = assetManager.saveAsset(CONNECTOR_ID, assetId, ASSET_NAME, updatedStream);
        assertEquals(updatedContents, Files.readString(updatedAsset.getFile().toPath(), StandardCharsets.UTF_8));
    }

    @Test
    void testGetAsset() throws IOException {
        final InputStream contents = new ByteArrayInputStream(ASSET_CONTENTS.getBytes(StandardCharsets.UTF_8));
        final Asset createdAsset = assetManager.createAsset(CONNECTOR_ID, ASSET_NAME, contents);

        final Optional<Asset> retrieved = assetManager.getAsset(createdAsset.getIdentifier());

        assertTrue(retrieved.isPresent());
        assertEquals(createdAsset.getIdentifier(), retrieved.get().getIdentifier());
        assertEquals(createdAsset.getName(), retrieved.get().getName());
        assertEquals(createdAsset.getOwnerIdentifier(), retrieved.get().getOwnerIdentifier());
    }

    @Test
    void testGetAssetReturnsEmptyForNonExistent() {
        final Optional<Asset> retrieved = assetManager.getAsset("non-existent-id");

        assertTrue(retrieved.isEmpty());
    }

    @Test
    void testGetAssets() throws IOException {
        final InputStream contents1 = new ByteArrayInputStream("content1".getBytes(StandardCharsets.UTF_8));
        final InputStream contents2 = new ByteArrayInputStream("content2".getBytes(StandardCharsets.UTF_8));
        final InputStream contents3 = new ByteArrayInputStream("content3".getBytes(StandardCharsets.UTF_8));

        assetManager.createAsset(CONNECTOR_ID, "asset1.txt", contents1);
        assetManager.createAsset(CONNECTOR_ID, "asset2.txt", contents2);
        assetManager.createAsset("other-connector", "asset3.txt", contents3);

        final List<Asset> connectorAssets = assetManager.getAssets(CONNECTOR_ID);
        assertEquals(2, connectorAssets.size());
        assertTrue(connectorAssets.stream().allMatch(a -> a.getOwnerIdentifier().equals(CONNECTOR_ID)));

        final List<Asset> otherAssets = assetManager.getAssets("other-connector");
        assertEquals(1, otherAssets.size());
        assertEquals("other-connector", otherAssets.get(0).getOwnerIdentifier());
    }

    @Test
    void testGetAssetsReturnsEmptyListForUnknownOwner() {
        final List<Asset> assets = assetManager.getAssets("unknown-owner");

        assertNotNull(assets);
        assertTrue(assets.isEmpty());
    }

    @Test
    void testCreateMissingAsset() {
        final Asset missingAsset = assetManager.createMissingAsset(CONNECTOR_ID, ASSET_NAME);

        assertNotNull(missingAsset);
        assertNotNull(missingAsset.getIdentifier());
        assertEquals(CONNECTOR_ID, missingAsset.getOwnerIdentifier());
        assertEquals(ASSET_NAME, missingAsset.getName());
        assertFalse(missingAsset.getFile().exists());
        assertTrue(missingAsset.getDigest().isEmpty());
    }

    @Test
    void testCreateMissingAssetCanBeRetrieved() {
        final Asset missingAsset = assetManager.createMissingAsset(CONNECTOR_ID, ASSET_NAME);

        final Optional<Asset> retrieved = assetManager.getAsset(missingAsset.getIdentifier());
        assertTrue(retrieved.isPresent());
        assertEquals(missingAsset.getIdentifier(), retrieved.get().getIdentifier());

        final List<Asset> ownerAssets = assetManager.getAssets(CONNECTOR_ID);
        assertEquals(1, ownerAssets.size());
    }

    @Test
    void testDeleteAsset() throws IOException {
        final InputStream contents = new ByteArrayInputStream(ASSET_CONTENTS.getBytes(StandardCharsets.UTF_8));
        final Asset createdAsset = assetManager.createAsset(CONNECTOR_ID, ASSET_NAME, contents);
        final Path assetFilePath = createdAsset.getFile().toPath();
        assertTrue(Files.exists(assetFilePath));

        final Optional<Asset> deleted = assetManager.deleteAsset(createdAsset.getIdentifier());

        assertTrue(deleted.isPresent());
        assertEquals(createdAsset.getIdentifier(), deleted.get().getIdentifier());
        assertFalse(Files.exists(assetFilePath));
        assertTrue(assetManager.getAsset(createdAsset.getIdentifier()).isEmpty());
    }

    @Test
    void testDeleteAssetReturnsEmptyForNonExistent() {
        final Optional<Asset> deleted = assetManager.deleteAsset("non-existent-id");

        assertTrue(deleted.isEmpty());
    }

    @Test
    void testInitializeCreatesStorageDirectory() {
        final Path newStorageDir = tempDir.resolve("new-storage");
        assertFalse(Files.exists(newStorageDir));

        final MockConnectorAssetManager newManager = new MockConnectorAssetManager();
        final AssetManagerInitializationContext context = createInitializationContext(newStorageDir);
        newManager.initialize(context);

        assertTrue(Files.exists(newStorageDir));
        assertTrue(Files.isDirectory(newStorageDir));
    }

    private AssetManagerInitializationContext createInitializationContext(final Path storageDirectory) {
        final Map<String, String> properties = new HashMap<>();
        properties.put("directory", storageDirectory.toAbsolutePath().toString());

        return new AssetManagerInitializationContext() {
            @Override
            public AssetReferenceLookup getAssetReferenceLookup() {
                return null;
            }

            @Override
            public Map<String, String> getProperties() {
                return properties;
            }

            @Override
            public NodeTypeProvider getNodeTypeProvider() {
                return null;
            }
        };
    }
}
