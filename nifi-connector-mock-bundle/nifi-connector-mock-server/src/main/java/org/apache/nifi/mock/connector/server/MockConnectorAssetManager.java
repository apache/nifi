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
import org.apache.nifi.asset.AssetManager;
import org.apache.nifi.asset.AssetManagerInitializationContext;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A Mock implementation of AssetManager for use in ConnectorTestRunner tests.
 * This implementation stores assets in a temporary directory structure.
 */
public class MockConnectorAssetManager implements AssetManager {

    private static final String ASSET_STORAGE_LOCATION_PROPERTY = "directory";
    private static final String DEFAULT_ASSET_STORAGE_LOCATION = "target/mock-connector-assets";

    private final Map<String, Asset> assets = new ConcurrentHashMap<>();
    private volatile File assetStorageLocation;

    @Override
    public void initialize(final AssetManagerInitializationContext context) {
        final String storageLocation = context.getProperties().getOrDefault(ASSET_STORAGE_LOCATION_PROPERTY, DEFAULT_ASSET_STORAGE_LOCATION);
        assetStorageLocation = new File(storageLocation);

        if (!assetStorageLocation.exists()) {
            try {
                Files.createDirectories(assetStorageLocation.toPath());
            } catch (final IOException e) {
                throw new RuntimeException("Failed to create asset storage directory: " + storageLocation, e);
            }
        }
    }

    @Override
    public Asset createAsset(final String ownerId, final String assetName, final InputStream contents) throws IOException {
        final String assetId = UUID.randomUUID().toString();
        return saveAsset(ownerId, assetId, assetName, contents);
    }

    @Override
    public Asset saveAsset(final String ownerId, final String assetId, final String assetName, final InputStream contents) throws IOException {
        final File assetFile = getFile(ownerId, assetId, assetName);
        final File parentDir = assetFile.getParentFile();

        if (!parentDir.exists()) {
            Files.createDirectories(parentDir.toPath());
        }

        Files.copy(contents, assetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

        final Asset asset = new MockAsset(assetId, ownerId, assetName, assetFile, null);
        assets.put(assetId, asset);
        return asset;
    }

    @Override
    public Optional<Asset> getAsset(final String id) {
        return Optional.ofNullable(assets.get(id));
    }

    @Override
    public List<Asset> getAssets(final String ownerId) {
        final List<Asset> ownerAssets = new ArrayList<>();
        for (final Asset asset : assets.values()) {
            if (asset.getOwnerIdentifier().equals(ownerId)) {
                ownerAssets.add(asset);
            }
        }
        return ownerAssets;
    }

    @Override
    public Asset createMissingAsset(final String ownerId, final String assetName) {
        final String assetId = UUID.randomUUID().toString();
        final File file = getFile(ownerId, assetId, assetName);
        final Asset asset = new MockAsset(assetId, ownerId, assetName, file, null);
        assets.put(assetId, asset);
        return asset;
    }

    @Override
    public Optional<Asset> deleteAsset(final String id) {
        final Asset removed = assets.remove(id);
        if (removed != null && removed.getFile().exists()) {
            try {
                Files.delete(removed.getFile().toPath());
            } catch (final IOException e) {
                throw new RuntimeException("Failed to delete asset " + id + " from storage file " + removed.getFile().getAbsolutePath(), e);
            }
        }

        return Optional.ofNullable(removed);
    }

    private File getFile(final String ownerId, final String assetId, final String assetName) {
        final Path parentPath = assetStorageLocation.toPath().normalize();
        final Path assetPath = Path.of(ownerId, assetId, assetName).normalize();
        return parentPath.resolve(assetPath).toFile();
    }

    /**
     * A simple Asset implementation for the mock.
     */
    private static class MockAsset implements Asset {
        private final String identifier;
        private final String ownerIdentifier;
        private final String name;
        private final File file;
        private final String digest;

        MockAsset(final String identifier, final String ownerIdentifier, final String name, final File file, final String digest) {
            this.identifier = identifier;
            this.ownerIdentifier = ownerIdentifier;
            this.name = name;
            this.file = file;
            this.digest = digest;
        }

        @Override
        public String getIdentifier() {
            return identifier;
        }

        @Override
        @Deprecated
        public String getParameterContextIdentifier() {
            return ownerIdentifier;
        }

        @Override
        public String getOwnerIdentifier() {
            return ownerIdentifier;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public File getFile() {
            return file;
        }

        @Override
        public Optional<String> getDigest() {
            return Optional.ofNullable(digest);
        }
    }
}
