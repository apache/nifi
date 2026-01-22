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

import org.apache.nifi.nar.FileDigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * AssetManager implementation for Connectors. This differs from the StandardAssetManager used for Parameter Contexts in that
 * Connectors want every asset added to be treated as a new asset, even if the same filename is being saved to the same Connector.
 * This is because all changes to the Connector needs to be stored separately in the working configuration until applied and can
 * not interfere with the active configuration.
 */
public class StandardConnectorAssetManager implements AssetManager {

    private static final Logger logger = LoggerFactory.getLogger(StandardConnectorAssetManager.class);

    public static final String ASSET_STORAGE_LOCATION_PROPERTY = "directory";
    public static final String DEFAULT_ASSET_STORAGE_LOCATION = "./connector_assets";

    private volatile File assetStorageLocation;
    private final Map<String, Asset> assets = new ConcurrentHashMap<>();

    @Override
    public void initialize(final AssetManagerInitializationContext context) {
        final String storageLocation = getStorageLocation(context);

        assetStorageLocation = new File(storageLocation);
        if (!assetStorageLocation.exists()) {
            try {
                Files.createDirectories(assetStorageLocation.toPath());
            } catch (IOException e) {
                throw new RuntimeException("The Connector Asset Manager's [%s] property is set to [%s] but the directory does not exist and cannot be created"
                    .formatted(ASSET_STORAGE_LOCATION_PROPERTY, storageLocation), e);
            }
        }

        try {
            recoverLocalAssets();
        } catch (final IOException e) {
            throw new RuntimeException("Unable to access connector assets", e);
        }
    }

    @Override
    public Asset createAsset(final String connectorId, final String assetName, final InputStream contents) throws IOException {
        final String assetId = generateAssetId();
        final File assetFile = getFile(connectorId, assetId, assetName);
        return saveAsset(connectorId, assetId, assetName, contents, assetFile);
    }

    @Override
    public Asset saveAsset(final String connectorId, final String assetId, final String assetName, final InputStream contents) throws IOException {
        final File assetFile = getFile(connectorId, assetId, assetName);
        return saveAsset(connectorId, assetId, assetName, contents, assetFile);
    }

    @Override
    public Optional<Asset> getAsset(final String id) {
        return Optional.ofNullable(assets.get(id));
    }

    @Override
    public List<Asset> getAssets(final String connectorId) {
        final List<Asset> allAssets = new ArrayList<>(assets.values());
        final List<Asset> ownerAssets = new ArrayList<>();
        for (final Asset asset : allAssets) {
            if (asset.getOwnerIdentifier().equals(connectorId)) {
                ownerAssets.add(asset);
            }
        }
        return ownerAssets;
    }

    @Override
    public Asset createMissingAsset(final String connectorId, final String assetName) {
        final String assetId = generateAssetId();
        final File file = getFile(connectorId, assetId, assetName);
        final Asset asset = new StandardAsset(assetId, connectorId, assetName, file, null);
        assets.put(assetId, asset);
        return asset;
    }

    @Override
    public Optional<Asset> deleteAsset(final String id) {
        final Asset removed = assets.remove(id);
        if (removed == null) {
            return Optional.empty();
        }

        final File assetFile = removed.getFile();
        if (assetFile.exists()) {
            deleteFile(assetFile);

            final File assetIdDir = assetFile.getParentFile();
            deleteFile(assetIdDir);

            final File connectorDir = assetIdDir.getParentFile();
            final File[] children = connectorDir.listFiles();
            if (children != null && children.length == 0) {
                deleteFile(connectorDir);
            }
        }

        return Optional.of(removed);
    }

    private Asset saveAsset(final String connectorId, final String assetId, final String assetName, final InputStream contents, final File assetFile) throws IOException {
        final File dir = assetFile.getParentFile();
        if (!dir.exists()) {
            try {
                Files.createDirectories(dir.toPath());
            } catch (final IOException ioe) {
                throw new IOException("Could not create directory in order to store connector asset", ioe);
            }
        }

        try {
            Files.copy(contents, assetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch (final Exception e) {
            throw new IOException("Failed to write connector asset to file " + assetFile.getAbsolutePath(), e);
        }

        final String digest = computeDigest(assetFile);
        final Asset asset = new StandardAsset(assetId, connectorId, assetName, assetFile, digest);
        assets.put(assetId, asset);
        return asset;
    }

    private void deleteFile(final File file) {
        try {
            Files.delete(file.toPath());
        } catch (final IOException e) {
            logger.warn("Failed to delete [{}]", file.getAbsolutePath(), e);
        }
    }

    private String generateAssetId() {
        return UUID.randomUUID().toString();
    }

    private File getFile(final String connectorId, final String assetId, final String assetName) {
        final Path parentPath = assetStorageLocation.toPath().normalize();
        final Path assetPath = Paths.get(connectorId, assetId, assetName).normalize();
        final Path fullPath = parentPath.resolve(assetPath);
        return fullPath.toFile();
    }

    private String getStorageLocation(final AssetManagerInitializationContext initializationContext) {
        final String storageLocation = initializationContext.getProperties().get(ASSET_STORAGE_LOCATION_PROPERTY);
        return storageLocation == null ? DEFAULT_ASSET_STORAGE_LOCATION : storageLocation;
    }

    private void recoverLocalAssets() throws IOException {
        final File[] connectorDirectories = assetStorageLocation.listFiles();
        if (connectorDirectories == null) {
            throw new IOException("Unable to list files for connector asset storage location %s".formatted(assetStorageLocation.getAbsolutePath()));
        }

        for (final File connectorDirectory : connectorDirectories) {
            if (!connectorDirectory.isDirectory()) {
                continue;
            }

            final String connectorId = connectorDirectory.getName();
            final File[] assetIdDirectories = connectorDirectory.listFiles();
            if (assetIdDirectories == null) {
                logger.warn("Unable to list directory [{}]", connectorDirectory.getAbsolutePath());
                continue;
            }

            for (final File assetIdDirectory : assetIdDirectories) {
                final String assetId = assetIdDirectory.getName();

                final File[] assetFiles = assetIdDirectory.listFiles();
                if (assetFiles == null) {
                    logger.warn("Unable to list directory [{}]", assetIdDirectory.getAbsolutePath());
                    continue;
                }

                for (final File assetFile : assetFiles) {
                    final String assetName = assetFile.getName();
                    final String digest = computeDigest(assetFile);
                    final Asset asset = new StandardAsset(assetId, connectorId, assetName, assetFile, digest);
                    assets.put(assetId, asset);
                }
            }
        }
    }

    private String computeDigest(final File file) throws IOException {
        return HexFormat.of().formatHex(FileDigestUtils.getDigest(file));
    }

}
