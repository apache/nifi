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
import java.nio.charset.StandardCharsets;
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

public class StandardAssetManager implements AssetManager {
    private static final Logger logger = LoggerFactory.getLogger(StandardAssetManager.class);

    public static final String ASSET_STORAGE_LOCATION_PROPERTY = "directory";
    public static final String DEFAULT_ASSET_STORAGE_LOCATION = "./assets";

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
                throw new RuntimeException("The Asset Manager's [%s] property is set to [%s] but the directory does not exist and cannot be created"
                        .formatted(ASSET_STORAGE_LOCATION_PROPERTY, storageLocation), e);
            }
        }

        try {
            recoverLocalAssets();
        } catch (final IOException e) {
            throw new RuntimeException("Unable to access assets", e);
        }
    }

    @Override
    public Asset createAsset(final String ownerId, final String assetName, final InputStream contents) throws IOException {
        final String assetId = createAssetId(ownerId, assetName);
        final File assetFile = getFile(ownerId, assetName);
        return saveAsset(ownerId, assetId, assetName, contents, assetFile);
    }

    @Override
    public Asset saveAsset(final String ownerId, final String assetId, final String assetName, final InputStream contents) throws IOException {
        final String generatedAssetId = createAssetId(ownerId, assetName);
        if (!generatedAssetId.equals(assetId)) {
            throw new IllegalArgumentException("The provided asset id [%s] does not match the id this asset manager generated for the given owner and name".formatted(assetId));
        }
        final File assetFile = getFile(ownerId, assetName);
        return saveAsset(ownerId, assetId, assetName, contents, assetFile);
    }

    @Override
    public Optional<Asset> getAsset(final String id) {
        return Optional.ofNullable(assets.get(id));
    }

    @Override
    public List<Asset> getAssets(final String ownerId) {
        final List<Asset> allAssets = new ArrayList<>(assets.values());
        final List<Asset> ownerAssets = new ArrayList<>();
        for (final Asset asset : allAssets) {
            if (asset.getOwnerIdentifier().equals(ownerId)) {
                ownerAssets.add(asset);
            }
        }
        return ownerAssets;
    }

    @Override
    public Asset createMissingAsset(final String ownerId, final String assetName) {
        final String assetId = createAssetId(ownerId, assetName);
        final File file = getFile(ownerId, assetName);
        final Asset asset = new StandardAsset(assetId, ownerId, assetName, file, null);
        assets.put(assetId, asset);
        return asset;
    }

    @Override
    public Optional<Asset> deleteAsset(final String id) {
        final Asset removed = assets.remove(id);
        if (removed == null) {
            return Optional.empty();
        }

        final File file = removed.getFile();
        if (file.exists()) {
            try {
                Files.delete(file.toPath());
            } catch (final IOException e) {
                logger.warn("Failed to remove asset file {}", file.getAbsolutePath(), e);
            }

            final File parentDir = file.getParentFile();
            final File[] children = parentDir.listFiles();
            if (children != null && children.length == 0) {
                try {
                    Files.delete(parentDir.toPath());
                } catch (IOException e) {
                    logger.warn("Failed to remove empty asset directory {}", parentDir.getAbsolutePath(), e);
                }
            }
        }

        return Optional.of(removed);
    }

    private Asset saveAsset(final String ownerId, final String assetId, final String assetName, final InputStream contents, final File assetFile) throws IOException {
        final File dir = assetFile.getParentFile();
        if (!dir.exists()) {
            try {
                Files.createDirectories(dir.toPath());
            } catch (final IOException ioe) {
                throw new IOException("Could not create directory in order to store asset", ioe);
            }
        }

        // Write contents to a temporary file, then move it to the final location.
        // This allows us to avoid a situation where we upload a file, then we attempt to overwrite it but fail, leaving a corrupt asset.
        final File tempFile = new File(dir, assetFile.getName() + ".tmp");
        logger.debug("Writing temp asset file [{}]", tempFile.getAbsolutePath());

        try {
            Files.copy(contents, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch (final Exception e) {
            throw new IOException("Failed to write asset to file " + tempFile.getAbsolutePath(), e);
        }

        Files.move(tempFile.toPath(), assetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);

        final String digest = computeDigest(assetFile);
        final Asset asset = new StandardAsset(assetId, ownerId, assetName, assetFile, digest);
        assets.put(assetId, asset);
        return asset;
    }

    private String createAssetId(final String ownerId, final String assetName) {
        final String seed = ownerId + "/" + assetName;
        return UUID.nameUUIDFromBytes(seed.getBytes(StandardCharsets.UTF_8)).toString();
    }

    private File getFile(final String ownerId, final String assetName) {
        final Path parentPath = assetStorageLocation.toPath().normalize();
        final Path assetPath = Paths.get(ownerId, assetName).normalize();
        final Path fullPath = parentPath.resolve(assetPath);
        return fullPath.toFile();
    }

    private String getStorageLocation(final AssetManagerInitializationContext initializationContext) {
        final String storageLocation = initializationContext.getProperties().get(ASSET_STORAGE_LOCATION_PROPERTY);
        return storageLocation == null ? DEFAULT_ASSET_STORAGE_LOCATION : storageLocation;
    }

    private void recoverLocalAssets() throws IOException {
        final File[] files = assetStorageLocation.listFiles();
        if (files == null) {
            throw new IOException("Unable to list files for asset storage location %s".formatted(assetStorageLocation.getAbsolutePath()));
        }

        for (final File file : files) {
            if (!file.isDirectory()) {
                continue;
            }

            final String ownerId = file.getName();
            final File[] assetFiles = file.listFiles();
            if (assetFiles == null) {
                logger.warn("Unable to determine which assets exist for owner {}", ownerId);
                continue;
            }

            for (final File assetFile : assetFiles) {
                final String assetId = createAssetId(ownerId, assetFile.getName());
                final String digest = computeDigest(assetFile);
                final Asset asset = new StandardAsset(assetId, ownerId, assetFile.getName(), assetFile, digest);
                assets.put(assetId, asset);
            }
        }
    }

    private String computeDigest(final File file) throws IOException {
        return HexFormat.of().formatHex(FileDigestUtils.getDigest(file));
    }
}
