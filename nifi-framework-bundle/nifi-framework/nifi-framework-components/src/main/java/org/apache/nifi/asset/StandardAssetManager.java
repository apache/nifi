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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class StandardAssetManager implements AssetManager {
    private static final Logger logger = LoggerFactory.getLogger(StandardAssetManager.class);

    public static final String ASSET_STORAGE_LOCATION_PROPERTY = "directory";
    private volatile File assetStorageLocation;
    private final Map<String, Asset> assets = new ConcurrentHashMap<>();


    @Override
    public void initialize(final AssetManagerInitializationContext context) {
        final String storageLocation = context.getProperties().get(ASSET_STORAGE_LOCATION_PROPERTY);
        if (storageLocation == null) {
            throw new IllegalStateException("The Asset Manager's [" + ASSET_STORAGE_LOCATION_PROPERTY + "] property must be set");
        }

        assetStorageLocation = new File(storageLocation);
        if (!assetStorageLocation.exists()) {
            try {
                Files.createDirectories(assetStorageLocation.toPath());
            } catch (IOException e) {
                throw new RuntimeException("The Asset Manager's [" + ASSET_STORAGE_LOCATION_PROPERTY + "] property is set to [" + storageLocation +
                                           "] but the directory does not exist and cannot be created", e);
            }
        }

        try {
            recoverLocalAssets();
        } catch (final IOException e) {
            throw new RuntimeException("Unable to access assets", e);
        }
    }

    private void recoverLocalAssets() throws IOException {
        final File[] files = assetStorageLocation.listFiles();
        if (files == null) {
            throw new IOException("Unable to list files for asset storage location " + assetStorageLocation.getAbsolutePath());
        }

        for (final File file : files) {
            if (!file.isDirectory()) {
                continue;
            }

            final String contextId = file.getName();
            final File[] assetFiles = file.listFiles();
            if (assetFiles == null) {
                logger.warn("Unable to determine which assets exist for Parameter Context " + contextId);
                continue;
            }

            for (final File assetFile : assetFiles) {
                final String assetId = contextId + "/" + assetFile.getName();
                final Asset asset = new StandardAsset(assetId, assetId, assetFile);
                assets.put(assetId, asset);
            }
        }
    }

    @Override
    public Asset createAsset(final String parameterContextId, final String assetName, final InputStream contents) throws IOException {
        final String assetId = createAssetId(parameterContextId, assetName);

        final File file = getFile(assetId);
        final File dir = file.getParentFile();
        if (!dir.exists()) {
            try {
                Files.createDirectories(dir.toPath());
            } catch (final IOException ioe) {
                throw new IOException("Could not create directory in order to store asset", ioe);
            }
        }

        // Write contents to a temporary file, then move it to the final location.
        // This allows us to avoid a situation where we upload a file, then we attempt to overwrite it but fail, leaving a corrupt asset.
        final File tempFile = new File(dir, file.getName() + ".tmp");
        try {
            Files.copy(contents, tempFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
        } catch (final Exception e) {
            throw new IOException("Failed to write asset to file " + tempFile.getAbsolutePath(), e);
        }

        Files.move(tempFile.toPath(), file.toPath(), StandardCopyOption.REPLACE_EXISTING);

        final Asset asset = new StandardAsset(assetId, assetName, file);
        assets.put(assetId, asset);

        return asset;
    }

    private File getFile(final String assetId) {
        return new File(assetStorageLocation, assetId);
    }

    @Override
    public Optional<Asset> getAsset(final String id) {
        return Optional.ofNullable(assets.get(id));
    }

    @Override
    public Asset createMissingAsset(final String parameterContextId, final String assetName) {
        final String assetId = createAssetId(parameterContextId, assetName);
        final File file = getFile(assetId);
        return assets.putIfAbsent(assetId, new StandardAsset(assetId, assetName, file));
    }

    private String createAssetId(final String parameterContextId, final String assetName) {
        return parameterContextId + "/" + assetName;
    }

    @Override
    public Optional<Asset> deleteAsset(final String id) {
        final Asset removed = assets.remove(id);
        final File file = getFile(id);
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

        return Optional.ofNullable(removed);
    }
}
