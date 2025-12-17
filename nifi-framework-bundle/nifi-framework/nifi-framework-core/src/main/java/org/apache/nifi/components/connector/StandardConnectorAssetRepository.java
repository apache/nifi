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
package org.apache.nifi.components.connector;

import org.apache.nifi.asset.Asset;
import org.apache.nifi.asset.AssetManager;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;

public class StandardConnectorAssetRepository implements ConnectorAssetRepository {

    private final AssetManager assetManager;

    public StandardConnectorAssetRepository(final AssetManager assetManager) {
        this.assetManager = assetManager;
    }

    @Override
    public Asset storeAsset(final String connectorId, final String assetId, final String assetName, final InputStream content) throws IOException {
        return assetManager.saveAsset(connectorId, assetId, assetName, content);
    }

    @Override
    public Optional<Asset> getAsset(String assetId) {
        return assetManager.getAsset(assetId);
    }

    @Override
    public List<Asset> getAssets(final String connectorId) {
        return assetManager.getAssets(connectorId);
    }

    @Override
    public void deleteAsset(final String assetId) {
        assetManager.deleteAsset(assetId);
    }

    @Override
    public void deleteAssets(final String connectorId) {
        final List<Asset> assets = assetManager.getAssets(connectorId);
        for (final Asset asset : assets) {
            assetManager.deleteAsset(asset.getIdentifier());
        }
    }

}
