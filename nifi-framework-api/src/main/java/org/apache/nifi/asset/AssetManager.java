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

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;

public interface AssetManager {
    /**
     * Initializes the AssetManager, providing the context necessary for the manager to operate.
     * @param context the context that provides all necessary initialization information
     */
    void initialize(AssetManagerInitializationContext context);

    /**
     * Creates a new Asset with the given name and contents.
     * @param ownerId the id of the resource that this asset belongs to
     * @param assetName the name of the asset
     * @param contents the contents of the asset
     * @return the created asset
     * @throws IOException if there is an error creating the asset
     */
    Asset createAsset(String ownerId, String assetName, InputStream contents) throws IOException;

    /**
     * Saves the given asset. If an asset already exists with the given identifier, the content will be replaced.
     *
     * @param ownerId the id of the resource that this asset belongs to
     * @param assetId the identifier of the asset
     * @param assetName the name of the asset
     * @param contents the new contents of the asset
     * @return the created or updated Asset
     * @throws IOException if there is an error writing the asset
     */
    Asset saveAsset(String ownerId, String assetId, String assetName, InputStream contents) throws IOException;

    /**
     * Retrieves the Asset with the given id, if it exists.
     * @param id the id of the asset to retrieve
     * @return the asset, if it exists
     */
    Optional<Asset> getAsset(String id);

    /**
     * Retrieves the Assets that belong to the given owner.
     * @param ownerId the id of the owner resource
     * @return the list of assets for the given owner
     */
    List<Asset> getAssets(String ownerId);

    /**
     * Creates an Asset with the given name and associates it with the given owner. If the asset already exists, it is returned. Otherwise, an asset is created
     * but the underlying file is not created. This allows the asset to be referenced but any component that attempts to use the asset will still see a File that does not exist, which
     * will typically lead to an invalid component.
     *
     * @param ownerId the id of the owner
     * @param assetName the name of the asset
     * @return the created asset
     */
    Asset createMissingAsset(String ownerId, String assetName);

    /**
     * Deletes the Asset with the given id, if it exists.
     * @param id the id of the asset to delete
     * @return the deleted asset, if it existed
     */
    Optional<Asset> deleteAsset(String id);
}
