/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.cluster.manager;

import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.dto.AssetDTO;
import org.apache.nifi.web.api.entity.AssetEntity;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AssetsEntityMergerTest {

    private static final String ASSET_ID_FORMAT = "1234/%s";
    private static final String ASSET_FILENAME_1 = "one.txt";
    private static final String ASSET_FILENAME_2 = "two.txt";
    private static final String ASSET_FILENAME_3 = "three.txt";
    private static final String ASSET_FILENAME_4 = "four.txt";

    @Test
    public void testMergeAssetEntities() {
        final AssetEntity assetEntity1 = createAssetEntity(ASSET_ID_FORMAT.formatted(ASSET_FILENAME_1), ASSET_FILENAME_1);
        final AssetEntity assetEntity2 = createAssetEntity(ASSET_ID_FORMAT.formatted(ASSET_FILENAME_2), ASSET_FILENAME_2);
        final AssetEntity assetEntity3 = createAssetEntity(ASSET_ID_FORMAT.formatted(ASSET_FILENAME_3), ASSET_FILENAME_3);
        final AssetEntity assetEntity4 = createAssetEntity(ASSET_ID_FORMAT.formatted(ASSET_FILENAME_4), ASSET_FILENAME_4);

        final Collection<AssetEntity> mergedResults = new ArrayList<>();
        mergedResults.add(assetEntity1);
        mergedResults.add(assetEntity2);
        mergedResults.add(assetEntity3);

        final NodeIdentifier node1Identifier = new NodeIdentifier("node1", "localhost", 8080, "localhost", 8081, "localhost", 8082, 8083, false);
        final Collection<AssetEntity> node1Response = new ArrayList<>();
        node1Response.add(assetEntity2);
        node1Response.add(assetEntity3);

        final NodeIdentifier node2Identifier = new NodeIdentifier("node2", "localhost", 8080, "localhost", 8081, "localhost", 8082, 8083, false);
        final Collection<AssetEntity> node2Response = new ArrayList<>();
        node2Response.add(assetEntity1);
        node2Response.add(assetEntity3);
        node2Response.add(assetEntity4);

        final Map<NodeIdentifier, Collection<AssetEntity>> nodeMap = Map.of(
                node1Identifier, node1Response,
                node2Identifier, node2Response
        );

        AssetsEntityMerger.mergeResponses(mergedResults, nodeMap);

        assertEquals(1, mergedResults.size());
        assertEquals(assetEntity3, mergedResults.stream().findFirst().orElse(null));
    }

    private AssetEntity createAssetEntity(final String id, final String name) {
        final AssetDTO assetDTO = new AssetDTO();
        assetDTO.setId(id);
        assetDTO.setName(name);

        final AssetEntity assetEntity = new AssetEntity();
        assetEntity.setAsset(assetDTO);
        return assetEntity;
    }
}
