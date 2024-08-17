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

import java.util.Map;
import java.util.Objects;

public class StandardAssetManagerInitializationContext implements AssetManagerInitializationContext {
    private final AssetReferenceLookup assetReferenceLookup;
    private final Map<String, String> properties;
    private final NodeTypeProvider nodeTypeProvider;

    public StandardAssetManagerInitializationContext(final AssetReferenceLookup assetReferenceLookup, final Map<String, String> properties, final NodeTypeProvider nodeTypeProvider) {
        this.assetReferenceLookup = Objects.requireNonNull(assetReferenceLookup);
        this.properties = Objects.requireNonNull(properties);
        this.nodeTypeProvider = Objects.requireNonNull(nodeTypeProvider);
    }

    @Override
    public AssetReferenceLookup getAssetReferenceLookup() {
        return assetReferenceLookup;
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public NodeTypeProvider getNodeTypeProvider() {
        return nodeTypeProvider;
    }
}
