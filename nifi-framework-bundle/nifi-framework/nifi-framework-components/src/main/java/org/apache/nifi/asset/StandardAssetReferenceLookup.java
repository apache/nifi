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

import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;
import org.apache.nifi.parameter.ParameterContextManager;

import java.util.HashSet;
import java.util.Set;

public class StandardAssetReferenceLookup implements AssetReferenceLookup {
    private final ParameterContextManager parameterContextManager;

    public StandardAssetReferenceLookup(final ParameterContextManager parameterContextManager) {
        this.parameterContextManager = parameterContextManager;
    }

    @Override
    public Set<Asset> getReferencedAssets() {
        final Set<Asset> assets = new HashSet<>();

        for (final ParameterContext context : parameterContextManager.getParameterContexts()) {
            for (final Parameter parameter : context.getParameters().values()) {
                assets.addAll(parameter.getReferencedAssets());
            }
        }

        return assets;
    }
}
