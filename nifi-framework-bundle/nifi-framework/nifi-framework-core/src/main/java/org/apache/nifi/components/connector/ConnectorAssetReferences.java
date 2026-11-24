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
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.parameter.Parameter;
import org.apache.nifi.parameter.ParameterContext;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Determines the identifiers of the assets referenced by a Connector.
 */
public final class ConnectorAssetReferences {

    private ConnectorAssetReferences() {
    }

    /**
     * Returns the identifiers of the assets referenced by the Connector's active and working flow contexts, including asset references in the
     * Connector configuration and assets referenced by parameters of the managed Process Group's Parameter Context.
     *
     * @param connector the Connector
     * @return the identifiers of the referenced assets
     */
    public static Set<String> getReferencedAssetIds(final ConnectorNode connector) {
        final Set<String> referencedAssetIds = new HashSet<>();
        collectReferencedAssetIds(connector.getActiveFlowContext(), referencedAssetIds);
        collectReferencedAssetIds(connector.getWorkingFlowContext(), referencedAssetIds);
        return referencedAssetIds;
    }

    private static void collectReferencedAssetIds(final FrameworkFlowContext flowContext, final Set<String> referencedAssetIds) {
        if (flowContext == null) {
            return;
        }

        final ProcessGroup managedProcessGroup = flowContext.getManagedProcessGroup();
        if (managedProcessGroup != null) {
            collectReferencedParameterContextAssetIds(managedProcessGroup.getParameterContext(), referencedAssetIds);
        }

        final ConnectorConfiguration configuration = flowContext.getConfigurationContext().toConnectorConfiguration();
        for (final NamedStepConfiguration namedStepConfiguration : configuration.getNamedStepConfigurations()) {
            final StepConfiguration stepConfiguration = namedStepConfiguration.configuration();
            final Map<String, ConnectorValueReference> stepPropertyValues = stepConfiguration.getPropertyValues();
            if (stepPropertyValues == null) {
                continue;
            }

            for (final ConnectorValueReference valueReference : stepPropertyValues.values()) {
                if (valueReference instanceof final AssetReference assetReference) {
                    referencedAssetIds.addAll(assetReference.getAssetIdentifiers());
                }
            }
        }
    }

    private static void collectReferencedParameterContextAssetIds(final ParameterContext parameterContext, final Set<String> referencedAssetIds) {
        if (parameterContext == null) {
            return;
        }

        for (final Parameter parameter : parameterContext.getEffectiveParameters().values()) {
            final List<Asset> referencedAssets = parameter.getReferencedAssets();
            if (referencedAssets == null) {
                continue;
            }

            for (final Asset referencedAsset : referencedAssets) {
                referencedAssetIds.add(referencedAsset.getIdentifier());
            }
        }
    }
}
