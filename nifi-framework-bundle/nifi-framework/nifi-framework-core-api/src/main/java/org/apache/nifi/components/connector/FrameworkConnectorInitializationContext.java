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

import org.apache.nifi.asset.AssetManager;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.components.connector.secrets.SecretsManager;
import org.apache.nifi.flow.VersionedExternalFlow;

public interface FrameworkConnectorInitializationContext extends ConnectorInitializationContext {

    SecretsManager getSecretsManager();

    AssetManager getAssetManager();

    /**
     * Verifies that the given FlowContext's Managed Process Group can be updated to the provided
     * {@link VersionedExternalFlow}, without performing any modifications. This mirrors the checks performed at
     * the start of {@link #updateFlow(FlowContext, VersionedExternalFlow, BundleCompatibility)} and is intended to
     * let callers reject update requests synchronously before any state mutation occurs. Note that if the provided
     * {@link BundleCompatibility} is not {@link BundleCompatibility#REQUIRE_EXACT_BUNDLE}, this method may update the
     * bundles referenced by the provided flow to match the bundles that would actually be used during the update,
     * matching the behavior of {@link #updateFlow(FlowContext, VersionedExternalFlow, BundleCompatibility)}.
     *
     * @param flowContext the context of the flow to be verified
     * @param versionedExternalFlow the new representation of the flow
     * @param bundleCompatability the strategy to use when resolving component bundles
     * @throws FlowUpdateException if the flow update would not be allowed
     */
    void verifyUpdateFlow(FlowContext flowContext, VersionedExternalFlow versionedExternalFlow, BundleCompatibility bundleCompatability) throws FlowUpdateException;
}
