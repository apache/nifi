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
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.controller.ClusterTopologyProvider;
import org.apache.nifi.flow.VersionedComponentState;
import org.apache.nifi.flow.VersionedExternalFlow;

import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Delegating {@link FrameworkConnectorMigrationContext} used on the listing/eligibility path. The
 * {@link org.apache.nifi.components.connector.migration.MigratableConnector#isMigrationSupported(org.apache.nifi.components.connector.migration.ConnectorMigrationContext)}
 * contract requires the call to be side-effect-free; in particular it must not invoke
 * {@link #copyAssetFromSource(String)}. This wrapper enforces that contract by forwarding every other accessor
 * to the delegate while throwing {@link IllegalStateException} from {@code copyAssetFromSource(String)}, even
 * when the underlying context would otherwise allow the copy. Implementations that ignore the JavaDoc and try
 * to copy assets during eligibility evaluation are caught here rather than silently mutating the Connector's
 * asset namespace during a read-only listing.
 */
final class EligibilityConnectorMigrationContext implements FrameworkConnectorMigrationContext {

    private final FrameworkConnectorMigrationContext delegate;

    EligibilityConnectorMigrationContext(final FrameworkConnectorMigrationContext delegate) {
        this.delegate = Objects.requireNonNull(delegate, "Delegate FrameworkConnectorMigrationContext is required");
    }

    @Override
    public VersionedExternalFlow getSourceFlow() {
        return delegate.getSourceFlow();
    }

    @Override
    public boolean isLocalMigration() {
        return delegate.isLocalMigration();
    }

    @Override
    public FrameworkFlowContext getActiveFlowContext() {
        return delegate.getActiveFlowContext();
    }

    @Override
    public AssetReference copyAssetFromSource(final String sourceAssetId) {
        throw new IllegalStateException("copyAssetFromSource() must not be invoked from isMigrationSupported(); the listing path is read-only.");
    }

    @Override
    public void setProperties(final String stepName, final Map<String, String> propertyValues) {
        throw new IllegalStateException("setProperties() must not be invoked from isMigrationSupported(); the listing path is read-only.");
    }

    @Override
    public void replaceProperties(final String stepName, final Map<String, String> propertyValues) {
        throw new IllegalStateException("replaceProperties() must not be invoked from isMigrationSupported(); the listing path is read-only.");
    }

    @Override
    public void setComponentState(final String managedComponentId, final VersionedComponentState state) {
        throw new IllegalStateException("setComponentState() must not be invoked from isMigrationSupported(); the listing path is read-only.");
    }

    @Override
    public AssetManager getSourceAssetManager() {
        return delegate.getSourceAssetManager();
    }

    @Override
    public ConnectorRepository getConnectorRepository() {
        return delegate.getConnectorRepository();
    }

    @Override
    public StateManagerProvider getStateManagerProvider() {
        return delegate.getStateManagerProvider();
    }

    @Override
    public ClusterTopologyProvider getClusterTopologyProvider() {
        return delegate.getClusterTopologyProvider();
    }

    @Override
    public Set<String> getCopiedAssetIds() {
        return delegate.getCopiedAssetIds();
    }
}
