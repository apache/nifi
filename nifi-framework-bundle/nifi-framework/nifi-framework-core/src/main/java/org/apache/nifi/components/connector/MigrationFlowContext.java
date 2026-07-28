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
import org.apache.nifi.components.connector.components.FlowContextType;
import org.apache.nifi.components.connector.components.ParameterContextFacade;
import org.apache.nifi.components.connector.components.ProcessGroupFacade;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.groups.ProcessGroup;

import java.util.Objects;

/**
 * Read-only {@link FrameworkFlowContext} wrapper handed to a {@code MigratableConnector} via
 * {@link org.apache.nifi.components.connector.migration.ConnectorMigrationContext#getActiveFlowContext()
 * ConnectorMigrationContext.getActiveFlowContext()}. Every accessor delegates straight to the underlying
 * framework context, but {@link #updateFlow(VersionedExternalFlow, AssetManager)} always throws.
 *
 * <p>
 * This is the single chokepoint that prevents a Connector from installing a new managed Process Group from inside
 * either {@code migrateConfiguration(...)} or {@code migrateState(...)}. The framework owns the
 * {@code updateFlow(...)} call site during migration: it drives the rebuild through the Connector's own
 * {@link Connector#applyUpdate(org.apache.nifi.components.connector.components.FlowContext,
 *   org.apache.nifi.components.connector.components.FlowContext) applyUpdate(...)} between the two phases, using
 * the configuration deltas the Connector recorded via
 * {@link org.apache.nifi.components.connector.migration.ConnectorMigrationContext#setProperties(String, java.util.Map)
 *   setProperties(...)} /
 * {@link org.apache.nifi.components.connector.migration.ConnectorMigrationContext#replaceProperties(String, java.util.Map)
 *   replaceProperties(...)}.
 * </p>
 *
 * <p>
 * {@link StandardConnectorInitializationContext#updateFlow(org.apache.nifi.components.connector.components.FlowContext,
 *   VersionedExternalFlow, BundleCompatibility) StandardConnectorInitializationContext.updateFlow(...)} delegates to
 * {@link FrameworkFlowContext#updateFlow(VersionedExternalFlow, AssetManager)}, so wrapping the
 * {@link FrameworkFlowContext} here is sufficient to refuse every {@code updateFlow(...)} entry point.
 * </p>
 */
final class MigrationFlowContext implements FrameworkFlowContext {

    private final FrameworkFlowContext delegate;

    MigrationFlowContext(final FrameworkFlowContext delegate) {
        this.delegate = Objects.requireNonNull(delegate, "Delegate FrameworkFlowContext is required");
    }

    @Override
    public ProcessGroup getManagedProcessGroup() {
        return delegate.getManagedProcessGroup();
    }

    @Override
    public MutableConnectorConfigurationContext getConfigurationContext() {
        return delegate.getConfigurationContext();
    }

    @Override
    public ProcessGroupFacade getRootGroup() {
        return delegate.getRootGroup();
    }

    @Override
    public ParameterContextFacade getParameterContext() {
        return delegate.getParameterContext();
    }

    @Override
    public FlowContextType getType() {
        return delegate.getType();
    }

    @Override
    public Bundle getBundle() {
        return delegate.getBundle();
    }

    @Override
    public void verifyUpdateFlow(final VersionedExternalFlow versionedExternalFlow) throws FlowUpdateException {
        delegate.verifyUpdateFlow(versionedExternalFlow);
    }

    @Override
    public void updateFlow(final VersionedExternalFlow versionedExternalFlow, final AssetManager assetManager) throws FlowUpdateException {
        throw new FlowUpdateException("updateFlow() cannot be called from MigratableConnector.migrateConfiguration() or migrateState();"
                + " record the desired post-migration configuration via ConnectorMigrationContext.setProperties() / replaceProperties()"
                + " and the framework will drive applyUpdate() to rebuild the managed flow using the same code path used on restart.");
    }

    @Override
    public void restoreTroubleshootingFlow(final VersionedProcessGroup troubleshootingProcessGroup) {
        throw new IllegalStateException("restoreTroubleshootingFlow() cannot be called from MigratableConnector.migrateConfiguration() or migrateState();"
                + " the migration context exposes a read-only view of the managed flow so that the framework remains the sole owner of"
                + " installing the managed Process Group.");
    }
}
