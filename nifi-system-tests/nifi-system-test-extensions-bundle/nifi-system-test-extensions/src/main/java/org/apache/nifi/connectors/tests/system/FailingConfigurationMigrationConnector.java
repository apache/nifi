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
package org.apache.nifi.connectors.tests.system;

import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.connector.AbstractConnector;
import org.apache.nifi.components.connector.ConfigurationStep;
import org.apache.nifi.components.connector.FlowUpdateException;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.components.connector.migration.ConnectorMigrationContext;
import org.apache.nifi.components.connector.migration.MigratableConnector;
import org.apache.nifi.flow.VersionedAsset;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedParameter;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedProcessGroup;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Test connector that fails during the configuration phase of migration so the framework rolls the migration back
 * as a whole. Used to verify rollback semantics: copied assets are deleted, the initial flow is restored, and the
 * connector remains in a state that allows a subsequent migration to succeed.
 */
public class FailingConfigurationMigrationConnector extends AbstractConnector implements MigratableConnector {
    private static final long FAILURE_DELAY_SECONDS = 5L;

    @Override
    public VersionedExternalFlow getInitialFlow() {
        final VersionedProcessGroup processGroup = new VersionedProcessGroup();
        processGroup.setName("Failing Migration Flow");
        processGroup.setProcessors(new HashSet<>());
        processGroup.setConnections(new HashSet<>());
        processGroup.setProcessGroups(new HashSet<>());
        processGroup.setControllerServices(new HashSet<>());

        final VersionedExternalFlow flow = new VersionedExternalFlow();
        flow.setFlowContents(processGroup);
        flow.setParameterContexts(Collections.emptyMap());
        return flow;
    }

    @Override
    public VersionedExternalFlow getActiveFlow(final FlowContext activeFlowContext) {
        return getInitialFlow();
    }

    @Override
    public boolean isMigrationSupported(final ConnectorMigrationContext context) {
        return true;
    }

    @Override
    public void migrateConfiguration(final ConnectorMigrationContext context) throws FlowUpdateException {
        // Copy an asset and sleep so the rollback path has observable side effects to undo, then throw to drive the
        // framework's "phase-1 failure" rollback branch.
        copyFirstAsset(context);
        try {
            TimeUnit.SECONDS.sleep(FAILURE_DELAY_SECONDS);
        } catch (final InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            throw new FlowUpdateException("Interrupted while waiting to fail the migration", interruptedException);
        }

        throw new FlowUpdateException("Intended migration failure for rollback testing");
    }

    @Override
    public void migrateState(final ConnectorMigrationContext context) {
        // Failure is intentionally triggered from migrateConfiguration so migrateState is never reached.
    }

    @Override
    protected void onStepConfigured(final String stepName, final FlowContext workingContext) {
    }

    @Override
    public List<ConfigVerificationResult> verifyConfigurationStep(final String stepName, final Map<String, String> propertyValueOverrides, final FlowContext flowContext) {
        return List.of();
    }

    @Override
    public List<ConfigurationStep> getConfigurationSteps() {
        return List.of();
    }

    @Override
    public void applyUpdate(final FlowContext workingFlowContext, final FlowContext activeFlowContext) {
    }

    private void copyFirstAsset(final ConnectorMigrationContext context) {
        if (!context.isLocalMigration()) {
            return;
        }

        final VersionedExternalFlow sourceFlow = context.getSourceFlow();
        if (sourceFlow.getParameterContexts() == null) {
            return;
        }

        for (final VersionedParameterContext parameterContext : sourceFlow.getParameterContexts().values()) {
            if (parameterContext.getParameters() == null) {
                continue;
            }

            for (final VersionedParameter parameter : parameterContext.getParameters()) {
                if (parameter.getReferencedAssets() == null) {
                    continue;
                }

                for (final VersionedAsset referencedAsset : parameter.getReferencedAssets()) {
                    context.copyAssetFromSource(referencedAsset.getIdentifier());
                    return;
                }
            }
        }
    }
}
