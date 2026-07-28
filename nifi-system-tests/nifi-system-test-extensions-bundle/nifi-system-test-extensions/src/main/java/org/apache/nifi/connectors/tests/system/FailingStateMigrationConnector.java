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
import org.apache.nifi.components.connector.ConnectorPropertyDescriptor;
import org.apache.nifi.components.connector.ConnectorPropertyGroup;
import org.apache.nifi.components.connector.FlowUpdateException;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.components.connector.migration.ConnectorMigrationContext;
import org.apache.nifi.components.connector.migration.MigratableConnector;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedProcessGroup;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Test connector that records a configuration change during the configuration phase of migration and then throws from
 * the state phase. Used to verify that a failure in {@code migrateState} prevents the framework from committing the
 * recorded configuration onto active and that the connector's persisted configuration therefore matches the
 * pre-migration state after a restart.
 */
public class FailingStateMigrationConnector extends AbstractConnector implements MigratableConnector {

    static final String MARKER_STEP_NAME = "State Migration Marker";
    static final String MARKER_PROPERTY_NAME = "Marker";
    static final String MARKER_VALUE_STAGED_BY_FAILED_MIGRATION = "staged-by-failed-migration";

    private static final ConnectorPropertyDescriptor MARKER_PROPERTY = new ConnectorPropertyDescriptor.Builder()
            .name(MARKER_PROPERTY_NAME)
            .description("Marker the test sets during migrateConfiguration to verify it is not persisted when migrateState fails.")
            .build();

    private static final ConnectorPropertyGroup MARKER_PROPERTY_GROUP = new ConnectorPropertyGroup.Builder()
            .name("State Migration Marker")
            .description("Holds a marker used by the failed-state-migration rollback system test.")
            .addProperty(MARKER_PROPERTY)
            .build();

    private static final ConfigurationStep MARKER_STEP = new ConfigurationStep.Builder()
            .name(MARKER_STEP_NAME)
            .description("Marker step updated by migrateConfiguration. The matching migrateState always throws, so the framework must not commit this step onto active.")
            .propertyGroups(List.of(MARKER_PROPERTY_GROUP))
            .build();

    @Override
    public VersionedExternalFlow getInitialFlow() {
        final VersionedProcessGroup processGroup = new VersionedProcessGroup();
        processGroup.setName("Failing State Migration Flow");
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
    public void migrateConfiguration(final ConnectorMigrationContext context) {
        // Record a configuration change so the framework has something to potentially commit. The matching
        // migrateState below always throws, so the framework must roll the migration back and this change must not
        // appear on the active configuration after a restart.
        context.setProperties(MARKER_STEP_NAME, Map.of(MARKER_PROPERTY_NAME, MARKER_VALUE_STAGED_BY_FAILED_MIGRATION));
    }

    @Override
    public void migrateState(final ConnectorMigrationContext context) throws FlowUpdateException {
        throw new FlowUpdateException("Intended state-migration failure for rollback testing");
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
        return List.of(MARKER_STEP);
    }

    @Override
    public void applyUpdate(final FlowContext workingFlowContext, final FlowContext activeFlowContext) {
    }
}
