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
package org.apache.nifi.tests.system.connectors;

import org.apache.nifi.web.api.dto.ConfigurationStepConfigurationDTO;
import org.apache.nifi.web.api.dto.ConnectorActionDTO;
import org.apache.nifi.web.api.dto.ConnectorConfigurationDTO;
import org.apache.nifi.web.api.dto.ConnectorValueReferenceDTO;
import org.apache.nifi.web.api.dto.PropertyGroupConfigurationDTO;
import org.apache.nifi.web.api.dto.StateEntryDTO;
import org.apache.nifi.web.api.entity.ComponentStateEntity;
import org.apache.nifi.web.api.entity.ConnectorEntity;
import org.apache.nifi.web.api.entity.FlowRegistryClientEntity;
import org.apache.nifi.web.api.entity.MigrationRequestEntity;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * System tests for the structural initial-flow check that gates the {@code MIGRATE} allowable action.
 *
 * <p>Two restart-related properties must hold:
 * <ul>
 *   <li>If a Connector is created and the framework is restarted before any migration is attempted, the
 *       check must still report the Connector as being at its initial flow. {@code MIGRATE} must remain
 *       allowed because both inputs to the comparison are recomputed from durable state after the restart:
 *       {@code getInitialFlow()} is rebuilt from the Connector's persisted configuration, and the current
 *       managed flow is repopulated from the same configuration.</li>
 *   <li>If a Connector has been modified by a successful migration, the check must report it as modified
 *       and {@code MIGRATE} must be reported as disallowed with a reason that explains why. A second
 *       migration attempt must be rejected by the framework with the same diagnostic.</li>
 * </ul>
 */
public class ConnectorVersionedFlowMigrationRestartIT extends ConnectorVersionedFlowMigrationLocalIT {

    private static final String MIGRATE_ACTION_NAME = "MIGRATE";

    @Override
    protected boolean isDestroyEnvironmentAfterEachTest() {
        return true;
    }

    @Override
    protected boolean isAllowFactoryReuse() {
        return false;
    }

    @Test
    public void testMigrateActionAllowedAfterRestartWhenConnectorIsStillAtInitialFlow() throws Exception {
        final ConnectorEntity connector = getClientUtil().createConnector("MigrationTargetConnector");
        final String connectorId = connector.getId();

        final ConnectorActionDTO migrateBeforeRestart = findMigrateAction(getNifiClient().getConnectorClient().getConnector(connectorId));
        assertNotNull(migrateBeforeRestart, "MIGRATE action must be present on a fresh Connector");
        assertEquals(Boolean.TRUE, migrateBeforeRestart.getAllowed(),
                "MIGRATE must be allowed on a fresh Connector whose flow matches getInitialFlow(); reason: "
                        + migrateBeforeRestart.getReasonNotAllowed());

        getNiFiInstance().stop();
        getNiFiInstance().start();
        setupClient();

        final ConnectorActionDTO migrateAfterRestart = findMigrateAction(getNifiClient().getConnectorClient().getConnector(connectorId));
        assertNotNull(migrateAfterRestart, "MIGRATE action must remain present after a restart");
        assertEquals(Boolean.TRUE, migrateAfterRestart.getAllowed(),
                "Restarting NiFi without modifying the Connector must not cause the initial-flow check to flip; reason: "
                        + migrateAfterRestart.getReasonNotAllowed());
    }

    @Test
    public void testMigrateActionDisallowedAfterConnectorFlowIsModified() throws Exception {
        final File outputFile = new File("target/migration/modified-after-migration.txt");
        outputFile.delete();

        final FlowRegistryClientEntity registryClient = registerClient();
        final SourceFixture sourceFixture = createSourceFixture("ModifiedAfterMigrationSource", registryClient, true, outputFile, true);
        prepareSourceForMigration(sourceFixture, outputFile);

        final ConnectorEntity connector = getClientUtil().createConnector("MigrationTargetConnector");
        final String connectorId = connector.getId();

        final ConnectorActionDTO migrateBeforeMigration = findMigrateAction(getNifiClient().getConnectorClient().getConnector(connectorId));
        assertNotNull(migrateBeforeMigration, "MIGRATE action must be present on a fresh, stopped Connector");
        assertEquals(Boolean.TRUE, migrateBeforeMigration.getAllowed(),
                "MIGRATE must be allowed on a fresh Connector whose flow still matches getInitialFlow(); reason: "
                        + migrateBeforeMigration.getReasonNotAllowed());

        final MigrationRequestEntity requestEntity = getClientUtil().startMigrationFromLocalSource(connectorId, sourceFixture.processGroup().getId());
        getClientUtil().waitForMigrationSuccess(connectorId, requestEntity.getRequest().getRequestId());

        final ConnectorActionDTO migrateAfterMigration = findMigrateAction(getNifiClient().getConnectorClient().getConnector(connectorId));
        assertNotNull(migrateAfterMigration, "MIGRATE action must remain present so its reasonNotAllowed can explain why it is unavailable");
        assertEquals(Boolean.FALSE, migrateAfterMigration.getAllowed(),
                "MIGRATE must be disallowed once the Connector's flow has been modified from its initial state");
        assertNotNull(migrateAfterMigration.getReasonNotAllowed(),
                "MIGRATE must report a reasonNotAllowed when it is disallowed");
        assertTrue(migrateAfterMigration.getReasonNotAllowed().toLowerCase().contains("modified"),
                "reasonNotAllowed must explain that the Connector has been modified; got: " + migrateAfterMigration.getReasonNotAllowed());

        // The migrate-time guard must also reject a second migration attempt; this is the safety net that prevents
        // the action gate from being bypassed by a direct REST call. Migration requests are asynchronous, so we
        // start the request and then verify that it ultimately fails with the same reason the action surfaced.
        final MigrationRequestEntity secondAttempt = getClientUtil().startMigrationFromLocalSource(connectorId, sourceFixture.processGroup().getId());
        final MigrationRequestEntity secondAttemptResult = getClientUtil().waitForMigrationFailure(connectorId, secondAttempt.getRequest().getRequestId());
        final String secondAttemptFailure = secondAttemptResult.getRequest().getFailureReason();
        assertNotNull(secondAttemptFailure, "A second migration attempt must report a failure reason");
        assertTrue(secondAttemptFailure.toLowerCase().contains("modified"),
                "Second migration attempt must be rejected because the Connector has been modified; got: " + secondAttemptFailure);
    }

    @Test
    public void testMigrateActionStaysDisallowedAfterMigrationAndAcrossRestart() throws Exception {
        final File outputFile = new File("target/migration/modified-after-restart.txt");
        outputFile.delete();

        final FlowRegistryClientEntity registryClient = registerClient();
        final SourceFixture sourceFixture = createSourceFixture("ModifiedAfterRestartSource", registryClient, true, outputFile, true);
        prepareSourceForMigration(sourceFixture, outputFile);

        final ConnectorEntity connector = getClientUtil().createConnector("MigrationTargetConnector");
        final String connectorId = connector.getId();

        final MigrationRequestEntity requestEntity = getClientUtil().startMigrationFromLocalSource(connectorId, sourceFixture.processGroup().getId());
        getClientUtil().waitForMigrationSuccess(connectorId, requestEntity.getRequest().getRequestId());

        getNiFiInstance().stop();
        getNiFiInstance().start();
        setupClient();

        // After restart the framework rebuilds the Connector from its persisted configuration. MigrationTargetConnector
        // stores the migrated source flow in its configuration and rebuilds the managed Process Group from that
        // configuration inside applyUpdate(...); the initial-flow check must therefore still report the Connector as
        // modified and MIGRATE must remain disallowed.
        final ConnectorActionDTO migrateAfterRestart = findMigrateAction(getNifiClient().getConnectorClient().getConnector(connectorId));
        assertNotNull(migrateAfterRestart, "MIGRATE action must remain present after a restart");
        assertEquals(Boolean.FALSE, migrateAfterRestart.getAllowed(),
                "MIGRATE must remain disallowed after a restart because the Connector's persisted configuration still records the migration");
        assertNotNull(migrateAfterRestart.getReasonNotAllowed(), "MIGRATE must report a reasonNotAllowed after a restart");
        assertTrue(migrateAfterRestart.getReasonNotAllowed().toLowerCase().contains("modified"),
                "reasonNotAllowed must continue to explain that the Connector has been modified after a restart; got: "
                        + migrateAfterRestart.getReasonNotAllowed());
    }

    /**
     * Verifies that the StateManager state {@code MigrationTargetConnector.migrateState} records via
     * {@code ConnectorMigrationContext.setComponentState} survives a NiFi restart. The source flow contains a
     * {@code StatefulCountProcessor} whose state is captured into the source flow. During migration the framework
     * writes that state directly into the live {@code StateManager} of the managed processor. The state then survives
     * a restart through the same mechanism as any normal component state: the {@code StateManager} persists each
     * component's state to its configured state provider (the local provider on disk, the cluster provider in
     * ZooKeeper), and the same component identifier is rehydrated on restart, so the state is read back from the
     * same provider. Migration does not re-apply the state on restart; it only seeds it once during the initial
     * migration.
     */
    @Test
    public void testComponentStateAppliedByMigrateStateSurvivesRestart() throws Exception {
        final File outputFile = new File("target/migration/component-state-after-restart.txt");
        outputFile.delete();

        final FlowRegistryClientEntity registryClient = registerClient();
        final SourceFixture sourceFixture = createSourceFixture("ComponentStateRestartSource", registryClient, true, outputFile, true);
        prepareSourceForMigration(sourceFixture, outputFile);

        final ConnectorEntity connector = getClientUtil().createConnector("MigrationTargetConnector");
        final String connectorId = connector.getId();

        final MigrationRequestEntity requestEntity = getClientUtil().startMigrationFromLocalSource(connectorId, sourceFixture.processGroup().getId());
        getClientUtil().waitForMigrationSuccess(connectorId, requestEntity.getRequest().getRequestId());

        final ConnectorEntity migratedConnector = getNifiClient().getConnectorClient().getConnector(connectorId);
        final String managedGroupId = migratedConnector.getComponent().getManagedProcessGroupId();
        final String migratedCountProcessorId = getProcessorId(connectorId, managedGroupId, "StatefulCountProcessor");

        final Map<String, String> stateBeforeRestart = readLocalState(connectorId, migratedCountProcessorId);
        assertFalse(stateBeforeRestart.isEmpty(),
                "Migrated processor must carry the StateManager state that the connector recorded via migrateState(...)");

        getNiFiInstance().stop();
        getNiFiInstance().start();
        setupClient();

        final Map<String, String> stateAfterRestart = readLocalState(connectorId, migratedCountProcessorId);
        assertEquals(stateBeforeRestart, stateAfterRestart,
                "StateManager state must be identical before and after a restart; component state is persisted by the StateManager's configured state provider and rehydrated on the next load");
    }

    /**
     * Verifies that a failure during the state phase of migration rolls back the staged configuration so that the
     * persisted active configuration matches the pre-migration state both immediately and after a NiFi restart. The
     * {@code FailingStateMigrationConnector} used here stages a configuration delta during {@code migrateConfiguration}
     * and then throws from {@code migrateState}. The framework must skip the per-step commit onto active, so the
     * marker property must remain unset both before and after the restart.
     */
    @Test
    public void testStateMigrationFailureLeavesConfigurationUnchangedAcrossRestart() throws Exception {
        final File outputFile = new File("target/migration/state-failure-restart-output.txt");
        outputFile.delete();

        final FlowRegistryClientEntity registryClient = registerClient();
        final SourceFixture sourceFixture = createSourceFixture("StateFailureSource", registryClient, true, outputFile, true);
        prepareSourceForMigration(sourceFixture, outputFile);

        final ConnectorEntity connector = getClientUtil().createConnector("FailingStateMigrationConnector");
        final String connectorId = connector.getId();

        // Sanity check: before any migration the marker property is unset.
        assertNull(readMarkerPropertyValue(connectorId), "Marker property must be unset on a fresh connector");

        final MigrationRequestEntity requestEntity = getClientUtil().startMigrationFromLocalSource(connectorId, sourceFixture.processGroup().getId());
        final MigrationRequestEntity completed = getClientUtil().waitForMigrationFailure(connectorId, requestEntity.getRequest().getRequestId());
        assertNotNull(completed.getRequest().getFailureReason(), "Migration must report a failure reason when migrateState throws");

        // The connector records a configuration change during migrateConfiguration, but the framework must not commit
        // it onto active because the state phase failed.
        assertNull(readMarkerPropertyValue(connectorId),
                "Marker property must remain unset after the state-migration failure; commit must run only after both phases succeed");

        getNiFiInstance().stop();
        getNiFiInstance().start();
        setupClient();

        // After a restart the persisted active configuration is rehydrated. The marker property must still be unset
        // because the framework rolled the migration back before persisting the staged configuration change.
        assertNull(readMarkerPropertyValue(connectorId),
                "Marker property must remain unset after a restart following a failed state migration");
    }

    private String readMarkerPropertyValue(final String connectorId) throws Exception {
        final ConnectorEntity connectorEntity = getNifiClient().getConnectorClient().getConnector(connectorId);
        final ConnectorConfigurationDTO activeConfiguration = connectorEntity.getComponent().getActiveConfiguration();
        if (activeConfiguration == null || activeConfiguration.getConfigurationStepConfigurations() == null) {
            return null;
        }
        for (final ConfigurationStepConfigurationDTO stepConfig : activeConfiguration.getConfigurationStepConfigurations()) {
            if (!"State Migration Marker".equals(stepConfig.getConfigurationStepName())) {
                continue;
            }
            if (stepConfig.getPropertyGroupConfigurations() == null) {
                continue;
            }
            for (final PropertyGroupConfigurationDTO group : stepConfig.getPropertyGroupConfigurations()) {
                if (group.getPropertyValues() == null) {
                    continue;
                }
                final ConnectorValueReferenceDTO valueReference = group.getPropertyValues().get("Marker");
                if (valueReference == null) {
                    continue;
                }
                return valueReference.getValue();
            }
        }
        return null;
    }

    private Map<String, String> readLocalState(final String connectorId, final String processorId) throws Exception {
        final ComponentStateEntity stateEntity = getNifiClient().getConnectorClient().getProcessorState(connectorId, processorId);
        assertNotNull(stateEntity.getComponentState(), "Component state response must include component state");
        assertNotNull(stateEntity.getComponentState().getLocalState(), "Component state response must include local state");
        final List<StateEntryDTO> entries = stateEntity.getComponentState().getLocalState().getState();
        if (entries == null || entries.isEmpty()) {
            return Map.of();
        }
        final Map<String, String> stateMap = new HashMap<>();
        for (final StateEntryDTO entry : entries) {
            stateMap.put(entry.getKey(), entry.getValue());
        }
        return Map.copyOf(stateMap);
    }

    private static ConnectorActionDTO findMigrateAction(final ConnectorEntity connector) {
        final List<ConnectorActionDTO> actions = connector.getComponent() == null ? null : connector.getComponent().getAvailableActions();
        if (actions == null) {
            return null;
        }
        for (final ConnectorActionDTO action : actions) {
            if (MIGRATE_ACTION_NAME.equals(action.getName())) {
                return action;
            }
        }
        return null;
    }
}
