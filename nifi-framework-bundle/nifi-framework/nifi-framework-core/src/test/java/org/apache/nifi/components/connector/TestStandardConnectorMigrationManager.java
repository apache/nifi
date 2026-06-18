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
import org.apache.nifi.components.connector.migration.ConnectorMigrationContext;
import org.apache.nifi.components.connector.migration.MigratableConnector;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceProvider;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.flow.ExternalControllerServiceReference;
import org.apache.nifi.flow.VersionedComponentState;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedNodeState;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.registry.flow.RegisteredFlow;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;
import org.apache.nifi.registry.flow.VersionControlInformation;
import org.apache.nifi.registry.flow.VersionedFlowState;
import org.apache.nifi.registry.flow.VersionedFlowStatus;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

public class TestStandardConnectorMigrationManager {

    private static final String CONNECTOR_ID = "connector-1";
    private static final String SOURCE_GROUP_ID = "source-group";
    private static final String SOURCE_GROUP_NAME = "Source Flow";

    @Test
    public void testMigrateFromUploadedPayloadRejectsLocalStateBeyondConnectedNodeCount() {
        final FlowController flowController = createFlowController(2);
        final ConnectorNode connectorNode = wireFreshConnector(flowController, CONNECTOR_ID);

        final StandardConnectorMigrationManager migrationManager = newMigrationManager(flowController);
        final VersionedExternalFlow sourceFlow = createSourceFlowWithLocalStateCount(3);

        final IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> migrationManager.migrateFromVersionedFlow(CONNECTOR_ID, null, sourceFlow));
        assertTrue(exception.getMessage().contains("Cannot import flow"), exception.getMessage());

        verify(connectorNode, never()).commitMigratedConfiguration(any(ConnectorConfiguration.class));
    }

    @Test
    public void testMigrateFromLocalSourceDisablesAndRenamesProcessGroupAfterSuccess() throws Exception {
        final FlowController flowController = createFlowController(1);
        final ProcessGroup sourceProcessGroup = wireSourceProcessGroup(flowController, SOURCE_GROUP_ID, SOURCE_GROUP_NAME);
        final ConnectorNode connectorNode = wireFreshConnector(flowController, CONNECTOR_ID);

        final StandardConnectorMigrationManager migrationManager = newMigrationManager(flowController);
        final VersionedExternalFlow sourceFlow = createSourceFlowWithLocalStateCount(1);

        migrationManager.migrateFromVersionedFlow(CONNECTOR_ID, SOURCE_GROUP_ID, sourceFlow);

        verify(connectorNode).commitMigratedConfiguration(any(ConnectorConfiguration.class));
        verify(sourceProcessGroup).setName("(Migrated) " + SOURCE_GROUP_NAME);
    }

    @Test
    public void testRenameIsIdempotentWhenSourceAlreadyHasMigratedPrefix() throws Exception {
        final FlowController flowController = createFlowController(1);
        final ProcessGroup sourceProcessGroup = wireSourceProcessGroup(flowController, SOURCE_GROUP_ID, "(Migrated) " + SOURCE_GROUP_NAME);

        final ProcessGroup sourceParent = mock(ProcessGroup.class);
        final ProcessorNode runningProcessor = mock(ProcessorNode.class);
        when(runningProcessor.getDesiredState()).thenReturn(ScheduledState.RUNNING);
        when(runningProcessor.getProcessGroup()).thenReturn(sourceParent);
        when(sourceProcessGroup.findAllProcessors()).thenReturn(List.of(runningProcessor));

        final ControllerServiceNode enabledService = mock(ControllerServiceNode.class);
        when(enabledService.getState()).thenReturn(ControllerServiceState.ENABLED);
        when(sourceProcessGroup.findAllControllerServices()).thenReturn(Set.of(enabledService));

        final ControllerServiceProvider controllerServiceProvider = mock(ControllerServiceProvider.class);
        when(flowController.getControllerServiceProvider()).thenReturn(controllerServiceProvider);

        wireFreshConnector(flowController, CONNECTOR_ID);

        final StandardConnectorMigrationManager migrationManager = newMigrationManager(flowController);
        final VersionedExternalFlow sourceFlow = createSourceFlowWithLocalStateCount(1);

        migrationManager.migrateFromVersionedFlow(CONNECTOR_ID, SOURCE_GROUP_ID, sourceFlow);

        // The source already had the (Migrated) prefix from a previous attempt, so it must not be re-prefixed.
        verify(sourceProcessGroup, never()).setName(anyString());

        // Disable must still run unconditionally even when the rename was skipped.
        verify(sourceParent).disableProcessor(runningProcessor);
        verify(controllerServiceProvider).disableControllerServicesAsync(List.of(enabledService));
    }

    @Test
    public void testRerunAfterFailureIsAllowed() throws Exception {
        final FlowController flowController = createFlowController(1);
        final ConnectorNode connectorNode = wireFreshConnector(flowController, CONNECTOR_ID);

        final MigratableConnector connector = (MigratableConnector) connectorNode.getConnector();
        doThrow(new FlowUpdateException("transient")).doNothing().when(connector).migrateConfiguration(any(ConnectorMigrationContext.class));

        final StandardConnectorMigrationManager migrationManager = newMigrationManager(flowController);
        final VersionedExternalFlow sourceFlow = createSourceFlowWithLocalStateCount(1);

        assertThrows(FlowUpdateException.class, () -> migrationManager.migrateFromVersionedFlow(CONNECTOR_ID, null, sourceFlow));

        // Second attempt must succeed because the prior attempt rolled back fully.
        migrationManager.migrateFromVersionedFlow(CONNECTOR_ID, null, sourceFlow);
        verify(connectorNode, atLeastOnce()).commitMigratedConfiguration(any(ConnectorConfiguration.class));
    }

    @Test
    public void testStateMigrationFailureSkipsCommitOfMigratedConfiguration() throws Exception {
        final FlowController flowController = createFlowController(1);
        final ConnectorNode connectorNode = wireFreshConnector(flowController, CONNECTOR_ID);
        final MigratableConnector connector = (MigratableConnector) connectorNode.getConnector();
        // Phase 1 succeeds (default behavior); phase 2 throws so the framework must roll back without committing
        // the merged configuration onto active.
        doThrow(new FlowUpdateException("phase-2 failure")).when(connector).migrateState(any(ConnectorMigrationContext.class));

        final StandardConnectorMigrationManager migrationManager = newMigrationManager(flowController);
        final VersionedExternalFlow sourceFlow = createSourceFlowWithLocalStateCount(1);

        assertThrows(FlowUpdateException.class, () -> migrationManager.migrateFromVersionedFlow(CONNECTOR_ID, null, sourceFlow));

        // applyMigratedConfiguration ran (phase 1 succeeded), but the merged configuration must not be committed
        // because the phase-2 failure means the migration as a whole did not succeed.
        verify(connectorNode).applyMigratedConfiguration(any());
        verify(connectorNode, never()).commitMigratedConfiguration(any(ConnectorConfiguration.class));
        verify(connectorNode).loadInitialFlow();
    }

    @Test
    public void testNonFlowUpdateExceptionFromMigrateStillTriggersRollback() throws Exception {
        final FlowController flowController = createFlowController(1);
        final ConnectorRepository connectorRepository = flowController.getConnectorRepository();
        final ConnectorNode connectorNode = wireFreshConnector(flowController, CONNECTOR_ID);
        final MigratableConnector connector = (MigratableConnector) connectorNode.getConnector();
        doThrow(new RuntimeException("boom")).when(connector).migrateConfiguration(any(ConnectorMigrationContext.class));

        final StandardConnectorMigrationManager migrationManager = newMigrationManager(flowController);
        final VersionedExternalFlow sourceFlow = createSourceFlowWithLocalStateCount(1);

        assertThrows(FlowUpdateException.class, () -> migrationManager.migrateFromVersionedFlow(CONNECTOR_ID, null, sourceFlow));

        verify(connectorNode).loadInitialFlow();
        verify(connectorRepository).discardWorkingConfiguration(connectorNode);
        verify(connectorNode, never()).commitMigratedConfiguration(any(ConnectorConfiguration.class));
    }

    @Test
    public void testRollbackClearsLocalAndClusterStateAndDeletesOnlyCopiedAssets() throws Exception {
        final FlowController flowController = createFlowController(1);
        final StateManagerProvider stateManagerProvider = flowController.getStateManagerProvider();
        final StateManager componentStateManager = mock(StateManager.class);
        when(stateManagerProvider.getStateManager(anyString())).thenReturn(componentStateManager);

        final ConnectorRepository connectorRepository = flowController.getConnectorRepository();
        final ConnectorNode connectorNode = wireFreshConnector(flowController, CONNECTOR_ID);

        final ProcessorNode processor = mock(ProcessorNode.class);
        when(processor.getIdentifier()).thenReturn("processor-runtime-id");
        final ControllerServiceNode controllerService = mock(ControllerServiceNode.class);
        when(controllerService.getIdentifier()).thenReturn("service-runtime-id");
        final ProcessGroup managedGroup = mock(ProcessGroup.class);
        when(managedGroup.findAllProcessors()).thenReturn(List.of(processor));
        when(managedGroup.findAllControllerServices()).thenReturn(Set.of(controllerService));
        when(connectorNode.getActiveFlowContext().getManagedProcessGroup()).thenReturn(managedGroup);

        final MigratableConnector connector = (MigratableConnector) connectorNode.getConnector();
        doThrow(new FlowUpdateException("nope")).when(connector).migrateConfiguration(any(ConnectorMigrationContext.class));

        final StandardConnectorMigrationManager migrationManager = newMigrationManager(flowController);
        final VersionedExternalFlow sourceFlow = createSourceFlowWithLocalStateCount(1);

        assertThrows(FlowUpdateException.class, () -> migrationManager.migrateFromVersionedFlow(CONNECTOR_ID, null, sourceFlow));

        // Rollback clears LOCAL and CLUSTER state for each component currently in the Connector's managed flow.
        verify(stateManagerProvider).getStateManager("processor-runtime-id");
        verify(stateManagerProvider).getStateManager("service-runtime-id");
        verify(componentStateManager, atLeastOnce()).clear(Scope.LOCAL);
        verify(componentStateManager, atLeastOnce()).clear(Scope.CLUSTER);

        verify(connectorRepository, never()).deleteAssets(eq(CONNECTOR_ID), any());
        verify(connectorNode).loadInitialFlow();
    }

    @Test
    public void testListingIncludesStaleAndLocallyModifiedSourcesAsNotReady() {
        final FlowController flowController = createFlowController(1);
        final ConnectorNode connectorNode = wireFreshConnector(flowController, CONNECTOR_ID);
        stubFrameworkMigrationGating(connectorNode);

        final ProcessGroup rootGroup = mock(ProcessGroup.class);
        when(flowController.getFlowManager().getRootGroup()).thenReturn(rootGroup);

        final ProcessGroup staleGroup = mockVersionedGroup("stale-group", VersionedFlowState.STALE);
        final ProcessGroup locallyModifiedGroup = mockVersionedGroup("locally-modified-group", VersionedFlowState.LOCALLY_MODIFIED);
        when(rootGroup.findAllProcessGroups()).thenReturn(List.of(staleGroup, locallyModifiedGroup));
        when(rootGroup.findProcessGroup("stale-group")).thenReturn(staleGroup);
        when(rootGroup.findProcessGroup("locally-modified-group")).thenReturn(locallyModifiedGroup);

        final StandardConnectorMigrationManager migrationManager = newMigrationManager(flowController);
        final List<ConnectorMigrationSource> sources = migrationManager.listMigrationSources(CONNECTOR_ID);

        assertEquals(2, sources.size(), "Stale and locally-modified groups must appear in the listing so users can remediate their state");

        assertIneligibilityReasonContains(findSource(sources, "stale-group"), "stale");
        assertIneligibilityReasonContains(findSource(sources, "locally-modified-group"), "local modifications");

        final IllegalStateException staleException = assertThrows(IllegalStateException.class,
                () -> migrationManager.verifyEligibility(CONNECTOR_ID, "stale-group"));
        assertTrue(staleException.getMessage().contains("stale"), staleException.getMessage());

        final IllegalStateException locallyModifiedException = assertThrows(IllegalStateException.class,
                () -> migrationManager.verifyEligibility(CONNECTOR_ID, "locally-modified-group"));
        assertTrue(locallyModifiedException.getMessage().contains("local modifications"), locallyModifiedException.getMessage());
    }

    @Test
    public void testVerifyEligibilitySurfacesEachReasonOnItsOwnLine() {
        final FlowController flowController = createFlowController(1);
        final ConnectorNode connectorNode = wireFreshConnector(flowController, CONNECTOR_ID);
        stubFrameworkMigrationGating(connectorNode);

        final ProcessGroup rootGroup = mock(ProcessGroup.class);
        when(flowController.getFlowManager().getRootGroup()).thenReturn(rootGroup);

        final ProcessGroup multiIssueGroup = mockEligibleVersionedGroup("multi-issue-group");

        final ProcessorNode runningProcessor = mock(ProcessorNode.class);
        when(runningProcessor.getPhysicalScheduledState()).thenReturn(ScheduledState.RUNNING);
        when(multiIssueGroup.findAllProcessors()).thenReturn(List.of(runningProcessor));

        final Connection connection = mock(Connection.class);
        final FlowFileQueue queue = mock(FlowFileQueue.class);
        when(queue.size()).thenReturn(new QueueSize(7, 2048L));
        when(connection.getFlowFileQueue()).thenReturn(queue);
        when(multiIssueGroup.findAllConnections()).thenReturn(List.of(connection));

        when(rootGroup.findProcessGroup("multi-issue-group")).thenReturn(multiIssueGroup);

        final StandardConnectorMigrationManager migrationManager = newMigrationManager(flowController);
        final IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> migrationManager.verifyEligibility(CONNECTOR_ID, "multi-issue-group"));

        // Each ineligibility reason must appear on its own line so the operator can identify and address every
        // condition independently instead of seeing them joined into a single run-on sentence.
        final String message = exception.getMessage();
        assertTrue(message.contains("There is 1 running or enabled processor."), message);
        assertTrue(message.contains("There are 7 queued FlowFiles."), message);
        assertTrue(message.contains(System.lineSeparator()), "verifyEligibility must separate reasons with the system line separator: " + message);
    }

    @Test
    public void testListingSurfacesAllConcurrentIneligibilityReasons() {
        final FlowController flowController = createFlowController(1);
        final ConnectorNode connectorNode = wireFreshConnector(flowController, CONNECTOR_ID);
        stubFrameworkMigrationGating(connectorNode);

        final ProcessGroup rootGroup = mock(ProcessGroup.class);
        when(flowController.getFlowManager().getRootGroup()).thenReturn(rootGroup);

        // A single group that simultaneously fails three soft-filter conditions: running processors, enabled controller
        // services, and queued FlowFiles. Every applicable reason must appear in the DTO so the user can remediate them
        // together rather than discovering them one at a time.
        final ProcessGroup multiIssueGroup = mockEligibleVersionedGroup("multi-issue-group");

        final ProcessorNode runningProcessor = mock(ProcessorNode.class);
        when(runningProcessor.getPhysicalScheduledState()).thenReturn(ScheduledState.RUNNING);
        final ProcessorNode stoppedProcessor = mock(ProcessorNode.class);
        when(stoppedProcessor.getPhysicalScheduledState()).thenReturn(ScheduledState.STOPPED);
        when(multiIssueGroup.findAllProcessors()).thenReturn(List.of(runningProcessor, stoppedProcessor));

        final ControllerServiceNode enabledService = mock(ControllerServiceNode.class);
        when(enabledService.getState()).thenReturn(ControllerServiceState.ENABLED);
        final ControllerServiceNode disabledService = mock(ControllerServiceNode.class);
        when(disabledService.getState()).thenReturn(ControllerServiceState.DISABLED);
        when(multiIssueGroup.findAllControllerServices()).thenReturn(Set.of(enabledService, disabledService));

        final Connection connection = mock(Connection.class);
        final FlowFileQueue queue = mock(FlowFileQueue.class);
        when(queue.size()).thenReturn(new QueueSize(7, 2048L));
        when(connection.getFlowFileQueue()).thenReturn(queue);
        when(multiIssueGroup.findAllConnections()).thenReturn(List.of(connection));

        when(rootGroup.findAllProcessGroups()).thenReturn(List.of(multiIssueGroup));

        final StandardConnectorMigrationManager migrationManager = newMigrationManager(flowController);
        final List<ConnectorMigrationSource> sources = migrationManager.listMigrationSources(CONNECTOR_ID);

        assertEquals(1, sources.size());
        final ConnectorMigrationSource source = sources.get(0);
        assertFalse(source.isReadyForMigration());
        final List<String> reasons = source.getIneligibilityReasons();
        assertEquals(3, reasons.size(), "All three concurrent ineligibility conditions must be surfaced: " + reasons);
        assertTrue(reasons.contains("There is 1 running or enabled processor."), reasons.toString());
        assertTrue(reasons.contains("There is 1 enabled controller service."), reasons.toString());
        assertTrue(reasons.contains("There are 7 queued FlowFiles."), reasons.toString());
    }

    @Test
    public void testListingMessageFormatsHandleSingularAndPluralCounts() {
        final FlowController flowController = createFlowController(1);
        final ConnectorNode connectorNode = wireFreshConnector(flowController, CONNECTOR_ID);
        stubFrameworkMigrationGating(connectorNode);

        final ProcessGroup rootGroup = mock(ProcessGroup.class);
        when(flowController.getFlowManager().getRootGroup()).thenReturn(rootGroup);

        final ProcessGroup pluralGroup = mockEligibleVersionedGroup("plural-group");
        final ProcessorNode runningProcessor1 = mock(ProcessorNode.class);
        when(runningProcessor1.getPhysicalScheduledState()).thenReturn(ScheduledState.RUNNING);
        final ProcessorNode runningProcessor2 = mock(ProcessorNode.class);
        when(runningProcessor2.getPhysicalScheduledState()).thenReturn(ScheduledState.RUNNING);
        final ProcessorNode runningProcessor3 = mock(ProcessorNode.class);
        when(runningProcessor3.getPhysicalScheduledState()).thenReturn(ScheduledState.RUNNING);
        when(pluralGroup.findAllProcessors()).thenReturn(List.of(runningProcessor1, runningProcessor2, runningProcessor3));

        when(rootGroup.findAllProcessGroups()).thenReturn(List.of(pluralGroup));

        final StandardConnectorMigrationManager migrationManager = newMigrationManager(flowController);
        final List<ConnectorMigrationSource> sources = migrationManager.listMigrationSources(CONNECTOR_ID);

        assertEquals(1, sources.size());
        assertEquals(List.of("There are 3 running or enabled processors."), sources.get(0).getIneligibilityReasons());
    }

    @Test
    public void testListingMarksProcessGroupsWithRemediableStateAsNotReady() {
        final FlowController flowController = createFlowController(1);
        final ConnectorNode connectorNode = wireFreshConnector(flowController, CONNECTOR_ID);
        stubFrameworkMigrationGating(connectorNode);

        final ProcessGroup rootGroup = mock(ProcessGroup.class);
        when(flowController.getFlowManager().getRootGroup()).thenReturn(rootGroup);

        final ProcessGroup runningGroup = mockEligibleVersionedGroup("running-group");
        final ProcessorNode runningProcessor = mock(ProcessorNode.class);
        when(runningProcessor.getPhysicalScheduledState()).thenReturn(ScheduledState.RUNNING);
        when(runningGroup.findAllProcessors()).thenReturn(List.of(runningProcessor));

        final ProcessGroup enabledServicesGroup = mockEligibleVersionedGroup("enabled-services-group");
        final ControllerServiceNode enabledService = mock(ControllerServiceNode.class);
        when(enabledService.getState()).thenReturn(ControllerServiceState.ENABLED);
        when(enabledServicesGroup.findAllControllerServices()).thenReturn(Set.of(enabledService));

        final ProcessGroup queuedGroup = mockEligibleVersionedGroup("queued-group");
        final Connection connection = mock(Connection.class);
        final FlowFileQueue queue = mock(FlowFileQueue.class);
        when(queue.size()).thenReturn(new QueueSize(5, 1024L));
        when(connection.getFlowFileQueue()).thenReturn(queue);
        when(queuedGroup.findAllConnections()).thenReturn(List.of(connection));

        when(rootGroup.findAllProcessGroups()).thenReturn(List.of(runningGroup, enabledServicesGroup, queuedGroup));

        final StandardConnectorMigrationManager migrationManager = newMigrationManager(flowController);
        final List<ConnectorMigrationSource> sources = migrationManager.listMigrationSources(CONNECTOR_ID);

        assertEquals(3, sources.size());
        assertIneligibilityReasonContains(findSource(sources, "running-group"), "running or enabled");
        assertIneligibilityReasonContains(findSource(sources, "enabled-services-group"), "enabled controller service");
        assertIneligibilityReasonContains(findSource(sources, "queued-group"), "queued FlowFile");
    }

    @Test
    public void testListingMarksProcessGroupReferencingExternalControllerServicesAsNotReady() {
        final FlowController flowController = createFlowController(1);
        final ConnectorNode connectorNode = wireFreshConnector(flowController, CONNECTOR_ID);
        stubFrameworkMigrationGating(connectorNode);

        final ProcessGroup rootGroup = mock(ProcessGroup.class);
        when(flowController.getFlowManager().getRootGroup()).thenReturn(rootGroup);

        final ProcessGroup externalReferencingGroup = mockEligibleVersionedGroup("external-services-group");
        when(rootGroup.findAllProcessGroups()).thenReturn(List.of(externalReferencingGroup));

        final RegisteredFlowSnapshot snapshotWithExternalServices = new RegisteredFlowSnapshot();
        snapshotWithExternalServices.setFlowContents(new VersionedProcessGroup());
        snapshotWithExternalServices.setFlow(new RegisteredFlow());
        snapshotWithExternalServices.setExternalControllerServices(Map.of("external-service", new ExternalControllerServiceReference()));
        final ConnectorFlowSnapshotProvider snapshotProvider = (groupId, includeReferencedServices, includeComponentState) -> snapshotWithExternalServices;
        final StandardConnectorMigrationManager migrationManager = new StandardConnectorMigrationManager(flowController, snapshotProvider);

        final List<ConnectorMigrationSource> sources = migrationManager.listMigrationSources(CONNECTOR_ID);

        assertEquals(1, sources.size());
        assertIneligibilityReasonContains(sources.get(0), "from outside the Process Group");
    }

    @Test
    public void testListingPopulatesAllDtoFieldsForReadySource() {
        final FlowController flowController = createFlowController(1);
        final ConnectorNode connectorNode = wireFreshConnector(flowController, CONNECTOR_ID);
        stubFrameworkMigrationGating(connectorNode);

        final ProcessGroup rootGroup = mock(ProcessGroup.class);
        when(rootGroup.getIdentifier()).thenReturn("root-group");
        when(flowController.getFlowManager().getRootGroup()).thenReturn(rootGroup);

        final ProcessGroup eligibleGroup = mockEligibleVersionedGroup("eligible-group");
        when(eligibleGroup.getParent()).thenReturn(rootGroup);
        final VersionControlInformation versionControlInformation = eligibleGroup.getVersionControlInformation();
        when(versionControlInformation.getFlowName()).thenReturn("My Flow");
        when(versionControlInformation.getRegistryIdentifier()).thenReturn("registry-1");
        when(versionControlInformation.getBucketIdentifier()).thenReturn("bucket-1");
        when(versionControlInformation.getFlowIdentifier()).thenReturn("flow-1");
        when(versionControlInformation.getVersion()).thenReturn("1");
        when(rootGroup.findAllProcessGroups()).thenReturn(List.of(eligibleGroup));

        final StandardConnectorMigrationManager migrationManager = newMigrationManager(flowController);
        final List<ConnectorMigrationSource> sources = migrationManager.listMigrationSources(CONNECTOR_ID);

        assertEquals(1, sources.size());
        final ConnectorMigrationSource source = sources.get(0);
        assertEquals("eligible-group", source.getProcessGroupId());
        assertEquals("root-group", source.getParentProcessGroupId());
        assertEquals("registry-1", source.getRegistryClientId());
        assertEquals("bucket-1", source.getBucketId());
        assertEquals("flow-1", source.getFlowId());
        assertEquals("My Flow", source.getFlowName());
        assertEquals("1", source.getVersion());
        assertTrue(source.isReadyForMigration());
        assertEquals(List.of(), source.getIneligibilityReasons());
    }

    @Test
    public void testListingExcludesNonRegistryBackedProcessGroups() {
        final FlowController flowController = createFlowController(1);
        wireFreshConnector(flowController, CONNECTOR_ID);

        final ProcessGroup rootGroup = mock(ProcessGroup.class);
        when(flowController.getFlowManager().getRootGroup()).thenReturn(rootGroup);

        // Process group has no version control information at all, so it must not appear in listing.
        final ProcessGroup nonRegistryBacked = mock(ProcessGroup.class);
        when(nonRegistryBacked.getIdentifier()).thenReturn("non-registry-backed");
        when(nonRegistryBacked.getName()).thenReturn("Non Registry Backed");
        when(nonRegistryBacked.getVersionControlInformation()).thenReturn(null);
        when(nonRegistryBacked.getConnectorIdentifier()).thenReturn(Optional.empty());
        when(rootGroup.findAllProcessGroups()).thenReturn(List.of(nonRegistryBacked));

        final StandardConnectorMigrationManager migrationManager = newMigrationManager(flowController);
        final List<ConnectorMigrationSource> sources = migrationManager.listMigrationSources(CONNECTOR_ID);

        assertTrue(sources.isEmpty(), "Process groups without VersionControlInformation must not appear in the migration sources listing");
    }

    @Test
    public void testEligibilityRejectsNonVersionControlledSource() {
        final FlowController flowController = createFlowController(1);
        wireFreshConnector(flowController, CONNECTOR_ID);

        final ProcessGroup rootGroup = mock(ProcessGroup.class);
        when(flowController.getFlowManager().getRootGroup()).thenReturn(rootGroup);
        final ProcessGroup nonVersioned = mock(ProcessGroup.class);
        when(nonVersioned.getIdentifier()).thenReturn("non-versioned");
        when(nonVersioned.getVersionControlInformation()).thenReturn(null);
        when(nonVersioned.getConnectorIdentifier()).thenReturn(Optional.empty());
        when(rootGroup.findProcessGroup("non-versioned")).thenReturn(nonVersioned);

        final StandardConnectorMigrationManager migrationManager = newMigrationManager(flowController);

        final IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> migrationManager.verifyEligibility(CONNECTOR_ID, "non-versioned"));
        assertTrue(exception.getMessage().contains("not under version control"), exception.getMessage());
    }

    @Test
    public void testValidateMigrationSourceRejectsExternalControllerServices() {
        final FlowController flowController = createFlowController(1);
        wireFreshConnector(flowController, CONNECTOR_ID);

        final StandardConnectorMigrationManager migrationManager = newMigrationManager(flowController);
        final VersionedExternalFlow sourceFlow = createSourceFlowWithLocalStateCount(1);
        sourceFlow.setExternalControllerServices(Map.of("external-service", new ExternalControllerServiceReference()));

        final IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> migrationManager.migrateFromVersionedFlow(CONNECTOR_ID, null, sourceFlow));
        assertTrue(exception.getMessage().contains("services outside its managed flow"), exception.getMessage());
    }

    @Test
    public void testRejectMigrationWhenTargetHasBeenModifiedSinceCreation() {
        final FlowController flowController = createFlowController(1);
        final ConnectorNode connectorNode = wireFreshConnector(flowController, CONNECTOR_ID);

        // The Connector's managed flow no longer matches the flow that was loaded when the Connector was
        // created, so a migration would overwrite user modifications.
        when(connectorNode.matchesInitialFlow()).thenReturn(false);

        final StandardConnectorMigrationManager migrationManager = newMigrationManager(flowController);
        final VersionedExternalFlow sourceFlow = createSourceFlowWithLocalStateCount(1);

        final IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> migrationManager.migrateFromVersionedFlow(CONNECTOR_ID, null, sourceFlow));
        assertTrue(exception.getMessage().contains("modified since it was created"), exception.getMessage());
    }

    @Test
    public void testRollbackFailureDoesNotMaskOriginalMigrationFailure() throws Exception {
        final FlowController flowController = createFlowController(1);
        final ConnectorNode connectorNode = wireFreshConnector(flowController, CONNECTOR_ID);
        final MigratableConnector connector = (MigratableConnector) connectorNode.getConnector();

        final FlowUpdateException originalFailure = new FlowUpdateException("original");
        doThrow(originalFailure).when(connector).migrateConfiguration(any(ConnectorMigrationContext.class));
        doThrow(new FlowUpdateException("rollback failed")).when(connectorNode).loadInitialFlow();

        final StandardConnectorMigrationManager migrationManager = newMigrationManager(flowController);
        final VersionedExternalFlow sourceFlow = createSourceFlowWithLocalStateCount(1);

        final FlowUpdateException thrown = assertThrows(FlowUpdateException.class,
                () -> migrationManager.migrateFromVersionedFlow(CONNECTOR_ID, null, sourceFlow));

        // The caller must see the original migration failure, not the rollback failure.
        assertEquals("original", thrown.getMessage());
    }

    @Test
    public void testIsMigrationSupportedFalseDoesNotTriggerRollback() throws Exception {
        final FlowController flowController = createFlowController(1);
        final ConnectorRepository connectorRepository = flowController.getConnectorRepository();
        final ConnectorNode connectorNode = wireFreshConnector(flowController, CONNECTOR_ID);
        when(((MigratableConnector) connectorNode.getConnector()).isMigrationSupported(any())).thenReturn(false);

        final StandardConnectorMigrationManager migrationManager = newMigrationManager(flowController);
        final VersionedExternalFlow sourceFlow = createSourceFlowWithLocalStateCount(1);

        assertThrows(FlowUpdateException.class, () -> migrationManager.migrateFromVersionedFlow(CONNECTOR_ID, null, sourceFlow));

        // An eligibility rejection must not exercise the rollback machinery: no flow was attempted.
        verify(connectorNode, never()).loadInitialFlow();
        verify(connectorRepository, never()).discardWorkingConfiguration(connectorNode);
        verify(connectorRepository, never()).deleteAssets(eq(CONNECTOR_ID), any());
    }

    @Test
    public void testEachIneligibleVersionedFlowStateHasDistinctDiagnostic() {
        final FlowController flowController = createFlowController(1);
        wireFreshConnector(flowController, CONNECTOR_ID);

        final ProcessGroup rootGroup = mock(ProcessGroup.class);
        when(flowController.getFlowManager().getRootGroup()).thenReturn(rootGroup);

        for (final VersionedFlowState state : VersionedFlowState.values()) {
            if (state == VersionedFlowState.UP_TO_DATE) {
                continue;
            }
            final ProcessGroup processGroup = mockVersionedGroup("group-" + state, state);
            when(rootGroup.findProcessGroup("group-" + state)).thenReturn(processGroup);
        }

        final StandardConnectorMigrationManager migrationManager = newMigrationManager(flowController);

        final String staleMessage = assertThrows(IllegalStateException.class,
                () -> migrationManager.verifyEligibility(CONNECTOR_ID, "group-STALE")).getMessage();
        final String locallyModifiedMessage = assertThrows(IllegalStateException.class,
                () -> migrationManager.verifyEligibility(CONNECTOR_ID, "group-LOCALLY_MODIFIED")).getMessage();
        final String locallyModifiedAndStaleMessage = assertThrows(IllegalStateException.class,
                () -> migrationManager.verifyEligibility(CONNECTOR_ID, "group-LOCALLY_MODIFIED_AND_STALE")).getMessage();
        final String syncFailureMessage = assertThrows(IllegalStateException.class,
                () -> migrationManager.verifyEligibility(CONNECTOR_ID, "group-SYNC_FAILURE")).getMessage();

        assertEquals(4, Set.of(staleMessage, locallyModifiedMessage, locallyModifiedAndStaleMessage, syncFailureMessage).size(),
                "Each non-UP_TO_DATE VersionedFlowState must produce a unique diagnostic message");
    }

    @Test
    public void testListMigrationSourcesReturnsEmptyForNonMigratableConnector() {
        final FlowController flowController = createFlowController(1);
        final ConnectorNode connectorNode = wireFreshConnector(flowController, CONNECTOR_ID, false);
        stubFrameworkMigrationGating(connectorNode);

        final ProcessGroup rootGroup = mock(ProcessGroup.class);
        when(flowController.getFlowManager().getRootGroup()).thenReturn(rootGroup);
        final ProcessGroup eligibleGroup = mockEligibleVersionedGroup("eligible-group");
        when(rootGroup.findAllProcessGroups()).thenReturn(List.of(eligibleGroup));

        final StandardConnectorMigrationManager migrationManager = newMigrationManager(flowController);

        assertTrue(migrationManager.listMigrationSources(CONNECTOR_ID).isEmpty(),
                "A Connector that does not implement MigratableConnector must never appear in the listing");
    }

    @Test
    public void testMigrateRejectsConnectorThatDoesNotImplementMigratableConnector() throws Exception {
        final FlowController flowController = createFlowController(1);
        final ConnectorRepository connectorRepository = flowController.getConnectorRepository();
        final ConnectorNode connectorNode = wireFreshConnector(flowController, CONNECTOR_ID, false);

        final StandardConnectorMigrationManager migrationManager = newMigrationManager(flowController);
        final VersionedExternalFlow sourceFlow = createSourceFlowWithLocalStateCount(1);

        final FlowUpdateException exception = assertThrows(FlowUpdateException.class,
                () -> migrationManager.migrateFromVersionedFlow(CONNECTOR_ID, null, sourceFlow));
        assertTrue(exception.getMessage().contains("does not support migration"), exception.getMessage());

        // Rejection happens before any flow manipulation is attempted, so rollback must not be exercised.
        verify(connectorNode, never()).loadInitialFlow();
        verify(connectorRepository, never()).deleteAssets(eq(CONNECTOR_ID), any());
    }

    @Test
    public void testMigrateRejectsRunningConnector() {
        final FlowController flowController = createFlowController(1);
        final ConnectorNode connectorNode = wireFreshConnector(flowController, CONNECTOR_ID);
        when(connectorNode.getCurrentState()).thenReturn(ConnectorState.RUNNING);
        when(connectorNode.getDesiredState()).thenReturn(ConnectorState.RUNNING);

        final StandardConnectorMigrationManager migrationManager = newMigrationManager(flowController);
        final VersionedExternalFlow sourceFlow = createSourceFlowWithLocalStateCount(1);

        final IllegalStateException exception = assertThrows(IllegalStateException.class,
                () -> migrationManager.migrateFromVersionedFlow(CONNECTOR_ID, null, sourceFlow));
        assertTrue(exception.getMessage().contains("must be stopped before it can be migrated"), exception.getMessage());
    }

    @Test
    public void testListMigrationSourcesPropagatesEligibilityWrapper() throws Exception {
        final FlowController flowController = createFlowController(1);
        final ConnectorRepository connectorRepository = flowController.getConnectorRepository();
        final ConnectorNode connectorNode = wireFreshConnector(flowController, CONNECTOR_ID);
        stubFrameworkMigrationGating(connectorNode);

        final MigratableConnector migratableConnector = (MigratableConnector) connectorNode.getConnector();
        when(migratableConnector.isMigrationSupported(any())).thenAnswer(invocation -> {
            final ConnectorMigrationContext context = invocation.getArgument(0);
            context.copyAssetFromSource("any-id");
            return true;
        });

        final ProcessGroup rootGroup = mock(ProcessGroup.class);
        when(flowController.getFlowManager().getRootGroup()).thenReturn(rootGroup);
        final ProcessGroup eligibleGroup = mockEligibleVersionedGroup("eligible-group");
        when(rootGroup.findAllProcessGroups()).thenReturn(List.of(eligibleGroup));

        final StandardConnectorMigrationManager migrationManager = newMigrationManager(flowController);

        assertTrue(migrationManager.listMigrationSources(CONNECTOR_ID).isEmpty(),
                "A Connector that calls copyAssetFromSource() from isMigrationSupported() must be filtered out by the eligibility wrapper");
        verify(connectorRepository, never()).storeAsset(anyString(), anyString(), anyString(), any(InputStream.class));
    }

    private static void assertIneligibilityReasonContains(final ConnectorMigrationSource source, final String expectedSubstring) {
        assertFalse(source.isReadyForMigration(), "Expected source " + source.getProcessGroupId() + " to be marked not ready for migration");
        final List<String> reasons = source.getIneligibilityReasons();
        boolean found = false;
        for (final String reason : reasons) {
            if (reason.contains(expectedSubstring)) {
                found = true;
                break;
            }
        }
        assertTrue(found, "Ineligibility reasons for source " + source.getProcessGroupId()
                + " should contain at least one entry with substring '" + expectedSubstring + "' but were: " + reasons);
    }

    private static ConnectorMigrationSource findSource(final List<ConnectorMigrationSource> sources, final String processGroupId) {
        for (final ConnectorMigrationSource source : sources) {
            if (processGroupId.equals(source.getProcessGroupId())) {
                return source;
            }
        }
        throw new AssertionError("No source with processGroupId " + processGroupId);
    }

    private ProcessGroup mockEligibleVersionedGroup(final String groupId) {
        final ProcessGroup processGroup = mockVersionedGroup(groupId, VersionedFlowState.UP_TO_DATE);
        when(processGroup.getName()).thenReturn(groupId);
        return processGroup;
    }

    private StandardConnectorMigrationManager newMigrationManager(final FlowController flowController) {
        // The snapshot provider is only exercised by the listing path. Tests that drive listing wire their own snapshots.
        final RegisteredFlowSnapshot emptySnapshot = new RegisteredFlowSnapshot();
        emptySnapshot.setFlowContents(new VersionedProcessGroup());
        emptySnapshot.setFlow(new RegisteredFlow());
        final ConnectorFlowSnapshotProvider snapshotProvider = (groupId, includeReferencedServices, includeComponentState) -> emptySnapshot;
        return new StandardConnectorMigrationManager(flowController, snapshotProvider);
    }

    private FlowController createFlowController(final int connectedNodeCount) {
        final FlowController flowController = mock(FlowController.class);
        when(flowController.getConnectorRepository()).thenReturn(mock(ConnectorRepository.class));
        when(flowController.getFlowManager()).thenReturn(mock(FlowManager.class));
        when(flowController.getExtensionManager()).thenReturn(mock(ExtensionManager.class));
        when(flowController.getAssetManager()).thenReturn(mock(AssetManager.class));
        when(flowController.getConnectorAssetManager()).thenReturn(mock(AssetManager.class));
        when(flowController.getStateManagerProvider()).thenReturn(mock(StateManagerProvider.class));
        when(flowController.getConnectedNodeCount()).thenReturn(connectedNodeCount);
        return flowController;
    }

    private ConnectorNode wireFreshConnector(final FlowController flowController, final String connectorId) {
        return wireFreshConnector(flowController, connectorId, true);
    }

    private ConnectorNode wireFreshConnector(final FlowController flowController, final String connectorId, final boolean migratable) {
        final ConnectorNode connectorNode = mock(ConnectorNode.class);
        when(connectorNode.getIdentifier()).thenReturn(connectorId);
        when(connectorNode.getCurrentState()).thenReturn(ConnectorState.STOPPED);
        when(connectorNode.getDesiredState()).thenReturn(ConnectorState.STOPPED);
        when(connectorNode.matchesInitialFlow()).thenReturn(true);
        final FrameworkFlowContext flowContext = mock(FrameworkFlowContext.class);
        when(connectorNode.getActiveFlowContext()).thenReturn(flowContext);
        // The manager seeds the migration context with a working clone of the connector's active configuration, so the
        // mock active flow context must expose a configuration context whose clone() returns a usable working config.
        final MutableConnectorConfigurationContext configurationContext = mock(MutableConnectorConfigurationContext.class);
        when(configurationContext.clone()).thenReturn(configurationContext);
        when(flowContext.getConfigurationContext()).thenReturn(configurationContext);
        // Mirror the contract of StandardConnectorNode.applyMigratedConfiguration: return a non-null merged
        // configuration the manager can hand to commitMigratedConfiguration once the state phase succeeds.
        try {
            when(connectorNode.applyMigratedConfiguration(any())).thenReturn(new ConnectorConfiguration(Set.of()));
        } catch (final FlowUpdateException e) {
            throw new AssertionError(e);
        }

        final Connector connector;
        if (migratable) {
            connector = mock(Connector.class, withSettings().extraInterfaces(MigratableConnector.class));
            when(((MigratableConnector) connector).isMigrationSupported(any())).thenReturn(true);
        } else {
            connector = mock(Connector.class);
        }
        when(connectorNode.getConnector()).thenReturn(connector);
        when(flowController.getConnectorRepository().getConnector(connectorId, ConnectorSyncMode.LOCAL_ONLY)).thenReturn(connectorNode);
        return connectorNode;
    }

    /**
     * Mirrors {@link StandardConnectorNode#isMigrationSupported} on the mock {@link ConnectorNode}: returns false
     * unless the underlying {@link Connector} implements {@link MigratableConnector}, and swallows any exception
     * thrown by the underlying call by returning false. Tests that exercise the listing/eligibility path must call
     * this so the mock reflects the framework-side gating rather than Mockito's default {@code false}.
     */
    private void stubFrameworkMigrationGating(final ConnectorNode connectorNode) {
        when(connectorNode.isMigrationSupported(any())).thenAnswer(invocation -> {
            final Connector connector = connectorNode.getConnector();
            if (!(connector instanceof final MigratableConnector migratableConnector)) {
                return false;
            }
            try {
                return migratableConnector.isMigrationSupported(invocation.getArgument(0));
            } catch (final Exception ignored) {
                return false;
            }
        });
    }

    private ProcessGroup wireSourceProcessGroup(final FlowController flowController, final String groupId, final String groupName) {
        final FlowManager flowManager = flowController.getFlowManager();
        final ProcessGroup rootGroup = mock(ProcessGroup.class);
        final ProcessGroup sourceProcessGroup = mock(ProcessGroup.class);
        when(flowManager.getRootGroup()).thenReturn(rootGroup);
        when(rootGroup.findProcessGroup(groupId)).thenReturn(sourceProcessGroup);
        when(sourceProcessGroup.getName()).thenReturn(groupName);
        return sourceProcessGroup;
    }

    private ProcessGroup mockVersionedGroup(final String groupId, final VersionedFlowState state) {
        final ProcessGroup processGroup = mock(ProcessGroup.class);
        when(processGroup.getIdentifier()).thenReturn(groupId);
        when(processGroup.getConnectorIdentifier()).thenReturn(Optional.empty());
        final VersionControlInformation versionControlInformation = mock(VersionControlInformation.class);
        final VersionedFlowStatus status = mock(VersionedFlowStatus.class);
        when(status.getState()).thenReturn(state);
        when(versionControlInformation.getStatus()).thenReturn(status);
        when(processGroup.getVersionControlInformation()).thenReturn(versionControlInformation);
        return processGroup;
    }

    private VersionedExternalFlow createSourceFlowWithLocalStateCount(final int localStateCount) {
        final VersionedComponentState componentState = new VersionedComponentState();
        componentState.setLocalNodeStates(createNodeStates(localStateCount));

        final VersionedProcessor processor = new VersionedProcessor();
        processor.setIdentifier("processor-1");
        processor.setName("count-1");
        processor.setComponentState(componentState);

        final VersionedProcessGroup processGroup = new VersionedProcessGroup();
        processGroup.setIdentifier("group-1");
        processGroup.setName("Source Flow");
        processGroup.setProcessors(Collections.singleton(processor));

        final VersionedExternalFlow sourceFlow = new VersionedExternalFlow();
        sourceFlow.setFlowContents(processGroup);
        sourceFlow.setExternalControllerServices(Collections.emptyMap());
        sourceFlow.setParameterContexts(Collections.emptyMap());
        sourceFlow.setParameterProviders(Collections.emptyMap());
        return sourceFlow;
    }

    private List<VersionedNodeState> createNodeStates(final int localStateCount) {
        final List<VersionedNodeState> nodeStates = new ArrayList<>();
        for (int i = 0; i < localStateCount; i++) {
            nodeStates.add(new VersionedNodeState(Map.of("count", Integer.toString(i + 1))));
        }
        return nodeStates;
    }
}
