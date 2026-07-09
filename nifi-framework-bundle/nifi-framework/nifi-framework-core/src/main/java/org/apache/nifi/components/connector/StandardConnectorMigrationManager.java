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

import org.apache.nifi.annotation.behavior.Stateful;
import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.components.connector.migration.MigratableConnector;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ScheduledState;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.ControllerServiceState;
import org.apache.nifi.flow.ExternalControllerServiceReference;
import org.apache.nifi.flow.VersionedComponentState;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedExternalFlowMetadata;
import org.apache.nifi.flow.VersionedNodeState;
import org.apache.nifi.flow.synchronization.VersionedComponentStateValidator;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.registry.flow.RegisteredFlow;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshotMetadata;
import org.apache.nifi.registry.flow.VersionControlInformation;
import org.apache.nifi.registry.flow.VersionedFlowState;
import org.apache.nifi.registry.flow.VersionedFlowStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BooleanSupplier;

public class StandardConnectorMigrationManager implements ConnectorMigrationManager {
    private static final Logger logger = LoggerFactory.getLogger(StandardConnectorMigrationManager.class);

    private static final String MIGRATED_SOURCE_PREFIX = "(Migrated) ";

    private final FlowController flowController;
    private final ConnectorFlowSnapshotProvider snapshotProvider;

    public StandardConnectorMigrationManager(final FlowController flowController, final ConnectorFlowSnapshotProvider snapshotProvider) {
        this.flowController = Objects.requireNonNull(flowController, "FlowController is required");
        this.snapshotProvider = Objects.requireNonNull(snapshotProvider, "ConnectorFlowSnapshotProvider is required");
    }

    @Override
    public List<ConnectorMigrationSource> listMigrationSources(final String connectorId) {
        final ConnectorNode connector = getRequiredConnector(connectorId);
        final List<ConnectorMigrationSource> migrationSources = new ArrayList<>();

        for (final ProcessGroup processGroup : getCandidateSourceGroups()) {
            buildMigrationSourceForListing(connector, processGroup).ifPresent(migrationSources::add);
        }

        return migrationSources;
    }

    /**
     * Builds a {@link ConnectorMigrationSource} for the given Process Group when it is structurally compatible with the
     * target Connector. Returns {@link Optional#empty()} when the Process Group fails a hard filter (not under version
     * control, already managed by a Connector, or rejected by the Connector's own structural compatibility check).
     * Returns a populated source — possibly with {@link ConnectorMigrationSource#isReadyForMigration()} set to
     * {@code false} and an ineligibility reason — when the group is compatible but currently in a state that requires
     * user remediation before migration can proceed.
     */
    private Optional<ConnectorMigrationSource> buildMigrationSourceForListing(final ConnectorNode connector, final ProcessGroup processGroup) {
        final VersionControlInformation versionControlInformation = processGroup.getVersionControlInformation();
        if (versionControlInformation == null) {
            return Optional.empty();
        }
        if (processGroup.getConnectorIdentifier().isPresent()) {
            return Optional.empty();
        }

        final VersionedExternalFlow sourceFlow = obtainSourceFlowForListing(processGroup);
        if (!isConnectorMigrationSupported(connector, sourceFlow)) {
            return Optional.empty();
        }

        final List<String> stateIneligibilityReasons = getStateIneligibilityReasons(processGroup, versionControlInformation, sourceFlow);
        return Optional.of(toMigrationSource(processGroup, versionControlInformation, stateIneligibilityReasons));
    }

    @Override
    public void verifyEligibility(final String connectorId, final String processGroupId) {
        final ConnectorNode connector = getRequiredConnector(connectorId);
        final ProcessGroup processGroup = getRequiredSourceProcessGroup(processGroupId);
        final String ineligibilityReason = getIneligibilityReason(connector, processGroup);
        if (ineligibilityReason != null) {
            throw new IllegalStateException(ineligibilityReason);
        }
    }

    @Override
    public void verifyConnectorReadyForMigration(final String connectorId) {
        final ConnectorNode connector = getRequiredConnector(connectorId);
        verifyConnectorCanReceiveMigration(connector);
        verifyTargetIsAtInitialFlow(connector);
        flowController.getConnectorRepository().verifyMigration(connectorId);
    }

    @Override
    public void migrateFromVersionedFlow(final String connectorId, final String processGroupId, final VersionedExternalFlow sourceFlow,
            final BooleanSupplier cancellationCheck) throws FlowUpdateException {
        // Mark the migration in progress so a concurrent flow synchronization does not reclaim the assets this
        // migration copies before the managed Process Group is rebuilt to reference them. The marker is cleared in
        // the finally block whether the migration succeeds, fails, or rolls back.
        flowController.getConnectorRepository().beginMigration(connectorId);
        try {
            performMigration(connectorId, processGroupId, sourceFlow, cancellationCheck);
        } finally {
            flowController.getConnectorRepository().endMigration(connectorId);
        }
    }

    private void performMigration(final String connectorId, final String processGroupId, final VersionedExternalFlow sourceFlow,
            final BooleanSupplier cancellationCheck) throws FlowUpdateException {
        final BooleanSupplier cancellation = cancellationCheck == null ? () -> false : cancellationCheck;
        final ConnectorNode connector = getRequiredConnector(connectorId);
        verifyConnectorCanReceiveMigration(connector);
        verifyTargetIsAtInitialFlow(connector);
        // Give the ConnectorConfigurationProvider the chance to reject the migration before any flow manipulation
        // begins, so a rejection aborts the migration without exercising the rollback machinery.
        flowController.getConnectorRepository().verifyMigration(connectorId);
        validateMigrationSource(sourceFlow);

        final boolean localMigration = processGroupId != null;
        final ProcessGroup sourceProcessGroup = localMigration ? getRequiredSourceProcessGroup(processGroupId) : null;
        // Seed the migration context with a working clone of the connector's active configuration. The connector's
        // setProperties/replaceProperties calls during migrateConfiguration(...) mutate this clone directly, so it
        // always holds the fully-merged result. The active configuration is left untouched until commit.
        final MutableConnectorConfigurationContext workingConfiguration = connector.getActiveFlowContext().getConfigurationContext().clone();
        final StandardFrameworkConnectorMigrationContext migrationContext = new StandardFrameworkConnectorMigrationContext(
                connectorId,
                sourceFlow,
                localMigration,
                connector.getActiveFlowContext(),
                workingConfiguration,
                flowController.getAssetManager(),
                flowController.getConnectorRepository(),
                flowController.getStateManagerProvider(),
                flowController
        );

        final Connector rawConnector = connector.getConnector();
        if (!(rawConnector instanceof final MigratableConnector migratableConnector)) {
            throw new FlowUpdateException("Connector " + connectorId + " does not support migration from the provided source flow.");
        }

        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(flowController.getExtensionManager(), rawConnector.getClass(), connectorId)) {
            if (!migratableConnector.isMigrationSupported(migrationContext)) {
                throw new FlowUpdateException("Connector " + connectorId + " does not support migration from the provided source flow.");
            }
        }

        // Phase 1: drive migrateConfiguration(...) -> applyMigratedConfiguration(...). The connector records the
        // configuration it wants on the other side of migration by mutating the working configuration clone seeded
        // into the migration context; the framework then drives Connector.applyUpdate(workingContext, activeContext)
        // so the managed Process Group is rebuilt from that merged configuration. The active configuration itself is
        // intentionally not written here: the merged configuration is held until the state phase below succeeds, then
        // committed by commitMigratedConfiguration(...). That ordering keeps rollback simple - any failure in either
        // phase can be recovered by restoring the initial flow because the active configuration was never mutated.
        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(flowController.getExtensionManager(), rawConnector.getClass(), connectorId)) {
            flowController.getConnectorRepository().syncAssetsFromProvider(connector);
            migratableConnector.migrateConfiguration(migrationContext);

            if (cancellation.getAsBoolean()) {
                throw new FlowUpdateException("Migration of Connector " + connectorId + " was cancelled after migrateConfiguration() completed.");
            }
        } catch (final FlowUpdateException e) {
            failMigration(connector, migrationContext);
            throw e;
        } catch (final Exception e) {
            failMigration(connector, migrationContext);
            throw new FlowUpdateException("Failed to migrate Connector " + connectorId + " from the provided source flow", e);
        }

        final ConnectorConfiguration mergedConfiguration;
        try {
            mergedConfiguration = connector.applyMigratedConfiguration(migrationContext.getMergedConfiguration());
        } catch (final FlowUpdateException e) {
            failMigration(connector, migrationContext);
            throw e;
        } catch (final Exception e) {
            failMigration(connector, migrationContext);
            throw new FlowUpdateException("Failed to apply migrated configuration for Connector " + connectorId, e);
        }

        // Phase 2: drive migrateState(...) -> drain staged component-state writes -> apply each entry to the live
        // StateManager. The managed Process Group has been rebuilt by applyMigratedConfiguration(...) above, so the
        // managed components exist before the connector tries to record state for them. Wrong-phase calls
        // (setProperties/replaceProperties) now throw IllegalStateException from the context; updateFlow(...) on
        // the active flow context still throws via the MigrationFlowContext wrapper.
        migrationContext.setPhase(StandardFrameworkConnectorMigrationContext.Phase.STATE);
        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(flowController.getExtensionManager(), rawConnector.getClass(), connectorId)) {
            migratableConnector.migrateState(migrationContext);

            if (cancellation.getAsBoolean()) {
                throw new FlowUpdateException("Migration of Connector " + connectorId + " was cancelled after migrateState() completed.");
            }
        } catch (final FlowUpdateException e) {
            failMigration(connector, migrationContext);
            throw e;
        } catch (final Exception e) {
            failMigration(connector, migrationContext);
            throw new FlowUpdateException("Failed to migrate state for Connector " + connectorId, e);
        }

        final Map<String, VersionedComponentState> stagedStates = migrationContext.drainStagedComponentStates();
        try {
            applyMigratedComponentStates(connector, stagedStates);
        } catch (final Exception e) {
            failMigration(connector, migrationContext);
            throw new FlowUpdateException("Failed to apply migrated component state for Connector " + connectorId, e);
        }

        // Commit: both phases have succeeded, so persist the merged configuration onto the active configuration.
        // From this point on the migration is durable across restarts: inheritConfiguration(...) on the next load will
        // rebuild the managed Process Group from the committed configuration via the same Connector.applyUpdate(...)
        // path used here.
        try {
            connector.commitMigratedConfiguration(mergedConfiguration);
        } catch (final Exception e) {
            failMigration(connector, migrationContext);
            throw new FlowUpdateException("Failed to commit migrated configuration for Connector " + connectorId, e);
        }

        migrationContext.setPhase(StandardFrameworkConnectorMigrationContext.Phase.COMPLETED);

        // The migration is durable now that the merged configuration is committed, so notify the
        // ConnectorConfigurationProvider that the migration completed. The source Process Group identifier is null for
        // an uploaded-payload migration. A failure here is logged rather than rolled back, because rolling back a
        // committed migration would silently resurrect it on the next restart.
        try {
            flowController.getConnectorRepository().notifyMigrationComplete(connectorId, processGroupId);
        } catch (final Exception e) {
            logger.warn("Connector {} migration completed and is durable, but notifying the configuration provider of completion failed", connectorId, e);
        }

        // Finalize: the migration is already durable at this point because the merged configuration has been committed
        // onto the active configuration. The remaining work is cosmetic - disabling and renaming the source Process
        // Group so the operator can tell it apart from the new managed flow. Failures (or a late cancellation) below
        // are logged but no longer trigger a rollback because rolling back a committed migration would leave the
        // active configuration migrated while the managed Process Group was reset to the initial flow, which would
        // silently resurrect the migration on the next restart.
        if (cancellation.getAsBoolean()) {
            logger.warn("Migration of Connector {} was cancelled after the merged configuration was committed; the migration is durable but the source Process Group was not finalized", connectorId);
            return;
        }

        if (sourceProcessGroup != null) {
            try {
                disableAndRenameSourceProcessGroup(sourceProcessGroup);
            } catch (final Exception e) {
                logger.warn("Failed to finalize source Process Group {} after a successful migration of Connector {}; manual cleanup may be required",
                        sourceProcessGroup.getIdentifier(), connectorId, e);
            }
        }
    }

    /**
     * Applies the staged component-state writes drained from {@code migrateState(...)} onto the live components in
     * the connector's managed Process Group. For each entry the framework:
     * <ol>
     *     <li>resolves the managed component by its versioned identifier;</li>
     *     <li>looks up the component's runtime instance identifier and fetches its {@link StateManager} via
     *         {@link org.apache.nifi.components.state.StateManagerProvider#getStateManager(String)
     *         StateManagerProvider.getStateManager(...)};</li>
     *     <li>writes the cluster-scope state, when the component declares {@link Scope#CLUSTER} on its
     *         {@code @Stateful} annotation;</li>
     *     <li>writes the local-scope state for this node, when the component declares {@link Scope#LOCAL}. The
     *         local-state entry is picked by node ordinal so a clustered migration installs the right per-node state
     *         on each node, matching the framework's normal versioned-flow synchronizer.</li>
     * </ol>
     * Skips components whose {@code @Stateful} annotation does not declare the requested scope and logs a warning,
     * matching the behavior of {@code StandardVersionedComponentSynchronizer.restoreComponentState(...)}.
     *
     * <p>
     * This loop applies entries sequentially and does not roll back partial writes on its own. The rollback path that
     * runs when any entry throws (see {@link #clearConnectorComponentState}) wipes every managed component's
     * StateManager state, so a partially-applied set of writes is cleared along with the rest of the migration's
     * side effects rather than left behind.
     * </p>
     */
    private void applyMigratedComponentStates(final ConnectorNode connector, final Map<String, VersionedComponentState> stagedStates) throws IOException {
        if (stagedStates.isEmpty()) {
            return;
        }

        final FrameworkFlowContext activeFlowContext = connector.getActiveFlowContext();
        if (activeFlowContext == null) {
            throw new IllegalStateException("Cannot apply migrated component state for Connector " + connector.getIdentifier()
                    + " because it has no active flow context.");
        }
        final ProcessGroup managedGroup = activeFlowContext.getManagedProcessGroup();
        if (managedGroup == null) {
            throw new IllegalStateException("Cannot apply migrated component state for Connector " + connector.getIdentifier()
                    + " because its managed Process Group does not exist.");
        }

        final Map<String, ProcessorNode> processorsByVersionedId = new HashMap<>();
        for (final ProcessorNode processor : managedGroup.findAllProcessors()) {
            processor.getVersionedComponentId().ifPresent(versionedId -> processorsByVersionedId.put(versionedId, processor));
        }

        final Map<String, ControllerServiceNode> servicesByVersionedId = new HashMap<>();
        for (final ControllerServiceNode service : managedGroup.findAllControllerServices()) {
            service.getVersionedComponentId().ifPresent(versionedId -> servicesByVersionedId.put(versionedId, service));
        }

        final int localNodeOrdinal = flowController.getLocalNodeOrdinal();

        for (final Map.Entry<String, VersionedComponentState> entry : stagedStates.entrySet()) {
            final String versionedId = entry.getKey();
            final VersionedComponentState desiredState = entry.getValue();

            final ProcessorNode processor = processorsByVersionedId.get(versionedId);
            if (processor != null) {
                applyComponentState(processor.getIdentifier(), processor.getComponent(), desiredState, localNodeOrdinal);
                continue;
            }
            final ControllerServiceNode service = servicesByVersionedId.get(versionedId);
            if (service != null) {
                applyComponentState(service.getIdentifier(), service.getControllerServiceImplementation(), desiredState, localNodeOrdinal);
                continue;
            }
            logger.warn("Connector {} migrateState() requested state for component [{}] but no managed Processor or Controller Service with that versioned identifier exists; skipping",
                    connector.getIdentifier(), versionedId);
        }
    }

    private void failMigration(final ConnectorNode connector, final StandardFrameworkConnectorMigrationContext migrationContext) {
        migrationContext.setPhase(StandardFrameworkConnectorMigrationContext.Phase.COMPLETED);
        rollbackMigration(connector, migrationContext);
    }

    private void applyComponentState(final String runtimeComponentId, final ConfigurableComponent component, final VersionedComponentState desiredState, final int localNodeOrdinal)
            throws IOException {
        final StateManager stateManager = flowController.getStateManagerProvider().getStateManager(runtimeComponentId);
        if (stateManager == null) {
            logger.warn("No StateManager registered for migrated component [{}]; skipping state write", runtimeComponentId);
            return;
        }

        final Set<Scope> declaredScopes = declaredStatefulScopes(component);

        final Map<String, String> clusterState = desiredState.getClusterState();
        if (clusterState != null && !clusterState.isEmpty()) {
            if (declaredScopes.contains(Scope.CLUSTER)) {
                stateManager.setState(clusterState, Scope.CLUSTER);
            } else {
                logger.warn("Component [{}] does not declare @Stateful scope CLUSTER; skipping cluster state migration", runtimeComponentId);
            }
        }

        final List<VersionedNodeState> localNodeStates = desiredState.getLocalNodeStates();
        if (localNodeStates != null && !localNodeStates.isEmpty()) {
            if (!declaredScopes.contains(Scope.LOCAL)) {
                logger.warn("Component [{}] does not declare @Stateful scope LOCAL; skipping local state migration", runtimeComponentId);
                return;
            }
            if (localNodeOrdinal < 0) {
                logger.warn("Local node ordinal is unknown; skipping local state migration for component [{}]", runtimeComponentId);
                return;
            }
            if (localNodeOrdinal >= localNodeStates.size()) {
                logger.warn("Component [{}] migration has {} local node state entries but this node ordinal is {}; skipping local state migration",
                        runtimeComponentId, localNodeStates.size(), localNodeOrdinal);
                return;
            }
            final VersionedNodeState nodeState = localNodeStates.get(localNodeOrdinal);
            if (nodeState == null || nodeState.getState() == null || nodeState.getState().isEmpty()) {
                return;
            }
            stateManager.setState(nodeState.getState(), Scope.LOCAL);
        }
    }

    private static Set<Scope> declaredStatefulScopes(final ConfigurableComponent component) {
        if (component == null) {
            return Set.of();
        }
        final Stateful stateful = component.getClass().getAnnotation(Stateful.class);
        if (stateful == null) {
            return Set.of();
        }
        return EnumSet.copyOf(Arrays.asList(stateful.scopes()));
    }

    private void rollbackMigration(final ConnectorNode connector, final StandardFrameworkConnectorMigrationContext migrationContext) {
        clearConnectorComponentState(connector);

        final Set<String> copiedAssetIds = migrationContext.getCopiedAssetIds();
        if (!copiedAssetIds.isEmpty()) {
            try {
                flowController.getConnectorRepository().deleteAssets(connector.getIdentifier(), copiedAssetIds);
            } catch (final Exception e) {
                logger.warn("Failed to delete copied assets {} for Connector {} during migration rollback", copiedAssetIds, connector.getIdentifier(), e);
            }
        }

        try {
            connector.loadInitialFlow();
        } catch (final FlowUpdateException e) {
            // Do not propagate the rollback failure: the caller's primary need is to see why the migration originally
            // failed, not how the recovery attempt failed. The operator can read the rollback failure from the log.
            logger.error("Failed to restore Connector {} to its initial state after migration failure; manual cleanup may be required",
                    connector.getIdentifier(), e);
            return;
        }

        flowController.getConnectorRepository().discardWorkingConfiguration(connector);
    }

    private void clearConnectorComponentState(final ConnectorNode connector) {
        // Migration is only permitted on a Connector whose active flow has not been modified from its initial flow.
        // Rollback restores that initial flow immediately after this call, so clearing state for every Processor and
        // Controller Service currently in the managed flow is acceptable: any component carried over from the initial
        // flow is rebuilt fresh by loadInitialFlow(), and any component introduced by the failed migration is
        // discarded. Clearing the whole managed flow this way avoids the brittle approach of trying to map source-flow
        // versioned identifiers to runtime component identifiers.
        final FrameworkFlowContext activeFlowContext = connector.getActiveFlowContext();
        if (activeFlowContext == null) {
            return;
        }

        final ProcessGroup managedGroup = activeFlowContext.getManagedProcessGroup();
        if (managedGroup == null) {
            return;
        }

        for (final ProcessorNode processor : managedGroup.findAllProcessors()) {
            clearComponentState(processor.getIdentifier());
        }
        for (final ControllerServiceNode controllerService : managedGroup.findAllControllerServices()) {
            clearComponentState(controllerService.getIdentifier());
        }
    }

    private void clearComponentState(final String componentIdentifier) {
        final StateManager stateManager = flowController.getStateManagerProvider().getStateManager(componentIdentifier);
        if (stateManager == null) {
            return;
        }

        try {
            stateManager.clear(Scope.LOCAL);
        } catch (final Exception e) {
            logger.warn("Failed to clear LOCAL state for component {} during migration rollback", componentIdentifier, e);
        }

        try {
            stateManager.clear(Scope.CLUSTER);
        } catch (final Exception e) {
            logger.warn("Failed to clear CLUSTER state for component {} during migration rollback", componentIdentifier, e);
        }
    }

    private ConnectorNode getRequiredConnector(final String connectorId) {
        final ConnectorNode connector = flowController.getConnectorRepository().getConnector(connectorId, ConnectorSyncMode.LOCAL_ONLY);
        if (connector == null) {
            throw new IllegalArgumentException("Could not find Connector with ID " + connectorId);
        }

        return connector;
    }

    private ProcessGroup getRequiredSourceProcessGroup(final String processGroupId) {
        final ProcessGroup rootGroup = flowController.getFlowManager().getRootGroup();
        final ProcessGroup processGroup = rootGroup == null ? null : rootGroup.findProcessGroup(processGroupId);
        if (processGroup == null) {
            throw new IllegalArgumentException("Could not find Process Group with ID " + processGroupId);
        }

        return processGroup;
    }

    private List<ProcessGroup> getCandidateSourceGroups() {
        final FlowManager flowManager = flowController.getFlowManager();
        final ProcessGroup rootGroup = flowManager.getRootGroup();
        if (rootGroup == null) {
            return Collections.emptyList();
        }

        return new ArrayList<>(rootGroup.findAllProcessGroups());
    }

    private String getIneligibilityReason(final ConnectorNode connector, final ProcessGroup processGroup) {
        final VersionControlInformation versionControlInformation = processGroup.getVersionControlInformation();
        if (versionControlInformation == null) {
            return "The Process Group is not under version control.";
        }

        if (processGroup.getConnectorIdentifier().isPresent()) {
            return "The Process Group is managed by another Connector and cannot be used as a migration source.";
        }

        final VersionedExternalFlow sourceFlow = obtainSourceFlowForListing(processGroup);

        final List<String> stateIneligibilityReasons = getStateIneligibilityReasons(processGroup, versionControlInformation, sourceFlow);
        if (!stateIneligibilityReasons.isEmpty()) {
            // Each entry describes a distinct condition the user can address. Joining with newlines keeps the
            // individual reasons identifiable (e.g. "There are 8 running Processors.\nThere are 3 queued FlowFiles.")
            // rather than collapsing them into a single run-on sentence when they are surfaced to the operator.
            return String.join(System.lineSeparator(), stateIneligibilityReasons);
        }

        if (!isConnectorMigrationSupported(connector, sourceFlow)) {
            return "The Connector does not support migration from this Process Group.";
        }

        return null;
    }

    /**
     * Reports every condition that currently prevents the Process Group from being used as a migration source.
     * Reasons describe conditions the user can remediate (stopping processors, draining queues, refreshing version
     * control, etc.) without changing the structural compatibility of the source flow. The returned list contains
     * one entry per applicable condition so the user can address them together; an empty list means the Process
     * Group is in a state that allows migration. Callers must ensure the supplied {@code versionControlInformation}
     * matches the {@code processGroup}; this method does not tolerate a null value.
     *
     * @return ordered list of state-based ineligibility reasons, or an empty list if the Process Group is in a state
     *         that allows migration
     */
    private List<String> getStateIneligibilityReasons(final ProcessGroup processGroup, final VersionControlInformation versionControlInformation, final VersionedExternalFlow sourceFlow) {
        final List<String> reasons = new ArrayList<>();

        final VersionedFlowStatus versionedFlowStatus = versionControlInformation.getStatus();
        final VersionedFlowState state = versionedFlowStatus == null ? null : versionedFlowStatus.getState();
        if (state != VersionedFlowState.UP_TO_DATE) {
            reasons.add(describeIneligibleVersionedFlowState(state));
        }

        final int runningProcessorCount = countRunningProcessors(processGroup);
        if (runningProcessorCount > 0) {
            reasons.add("There " + pluralizeIsAre(runningProcessorCount) + " " + runningProcessorCount + " running or enabled "
                    + pluralizeNoun(runningProcessorCount, "processor", "processors") + ".");
        }

        final int enabledServiceCount = countEnabledControllerServices(processGroup);
        if (enabledServiceCount > 0) {
            reasons.add("There " + pluralizeIsAre(enabledServiceCount) + " " + enabledServiceCount + " enabled controller "
                    + pluralizeNoun(enabledServiceCount, "service", "services") + ".");
        }

        final long queuedFlowFileCount = countQueuedFlowFiles(processGroup);
        if (queuedFlowFileCount > 0) {
            reasons.add("There " + pluralizeIsAre(queuedFlowFileCount) + " " + queuedFlowFileCount + " queued "
                    + pluralizeNoun(queuedFlowFileCount, "FlowFile", "FlowFiles") + ".");
        }

        final int externalControllerServiceCount = countExternalControllerServices(sourceFlow);
        if (externalControllerServiceCount > 0) {
            reasons.add("The Process Group references " + externalControllerServiceCount + " controller "
                    + pluralizeNoun(externalControllerServiceCount, "service", "services") + " from outside the Process Group.");
        }

        return reasons;
    }

    private static String pluralizeIsAre(final long count) {
        return count == 1L ? "is" : "are";
    }

    private static String pluralizeNoun(final long count, final String singular, final String plural) {
        return count == 1L ? singular : plural;
    }

    private boolean isConnectorMigrationSupported(final ConnectorNode connector, final VersionedExternalFlow sourceFlow) {
        // isMigrationSupported is a side-effect-free predicate wrapped by EligibilityConnectorMigrationContext, which
        // rejects every write API. The working configuration is therefore never mutated here, but the context still
        // requires one, so hand it a throwaway clone of the connector's active configuration.
        final MutableConnectorConfigurationContext workingConfiguration = connector.getActiveFlowContext().getConfigurationContext().clone();
        final StandardFrameworkConnectorMigrationContext underlyingContext = new StandardFrameworkConnectorMigrationContext(
                connector.getIdentifier(),
                sourceFlow,
                true,
                connector.getActiveFlowContext(),
                workingConfiguration,
                flowController.getAssetManager(),
                flowController.getConnectorRepository(),
                flowController.getStateManagerProvider(),
                flowController
        );
        final FrameworkConnectorMigrationContext migrationContext = new EligibilityConnectorMigrationContext(underlyingContext);
        return connector.isMigrationSupported(migrationContext);
    }

    private static String describeIneligibleVersionedFlowState(final VersionedFlowState state) {
        if (state == null) {
            return "There are no version control state details available for this Process Group.";
        }

        return switch (state) {
            case STALE -> "The versioned flow is stale; bring it back to the latest published version before migrating.";
            case LOCALLY_MODIFIED -> "There are local modifications relative to the registered flow; revert or publish them before migrating.";
            case LOCALLY_MODIFIED_AND_STALE -> "The versioned flow is stale and has local modifications; revert them and refresh to the latest version before migrating.";
            case SYNC_FAILURE -> "Synchronization with the flow registry has failed; restore connectivity and refresh the version control status before migrating.";
            default -> "The versioned flow is not up to date with its registered flow.";
        };
    }

    private void verifyConnectorCanReceiveMigration(final ConnectorNode connector) {
        if (connector.getCurrentState() != ConnectorState.STOPPED || connector.getDesiredState() != ConnectorState.STOPPED) {
            throw new IllegalStateException("Connector " + connector.getIdentifier() + " must be stopped before it can be migrated.");
        }
    }

    private void verifyTargetIsAtInitialFlow(final ConnectorNode connector) {
        if (!connector.matchesInitialFlow()) {
            throw new IllegalStateException("Connector " + connector.getIdentifier()
                    + " has been modified since it was created; migration would overwrite those modifications.");
        }
    }

    private void validateMigrationSource(final VersionedExternalFlow sourceFlow) {
        final Map<String, ExternalControllerServiceReference> externalControllerServices =
                Optional.ofNullable(sourceFlow.getExternalControllerServices()).orElse(Collections.emptyMap());
        if (!externalControllerServices.isEmpty()) {
            throw new IllegalStateException("Connector cannot reference services outside its managed flow.");
        }

        final int connectedNodeCount = flowController.getConnectedNodeCount();
        VersionedComponentStateValidator.validateLocalStateTopology(sourceFlow.getFlowContents(), connectedNodeCount);
    }

    private void disableAndRenameSourceProcessGroup(final ProcessGroup processGroup) {
        disableSourceProcessors(processGroup);
        disableSourceControllerServices(processGroup);

        final String currentName = processGroup.getName();
        if (currentName != null && currentName.startsWith(MIGRATED_SOURCE_PREFIX)) {
            return;
        }

        processGroup.setName(MIGRATED_SOURCE_PREFIX + currentName);
    }

    private void disableSourceProcessors(final ProcessGroup processGroup) {
        for (final ProcessorNode processor : processGroup.findAllProcessors()) {
            if (processor.getDesiredState() == ScheduledState.DISABLED) {
                continue;
            }

            final ProcessGroup parent = processor.getProcessGroup();
            if (parent == null) {
                continue;
            }

            try {
                parent.disableProcessor(processor);
            } catch (final Exception e) {
                logger.warn("Failed to disable processor {} while finalizing migrated source Process Group {}", processor.getIdentifier(), processGroup.getIdentifier(), e);
            }
        }
    }

    private void disableSourceControllerServices(final ProcessGroup processGroup) {
        final List<ControllerServiceNode> enabledServices = new ArrayList<>();
        for (final ControllerServiceNode controllerService : processGroup.findAllControllerServices()) {
            if (controllerService.getState() != ControllerServiceState.DISABLED) {
                enabledServices.add(controllerService);
            }
        }

        if (enabledServices.isEmpty()) {
            return;
        }

        try {
            flowController.getControllerServiceProvider().disableControllerServicesAsync(enabledServices);
        } catch (final Exception e) {
            logger.warn("Failed to disable controller services while finalizing migrated source Process Group {}", processGroup.getIdentifier(), e);
        }
    }

    private int countRunningProcessors(final ProcessGroup processGroup) {
        int count = 0;
        for (final ProcessorNode processor : processGroup.findAllProcessors()) {
            final ScheduledState physicalScheduledState = processor.getPhysicalScheduledState();
            if (physicalScheduledState != ScheduledState.STOPPED && physicalScheduledState != ScheduledState.DISABLED) {
                count++;
            }
        }
        return count;
    }

    private int countEnabledControllerServices(final ProcessGroup processGroup) {
        int count = 0;
        for (final ControllerServiceNode controllerService : processGroup.findAllControllerServices()) {
            if (controllerService.getState() != ControllerServiceState.DISABLED) {
                count++;
            }
        }
        return count;
    }

    private long countQueuedFlowFiles(final ProcessGroup processGroup) {
        long total = 0L;
        for (final Connection connection : processGroup.findAllConnections()) {
            final QueueSize queueSize = connection.getFlowFileQueue().size();
            total += queueSize.getObjectCount();
        }
        return total;
    }

    private static int countExternalControllerServices(final VersionedExternalFlow sourceFlow) {
        final Map<String, ExternalControllerServiceReference> externalControllerServices = sourceFlow.getExternalControllerServices();
        return externalControllerServices == null ? 0 : externalControllerServices.size();
    }

    private ConnectorMigrationSource toMigrationSource(final ProcessGroup processGroup, final VersionControlInformation versionControlInformation, final List<String> ineligibilityReasons) {
        final ConnectorMigrationSource migrationSource = new ConnectorMigrationSource();
        migrationSource.setProcessGroupId(processGroup.getIdentifier());
        migrationSource.setProcessGroupName(processGroup.getName());
        final ProcessGroup parent = processGroup.getParent();
        migrationSource.setParentProcessGroupId(parent == null ? null : parent.getIdentifier());
        migrationSource.setRegistryClientId(versionControlInformation.getRegistryIdentifier());
        migrationSource.setBucketId(versionControlInformation.getBucketIdentifier());
        migrationSource.setFlowId(versionControlInformation.getFlowIdentifier());
        migrationSource.setFlowName(versionControlInformation.getFlowName());
        migrationSource.setVersion(versionControlInformation.getVersion());
        migrationSource.setReadyForMigration(ineligibilityReasons.isEmpty());
        migrationSource.setIneligibilityReasons(List.copyOf(ineligibilityReasons));
        return migrationSource;
    }

    private VersionedExternalFlow obtainSourceFlowForListing(final ProcessGroup processGroup) {
        // Listing-time eligibility checks intentionally exclude component state. Materializing component state is
        // expensive and is only required for the actual migration. The Connector's isMigrationSupported(...) must rely
        // solely on structural information (processor types, parameter names, exported metadata) at listing time;
        // per-state precondition checks belong inside the Connector's own migrateConfiguration/migrateState implementations.
        final RegisteredFlowSnapshot snapshot = snapshotProvider.getCurrentFlowSnapshotByGroupId(processGroup.getIdentifier(), true, false);
        return createExternalFlow(snapshot);
    }

    public VersionedExternalFlow createExternalFlow(final RegisteredFlowSnapshot flowSnapshot) {
        final VersionedExternalFlowMetadata externalFlowMetadata = new VersionedExternalFlowMetadata();
        final RegisteredFlowSnapshotMetadata snapshotMetadata = flowSnapshot.getSnapshotMetadata();
        if (snapshotMetadata != null) {
            externalFlowMetadata.setAuthor(snapshotMetadata.getAuthor());
            externalFlowMetadata.setBucketIdentifier(snapshotMetadata.getBucketIdentifier());
            externalFlowMetadata.setComments(snapshotMetadata.getComments());
            externalFlowMetadata.setFlowIdentifier(snapshotMetadata.getFlowIdentifier());
            externalFlowMetadata.setTimestamp(snapshotMetadata.getTimestamp());
            externalFlowMetadata.setVersion(snapshotMetadata.getVersion());
        }

        final RegisteredFlow registeredFlow = flowSnapshot.getFlow();
        if (registeredFlow == null) {
            externalFlowMetadata.setFlowName(flowSnapshot.getFlowContents().getName());
        } else {
            externalFlowMetadata.setFlowName(registeredFlow.getName());
        }

        final VersionedExternalFlow externalFlow = new VersionedExternalFlow();
        externalFlow.setFlowContents(flowSnapshot.getFlowContents());
        externalFlow.setExternalControllerServices(flowSnapshot.getExternalControllerServices());
        externalFlow.setParameterContexts(flowSnapshot.getParameterContexts());
        externalFlow.setParameterProviders(flowSnapshot.getParameterProviders());
        externalFlow.setMetadata(externalFlowMetadata);
        return externalFlow;
    }
}
