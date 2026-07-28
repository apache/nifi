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
import org.apache.nifi.authorization.Resource;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.resource.ResourceFactory;
import org.apache.nifi.authorization.resource.ResourceType;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.components.connector.components.ParameterContextFacade;
import org.apache.nifi.components.connector.migration.ConnectorMigrationContext;
import org.apache.nifi.components.connector.migration.MigratableConnector;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.components.state.StateMap;
import org.apache.nifi.components.validation.DisabledServiceValidationResult;
import org.apache.nifi.components.validation.ValidationState;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.components.validation.ValidationTrigger;
import org.apache.nifi.connectable.FlowFileActivity;
import org.apache.nifi.connectable.FlowFileTransferCounts;
import org.apache.nifi.connectable.Port;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.queue.DropFlowFileStatus;
import org.apache.nifi.controller.queue.QueueSize;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.ScheduledState;
import org.apache.nifi.flow.VersionedConfigurationStep;
import org.apache.nifi.flow.VersionedConnectorValueReference;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.logging.GroupedComponent;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

public class StandardConnectorNode implements ConnectorNode, GroupedComponent {
    private static final Logger logger = LoggerFactory.getLogger(StandardConnectorNode.class);

    private static final Set<org.apache.nifi.controller.ScheduledState> STOPPED_STATES =
            EnumSet.of(org.apache.nifi.controller.ScheduledState.STOPPED, org.apache.nifi.controller.ScheduledState.DISABLED);

    private final String identifier;
    private final FlowManager flowManager;
    private final ExtensionManager extensionManager;
    private final StateManagerProvider stateManagerProvider;
    private final Authorizable parentAuthorizable;
    private final ConnectorDetails connectorDetails;
    private final String componentType;
    private final String componentCanonicalClass;
    private final BundleCoordinate bundleCoordinate;
    private final ConnectorStateTransition stateTransition;
    private final AtomicReference<String> versionedComponentId = new AtomicReference<>();
    private final FlowContextFactory flowContextFactory;
    private final FrameworkFlowContext activeFlowContext;

    private final AtomicReference<ValidationState> validationState = new AtomicReference<>(new ValidationState(ValidationStatus.VALIDATING, Collections.emptyList()));
    private final ConnectorValidationTrigger validationTrigger;
    private final boolean extensionMissing;
    private volatile boolean triggerValidation = true;
    private final AtomicReference<CompletableFuture<Void>> drainFutureRef = new AtomicReference<>();
    private volatile ValidationResult unresolvedBundleValidationResult = null;

    private volatile FrameworkFlowContext workingFlowContext;

    private volatile String name;
    private volatile FrameworkConnectorInitializationContext initializationContext;

    private final Object loggingAttributesLock = new Object();
    private volatile Map<String, String> customLoggingAttributes = Map.of();
    private volatile Map<String, String> mergedLoggingAttributes = Map.of();

    public StandardConnectorNode(final String identifier, final FlowManager flowManager, final ExtensionManager extensionManager,
        final StateManagerProvider stateManagerProvider, final Authorizable parentAuthorizable, final ConnectorDetails connectorDetails,
        final String componentType, final String componentCanonicalClass, final MutableConnectorConfigurationContext configurationContext,
        final ConnectorStateTransition stateTransition, final FlowContextFactory flowContextFactory,
        final ConnectorValidationTrigger validationTrigger, final boolean extensionMissing) {

        this.identifier = identifier;
        this.flowManager = flowManager;
        this.extensionManager = extensionManager;
        this.stateManagerProvider = stateManagerProvider;
        this.parentAuthorizable = parentAuthorizable;
        this.connectorDetails = connectorDetails;
        this.componentType = componentType;
        this.componentCanonicalClass = componentCanonicalClass;
        this.bundleCoordinate = connectorDetails.getBundleCoordinate();
        this.stateTransition = stateTransition;
        this.flowContextFactory = flowContextFactory;
        this.validationTrigger = validationTrigger;
        this.extensionMissing = extensionMissing;

        this.name = connectorDetails.getConnector().getClass().getSimpleName();

        final Bundle activeFlowBundle = new Bundle(bundleCoordinate.getGroup(), bundleCoordinate.getId(), bundleCoordinate.getVersion());
        this.activeFlowContext = flowContextFactory.createActiveFlowContext(identifier, connectorDetails.getComponentLog(), activeFlowBundle);

        rebuildLoggingAttributes();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(final String name) {
        this.name = name;
        rebuildLoggingAttributes();
    }

    /**
     * Returns the {@link ProcessGroup} that holds the connector-managed flow (the connector's active
     * managed root group), or {@code null} before the active flow context has been established.
     *
     * <p>This serves two purposes:
     * <ul>
     *   <li>It satisfies the {@link GroupedComponent} contract. The connector's own {@code ComponentLog}
     *       uses a {@code StandardLoggingContext} bound to this node (see {@code ExtensionBuilder}), so the
     *       connector's own log lines source their MDC from this group's
     *       {@link ProcessGroup#getLoggingAttributes() logging attributes}.</li>
     *   <li>It is the target onto which {@link #rebuildLoggingAttributes()} pushes the merged connector
     *       logging attributes (via {@link ProcessGroup#setConnectorLoggingAttributes(Map)}); those then
     *       cascade down the managed flow so components running inside it inherit the connector MDC through
     *       their own logging contexts.</li>
     * </ul>
     *
     * @return the connector's managed root process group, or {@code null} before the active flow context
     *         has been established
     */
    @Override
    public ProcessGroup getProcessGroup() {
        final FrameworkFlowContext context = activeFlowContext;
        return context == null ? null : context.getManagedProcessGroup();
    }

    /**
     * Replaces the connector-supplied custom logging attributes. Reserved keys (those used by the
     * framework, see {@link ConnectorLoggingAttribute}) are filtered out and a WARN is logged for
     * each dropped entry.
     *
     * @param attributes the proposed custom attributes; {@code null} or empty clears the current set
     */
    @Override
    public void setCustomLoggingAttributes(final Map<String, String> attributes) {
        final Map<String, String> filtered = filterReservedKeys(attributes);

        synchronized (loggingAttributesLock) {
            this.customLoggingAttributes = filtered;
        }
        rebuildLoggingAttributes();
    }

    /**
     * Returns an immutable snapshot of the merged framework + custom logging attributes currently
     * advertised by this connector. The framework keys are populated by the framework from the
     * connector's identifier, name, component type, and bundle coordinate.
     *
     * @return an immutable map of the connector's merged framework and custom logging attributes; never {@code null}
     */
    @Override
    public Map<String, String> getLoggingAttributes() {
        return mergedLoggingAttributes;
    }

    private Map<String, String> filterReservedKeys(final Map<String, String> attributes) {
        if (attributes == null || attributes.isEmpty()) {
            return Map.of();
        }
        final Map<String, String> filtered = new HashMap<>(attributes.size());
        for (final Map.Entry<String, String> entry : attributes.entrySet()) {
            final String key = entry.getKey();
            if (key == null || key.isEmpty()) {
                continue;
            }
            if (ConnectorLoggingAttribute.isReserved(key)) {
                logger.warn("{} attempted to set reserved logging attribute [{}]; dropping the entry. Reserved keys are managed by the framework.", this, key);
                continue;
            }
            filtered.put(key, entry.getValue());
        }
        return Collections.unmodifiableMap(filtered);
    }

    private void rebuildLoggingAttributes() {
        final Map<String, String> merged;
        final Map<String, String> custom;
        synchronized (loggingAttributesLock) {
            custom = customLoggingAttributes;
            merged = new HashMap<>(custom.size() + ConnectorLoggingAttribute.values().length);
            merged.putAll(custom);
            // Framework keys are applied last so they always win against any not-yet-filtered overlap.
            merged.put(ConnectorLoggingAttribute.CONNECTOR_ID.getAttribute(), identifier);
            merged.put(ConnectorLoggingAttribute.CONNECTOR_NAME.getAttribute(), name == null ? "" : name);
            merged.put(ConnectorLoggingAttribute.CONNECTOR_COMPONENT.getAttribute(), componentCanonicalClass == null ? "" : componentCanonicalClass);
            if (bundleCoordinate != null) {
                merged.put(ConnectorLoggingAttribute.CONNECTOR_BUNDLE_GROUP.getAttribute(), bundleCoordinate.getGroup());
                merged.put(ConnectorLoggingAttribute.CONNECTOR_BUNDLE_ARTIFACT.getAttribute(), bundleCoordinate.getId());
                merged.put(ConnectorLoggingAttribute.CONNECTOR_BUNDLE_VERSION.getAttribute(), bundleCoordinate.getVersion());
            }
            this.mergedLoggingAttributes = Collections.unmodifiableMap(merged);
        }

        pushLoggingAttributesToManagedFlow(merged);
    }

    private void pushLoggingAttributesToManagedFlow(final Map<String, String> attributes) {
        final ProcessGroup managedProcessGroup = getProcessGroup();
        if (managedProcessGroup != null) {
            managedProcessGroup.setConnectorLoggingAttributes(attributes);
        }
    }

    @Override
    public void transitionStateForUpdating() {
        if (getCurrentState() == ConnectorState.TROUBLESHOOTING) {
            throw new IllegalStateException("Cannot apply an update to " + this + " while it is in Troubleshooting mode; exit Troubleshooting mode before applying updates.");
        }

        final ConnectorState initialState = getCurrentState();
        if (initialState == ConnectorState.UPDATING || initialState == ConnectorState.PREPARING_FOR_UPDATE) {
            return;
        }

        stateTransition.setDesiredState(ConnectorState.UPDATING);
        stateTransition.setCurrentState(ConnectorState.PREPARING_FOR_UPDATE);
    }

    @Override
    public void prepareForUpdate() throws FlowUpdateException {
        if (getCurrentState() != ConnectorState.PREPARING_FOR_UPDATE) {
            throw new IllegalStateException("Cannot prepare update for " + this + " because its state is currently " + getCurrentState()
                                            + "; it must be PREPARING_FOR_UPDATE.");
        }
        if (getDesiredState() != ConnectorState.UPDATING) {
            throw new IllegalStateException("Cannot prepare update for " + this + " because its desired state is currently " + getDesiredState()
                                            + "; it must be UPDATING.");
        }

        logger.debug("Preparing {} for update", this);
        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, getConnector().getClass(), getIdentifier())) {
            getConnector().prepareForUpdate(workingFlowContext, activeFlowContext);
            stateTransition.setCurrentState(ConnectorState.UPDATING);
            logger.debug("Successfully prepared {} for update", this);
        } catch (final Throwable t) {
            logger.error("Failed to prepare update for {}", this, t);

            try {
                abortUpdate(t);
            } catch (final Throwable abortFailure) {
                logger.error("Failed to abort update preparation for {}", this, abortFailure);
            }

            throw t;
        }
    }

    @Override
    public ConnectorConfiguration applyMigratedConfiguration(final MutableConnectorConfigurationContext mergedConfiguration) throws FlowUpdateException {
        if (initializationContext == null) {
            throw new IllegalStateException("Cannot apply migrated configuration because " + this + " has not been initialized yet.");
        }
        Objects.requireNonNull(mergedConfiguration, "Merged configuration is required");

        logger.debug("Applying migrated configuration to {}", this);
        // mergedConfiguration is the working configuration the connector mutated through the migration context. It was
        // seeded with a clone of this connector's active configuration, so it already holds the fully-merged result of
        // every setProperties/replaceProperties call the connector made. The clone is the working flow context that
        // drives Connector.applyUpdate(working, active) below. The live active configuration is intentionally left
        // untouched at this point: the framework only commits the merged configuration onto the active configuration
        // after the state-migration phase has also succeeded (see commitMigratedConfiguration). This means a failure in
        // migrateState(...) or in applying the staged component states can be rolled back simply by restoring the
        // initial flow, without having to revert any active-configuration mutations.
        mergedConfiguration.resolvePropertyValues();

        final FrameworkFlowContext migrationWorkingContext = flowContextFactory.createWorkingFlowContext(
                identifier, connectorDetails.getComponentLog(), mergedConfiguration, activeFlowContext.getBundle());

        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, getConnector().getClass(), getIdentifier())) {
            // Same applyUpdate(...) call signature the framework uses for regular working->active updates. The connector
            // calls getInitializationContext().updateFlow(activeFlowContext, ...) inside applyUpdate(...); the
            // activeFlowContext here is the real FrameworkFlowContext, so updateFlow(...) installs the rebuilt managed
            // flow normally (it is only the migration-scoped wrapper handed to migrateConfiguration/migrateState that
            // refuses updateFlow(...)).
            getConnector().applyUpdate(migrationWorkingContext, activeFlowContext);
        } catch (final FlowUpdateException e) {
            throw e;
        } catch (final Throwable t) {
            throw new FlowUpdateException("Failed to apply migrated configuration for " + this, t);
        }

        logger.info("Applied migrated configuration to {}; managed Process Group has been rebuilt and the merged"
                + " configuration is pending commit", this);
        return mergedConfiguration.toConnectorConfiguration();
    }

    @Override
    public void commitMigratedConfiguration(final ConnectorConfiguration mergedConfiguration) {
        if (initializationContext == null) {
            throw new IllegalStateException("Cannot commit migrated configuration because " + this + " has not been initialized yet.");
        }
        Objects.requireNonNull(mergedConfiguration, "Merged configuration is required");

        // Write the merged configuration onto the active configuration so the migration outcome is persisted to
        // flow.json.gz. Each step is written via replaceProperties so the resulting active configuration matches the
        // merged configuration exactly, including any properties the connector removed during migration. This is the
        // durability boundary of the two-phase migration: once this commit runs the migration is part of the persisted
        // flow and will be restored on every restart via inheritConfiguration(...).
        for (final NamedStepConfiguration stepConfig : mergedConfiguration.getNamedStepConfigurations()) {
            activeFlowContext.getConfigurationContext().replaceProperties(stepConfig.stepName(), stepConfig.configuration());
        }

        resetValidationState();
        recreateWorkingFlowContext();

        logger.info("Committed migrated configuration onto active configuration for {}", this);
    }

    @Override
    public void inheritConfiguration(final List<VersionedConfigurationStep> activeConfig, final List<VersionedConfigurationStep> workingConfig,
                final Bundle flowContextBundle) throws FlowUpdateException {

        logger.debug("Inheriting configuration for {}", this);
        final MutableConnectorConfigurationContext configurationContext = createConfigurationContext(activeConfig);
        final FrameworkFlowContext inheritContext = flowContextFactory.createWorkingFlowContext(identifier,
            connectorDetails.getComponentLog(), configurationContext, flowContextBundle);

        // Apply the update for the active config
        applyUpdate(inheritContext);

        // Configure the working config but do not apply
        for (final VersionedConfigurationStep step : workingConfig) {
            final StepConfiguration stepConfig = createStepConfiguration(step);
            setConfiguration(step.getName(), stepConfig, true);
        }

        logger.debug("Successfully inherited configuration for {}", this);
    }

    private StepConfiguration createStepConfiguration(final VersionedConfigurationStep step) {
        final Map<String, ConnectorValueReference> convertedProperties = new HashMap<>();
        if (step.getProperties() != null) {
            for (final Map.Entry<String, VersionedConnectorValueReference> entry : step.getProperties().entrySet()) {
                final ConnectorValueReference valueReference = createValueReference(entry.getValue());
                convertedProperties.put(entry.getKey(), valueReference);
            }
        }

        return new StepConfiguration(convertedProperties);
    }

    private MutableConnectorConfigurationContext createConfigurationContext(final List<VersionedConfigurationStep> flowConfiguration) {
        final StandardConnectorConfigurationContext configurationContext = new StandardConnectorConfigurationContext(
            initializationContext.getAssetManager(), initializationContext.getSecretsManager());

        for (final VersionedConfigurationStep versionedConfigStep : flowConfiguration) {
            final StepConfiguration stepConfig = createStepConfiguration(versionedConfigStep);
            configurationContext.setProperties(versionedConfigStep.getName(), stepConfig);
        }

        return configurationContext;
    }

    private ConnectorValueReference createValueReference(final VersionedConnectorValueReference versionedReference) {
        final ConnectorValueType valueType = ConnectorValueType.valueOf(versionedReference.getValueType());
        return switch (valueType) {
            case STRING_LITERAL -> new StringLiteralValue(versionedReference.getValue());
            case ASSET_REFERENCE -> new AssetReference(versionedReference.getAssetIds());
            case SECRET_REFERENCE -> new SecretReference(versionedReference.getProviderId(), versionedReference.getProviderName(),
                versionedReference.getSecretName(), versionedReference.getFullyQualifiedSecretName());
        };
    }

    @Override
    public void applyUpdate() throws FlowUpdateException {
        try {
            applyUpdate(workingFlowContext);
        } catch (final FlowUpdateException e) {
            // Since we failed to update, make sure that we stop the Connector. Note that we do not do this for all
            // throwables because IllegalStateException for example indicates that we did not even attempt to perform the update.
            try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, connectorDetails.getConnector().getClass(), getIdentifier())) {
                connectorDetails.getConnector().stop(activeFlowContext);
            } catch (final Throwable stopThrowable) {
                e.addSuppressed(stopThrowable);
            }

            throw e;
        }
    }

    private void applyUpdate(final FrameworkFlowContext contextToInherit) throws FlowUpdateException {
        final ConnectorState currentState = getCurrentState();
        if (currentState != ConnectorState.UPDATING) {
            throw new IllegalStateException("Cannot finish update for " + this + " because its state is currently " + currentState
                                            + "; it must be UPDATING.");
        }

        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, getConnector().getClass(), getIdentifier())) {
            getConnector().applyUpdate(contextToInherit, activeFlowContext);

            // Update the active flow context based on the properties of the provided context, as the connector has now been updated.
            final ConnectorConfiguration workingConfig = contextToInherit.getConfigurationContext().toConnectorConfiguration();
            for (final NamedStepConfiguration stepConfig : workingConfig.getNamedStepConfigurations()) {
                activeFlowContext.getConfigurationContext().replaceProperties(stepConfig.stepName(), stepConfig.configuration());
            }

            getComponentLog().info("Working Context has been applied to Active Context");

            // The update has been completed. Tear down and recreate the working flow context to ensure it is in a clean state.
            resetValidationState();
            recreateWorkingFlowContext();
        } catch (final Throwable t) {
            logger.error("Failed to finish update for {}", this, t);
            stateTransition.setCurrentState(ConnectorState.UPDATE_FAILED);
            stateTransition.setDesiredState(ConnectorState.UPDATE_FAILED);

            throw new FlowUpdateException("Failed to finish update for " + this, t);
        }

        stateTransition.setCurrentState(ConnectorState.UPDATED);
        stateTransition.setDesiredState(ConnectorState.UPDATED);
        logger.info("Successfully applied update for {}", this);
    }

    private void destroyWorkingContext() {
        if (this.workingFlowContext == null) {
            return;
        }

        try {
            workingFlowContext.getManagedProcessGroup().purge().get(1, TimeUnit.MINUTES);
        } catch (final Exception e) {
            logger.warn("Failed to purge working flow context for {}", this, e);
        }

        flowManager.onProcessGroupRemoved(workingFlowContext.getManagedProcessGroup());

        this.workingFlowContext = null;
    }

    @Override
    public void abortUpdate(final Throwable cause) {
        stateTransition.setCurrentState(ConnectorState.UPDATE_FAILED);
        stateTransition.setDesiredState(ConnectorState.UPDATE_FAILED);

        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, getConnector().getClass(), getIdentifier())) {
            getConnector().abortUpdate(workingFlowContext, cause);
        }
        logger.debug("Aborted update for {}", this);
    }

    @Override
    public void markInvalid(final String subject, final String explanation) {
        validationState.set(new ValidationState(ValidationStatus.INVALID, List.of(new ValidationResult.Builder()
                .subject(subject)
                .valid(false)
                .explanation(explanation)
                .build())));
    }

    @Override
    public void setConfiguration(final String stepName, final StepConfiguration configuration) throws FlowUpdateException {
        if (getCurrentState() == ConnectorState.TROUBLESHOOTING) {
            throw new IllegalStateException("Cannot modify configuration step " + stepName + " for " + this
                + " while it is in Troubleshooting mode; exit Troubleshooting mode before modifying Connector configuration.");
        }

        setConfiguration(stepName, configuration, false);
    }

    private void setConfiguration(final String stepName, final StepConfiguration configuration, final boolean forceOnConfigurationStepConfigured) throws FlowUpdateException {
        final ConfigurationUpdateResult updateResult = workingFlowContext.getConfigurationContext().setProperties(stepName, configuration);
        if (updateResult == ConfigurationUpdateResult.NO_CHANGES && !forceOnConfigurationStepConfigured) {
            return;
        }
        notifyStepConfigured(stepName);
    }

    @Override
    public void replaceWorkingConfiguration(final String stepName, final StepConfiguration configuration) throws FlowUpdateException {
        // The configuration provider's view is authoritative: any property absent from the provided
        // configuration is removed from the step.
        final ConfigurationUpdateResult updateResult = workingFlowContext.getConfigurationContext().replaceProperties(stepName, configuration);
        if (updateResult == ConfigurationUpdateResult.NO_CHANGES) {
            return;
        }
        notifyStepConfigured(stepName);
    }

    private void notifyStepConfigured(final String stepName) throws FlowUpdateException {
        final Connector connector = connectorDetails.getConnector();
        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, connector.getClass(), getIdentifier())) {
            logger.debug("Notifying {} of configuration change for configuration step {}", this, stepName);
            connector.onConfigurationStepConfigured(stepName, workingFlowContext);
            logger.debug("Successfully notified {} of configuration change for step {}", this, stepName);
        } catch (final FlowUpdateException e) {
            throw e;
        } catch (final Exception e) {
            logger.error("Failed to invoke onConfigured for {}", this, e);
            throw new RuntimeException("Failed to invoke onConfigured for " + this, e);
        }
    }

    @Override
    public ConnectorState getCurrentState() {
        return stateTransition.getCurrentState();
    }

    @Override
    public ConnectorState getDesiredState() {
        return stateTransition.getDesiredState();
    }


    @Override
    public Optional<Duration> getIdleDuration() {
        final ProcessGroup processGroup = getActiveFlowContext().getManagedProcessGroup();
        final FlowFileActivity activity = processGroup.getFlowFileActivity();
        final OptionalLong lastActivityTimestamp = activity.getLatestActivityTime();
        if (lastActivityTimestamp.isEmpty()) {
            return Optional.empty();
        }

        if (processGroup.isDataQueued()) {
            return Optional.empty();
        }

        final Duration idleDuration = Duration.ofMillis(System.currentTimeMillis() - lastActivityTimestamp.getAsLong());
        return Optional.of(idleDuration);
    }

    @Override
    public FlowFileTransferCounts getFlowFileTransferCounts() {
        return getActiveFlowContext().getManagedProcessGroup().getFlowFileActivity().getTransferCounts();
    }

    @Override
    public Future<Void> start(final FlowEngine scheduler) {
        final CompletableFuture<Void> startCompleteFuture = new CompletableFuture<>();
        start(scheduler, startCompleteFuture);
        return startCompleteFuture;
    }

    private void start(final FlowEngine scheduler, final CompletableFuture<Void> startCompleteFuture) {
        try {
            stateTransition.setDesiredState(ConnectorState.RUNNING);
            activeFlowContext.getConfigurationContext().resolvePropertyValues();

            verifyCanStart();

            final ConnectorState currentState = getCurrentState();
            switch (currentState) {
                case STARTING -> {
                    logger.debug("{} is already starting; adding future to pending start futures", this);
                    stateTransition.addPendingStartFuture(startCompleteFuture);
                }
                case RUNNING -> {
                    logger.debug("{} is already {}; will not attempt to start", this, currentState);
                    startCompleteFuture.complete(null);
                }
                case STOPPING -> {
                    // We have set the Desired State to RUNNING so when the Connector fully stops, it will be started again automatically
                    logger.info("{} is currently stopping so will not trigger Connector to start until it has fully stopped", this);
                    stateTransition.addPendingStartFuture(startCompleteFuture);
                }
                case STOPPED, PREPARING_FOR_UPDATE, UPDATED -> {
                    stateTransition.setCurrentState(ConnectorState.STARTING);
                    scheduler.schedule(() -> startComponent(scheduler, startCompleteFuture), 0, TimeUnit.SECONDS);
                }
                default -> {
                    logger.warn("{} is in state {} and cannot be started", this, currentState);
                    stateTransition.addPendingStartFuture(startCompleteFuture);
                }
            }
        } catch (final Exception e) {
            logger.error("Failed to start {}", this, e);
            startCompleteFuture.completeExceptionally(e);
        }
    }

    @Override
    public Future<Void> stop(final FlowEngine scheduler) {
        if (getCurrentState() == ConnectorState.TROUBLESHOOTING) {
            throw new IllegalStateException("Cannot stop " + this + " while it is in Troubleshooting mode; exit Troubleshooting mode to resume normal lifecycle control.");
        }

        logger.info("Stopping {}", this);
        final CompletableFuture<Void> stopCompleteFuture = new CompletableFuture<>();

        stateTransition.setDesiredState(ConnectorState.STOPPED);

        boolean stateUpdated = false;
        while (!stateUpdated) {
            final ConnectorState currentState = getCurrentState();
            if (currentState == ConnectorState.STOPPED) {
                logger.info("{} is already stopped.", this);
                stopCompleteFuture.complete(null);
                return stopCompleteFuture;
            }

            if (currentState == ConnectorState.STOPPING) {
                logger.debug("{} is already stopping; adding future to pending stop futures", this);
                stateTransition.addPendingStopFuture(stopCompleteFuture);
                return stopCompleteFuture;
            }

            stateUpdated = stateTransition.trySetCurrentState(currentState, ConnectorState.STOPPING);
        }

        scheduler.schedule(() -> stopComponent(scheduler, stopCompleteFuture), 0, TimeUnit.SECONDS);

        return stopCompleteFuture;
    }

    @Override
    public Future<Void> drainFlowFiles() {
        logger.debug("Draining FlowFiles for {}", this);
        requireStopped("drain FlowFiles", ConnectorState.DRAINING);

        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, connectorDetails.getConnector().getClass(), getIdentifier())) {
            getComponentLog().info("Draining FlowFiles from {}", this);
            final CompletableFuture<Void> drainFuture = connectorDetails.getConnector().drainFlowFiles(activeFlowContext);
            drainFutureRef.set(drainFuture);

            final CompletableFuture<Void> stateUpdateFuture = drainFuture.whenComplete((result, failureCause) -> {
                drainFutureRef.set(null);

                if (failureCause == null) {
                    logger.info("Successfully drained FlowFiles for {}", this);
                } else {
                    logger.error("Failed to drain FlowFiles for {}", this, failureCause);
                }

                try {
                    connectorDetails.getConnector().stop(activeFlowContext);
                } catch (final Exception e) {
                    logger.warn("Failed to stop {} after draining FlowFiles", this, e);
                }

                stateTransition.setCurrentState(ConnectorState.STOPPED);
                logger.info("All components of {} are now stopped after draining FlowFiles.", this);
            });

            return stateUpdateFuture;
        } catch (final Throwable t) {
            logger.error("Failed to drain FlowFiles for {}", this, t);
            stateTransition.setCurrentState(ConnectorState.STOPPED);
            throw t;
        }
    }

    @Override
    public void cancelDrainFlowFiles() {
        final Future<Void> future = this.drainFutureRef.getAndSet(null);
        if (future == null) {
            logger.debug("No active drain to cancel for {}; drain may have already completed", this);
            return;
        }

        future.cancel(true);
        logger.info("Cancelled draining of FlowFiles for {}", this);
    }

    @Override
    public void verifyCancelDrainFlowFiles() throws IllegalStateException {
        final ConnectorState state = getCurrentState();

        // Allow if we're currently draining or if we're stopped; if stopped the cancel drain action will be a no-op
        // but we don't want to throw an IllegalStateException in that case because doing so would mean that if one
        // node in the cluster is stopped while another is draining we cannot cancel the drain.
        if (state != ConnectorState.DRAINING && state != ConnectorState.STOPPED) {
            throw new IllegalStateException("Cannot cancel draining of FlowFiles for " + this + " because its current state is " + state + "; it must be DRAINING.");
        }
    }

    @Override
    public void verifyCanPurgeFlowFiles() throws IllegalStateException {
        final ConnectorState desiredState = getDesiredState();
        if (desiredState != ConnectorState.STOPPED) {
            throw new IllegalStateException("Cannot purge FlowFiles for " + this + " because its desired state is currently " + desiredState + "; it must be STOPPED.");
        }

        final ConnectorState currentState = getCurrentState();
        if (currentState != ConnectorState.STOPPED) {
            throw new IllegalStateException("Cannot purge FlowFiles for " + this + " because its current state is " + currentState + "; it must be STOPPED.");
        }
    }

    @Override
    public Future<Void> purgeFlowFiles(final String requestor) {
        logger.debug("Purging FlowFiles for {}", this);
        requireStopped("purge FlowFiles", ConnectorState.PURGING);

        try {
            final String dropRequestId = UUID.randomUUID().toString();
            final DropFlowFileStatus status = activeFlowContext.getManagedProcessGroup().dropAllFlowFiles(dropRequestId, requestor);
            final CompletableFuture<Void> future = status.getCompletionFuture();
            final CompletableFuture<Void> stateUpdateFuture = future.whenComplete((result, failureCause) -> {
                stateTransition.setCurrentState(ConnectorState.STOPPED);

                if (failureCause == null) {
                    logger.info("Successfully purged FlowFiles for {}", this);
                } else {
                    logger.error("Failed to purge FlowFiles for {}", this, failureCause);
                }
            });
            return stateUpdateFuture;
        } catch (final Throwable t) {
            logger.error("Failed to purge FlowFiles for {}", this, t);
            stateTransition.setCurrentState(ConnectorState.STOPPED);
            throw t;
        }
    }

    private void requireStopped(final String action, final ConnectorState newState) {
        final ConnectorState desiredState = getDesiredState();
        if (desiredState != ConnectorState.STOPPED) {
            throw new IllegalStateException("Cannot " + action + " for " + this + " because its desired state is currently " + desiredState + "; it must be STOPPED.");
        }

        boolean stateUpdated = false;
        while (!stateUpdated) {
            final ConnectorState currentState = getCurrentState();
            if (currentState != ConnectorState.STOPPED) {
                throw new IllegalStateException("Cannot " + action + " for " + this + " because its current state is " + currentState + "; it must be STOPPED.");
            }

            stateUpdated = stateTransition.trySetCurrentState(ConnectorState.STOPPED, newState);
        }
    }

    private void stopComponent(final FlowEngine scheduler, final CompletableFuture<Void> stopCompleteFuture) {
        logger.debug("Stopping component for {}", this);
        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, connectorDetails.getConnector().getClass(), getIdentifier())) {
            connectorDetails.getConnector().stop(activeFlowContext);
        } catch (final Exception e) {
            logger.error("Failed to stop {}. Will try again in 10 seconds", this, e);
            scheduler.schedule(() -> stopComponent(scheduler, stopCompleteFuture), 10, TimeUnit.SECONDS);
            return;
        }

        stateTransition.setCurrentState(ConnectorState.STOPPED);
        stopCompleteFuture.complete(null);
        logger.info("Successfully stopped {}", this);

        final ConnectorState desiredState = getDesiredState();
        if (desiredState == ConnectorState.RUNNING) {
            logger.info("{} was requested to be RUNNING while it was stopping so will attempt to start again", this);
            start(scheduler, new CompletableFuture<>());
        }
    }

    private void startComponent(final ScheduledExecutorService scheduler, final CompletableFuture<Void> startCompleteFuture) {
        logger.debug("Starting component for {}", this);
        final ConnectorState desiredState = getDesiredState();
        if (desiredState != ConnectorState.RUNNING) {
            logger.info("Will not start {} because the desired state is no longer RUNNING but is now {}", this, desiredState);
            return;
        }

        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, connectorDetails.getConnector().getClass(), getIdentifier())) {
            connectorDetails.getConnector().start(activeFlowContext);
        } catch (final Exception e) {
            logger.error("Failed to start {}. Will try again in 10 seconds", this, e);
            scheduler.schedule(() -> startComponent(scheduler, startCompleteFuture), 10, TimeUnit.SECONDS);
            return;
        }

        stateTransition.setCurrentState(ConnectorState.RUNNING);
        startCompleteFuture.complete(null);
        logger.info("Successfully started {}", this);
    }


    @Override
    public void verifyCanDelete() {
        final QueueSize queueSize = getActiveFlowContext().getManagedProcessGroup().getQueueSize();
        if (queueSize.getObjectCount() > 0) {
            throw new IllegalStateException("Cannot delete " + this + " because its Process Group has " + queueSize.getObjectCount()
                + " FlowFiles queued; all FlowFiles must be removed before it can be deleted.");
        }

        final ConnectorState currentState = getCurrentState();
        if (currentState == ConnectorState.TROUBLESHOOTING) {
            throw new IllegalStateException("Cannot delete " + this + " because it is in Troubleshooting mode; exit Troubleshooting before deleting.");
        }
        if (currentState == ConnectorState.STOPPED || currentState == ConnectorState.UPDATE_FAILED || currentState == ConnectorState.UPDATED) {
            return;
        }

        throw new IllegalStateException("Cannot delete " + this + " because its state is currently " + currentState + "; it must be stopped before it can be deleted.");
    }

    @Override
    public void verifyCanStart() {
        if (getCurrentState() == ConnectorState.TROUBLESHOOTING) {
            throw new IllegalStateException("Cannot start " + this + " because it is in Troubleshooting mode.");
        }
        final ValidationState state = performValidation();
        if (state.getStatus() != ValidationStatus.VALID) {
            throw new IllegalStateException("Cannot start " + this + " because it is not valid: " + state.getValidationErrors());
        }
    }

    @Override
    public void verifyCanEnterTroubleshooting() {
        if (isExtensionMissing()) {
            throw new IllegalStateException("Cannot enter Troubleshooting mode for " + this + " because it is a Ghost Connector (its underlying extension is missing).");
        }

        final ConnectorState currentState = getCurrentState();
        switch (currentState) {
            case TROUBLESHOOTING -> throw new IllegalStateException("Cannot enter Troubleshooting mode for " + this + " because it is already in Troubleshooting mode.");
            case STARTING, STOPPING, DRAINING, PURGING, PREPARING_FOR_UPDATE, UPDATING ->
                throw new IllegalStateException("Cannot enter Troubleshooting mode for " + this + " because its state is currently "
                    + currentState + "; it must be in a stable state before entering Troubleshooting.");
            default -> {
                // STOPPED, RUNNING, UPDATED, UPDATE_FAILED are all acceptable to enter Troubleshooting from
            }
        }
    }

    @Override
    public void verifyCanEndTroubleshooting() {
        final Optional<String> reason = getReasonCannotEndTroubleshooting();
        if (reason.isPresent()) {
            throw new IllegalStateException("Cannot end Troubleshooting mode for " + this + ": " + reason.get());
        }
    }

    private Optional<String> getReasonCannotEndTroubleshooting() {
        final Optional<String> quickReason = getQuickReasonCannotEndTroubleshooting();
        if (quickReason.isPresent()) {
            return quickReason;
        }

        // After confirming all components are stopped or disabled, check if the managed flow can be safely reverted to the
        // Connector's authoritative flow. This mirrors exactly what endTroubleshooting() will do so that any problem
        // (e.g. a Connection whose contents cannot be preserved or a component that cannot be replaced) is reported
        // synchronously rather than surfacing halfway through the state change. This check is intentionally not run by
        // createEndTroubleshootingAction (which is called on every GET of the Connector entity) because it requires
        // resolving the authoritative flow from the Connector plugin and running a full flow-comparison; that work is
        // only paid by the explicit verify REST endpoint and by endTroubleshooting itself.
        final VersionedExternalFlow authoritativeFlow = resolveAuthoritativeFlow();
        try {
            initializationContext.verifyUpdateFlow(activeFlowContext, authoritativeFlow, BundleCompatibility.RESOLVE_BUNDLE);
        } catch (final FlowUpdateException e) {
            return Optional.of("The Managed Process Group cannot be reverted to the Connector's authoritative flow: " + e.getMessage());
        }

        return Optional.empty();
    }

    /**
     * Returns the cheap-to-compute portion of {@link #getReasonCannotEndTroubleshooting()}: the Connector must be in
     * Troubleshooting, and every component in the managed flow must be stopped or disabled. Suitable for callers that
     * are invoked on every REST GET of the Connector entity, where running the full flow-revert preflight on every
     * call would add significant latency for large managed flows.
     */
    private Optional<String> getQuickReasonCannotEndTroubleshooting() {
        final ConnectorState currentState = getCurrentState();
        if (currentState != ConnectorState.TROUBLESHOOTING) {
            return Optional.of("Connector is not in Troubleshooting mode; current state is " + currentState);
        }

        return findReasonComponentsNotStopped(getActiveFlowContext().getManagedProcessGroup());
    }

    private VersionedExternalFlow resolveAuthoritativeFlow() {
        final VersionedExternalFlow authoritativeFlow;
        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, getConnector().getClass(), getIdentifier())) {
            authoritativeFlow = getConnector().getActiveFlow(activeFlowContext);
        }

        if (authoritativeFlow == null || authoritativeFlow.getFlowContents() == null) {
            logger.warn("Connector {} returned a null authoritative flow from getActiveFlow; using an empty flow.", this);
            final VersionedExternalFlow empty = new VersionedExternalFlow();
            empty.setFlowContents(new VersionedProcessGroup());
            return empty;
        }

        return authoritativeFlow;
    }

    private Optional<String> findReasonComponentsNotStopped(final ProcessGroup group) {
        for (final ProcessorNode processor : group.getProcessors()) {
            final org.apache.nifi.controller.ScheduledState scheduledState = processor.getScheduledState();
            if (!STOPPED_STATES.contains(scheduledState)) {
                return Optional.of("Processor " + processor.getIdentifier() + " is in state " + scheduledState + "; it must be STOPPED or DISABLED");
            }
        }

        for (final Port port : group.getInputPorts()) {
            final org.apache.nifi.controller.ScheduledState scheduledState = port.getScheduledState();
            if (!STOPPED_STATES.contains(scheduledState)) {
                return Optional.of("Input Port " + port.getIdentifier() + " is in state " + scheduledState + "; it must be STOPPED or DISABLED");
            }
        }

        for (final Port port : group.getOutputPorts()) {
            final org.apache.nifi.controller.ScheduledState scheduledState = port.getScheduledState();
            if (!STOPPED_STATES.contains(scheduledState)) {
                return Optional.of("Output Port " + port.getIdentifier() + " is in state " + scheduledState + "; it must be STOPPED or DISABLED");
            }
        }

        for (final RemoteProcessGroup remoteProcessGroup : group.getRemoteProcessGroups()) {
            if (remoteProcessGroup.isTransmitting()) {
                return Optional.of("Remote Process Group " + remoteProcessGroup.getIdentifier() + " is transmitting; it must be stopped");
            }
        }

        for (final ControllerServiceNode serviceNode : group.getControllerServices(false)) {
            if (serviceNode.isActive()) {
                return Optional.of("Controller Service " + serviceNode.getIdentifier() + " is active; all Controller Services within the managed flow must be disabled");
            }
        }

        for (final ProcessGroup childGroup : group.getProcessGroups()) {
            final Optional<String> childReason = findReasonComponentsNotStopped(childGroup);
            if (childReason.isPresent()) {
                return childReason;
            }
        }

        return Optional.empty();
    }

    @Override
    public void enterTroubleshooting() {
        verifyCanEnterTroubleshooting();
        logger.info("Transitioning {} into TROUBLESHOOTING state", this);

        // Deliberately do NOT stop or otherwise mutate the managed flow here. The explicit contract of Troubleshooting
        // mode (NIP-28) is that a user can "break glass" on a live, running Connector to inspect or stabilize a
        // production issue without first having to shut the flow down. The components that were running before entering
        // Troubleshooting remain running; the user may stop individual components as needed in order to edit them, and
        // they must all be stopped/disabled before the Connector can leave Troubleshooting mode (enforced in
        // verifyCanEndTroubleshooting).
        stateTransition.setDesiredState(ConnectorState.TROUBLESHOOTING);
        stateTransition.setCurrentState(ConnectorState.TROUBLESHOOTING);
    }

    @Override
    public void restoreTroubleshootingState() {
        logger.info("Restoring {} to TROUBLESHOOTING state from persisted flow", this);
        stateTransition.setDesiredState(ConnectorState.TROUBLESHOOTING);
        stateTransition.setCurrentState(ConnectorState.TROUBLESHOOTING);
    }

    @Override
    public void endTroubleshooting() throws FlowUpdateException {
        verifyCanEndTroubleshooting();
        logger.info("Exiting TROUBLESHOOTING state for {} by restoring Connector's authoritative flow", this);

        final VersionedExternalFlow flowToApply = resolveAuthoritativeFlow();

        // Route the update through the ConnectorInitializationContext so that bundle coordinates referenced by the
        // authoritative flow are resolved against the currently-available bundles. This mirrors how the initial flow
        // is applied in initializeConnector and avoids failing validation when the Connector hard-codes a bundle
        // version that differs from the currently-installed NAR (which is common in test Connectors).
        initializationContext.updateFlow(activeFlowContext, flowToApply, BundleCompatibility.RESOLVE_BUNDLE);

        stateTransition.setDesiredState(ConnectorState.STOPPED);
        stateTransition.setCurrentState(ConnectorState.STOPPED);
        logger.info("Successfully exited TROUBLESHOOTING state for {}; Connector is now STOPPED", this);
    }

    @Override
    public Connector getConnector() {
        return connectorDetails.getConnector();
    }

    @Override
    public String getComponentType() {
        return componentType;
    }

    @Override
    public String getCanonicalClassName() {
        return componentCanonicalClass;
    }

    @Override
    public BundleCoordinate getBundleCoordinate() {
        return bundleCoordinate;
    }

    @Override
    public boolean isExtensionMissing() {
        return extensionMissing;
    }

    @Override
    public List<DescribedValue> fetchAllowableValues(final String stepName, final String propertyName) {
        if (workingFlowContext == null) {
            throw new IllegalStateException("Cannot fetch Allowable Values for %s.%s because %s is not being updated.".formatted(
                stepName, propertyName, this));
        }

        workingFlowContext.getConfigurationContext().resolvePropertyValues();

        try (NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, getConnector().getClass(), getIdentifier())) {
            return getConnector().fetchAllowableValues(stepName, propertyName, workingFlowContext);
        }
    }

    @Override
    public List<DescribedValue> fetchAllowableValues(final String stepName, final String propertyName, final String filter) {
        if (workingFlowContext == null) {
            throw new IllegalStateException("Cannot fetch Allowable Values for %s.%s because %s is not being updated.".formatted(
                stepName, propertyName, this));
        }

        workingFlowContext.getConfigurationContext().resolvePropertyValues();

        try (NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, getConnector().getClass(), getIdentifier())) {
            return getConnector().fetchAllowableValues(stepName, propertyName, workingFlowContext, filter);
        }
    }

    @Override
    public void initializeConnector(final FrameworkConnectorInitializationContext initializationContext) {
        logger.debug("Initializing {}", this);
        this.initializationContext = initializationContext;

        try (NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, getConnector().getClass(), getIdentifier())) {
            getConnector().initialize(initializationContext);
        }

        recreateWorkingFlowContext();
        logger.info("Successfully initialized {}", this);
    }

    @Override
    public void loadInitialFlow() throws FlowUpdateException {
        logger.debug("Loading initial flow for {}", this);
        if (initializationContext == null) {
            throw new IllegalStateException("Cannot load initial flow because " + this + " has not been initialized yet.");
        }

        final VersionedExternalFlow initialFlow;
        try (NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, getConnector().getClass(), getIdentifier())) {
            initialFlow = getConnector().getInitialFlow();
        }

        if (initialFlow == null) {
            logger.info("{} has no initial flow to load", this);
        } else {
            final ValidationResult unresolvedBundleResult = validateBundlesCanBeResolved(initialFlow.getFlowContents(), initializationContext.getComponentBundleLookup());

            if (unresolvedBundleResult != null) {
                logger.error("Cannot load initial flow for {} because some component bundles cannot be resolved: {}", this, unresolvedBundleResult.getExplanation());
                unresolvedBundleValidationResult = unresolvedBundleResult;
            } else {
                logger.info("Loading initial flow for {}", this);
                // Update all RUNNING components to ENABLED before applying the initial flow so that components
                // are not started before being configured.
                stopComponents(initialFlow.getFlowContents());
                initializationContext.updateFlow(activeFlowContext, initialFlow, BundleCompatibility.RESOLVE_BUNDLE);
            }
        }

        resetValidationState();
        recreateWorkingFlowContext();
    }

    @Override
    public boolean isMigrationSupported(final ConnectorMigrationContext context) {
        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, getConnector().getClass(), getIdentifier())) {
            final Connector connector = getConnector();
            if (!(connector instanceof final MigratableConnector migratableConnector)) {
                return false;
            }
            return migratableConnector.isMigrationSupported(context);
        } catch (final Exception e) {
            getComponentLog().warn("Failed to evaluate whether migration is supported for {}; assuming migration is not supported", context.getSourceFlow(), e);
            return false;
        }
    }

    /**
     * Determines whether the Connector has been modified since it was created. Rather than comparing the managed flow
     * structure (which a Connector derives entirely from its configuration), this considers the Connector modified when
     * either of the following holds for the Active or Working configuration:
     * <ul>
     *   <li>any configured property differs from the property's declared default value; or</li>
     *   <li>any Processor or Controller Service in the managed flow has stored component state.</li>
     * </ul>
     * A configuration whose properties are all at their defaults produces the initial flow, so an unmodified Connector
     * can be safely migrated without discarding user changes. Any deviation in configuration, or any component state
     * accumulated by running the flow, means migration would overwrite those changes and is therefore disallowed.
     *
     * @return {@code true} if the Connector's Active or Working configuration deviates from its defaults or any managed
     *         component has stored state; {@code false} otherwise
     */
    @Override
    public boolean isModified() {
        final Map<String, Map<String, String>> defaultValuesByStep = buildDefaultValuesByStep();

        if (configurationDiffersFromDefaults(activeFlowContext, defaultValuesByStep)
            || configurationDiffersFromDefaults(workingFlowContext, defaultValuesByStep)) {
            return true;
        }

        return hasComponentState(activeFlowContext) || hasComponentState(workingFlowContext);
    }

    /**
     * Builds a mapping of configuration step name to the declared default value of each property within that step, as
     * defined by the Connector's {@link ConfigurationStep configuration steps}. A property with no default is
     * represented by a {@code null} value so that an unset (or explicitly null) configured value compares equal to it.
     */
    private Map<String, Map<String, String>> buildDefaultValuesByStep() {
        final Map<String, Map<String, String>> defaultValuesByStep = new HashMap<>();

        final List<ConfigurationStep> configurationSteps = getConfigurationSteps();
        if (configurationSteps == null) {
            return defaultValuesByStep;
        }

        for (final ConfigurationStep configurationStep : configurationSteps) {
            final Map<String, String> propertyDefaults = new HashMap<>();
            for (final ConnectorPropertyGroup propertyGroup : configurationStep.getPropertyGroups()) {
                for (final ConnectorPropertyDescriptor descriptor : propertyGroup.getProperties()) {
                    propertyDefaults.put(descriptor.getName(), descriptor.getDefaultValue());
                }
            }
            defaultValuesByStep.put(configurationStep.getName(), propertyDefaults);
        }

        return defaultValuesByStep;
    }

    /**
     * Determines whether the configuration held by the given flow context deviates from the Connector's default
     * configuration. A property is treated as modified when it is configured with a String literal whose value differs
     * from the property's declared default, or with a populated Secret or Asset reference (neither of which can
     * represent a default). A structurally-empty Secret or Asset reference is a placeholder for an unset property and is
     * not treated as a modification.
     */
    private boolean configurationDiffersFromDefaults(final FrameworkFlowContext flowContext, final Map<String, Map<String, String>> defaultValuesByStep) {
        if (flowContext == null) {
            return false;
        }

        final MutableConnectorConfigurationContext configurationContext = flowContext.getConfigurationContext();
        if (configurationContext == null) {
            return false;
        }

        final ConnectorConfiguration configuration = configurationContext.toConnectorConfiguration();
        for (final NamedStepConfiguration namedStepConfiguration : configuration.getNamedStepConfigurations()) {
            final Map<String, String> propertyDefaults = defaultValuesByStep.getOrDefault(namedStepConfiguration.stepName(), Map.of());

            for (final Map.Entry<String, ConnectorValueReference> propertyEntry : namedStepConfiguration.configuration().getPropertyValues().entrySet()) {
                final String propertyName = propertyEntry.getKey();
                final ConnectorValueReference valueReference = propertyEntry.getValue();
                if (valueReference == null) {
                    continue;
                }

                if (valueReference instanceof final StringLiteralValue stringLiteralValue) {
                    if (!Objects.equals(stringLiteralValue.getValue(), propertyDefaults.get(propertyName))) {
                        logger.debug("{} differs from its initial flow because property [{}] of configuration step [{}] is not set to its default value",
                            this, propertyName, namedStepConfiguration.stepName());
                        return true;
                    }
                } else if (!isStructurallyEmptyReference(valueReference)) {
                    logger.debug("{} differs from its initial flow because property [{}] of configuration step [{}] is configured with a {} reference",
                        this, propertyName, namedStepConfiguration.stepName(), valueReference.getValueType());
                    return true;
                }
            }
        }

        return false;
    }

    /**
     * Determines whether the given non-{@code StringLiteralValue} reference is structurally empty, meaning it carries no
     * actual referenced value and acts as a placeholder for an unset property rather than a configured one. A
     * structurally-empty {@link SecretReference} has no provider or secret name; a structurally-empty
     * {@link AssetReference} has no asset identifiers.
     */
    private boolean isStructurallyEmptyReference(final ConnectorValueReference valueReference) {
        return switch (valueReference) {
            case SecretReference secretReference -> isEmptySecretReference(secretReference);
            case AssetReference assetReference -> assetReference.getAssetIdentifiers() == null || assetReference.getAssetIdentifiers().isEmpty();
            default -> false;
        };
    }

    /**
     * Determines whether any Processor or Controller Service within the given flow context's managed Process Group has
     * stored component state in either the local or cluster scope. A component with stored state has been run since the
     * Connector was created, so the managed flow no longer reflects the Connector's initial flow.
     */
    private boolean hasComponentState(final FrameworkFlowContext flowContext) {
        if (flowContext == null) {
            return false;
        }

        final ProcessGroup managedProcessGroup = flowContext.getManagedProcessGroup();
        if (managedProcessGroup == null) {
            return false;
        }

        for (final ProcessorNode processor : managedProcessGroup.findAllProcessors()) {
            if (componentHasStoredState(processor.getIdentifier())) {
                logger.debug("{} differs from its initial flow because Processor [{}] has stored component state", this, processor.getIdentifier());
                return true;
            }
        }

        for (final ControllerServiceNode controllerService : managedProcessGroup.findAllControllerServices()) {
            if (componentHasStoredState(controllerService.getIdentifier())) {
                logger.debug("{} differs from its initial flow because Controller Service [{}] has stored component state", this, controllerService.getIdentifier());
                return true;
            }
        }

        return false;
    }

    private boolean componentHasStoredState(final String componentIdentifier) {
        final StateManager stateManager = stateManagerProvider.getStateManager(componentIdentifier);
        if (stateManager == null) {
            return false;
        }

        return hasStoredState(stateManager, Scope.LOCAL) || hasStoredState(stateManager, Scope.CLUSTER);
    }

    private boolean hasStoredState(final StateManager stateManager, final Scope scope) {
        try {
            final StateMap stateMap = stateManager.getState(scope);
            return stateMap != null && !stateMap.toMap().isEmpty();
        } catch (final IOException e) {
            logger.warn("Failed to read {} state for a component of {} while checking whether it matches its initial flow; treating the component as having no state in this scope", scope, this, e);
            return false;
        }
    }

    private void stopComponents(final VersionedProcessGroup group) {
        for (final VersionedProcessor processor : group.getProcessors()) {
            if (processor.getScheduledState() == ScheduledState.RUNNING) {
                processor.setScheduledState(ScheduledState.ENABLED);
            }
        }

        for (final VersionedControllerService service : group.getControllerServices()) {
            if (service.getScheduledState() == ScheduledState.RUNNING) {
                service.setScheduledState(ScheduledState.ENABLED);
            }
        }

        for (final VersionedProcessGroup childGroup : group.getProcessGroups()) {
            stopComponents(childGroup);
        }
    }

    /**
     * Ensures that all bundles required by the given Process Group can be resolved. We do this in order to make the Connector
     * invalid if any Processor or Controller Service cannot be properly instantiated due to missing bundles. We intentionally
     * differentiate between making the Connector invalid versus Ghosting the Connector for a few reasons:
     * <ul>
     *     <li>
     *         Ghosting the Connector would prevent us from even getting the Configuration Steps, and it results in all Properties becoming sensitive. This can lead to confusion.
     *     </li>
     *     <li>
     *         The flow may change dynamically and so it's possible for a Connector to be valid given its initial flow and then become invalid
     *     based on configuration because the new configuration requires a new component that is unavailable. We would not suddenly change from
     *     a valid Connector to a Ghosted Connector, we could only become invalid. We do not want a missing component in the Initial Flow to be
     *     treated differently than a missing component from a subsequent flow update.
     *     </li>
     *     <li>
     *         Ghosting should be reserved for situations where the extension itself is missing.
     *     </li>
     * </ul>
     *
     * @param group the process group to validate
     * @param bundleLookup the bundle lookup
     * @return a ValidationResult describing the missing bundles if any are missing; null if all bundles can be resolved
     */
    private ValidationResult validateBundlesCanBeResolved(final VersionedProcessGroup group, final ComponentBundleLookup bundleLookup) {
        final Set<String> missingBundles = new HashSet<>();
        final Set<String> missingProcessorTypes = new HashSet<>();
        final Set<String> missingControllerServiceTypes = new HashSet<>();

        collectUnresolvedBundles(group, bundleLookup, missingBundles, missingProcessorTypes, missingControllerServiceTypes);

        if (missingBundles.isEmpty()) {
            return null;
        }

        final StringBuilder explanation = new StringBuilder();
        explanation.append("%d Processors and %d Controller Services unavailable from %d missing bundles".formatted(
            missingProcessorTypes.size(), missingControllerServiceTypes.size(), missingBundles.size()));
        explanation.append("\nMissing Bundles: %s".formatted(missingBundles));
        if (!missingProcessorTypes.isEmpty()) {
            explanation.append("\nMissing Processors: %s".formatted(missingProcessorTypes));
        }
        if (!missingControllerServiceTypes.isEmpty()) {
            explanation.append("\nMissing Controller Services: %s".formatted(missingControllerServiceTypes));
        }

        return new ValidationResult.Builder()
            .subject("Missing Bundles")
            .valid(false)
            .explanation(explanation.toString())
            .build();
    }

    private void collectUnresolvedBundles(final VersionedProcessGroup group, final ComponentBundleLookup bundleLookup,
                                          final Set<String> missingBundles, final Set<String> missingProcessorTypes,
                                          final Set<String> missingControllerServiceTypes) {
        if (group.getProcessors() != null) {
            for (final VersionedProcessor processor : group.getProcessors()) {
                if (!isBundleResolvable(processor.getType(), processor.getBundle(), bundleLookup)) {
                    missingBundles.add(formatBundle(processor.getBundle()));
                    missingProcessorTypes.add(processor.getType());
                }
            }
        }

        if (group.getControllerServices() != null) {
            for (final VersionedControllerService service : group.getControllerServices()) {
                if (!isBundleResolvable(service.getType(), service.getBundle(), bundleLookup)) {
                    missingBundles.add(formatBundle(service.getBundle()));
                    missingControllerServiceTypes.add(service.getType());
                }
            }
        }

        if (group.getProcessGroups() != null) {
            for (final VersionedProcessGroup childGroup : group.getProcessGroups()) {
                collectUnresolvedBundles(childGroup, bundleLookup, missingBundles, missingProcessorTypes, missingControllerServiceTypes);
            }
        }
    }

    private String formatBundle(final Bundle bundle) {
        return "%s:%s:%s".formatted(bundle.getGroup(), bundle.getArtifact(), bundle.getVersion());
    }

    private boolean isBundleResolvable(final String componentType, final Bundle currentBundle, final ComponentBundleLookup bundleLookup) {
        final List<Bundle> availableBundles = bundleLookup.getAvailableBundles(componentType);

        if (availableBundles.contains(currentBundle)) {
            return true;
        }

        // With RESOLVE_BUNDLE, a bundle can be resolved only if exactly one alternative bundle is available
        return availableBundles.size() == 1;
    }

    @Override
    public void recreateWorkingFlowContext() {
        destroyWorkingContext();
        workingFlowContext = flowContextFactory.createWorkingFlowContext(identifier,
            connectorDetails.getComponentLog(), activeFlowContext.getConfigurationContext(), activeFlowContext.getBundle());

        getComponentLog().info("Working Flow Context has been set");

        // Re-fire onConfigurationStepConfigured for every step so flow parameters derived from the
        // configuration (e.g., resolved asset paths, secret values) are refreshed against the new
        // working context. Step failures are logged so the remaining steps can still be refreshed.
        // Skipped before the connector has been initialized because there is no flow to update yet.
        if (initializationContext == null) {
            return;
        }
        final ConnectorConfiguration config = workingFlowContext.getConfigurationContext().toConnectorConfiguration();
        for (final NamedStepConfiguration stepConfig : config.getNamedStepConfigurations()) {
            try {
                setConfiguration(stepConfig.stepName(), stepConfig.configuration(), true);
            } catch (final Exception e) {
                logger.warn("Failed to refresh resolved configuration for step [{}] of {}",
                    stepConfig.stepName(), this, e);
            }
        }

        getComponentLog().info("Working Flow Context configuration has been refreshed");
    }

    @Override
    public void pauseValidationTrigger() {
        triggerValidation = false;
    }

    @Override
    public void resumeValidationTrigger() {
        triggerValidation = true;
        logger.debug("Resuming Triggering of Validation State for {}; Resetting validation state", this);
        resetValidationState();
    }

    @Override
    public boolean isValidationPaused() {
        return !triggerValidation;
    }

    @Override
    public List<ConfigVerificationResult> verifyConfigurationStep(final String stepName, final StepConfiguration configurationOverrides) {
        if (getCurrentState() == ConnectorState.TROUBLESHOOTING) {
            throw new IllegalStateException("Cannot verify configuration step " + stepName + " for " + this
                + " while it is in Troubleshooting mode; exit Troubleshooting mode before running configuration verification.");
        }

        logger.debug("Verifying configuration step {} for {}", stepName, this);
        final List<ConfigVerificationResult> results = new ArrayList<>();
        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, getConnector().getClass(), getIdentifier())) {

            final Optional<ConfigurationStep> optionalStep = getConfigurationStep(stepName);
            if (optionalStep.isEmpty()) {
                results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Property Validation")
                    .outcome(Outcome.FAILED)
                    .explanation("Configuration step with name '" + stepName + "' does not exist.")
                    .build());
                return results;
            }

            final ConfigurationStep configurationStep = optionalStep.get();
            final List<SecretReference> invalidSecretRefs = new ArrayList<>();
            final List<AssetReference> invalidAssetRefs = new ArrayList<>();
            // Bypass the Secret value cache during verification so the user sees results based on the current
            // Secret values rather than potentially stale cached values awaiting TTL expiration.
            final Map<String, String> resolvedPropertyOverrides = resolvePropertyReferences(configurationStep, configurationOverrides, invalidSecretRefs, invalidAssetRefs, false);

            final DescribedValueProvider allowableValueProvider = (step, propertyName) -> fetchAllowableValues(step, propertyName, workingFlowContext);

            final MutableConnectorConfigurationContext configContext = workingFlowContext.getConfigurationContext().createWithOverrides(stepName, resolvedPropertyOverrides);
            final ConnectorConfiguration connectorConfig = configContext.toConnectorConfiguration();
            final ParameterContextFacade paramContext = workingFlowContext.getParameterContext();
            final ConnectorValidationContext validationContext = new StandardConnectorValidationContext(connectorConfig, allowableValueProvider, paramContext);

            final List<ValidationResult> validationResults = new ArrayList<>();
            validatePropertyReferences(configurationStep, configurationOverrides, validationResults);

            // If there are any invalid secrets or assets referenced, add Validation Results for them.
            addInvalidReferenceResults(validationResults, invalidSecretRefs, invalidAssetRefs);

            // If there are any framework-level validation failures, we do not run the Connector-specific validation because
            // doing so would mean that we must provide weak guarantees about the state of the configuration when the Connector's
            // validation is invoked. But if there are no framework-level validation failures, we can proceed to invoke the
            // Connector's validation logic.
            if (validationResults.isEmpty()) {
                final List<ValidationResult> implValidationResults = getConnector().validateConfigurationStep(configurationStep, configContext, validationContext);
                validationResults.addAll(implValidationResults);
            }

            final List<ConfigVerificationResult> invalidConfigResults = validationResults.stream()
                .filter(result -> !result.isValid())
                .map(this::createConfigVerificationResult)
                .toList();

            if (invalidConfigResults.isEmpty()) {
                results.add(new ConfigVerificationResult.Builder()
                    .verificationStepName("Property Validation")
                    .outcome(Outcome.SUCCESSFUL)
                    .build());

                results.addAll(getConnector().verifyConfigurationStep(stepName, resolvedPropertyOverrides, workingFlowContext));
            } else {
                results.addAll(invalidConfigResults);
            }

            logger.debug("Completed verification of configuration step {} for {}", stepName, this);
            return results;
        }
    }

    private ConfigVerificationResult createConfigVerificationResult(final ValidationResult validationResult) {
        return new ConfigVerificationResult.Builder()
            .verificationStepName("Property Validation - " + validationResult.getSubject())
            .outcome(validationResult.isValid() ? Outcome.SUCCESSFUL : Outcome.FAILED)
            .subject(validationResult.getSubject())
            .explanation(validationResult.getExplanation())
            .build();
    }

    private Map<String, String> resolvePropertyReferences(final ConfigurationStep configurationStep, final StepConfiguration configurationOverrides,
                                                          final List<SecretReference> invalidSecretRefs, final List<AssetReference> invalidAssetRefs, final boolean useCache) {

        final Map<String, String> resolvedProperties = new HashMap<>();
        final Map<String, ConnectorPropertyDescriptor> descriptorLookup = buildPropertyDescriptorLookup(configurationStep);

        try {
            // Secret References can be expensive to lookup so we don't want to call getSecret() for each one. Instead, we
            // want to find all Secrets by Provider and then call fetchSecrets() once per Provider.
            // Structurally-empty SECRET_REFERENCE entries (a typed reference with no FQN/secretName) are skipped here so the
            // connector's own required-property validation produces "<name> is required" instead of "[null] could not be found".
            final Set<SecretReference> secretReferences = configurationOverrides.getPropertyValues().entrySet().stream()
                .filter(entry -> entry.getValue() != null && entry.getValue().getValueType() == ConnectorValueType.SECRET_REFERENCE)
                .filter(entry -> !isEmptySecretReference((SecretReference) entry.getValue()))
                .filter(entry -> {
                    final ConnectorPropertyDescriptor descriptor = descriptorLookup.get(entry.getKey());
                    return descriptor == null || isPropertyDependencySatisfied(descriptor, descriptorLookup::get, configurationOverrides);
                })
                .map(entry -> (SecretReference) entry.getValue())
                .collect(Collectors.toSet());

            final Map<SecretReference, Secret> secretsByReference = secretReferences.isEmpty()
                ? Map.of()
                : initializationContext.getSecretsManager().getSecrets(secretReferences, useCache);
            secretsByReference.forEach((ref, secret) -> {
                if (secret == null) {
                    invalidSecretRefs.add(ref);
                }
            });

            for (final Map.Entry<String, ConnectorValueReference> entry : configurationOverrides.getPropertyValues().entrySet()) {
                final String propertyName = entry.getKey();
                final ConnectorValueReference valueReference = entry.getValue();

                if (valueReference == null) {
                    continue;
                }

                final ConnectorPropertyDescriptor descriptor = descriptorLookup.get(propertyName);
                if (descriptor != null && !isPropertyDependencySatisfied(descriptor, descriptorLookup::get, configurationOverrides)) {
                    // Omit values for properties that are not applicable so merged configuration does not retain stale overrides
                    // (createWithOverrides removes keys when the override value is null).
                    resolvedProperties.put(propertyName, null);
                    continue;
                }

                // We've already looked up secrets above, so use the cached value here.
                if (valueReference.getValueType() == ConnectorValueType.SECRET_REFERENCE) {
                    final SecretReference secretReference = (SecretReference) valueReference;
                    if (isEmptySecretReference(secretReference)) {
                        // A typed-but-empty SECRET_REFERENCE acts like an unset property. The connector's own
                        // validateConfigurationStep emits "<name> is required" for required properties, which is a more
                        // actionable message than "[null] could not be found" from secret resolution.
                        resolvedProperties.put(propertyName, null);
                        continue;
                    }
                    final Secret secret = secretsByReference.get(secretReference);
                    final String resolvedValue = (secret == null) ? null : secret.getValue();
                    resolvedProperties.put(propertyName, resolvedValue);
                    continue;
                }

                final String resolvedValue = resolvePropertyReference(valueReference);
                resolvedProperties.put(propertyName, resolvedValue);

                if (resolvedValue == null && valueReference.getValueType() == ConnectorValueType.ASSET_REFERENCE) {
                    invalidAssetRefs.add((AssetReference) valueReference);
                }
            }
        } catch (final IOException ioe) {
            throw new UncheckedIOException("Failed to resolve Secret references for " + this, ioe);
        }

        return resolvedProperties;
    }

    private static Map<String, ConnectorPropertyDescriptor> buildPropertyDescriptorLookup(final ConfigurationStep configurationStep) {
        final Map<String, ConnectorPropertyDescriptor> lookup = new HashMap<>();
        for (final ConnectorPropertyGroup propertyGroup : configurationStep.getPropertyGroups()) {
            for (final ConnectorPropertyDescriptor descriptor : propertyGroup.getProperties()) {
                lookup.put(descriptor.getName(), descriptor);
            }
        }
        return lookup;
    }

    /**
     * Returns the configured String value of a controlling property for the purposes of evaluating a
     * {@link ConnectorPropertyDependency} against a raw {@link StepConfiguration} payload.
     *
     * <p>This is an approximation of the value lookup performed inside
     * {@code AbstractConnector.isDependencySatisfied(...)}, scoped to the framework's pre-resolution data model. It
     * intentionally diverges from the connector-side implementation in two cases:
     * <ul>
     *   <li>A {@link StringLiteralValue} whose {@link StringLiteralValue#getValue() value} is {@code null} falls back to
     *       {@link ConnectorPropertyDescriptor#getDefaultValue() the descriptor default}, whereas the connector-side
     *       implementation treats a null literal value as no value (and would mark the dependency unsatisfied).</li>
     *   <li>{@link AssetReference} and {@link SecretReference} controlling properties are treated as having no
     *       String value (returns {@code null}, which renders the dependency unsatisfied), whereas the connector-side
     *       implementation works against a {@code ConnectorPropertyValue} whose value has already been resolved.</li>
     * </ul>
     * Both divergences are acceptable because controlling properties (the property a dependency points at) are, in
     * practice, {@code STRING_LITERAL} values drawn from a fixed set of allowable values — typically an enum of strategies
     * or modes. They are not asset or secret references, and a typed-but-null literal can only occur when the controlling
     * property has been explicitly cleared, in which case treating it as the descriptor default matches the user's intent
     * for the typical "switch back to the default mode" UX.
     */
    private static String getConfiguredValueForDependency(final StepConfiguration stepConfig, final ConnectorPropertyDescriptor descriptor) {
        final ConnectorValueReference ref = stepConfig.getPropertyValue(descriptor.getName());
        if (ref == null) {
            return descriptor.getDefaultValue();
        }
        if (ref instanceof final StringLiteralValue stringLiteralValue) {
            final String value = stringLiteralValue.getValue();
            return value != null ? value : descriptor.getDefaultValue();
        }
        return null;
    }

    private static boolean isPropertyDependencySatisfied(final ConnectorPropertyDescriptor propertyDescriptor,
            final Function<String, ConnectorPropertyDescriptor> propertyDescriptorLookup, final StepConfiguration stepConfig) {
        return isPropertyDependencySatisfied(propertyDescriptor, propertyDescriptorLookup, stepConfig, new HashSet<>());
    }

    /**
     * A {@link SecretReference} is structurally empty when neither the fully qualified name nor the simple secret name is
     * populated. Such references cannot be resolved against any {@link org.apache.nifi.components.connector.secrets.SecretsManager}
     * and represent an unset property — typically a placeholder emitted by an external configuration provider before the user
     * has filled in the value. Treating these the same as a missing value lets connector-level validation surface the more
     * actionable "is required" message instead of a "[null] could not be found" failure.
     */
    private static boolean isEmptySecretReference(final SecretReference secretReference) {
        return secretReference.getFullyQualifiedName() == null && secretReference.getSecretName() == null;
    }

    // TODO: consider extracting to a utility class in nifi-api that can be shared with AbstractConnector.isDependencySatisfied()
    private static boolean isPropertyDependencySatisfied(final ConnectorPropertyDescriptor propertyDescriptor,
            final Function<String, ConnectorPropertyDescriptor> propertyDescriptorLookup, final StepConfiguration stepConfig, final Set<String> propertiesSeen) {

        final Set<ConnectorPropertyDependency> dependencies = propertyDescriptor.getDependencies();
        if (dependencies.isEmpty()) {
            return true;
        }

        final boolean added = propertiesSeen.add(propertyDescriptor.getName());
        if (!added) {
            return false;
        }

        try {
            for (final ConnectorPropertyDependency dependency : dependencies) {
                final String dependencyName = dependency.getPropertyName();

                final ConnectorPropertyDescriptor dependencyDescriptor = propertyDescriptorLookup.apply(dependencyName);
                if (dependencyDescriptor == null) {
                    return false;
                }

                final String dependencyValue = getConfiguredValueForDependency(stepConfig, dependencyDescriptor);
                if (dependencyValue == null) {
                    return false;
                }

                if (!isPropertyDependencySatisfied(dependencyDescriptor, propertyDescriptorLookup, stepConfig, propertiesSeen)) {
                    return false;
                }

                final Set<String> dependentValues = dependency.getDependentValues();
                if (dependentValues != null && !dependentValues.contains(dependencyValue)) {
                    return false;
                }
            }

            return true;
        } finally {
            propertiesSeen.remove(propertyDescriptor.getName());
        }
    }

    private String resolvePropertyReference(final ConnectorValueReference valueReference) throws IOException {
        if (valueReference == null) {
            return null;
        }

        return switch (valueReference) {
            case StringLiteralValue stringLiteralValue -> stringLiteralValue.getValue();
            case AssetReference assetReference -> resolveAssetReferences(assetReference);
            case SecretReference secretReference -> initializationContext.getSecretsManager()
                .getSecret(secretReference)
                .map(Secret::getValue)
                .orElse(null);
        };
    }

    private String resolveAssetReferences(final AssetReference assetReference) {
        final Set<String> resolvedAssetValues = new HashSet<>();
        for (final String assetId : assetReference.getAssetIdentifiers()) {
            initializationContext.getAssetManager().getAsset(assetId)
                .map(Asset::getFile)
                .map(File::getAbsolutePath)
                .ifPresent(resolvedAssetValues::add);
        }

        logger.debug("Resolved {} to {} for {}", assetReference, resolvedAssetValues, this);
        return String.join(",", resolvedAssetValues);
    }

    private Optional<ConfigurationStep> getConfigurationStep(final String stepName) {
        for (final ConfigurationStep step : getConfigurationSteps()) {
            if (Objects.equals(step.getName(), stepName)) {
                return Optional.of(step);
            }
        }

        return Optional.empty();
    }

    @Override
    public List<ConfigVerificationResult> verify() {
        logger.debug("Verifying {}", this);
        final List<ConfigVerificationResult> results = new ArrayList<>();

        final ValidationState state = performValidation();
        if (state.getStatus() == ValidationStatus.INVALID) {
            final List<String> validationFailureExplanations = state.getValidationErrors().stream()
                .map(ValidationResult::getExplanation)
                .toList();

            results.add(new ConfigVerificationResult.Builder()
                .verificationStepName("Property Validation")
                .outcome(Outcome.FAILED)
                .explanation("There are " + validationFailureExplanations.size() + " validation failures: " + validationFailureExplanations)
                .build());

            logger.debug("Completed verification for {} with validation failures", this);
            return results;
        }

        workingFlowContext.getConfigurationContext().resolvePropertyValues();

        try (NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, getConnector().getClass(), getIdentifier())) {
            results.addAll(getConnector().verify(workingFlowContext));
        }

        logger.debug("Completed verification for {}", this);
        return results;
    }


    @Override
    public String getIdentifier() {
        return identifier;
    }

    @Override
    public String getProcessGroupIdentifier() {
        return null;
    }

    @Override
    public ComponentLog getComponentLog() {
        return connectorDetails.getComponentLog();
    }

    @Override
    public List<ConfigurationStep> getConfigurationSteps() {
        try (NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, getConnector().getClass(), getIdentifier())) {
            return getConnector().getConfigurationSteps();
        }
    }

    @Override
    public FrameworkFlowContext getActiveFlowContext() {
        return activeFlowContext;
    }

    @Override
    public FrameworkFlowContext getWorkingFlowContext() {
        return workingFlowContext;
    }

    @Override
    public void discardWorkingConfiguration() {
        if (getCurrentState() == ConnectorState.TROUBLESHOOTING) {
            throw new IllegalStateException("Cannot discard the working configuration for " + this + " while it is in Troubleshooting mode; "
                + "exit Troubleshooting mode before discarding configuration changes.");
        }

        recreateWorkingFlowContext();
        logger.debug("Discarded working configuration for {}", this);
    }

    @Override
    public List<ConnectorAction> getAvailableActions() {
        final List<ConnectorAction> actions = new ArrayList<>();
        final ConnectorState currentState = getCurrentState();
        final boolean dataQueued = activeFlowContext.getManagedProcessGroup().isDataQueued();
        final boolean stopped = isStopped();
        final boolean troubleshooting = currentState == ConnectorState.TROUBLESHOOTING;

        actions.add(createStartAction(stopped && !troubleshooting, troubleshooting));
        actions.add(createStopAction(currentState));
        actions.add(createConfigureAction(troubleshooting));
        actions.add(createDiscardWorkingConfigAction(troubleshooting));
        actions.add(createPurgeFlowFilesAction(stopped && !troubleshooting, dataQueued));
        actions.add(createDrainFlowFilesAction(stopped && !troubleshooting, dataQueued));
        actions.add(createCancelDrainFlowFilesAction(currentState == ConnectorState.DRAINING));
        actions.add(createApplyUpdatesAction(currentState, troubleshooting));
        actions.add(createMigrateAction(stopped && !troubleshooting));
        actions.add(createDeleteAction(stopped && !troubleshooting, dataQueued));
        actions.add(createEnterTroubleshootingAction(currentState));
        actions.add(createEndTroubleshootingAction());

        return actions;
    }

    private ConnectorAction createMigrateAction(final boolean stopped) {
        final boolean allowed;
        final String reason;

        if (!stopped) {
            allowed = false;
            reason = "Connector must be stopped";
        } else if (!(getConnector() instanceof MigratableConnector)) {
            allowed = false;
            reason = "Connector does not support migration from a Versioned flow";
        } else if (isModified()) {
            allowed = false;
            reason = "Connector has been modified since it was created; migration would overwrite those modifications";
        } else {
            allowed = true;
            reason = null;
        }

        return new StandardConnectorAction("MIGRATE", "Migrate a Versioned flow's assets and configuration into this Connector", allowed, reason);
    }

    private boolean isStopped() {
        final ConnectorState currentState = getCurrentState();
        if (currentState == ConnectorState.STOPPED) {
            return true;
        }
        if (currentState == ConnectorState.UPDATED || currentState == ConnectorState.UPDATE_FAILED) {
            return !hasActiveThread(getActiveFlowContext().getManagedProcessGroup());
        }

        return false;
    }

    private ConnectorAction createStartAction(final boolean stopped, final boolean troubleshooting) {
        final boolean allowed;
        final String reason;

        if (troubleshooting) {
            allowed = false;
            reason = "Connector is in Troubleshooting mode";
        } else if (!stopped) {
            allowed = false;
            reason = "Connector is not stopped";
        } else {
            final Collection<ValidationResult> validationResults = getValidationErrors();
            if (validationResults.isEmpty()) {
                allowed = true;
                reason = null;
            } else {
                allowed = false;
                reason = "Connector is not valid: " + validationResults.stream()
                    .map(ValidationResult::getExplanation)
                    .collect(Collectors.joining("; "));
            }
        }

        return new StandardConnectorAction("START", "Start the connector", allowed, reason);
    }

    private ConnectorAction createStopAction(final ConnectorState currentState) {
        final boolean allowed;
        final String reason;
        if (currentState == ConnectorState.TROUBLESHOOTING) {
            allowed = false;
            reason = "Connector is in Troubleshooting mode";
        } else if (currentState == ConnectorState.RUNNING || currentState == ConnectorState.STARTING) {
            allowed = true;
            reason = null;
        } else if (currentState == ConnectorState.UPDATED || currentState == ConnectorState.UPDATE_FAILED) {
            allowed = hasActiveThread(activeFlowContext.getManagedProcessGroup());
            reason = allowed ? null : "Connector is not running";
        } else {
            allowed = false;
            reason = "Connector is not running";
        }

        return new StandardConnectorAction("STOP", "Stop the connector", allowed, reason);
    }

    private ConnectorAction createConfigureAction(final boolean troubleshooting) {
        if (troubleshooting) {
            return new StandardConnectorAction("CONFIGURE", "Configure the connector", false, "Connector is in Troubleshooting mode");
        }
        return new StandardConnectorAction("CONFIGURE", "Configure the connector", true, null);
    }

    private ConnectorAction createEnterTroubleshootingAction(final ConnectorState currentState) {
        if (isExtensionMissing()) {
            return new StandardConnectorAction("ENTER_TROUBLESHOOTING", "Enter Troubleshooting mode for the connector", false,
                "Connector's extension is missing");
        }
        switch (currentState) {
            case TROUBLESHOOTING:
                return new StandardConnectorAction("ENTER_TROUBLESHOOTING", "Enter Troubleshooting mode for the connector", false,
                    "Connector is already in Troubleshooting mode");
            case STARTING, STOPPING, DRAINING, PURGING, PREPARING_FOR_UPDATE, UPDATING:
                return new StandardConnectorAction("ENTER_TROUBLESHOOTING", "Enter Troubleshooting mode for the connector", false,
                    "Connector is currently transitioning; current state is " + currentState);
            default:
                return new StandardConnectorAction("ENTER_TROUBLESHOOTING", "Enter Troubleshooting mode for the connector", true, null);
        }
    }

    private ConnectorAction createEndTroubleshootingAction() {
        // Use the quick reason check rather than the full check because this is invoked on every REST GET of the
        // Connector entity. The full check resolves the authoritative flow from the Connector plugin and runs a
        // flow-comparison that can be expensive for large managed flows.
        final Optional<String> reason = getQuickReasonCannotEndTroubleshooting();
        return new StandardConnectorAction("END_TROUBLESHOOTING", "Exit Troubleshooting mode for the connector", reason.isEmpty(), reason.orElse(null));
    }

    private ConnectorAction createDiscardWorkingConfigAction(final boolean troubleshooting) {
        final boolean allowed;
        final String reason;

        if (troubleshooting) {
            allowed = false;
            reason = "Connector is in Troubleshooting mode";
        } else if (!hasWorkingConfigurationChanges()) {
            allowed = false;
            reason = "No pending changes to discard";
        } else {
            allowed = true;
            reason = null;
        }

        return new StandardConnectorAction("DISCARD_WORKING_CONFIGURATION", "Discard any changes made to the working configuration", allowed, reason);
    }

    private boolean hasActiveThread(final ProcessGroup group) {
        for (final ProcessorNode processor : group.getProcessors()) {
            if (processor.getActiveThreadCount() > 0) {
                return true;
            }
        }

        for (final ProcessGroup childGroup : group.getProcessGroups()) {
            if (hasActiveThread(childGroup)) {
                return true;
            }
        }

        return false;
    }

    private ConnectorAction createPurgeFlowFilesAction(final boolean stopped, final boolean dataQueued) {
        return createDataQueuedAction(stopped, dataQueued, "PURGE_FLOWFILES", "Purge all FlowFiles from the connector, dropping all data without processing it");
    }

    private ConnectorAction createDrainFlowFilesAction(final boolean stopped, final boolean dataQueued) {
        return createDataQueuedAction(stopped, dataQueued, "DRAIN_FLOWFILES", "Process data that is currently in the flow but do not ingest any additional data");
    }

    private static ConnectorAction createDataQueuedAction(final boolean stopped, final boolean dataQueued, final String actionName, final String description) {
        final boolean allowed;
        final String reason;

        if (!stopped) {
            allowed = false;
            reason = "Connector must be stopped";
        } else if (!dataQueued) {
            allowed = false;
            reason = "No data is queued";
        } else {
            allowed = true;
            reason = null;
        }

        return new StandardConnectorAction(actionName, description, allowed, reason);
    }

    private ConnectorAction createCancelDrainFlowFilesAction(final boolean draining) {
        if (draining) {
            return new StandardConnectorAction("CANCEL_DRAIN_FLOWFILES", "Cancel the ongoing drain of FlowFiles", true, null);
        }

        return new StandardConnectorAction("CANCEL_DRAIN_FLOWFILES", "Cancel the ongoing drain of FlowFiles", false,
            "Connector is not currently draining FlowFiles");
    }

    private ConnectorAction createApplyUpdatesAction(final ConnectorState currentState, final boolean troubleshooting) {
        final boolean allowed;
        final String reason;

        if (troubleshooting) {
            allowed = false;
            reason = "Connector is in Troubleshooting mode";
        } else if (currentState == ConnectorState.PREPARING_FOR_UPDATE || currentState == ConnectorState.UPDATING) {
            allowed = false;
            reason = "Connector is updating";
        } else if (!hasWorkingConfigurationChanges()) {
            allowed = false;
            reason = "No pending changes";
        } else {
            allowed = true;
            reason = null;
        }

        return new StandardConnectorAction("APPLY_UPDATES", "Apply the working configuration to the active configuration", allowed, reason);
    }

    private ConnectorAction createDeleteAction(final boolean stopped, final boolean dataQueued) {
        final boolean allowed;
        final String reason;

        if (!stopped) {
            allowed = false;
            reason = "Connector must be stopped";
        } else if (dataQueued) {
            allowed = false;
            reason = "Data is queued";
        } else {
            allowed = true;
            reason = null;
        }

        return new StandardConnectorAction("DELETE", "Delete the connector", allowed, reason);
    }

    private boolean hasWorkingConfigurationChanges() {
        final FrameworkFlowContext workingContext = this.workingFlowContext;
        if (workingContext == null) {
            return false;
        }

        final ConnectorConfiguration activeConfig = activeFlowContext.getConfigurationContext().toConnectorConfiguration();
        final ConnectorConfiguration workingConfig = workingContext.getConfigurationContext().toConnectorConfiguration();
        return !Objects.equals(activeConfig, workingConfig);
    }

    @Override
    public Authorizable getParentAuthorizable() {
        return parentAuthorizable;
    }

    @Override
    public Resource getResource() {
        return ResourceFactory.getComponentResource(ResourceType.Connector, getIdentifier(), getName());
    }

    @Override
    public Optional<String> getVersionedComponentId() {
        return Optional.ofNullable(versionedComponentId.get());
    }

    @Override
    public void setVersionedComponentId(final String versionedComponentId) {
        boolean updated = false;
        while (!updated) {
            final String currentId = this.versionedComponentId.get();

            if (currentId == null) {
                updated = this.versionedComponentId.compareAndSet(null, versionedComponentId);
            } else if (currentId.equals(versionedComponentId)) {
                return;
            } else if (versionedComponentId == null) {
                updated = this.versionedComponentId.compareAndSet(currentId, null);
            } else {
                throw new IllegalStateException(this + " is already under version control");
            }
        }
    }

    private void resetValidationState() {
        validationState.set(new ValidationState(ValidationStatus.VALIDATING, Collections.emptyList()));
        validationTrigger.triggerAsync(this);
        logger.debug("Validation state has been reset for {}", this);
    }

    @Override
    public ValidationStatus getValidationStatus() {
        return validationState.get().getStatus();
    }

    @Override
    public ValidationState getValidationState() {
        return validationState.get();
    }

    @Override
    public Collection<ValidationResult> getValidationErrors() {
        final ValidationState state = validationState.get();
        return state.getValidationErrors();
    }

    @Override
    public ValidationState performValidation() {
        logger.debug("Performing validation for {}", this);
        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, getConnector().getClass(), getIdentifier())) {

            final List<ValidationResult> allResults = new ArrayList<>();

            if (unresolvedBundleValidationResult != null) {
                allResults.add(unresolvedBundleValidationResult);
            } else {
                final ConnectorValidationContext validationContext = createValidationContext(activeFlowContext);

                validateManagedFlowComponents(allResults);
                validatePropertyReferences(allResults);

                if (allResults.isEmpty()) {
                    try {
                        final List<ValidationResult> implValidationResults = getConnector().validate(activeFlowContext, validationContext);
                        allResults.addAll(implValidationResults);
                    } catch (final Exception e) {
                        allResults.add(new ValidationResult.Builder()
                            .subject("Validation Failure")
                            .valid(false)
                            .explanation("Encountered a failure while attempting to perform validation: " + e.getMessage())
                            .build());
                    }
                }
            }

            final ValidationState resultState;
            if (allResults.isEmpty()) {
                resultState = new ValidationState(ValidationStatus.VALID, Collections.emptyList());
            } else {
                // Filter out any results that are 'valid' and any results that are invalid due to the fact that a Controller Service is disabled,
                // since these will not be relevant when started.
                final List<ValidationResult> relevantResults = allResults.stream()
                    .filter(result -> !result.isValid())
                    .filter(result -> !DisabledServiceValidationResult.isMatch(result))
                    .toList();

                if (relevantResults.isEmpty()) {
                    resultState = new ValidationState(ValidationStatus.VALID, Collections.emptyList());
                } else {
                    resultState = new ValidationState(ValidationStatus.INVALID, relevantResults);
                }
            }

            validationState.set(resultState);

            if (resultState.getStatus() == ValidationStatus.VALID) {
                logger.info("Validation completed for {}. Connector is valid.", this);
            } else {
                logger.info("Validation completed for {}. Connector is invalid: {}", this,
                    resultState.getValidationErrors().stream()
                        .map(ValidationResult::getExplanation)
                        .collect(Collectors.joining("; ")));
            }

            return resultState;
        }
    }

    @Override
    public void validateComponents(final ValidationTrigger validationTrigger) {
        final ProcessGroup managedGroup = activeFlowContext.getManagedProcessGroup();

        for (final ProcessorNode processor : managedGroup.findAllProcessors()) {
            validationTrigger.trigger(processor);
        }

        for (final ControllerServiceNode service : managedGroup.findAllControllerServices()) {
            validationTrigger.trigger(service);
        }
    }

    private void validateManagedFlowComponents(final List<ValidationResult> results) {
        final ProcessGroup managedProcessGroup = activeFlowContext.getManagedProcessGroup();

        // Check for any missing / ghosted Processors
        final Set<String> missingProcessors = new HashSet<>();
        for (final ProcessorNode processor : managedProcessGroup.findAllProcessors()) {
            if (processor.isExtensionMissing()) {
                missingProcessors.add(getSimpleClassName(processor.getCanonicalClassName()) + " from NAR " + processor.getBundleCoordinate());
            }
        }

        if (!missingProcessors.isEmpty()) {
            results.add(new ValidationResult.Builder()
                .subject("Missing Processors")
                .valid(false)
                .explanation("The following processors are missing: " + missingProcessors)
                .build());
        }

        // Check for any missing / ghosted Controller Services
        final Set<String> missingControllerServices = new HashSet<>();
        for (final ControllerServiceNode controllerService : managedProcessGroup.findAllControllerServices()) {
            if (controllerService.isExtensionMissing()) {
                missingControllerServices.add(getSimpleClassName(controllerService.getCanonicalClassName()) + " from NAR " + controllerService.getBundleCoordinate());
            }
        }

        if (!missingControllerServices.isEmpty()) {
            results.add(new ValidationResult.Builder()
                .subject("Missing Controller Services")
                .valid(false)
                .explanation("The following controller services are missing: " + missingControllerServices)
                .build());
        }
    }

    private static String getSimpleClassName(final String canonicalClassName) {
        final int lastDot = canonicalClassName.lastIndexOf('.');
        return lastDot < 0 ? canonicalClassName : canonicalClassName.substring(lastDot + 1);
    }

    private void validatePropertyReferences(final List<ValidationResult> allResults) {
        final List<ConfigurationStep> configurationSteps = getConnector().getConfigurationSteps();
        final ConnectorConfiguration connectorConfiguration = activeFlowContext.getConfigurationContext().toConnectorConfiguration();

        for (final ConfigurationStep step : configurationSteps) {
            final NamedStepConfiguration namedStepConfig = connectorConfiguration.getNamedStepConfiguration(step.getName());
            if (namedStepConfig == null) {
                continue;
            }

            final StepConfiguration stepConfiguration = namedStepConfig.configuration();
            validatePropertyReferences(step, namedStepConfig.configuration(), allResults);

            // Check for invalid Secret and Asset references
            final List<SecretReference> invalidSecrets = new ArrayList<>();
            final List<AssetReference> invalidAssets = new ArrayList<>();
            // Regular validation may run frequently, so cached Secret values are used here to avoid
            // repeatedly fetching from the underlying Secret Providers on every validation cycle.
            resolvePropertyReferences(step, stepConfiguration, invalidSecrets, invalidAssets, true);
            addInvalidReferenceResults(allResults, invalidSecrets, invalidAssets);
        }
    }

    private void addInvalidReferenceResults(final List<ValidationResult> results, final List<SecretReference> invalidSecretRefs, final List<AssetReference> invalidAssetRefs) {
        for (final SecretReference invalidSecretRef : invalidSecretRefs) {
            final String secretName = invalidSecretRef.getFullyQualifiedName() != null ? invalidSecretRef.getFullyQualifiedName() : invalidSecretRef.getSecretName();
            results.add(new ValidationResult.Builder()
                .subject("Secret Reference")
                .valid(false)
                .explanation("The referenced secret [" + secretName + "] could not be found")
                .build());
        }

        for (final AssetReference invalidAssetRef : invalidAssetRefs) {
            results.add(new ValidationResult.Builder()
                .subject("Asset Reference")
                .valid(false)
                .explanation("The referenced assets [" + StringUtils.join(invalidAssetRef.getAssetIdentifiers(), ",") + "] could not be found")
                .build());
        }
    }

    private void validatePropertyReferences(final ConfigurationStep step, final StepConfiguration stepConfig, final List<ValidationResult> allResults) {
        final Map<String, ConnectorPropertyDescriptor> descriptorLookup = buildPropertyDescriptorLookup(step);
        for (final ConnectorPropertyGroup propertyGroup : step.getPropertyGroups()) {
            for (final ConnectorPropertyDescriptor descriptor : propertyGroup.getProperties()) {
                if (!isPropertyDependencySatisfied(descriptor, descriptorLookup::get, stepConfig)) {
                    continue;
                }

                final PropertyType propertyType = descriptor.getType();
                final ConnectorValueReference reference = stepConfig.getPropertyValue(descriptor.getName());

                final String subject = step.getName() + " / " + descriptor.getName();

                if (!isReferenceAllowed(reference, propertyType)) {
                    final String providedReferenceType = switch (reference.getValueType()) {
                        case ASSET_REFERENCE -> "<Asset reference>";
                        case SECRET_REFERENCE -> "<Secret reference>";
                        case STRING_LITERAL -> "<Explicit value>";
                    };

                    final String expectedReferenceType = propertyType == PropertyType.SECRET ? "a Secret reference" : "an Explicit value";

                    allResults.add(new ValidationResult.Builder()
                        .subject(subject)
                        .input(providedReferenceType)
                        .explanation("This property must be configured with " + expectedReferenceType)
                        .build());
                }
            }
        }
    }

    private boolean isReferenceAllowed(final ConnectorValueReference reference, final PropertyType propertyType) {
        // If the reference is null or its value is unset, then it is allowed
        if (reference == null) {
            return true;
        }

        switch (reference) {
            case StringLiteralValue stringLiteralValue -> {
                if (stringLiteralValue.getValue() == null) {
                    return true;
                }
            }
            case AssetReference assetReference -> {
                if (assetReference.getAssetIdentifiers() == null || assetReference.getAssetIdentifiers().isEmpty()) {
                    return true;
                }
            }
            case SecretReference secretReference -> {
                if (isEmptySecretReference(secretReference)) {
                    return true;
                }
            }
        }

        if (propertyType == PropertyType.SECRET) {
            return reference.getValueType() == ConnectorValueType.SECRET_REFERENCE;
        }

        if (propertyType == PropertyType.ASSET || propertyType == PropertyType.ASSET_LIST) {
            return reference.getValueType() == ConnectorValueType.ASSET_REFERENCE;
        }

        return reference.getValueType() != ConnectorValueType.SECRET_REFERENCE && reference.getValueType() != ConnectorValueType.ASSET_REFERENCE;
    }

    private ConnectorValidationContext createValidationContext(final FrameworkFlowContext context) {
        final DescribedValueProvider allowableValueProvider = (stepName, propertyName) ->
            fetchAllowableValues(stepName, propertyName, context);
        final ConnectorConfiguration connectorConfiguration = context.getConfigurationContext().toConnectorConfiguration();
        return new StandardConnectorValidationContext(connectorConfiguration, allowableValueProvider, context.getParameterContext());
    }

    private List<DescribedValue> fetchAllowableValues(final String stepName, final String propertyName, final FlowContext context) {
        final List<DescribedValue> allowableValues;
        try (NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, getConnector().getClass(), getIdentifier())) {
            allowableValues = getConnector().fetchAllowableValues(stepName, propertyName, context);
        }

        if (allowableValues == null || allowableValues.isEmpty()) {
            return Collections.emptyList();
        }

        return allowableValues;
    }

    @Override
    public boolean equals(final Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final StandardConnectorNode that = (StandardConnectorNode) o;
        return Objects.equals(identifier, that.identifier);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(identifier);
    }

    @Override
    public String toString() {
        return "StandardConnectorNode[id=" + identifier + ", name=" + name + ", state=" + stateTransition.getCurrentState() + "]";
    }
}
