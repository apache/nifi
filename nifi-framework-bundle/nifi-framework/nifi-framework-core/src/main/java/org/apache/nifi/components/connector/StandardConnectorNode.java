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
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.ConfigVerificationResult;
import org.apache.nifi.components.ConfigVerificationResult.Outcome;
import org.apache.nifi.components.DescribedValue;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.connector.components.FlowContext;
import org.apache.nifi.components.connector.components.ParameterContextFacade;
import org.apache.nifi.components.validation.DisabledServiceValidationResult;
import org.apache.nifi.components.validation.ValidationState;
import org.apache.nifi.components.validation.ValidationStatus;
import org.apache.nifi.connectable.FlowFileActivity;
import org.apache.nifi.connectable.FlowFileTransferCounts;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.queue.DropFlowFileStatus;
import org.apache.nifi.controller.queue.QueueSize;
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
import org.apache.nifi.logging.ComponentLog;
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
import java.util.stream.Collectors;

public class StandardConnectorNode implements ConnectorNode {
    private static final Logger logger = LoggerFactory.getLogger(StandardConnectorNode.class);

    private final String identifier;
    private final FlowManager flowManager;
    private final ExtensionManager extensionManager;
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

    private volatile FrameworkFlowContext workingFlowContext;

    private volatile String name;
    private volatile FrameworkConnectorInitializationContext initializationContext;


    public StandardConnectorNode(final String identifier, final FlowManager flowManager, final ExtensionManager extensionManager,
        final Authorizable parentAuthorizable, final ConnectorDetails connectorDetails, final String componentType, final String componentCanonicalClass,
        final MutableConnectorConfigurationContext configurationContext,
        final ConnectorStateTransition stateTransition, final FlowContextFactory flowContextFactory,
        final ConnectorValidationTrigger validationTrigger, final boolean extensionMissing) {

        this.identifier = identifier;
        this.flowManager = flowManager;
        this.extensionManager = extensionManager;
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
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setName(final String name) {
        this.name = name;
    }

    @Override
    public void transitionStateForUpdating() {
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

        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, getConnector().getClass(), getIdentifier())) {
            getConnector().prepareForUpdate(workingFlowContext, activeFlowContext);
            stateTransition.setCurrentState(ConnectorState.UPDATING);
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
    public void inheritConfiguration(final List<VersionedConfigurationStep> activeConfig, final List<VersionedConfigurationStep> workingConfig,
                final Bundle flowContextBundle) throws FlowUpdateException {

        final MutableConnectorConfigurationContext configurationContext = createConfigurationContext(activeConfig);
        final FrameworkFlowContext inheritContext = flowContextFactory.createWorkingFlowContext(identifier,
            connectorDetails.getComponentLog(), configurationContext, flowContextBundle);

        // Apply the update for the active config
        applyUpdate(inheritContext);

        // Configure the working config but do not apply
        for (final VersionedConfigurationStep step : workingConfig) {
            final StepConfiguration stepConfig = createStepConfiguration(step);
            setConfiguration(step.getName(), stepConfig);
        }
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
    }

    @Override
    public void setConfiguration(final String stepName, final StepConfiguration configuration) throws FlowUpdateException {
        // Update properties and check if the configuration changed.
        final ConfigurationUpdateResult updateResult = workingFlowContext.getConfigurationContext().setProperties(stepName, configuration);
        if (updateResult == ConfigurationUpdateResult.NO_CHANGES) {
            return;
        }

        // If there were changes, trigger Processor to be notified of the change.
        final Connector connector = connectorDetails.getConnector();
        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, connector.getClass(), getIdentifier())) {
            logger.debug("Notifying {} of configuration change for configuration step {}", this, stepName);
            connector.onConfigurationStepConfigured(stepName, workingFlowContext);
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
            verifyCanStart();

            stateTransition.setDesiredState(ConnectorState.RUNNING);
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
        requireStopped("drain FlowFiles", ConnectorState.DRAINING);

        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, connectorDetails.getConnector().getClass(), getIdentifier())) {
            getComponentLog().info("Draining FlowFiles from {}", this);
            final CompletableFuture<Void> drainFuture = connectorDetails.getConnector().drainFlowFiles(activeFlowContext);
            drainFutureRef.set(drainFuture);

            final CompletableFuture<Void> stateUpdateFuture = drainFuture.whenComplete((result, failureCause) -> {
                drainFutureRef.set(null);
                logger.info("Successfully drained FlowFiles from {}; ensuring all components are stopped.", this);

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
    public Future<Void> purgeFlowFiles(final String requestor) {
        requireStopped("purge FlowFiles", ConnectorState.PURGING);

        try {
            final String dropRequestId = UUID.randomUUID().toString();
            final DropFlowFileStatus status = activeFlowContext.getManagedProcessGroup().dropAllFlowFiles(dropRequestId, requestor);
            final CompletableFuture<Void> future = status.getCompletionFuture();
            final CompletableFuture<Void> stateUpdateFuture = future.whenComplete((result, failureCause) -> stateTransition.setCurrentState(ConnectorState.STOPPED));
            return stateUpdateFuture;
        } catch (final Throwable t) {
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
                throw new IllegalStateException("Cannot " + action + " for " + this + " because its current state is currently " + currentState + "; it must be STOPPED.");
            }

            stateUpdated = stateTransition.trySetCurrentState(ConnectorState.STOPPED, newState);
        }
    }

    private void stopComponent(final FlowEngine scheduler, final CompletableFuture<Void> stopCompleteFuture) {
        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, connectorDetails.getConnector().getClass(), getIdentifier())) {
            connectorDetails.getConnector().stop(activeFlowContext);
        } catch (final Exception e) {
            logger.error("Failed to stop {}. Will try again in 10 seconds", this, e);
            scheduler.schedule(() -> stopComponent(scheduler, stopCompleteFuture), 10, TimeUnit.SECONDS);
            return;
        }

        stateTransition.setCurrentState(ConnectorState.STOPPED);
        stopCompleteFuture.complete(null);

        final ConnectorState desiredState = getDesiredState();
        if (desiredState == ConnectorState.RUNNING) {
            logger.info("{} was requested to be RUNNING while it was stopping so will attempt to start again", this);
            start(scheduler, new CompletableFuture<>());
        }
    }

    private void startComponent(final ScheduledExecutorService scheduler, final CompletableFuture<Void> startCompleteFuture) {
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
    }


    @Override
    public void verifyCanDelete() {
        final QueueSize queueSize = getActiveFlowContext().getManagedProcessGroup().getQueueSize();
        if (queueSize.getObjectCount() > 0) {
            throw new IllegalStateException("Cannot delete " + this + " because its Process Group has " + queueSize.getObjectCount()
                + " FlowFiles queued; all FlowFiles must be removed before it can be deleted.");
        }

        final ConnectorState currentState = getCurrentState();
        if (currentState == ConnectorState.STOPPED || currentState == ConnectorState.UPDATE_FAILED || currentState == ConnectorState.UPDATED) {
            return;
        }

        throw new IllegalStateException("Cannot delete " + this + " because its state is currently " + currentState + "; it must be stopped before it can be deleted.");
    }

    @Override
    public void verifyCanStart() {
        final ValidationState state = performValidation();
        if (state.getStatus() != ValidationStatus.VALID) {
            throw new IllegalStateException("Cannot start " + this + " because it is not valid: " + state.getValidationErrors());
        }
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
    public List<AllowableValue> fetchAllowableValues(final String stepName, final String propertyName) {
        if (workingFlowContext == null) {
            throw new IllegalStateException("Cannot fetch Allowable Values for %s.%s because %s is not being updated.".formatted(
                stepName, propertyName, this));
        }

        try (NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, getConnector().getClass(), getIdentifier())) {
            return getConnector().fetchAllowableValues(stepName, propertyName, workingFlowContext);
        }
    }

    @Override
    public List<AllowableValue> fetchAllowableValues(final String stepName, final String propertyName, final String filter) {
        if (workingFlowContext == null) {
            throw new IllegalStateException("Cannot fetch Allowable Values for %s.%s because %s is not being updated.".formatted(
                stepName, propertyName, this));
        }

        try (NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, getConnector().getClass(), getIdentifier())) {
            return getConnector().fetchAllowableValues(stepName, propertyName, workingFlowContext, filter);
        }
    }

    @Override
    public void initializeConnector(final FrameworkConnectorInitializationContext initializationContext) {
        this.initializationContext = initializationContext;

        try (NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, getConnector().getClass(), getIdentifier())) {
            getConnector().initialize(initializationContext);
        }

        recreateWorkingFlowContext();
    }

    @Override
    public void loadInitialFlow() throws FlowUpdateException {
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
            logger.info("Loading initial flow for {}", this);
            // Update all RUNNING components to ENABLED before applying the initial flow so that components
            // are not started before being configured.
            stopComponents(initialFlow.getFlowContents());
            initializationContext.updateFlow(activeFlowContext, initialFlow);
        }

        resetValidationState();
        recreateWorkingFlowContext();
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

    private void recreateWorkingFlowContext() {
        destroyWorkingContext();
        workingFlowContext = flowContextFactory.createWorkingFlowContext(identifier,
            connectorDetails.getComponentLog(), activeFlowContext.getConfigurationContext(), activeFlowContext.getBundle());

        getComponentLog().info("Working Flow Context has been recreated");
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
        final List<SecretReference> invalidSecretRefs = new ArrayList<>();
        final List<AssetReference> invalidAssetRefs = new ArrayList<>();
        final Map<String, String> resolvedPropertyOverrides = resolvePropertyReferences(configurationOverrides, invalidSecretRefs, invalidAssetRefs);

        final List<ConfigVerificationResult> results = new ArrayList<>();
        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, getConnector().getClass(), getIdentifier())) {

            final DescribedValueProvider allowableValueProvider = (step, propertyName) -> fetchAllowableValues(step, propertyName, workingFlowContext);

            final MutableConnectorConfigurationContext configContext = workingFlowContext.getConfigurationContext().createWithOverrides(stepName, resolvedPropertyOverrides);
            final ConnectorConfiguration connectorConfig = configContext.toConnectorConfiguration();
            final ParameterContextFacade paramContext = workingFlowContext.getParameterContext();
            final ConnectorValidationContext validationContext = new StandardConnectorValidationContext(connectorConfig, allowableValueProvider, paramContext);

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

    private Map<String, String> resolvePropertyReferences(final StepConfiguration configurationOverrides, final List<SecretReference> invalidSecretRefs,
                                                          final List<AssetReference> invalidAssetRefs) {

        final Map<String, String> resolvedProperties = new HashMap<>();

        try {
            // Secret References can be expensive to lookup so we don't want to call getSecret() for each one. Instead, we
            // want to find all Secrets by Provider and then call fetchSecrets() once per Provider.
            final Set<SecretReference> secretReferences = configurationOverrides.getPropertyValues().values().stream()
                .filter(Objects::nonNull)
                .filter(ref -> ref.getValueType() == ConnectorValueType.SECRET_REFERENCE)
                .map(ref -> (SecretReference) ref)
                .collect(Collectors.toSet());

            final Map<SecretReference, Secret> secretsByReference = initializationContext.getSecretsManager().getSecrets(secretReferences);
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

                // We've already looked up secrets above, so use the cached value here.
                if (valueReference.getValueType() == ConnectorValueType.SECRET_REFERENCE) {
                    final SecretReference secretReference = (SecretReference) valueReference;
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

            return results;
        }

        try (NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, getConnector().getClass(), getIdentifier())) {
            results.addAll(getConnector().verify(workingFlowContext));
        }

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
        recreateWorkingFlowContext();
    }

    @Override
    public List<ConnectorAction> getAvailableActions() {
        final List<ConnectorAction> actions = new ArrayList<>();
        final ConnectorState currentState = getCurrentState();
        final boolean dataQueued = activeFlowContext.getManagedProcessGroup().isDataQueued();
        final boolean stopped = isStopped();

        actions.add(createStartAction(stopped));
        actions.add(createStopAction(currentState));
        actions.add(createConfigureAction());
        actions.add(createDiscardWorkingConfigAction());
        actions.add(createPurgeFlowFilesAction(stopped, dataQueued));
        actions.add(createDrainFlowFilesAction(stopped, dataQueued));
        actions.add(createCancelDrainFlowFilesAction(currentState == ConnectorState.DRAINING));
        actions.add(createApplyUpdatesAction(currentState));
        actions.add(createDeleteAction(stopped, dataQueued));

        return actions;
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

    private ConnectorAction createStartAction(final boolean stopped) {
        final boolean allowed;
        final String reason;

        if (!stopped) {
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
        if (currentState == ConnectorState.RUNNING || currentState == ConnectorState.STARTING) {
            allowed = true;
        } else if (currentState == ConnectorState.UPDATED || currentState == ConnectorState.UPDATE_FAILED) {
            allowed = hasActiveThread(activeFlowContext.getManagedProcessGroup());
        } else {
            allowed = false;
        }

        final String reason = allowed ? null : "Connector is not running";
        return new StandardConnectorAction("STOP", "Stop the connector", allowed, reason);
    }

    private ConnectorAction createConfigureAction() {
        return new StandardConnectorAction("CONFIGURE", "Configure the connector", true, null);
    }

    private ConnectorAction createDiscardWorkingConfigAction() {
        final boolean allowed = hasWorkingConfigurationChanges();
        final String reason = allowed ? null : "No pending changes to discard";

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

    private ConnectorAction createApplyUpdatesAction(final ConnectorState currentState) {
        final boolean allowed;
        final String reason;

        if (currentState == ConnectorState.PREPARING_FOR_UPDATE || currentState == ConnectorState.UPDATING) {
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
        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, getConnector().getClass(), getIdentifier())) {

            final ConnectorValidationContext validationContext = createValidationContext(activeFlowContext);

            final List<ValidationResult> allResults = new ArrayList<>();
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
            return resultState;
        }
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
            resolvePropertyReferences(stepConfiguration, invalidSecrets, invalidAssets);
            addInvalidReferenceResults(allResults, invalidSecrets, invalidAssets);
        }
    }

    private void addInvalidReferenceResults(final List<ValidationResult> results, final List<SecretReference> invalidSecretRefs, final List<AssetReference> invalidAssetRefs) {
        for (final SecretReference invalidSecretRef : invalidSecretRefs) {
            results.add(new ValidationResult.Builder()
                .subject("Secret Reference")
                .valid(false)
                .explanation("The referenced secret [" + invalidSecretRef.getFullyQualifiedName() + "] could not be found")
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
        for (final ConnectorPropertyGroup propertyGroup : step.getPropertyGroups()) {
            for (final ConnectorPropertyDescriptor descriptor : propertyGroup.getProperties()) {
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
                if (secretReference.getSecretName() == null) {
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
        final List<AllowableValue> allowableValues;
        try (NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, getConnector().getClass(), getIdentifier())) {
            allowableValues = getConnector().fetchAllowableValues(stepName, propertyName, activeFlowContext);
        }

        if (allowableValues == null || allowableValues.isEmpty()) {
            return Collections.emptyList();
        }

        return allowableValues.stream()
            .map(av -> (DescribedValue) av)
            .toList();
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
