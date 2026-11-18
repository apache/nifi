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

import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.VersionedExternalFlow;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.logging.ComponentLog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

/**
 * The flow evolution state of one Connector: the authoritative snapshot that records the outgoing flow, the progress through
 * the evolve-drain phase, and the current evolution attempt. The evolution lock guards every change to the snapshot and the
 * attempt, and every Connector state transition that an evolution makes. The progress and the snapshot may be read without it.
 */
final class ConnectorFlowEvolution {
    private static final Logger logger = LoggerFactory.getLogger(ConnectorFlowEvolution.class);

    /**
     * The parts of the Connector that a flow evolution reads or calls.
     */
    interface EvolvingConnector {
        boolean isExtensionMissing();

        /**
         * @return the bundle of the loaded Connector, or null when it is not known
         */
        Bundle getLoadedBundle();

        FrameworkFlowContext getActiveFlowContext();

        ComponentLog getComponentLog();

        ParameterSecretReferences.Resolution resolveParameterSecrets(VersionedParameterContext parameterContext, Map<String, SecretReference> parameterSecretReferences);

        void setUnresolvedParameterSecretReferences(Map<String, SecretReference> unresolvedReferences);

        /**
         * @return a validation result for each managed component whose extension is missing
         */
        List<ValidationResult> validateManagedFlowComponents();

        CompletableFuture<Void> invokeFlowEvolutionHook(FlowEvolutionContext context) throws FlowUpdateException;

        /**
         * @return the flow that the upgraded Connector builds from its working configuration
         */
        VersionedExternalFlow getUpgradedFlow() throws Exception;

        ConnectorFlowSnapshot captureSnapshot();
    }

    private final EvolvingConnector connector;
    private final ConnectorStateTransition stateTransition;
    private final Object lock = new Object();
    private final AtomicReference<FlowEvolutionProgress> progress = new AtomicReference<>(FlowEvolutionProgress.NONE);
    private volatile ConnectorFlowSnapshot authoritativeSnapshot;
    // Written only while holding the lock
    private volatile FlowEvolutionAttempt latestAttempt;

    ConnectorFlowEvolution(final EvolvingConnector connector, final ConnectorStateTransition stateTransition) {
        this.connector = Objects.requireNonNull(connector, "Evolving Connector is required");
        this.stateTransition = Objects.requireNonNull(stateTransition, "State Transition is required");
    }

    boolean isPending() {
        synchronized (lock) {
            if (connector.isExtensionMissing()) {
                return false;
            }

            final ConnectorFlowSnapshot snapshot = authoritativeSnapshot;
            final Bundle snapshotBundle = snapshot == null ? null : snapshot.bundle();
            return snapshotBundle != null && !sameBundle(snapshotBundle, connector.getLoadedBundle());
        }
    }

    FlowEvolutionStatus getReportedStatus() {
        return progress.get().reportedStatus();
    }

    boolean isDrainComplete() {
        return progress.get().isDrainComplete();
    }

    boolean isApplyingEvolvedFlow() {
        return getReportedStatus() == FlowEvolutionStatus.EVOLVE_DRAIN_COMPLETE;
    }

    boolean hasFailed() {
        return progress.get().hasFailed();
    }

    /**
     * @return the explanation of the current failure, or null when the evolution has not failed
     */
    String getFailureExplanation() {
        return progress.get().failureExplanation();
    }

    ConnectorFlowSnapshot getSnapshot() {
        return authoritativeSnapshot;
    }

    private VersionedProcessGroup getSnapshotFlow() {
        final ConnectorFlowSnapshot snapshot = authoritativeSnapshot;
        return snapshot == null ? null : snapshot.flow();
    }

    /**
     * Moves a pending upgrade from STOPPED or UPDATE_FAILED to DRAINING and opens a new attempt.
     *
     * @param scheduler lifecycle scheduler used to keep stopping the outgoing flow when the attempt fails and cannot stop it right away
     */
    void prepare(final FlowEngine scheduler) {
        Objects.requireNonNull(scheduler, "Scheduler is required");
        synchronized (lock) {
            if (!isPending()) {
                throw new IllegalStateException("Cannot prepare flow evolution for " + connector + " because no evolution is pending");
            }
            // Hook progress outlives an attempt only once every node has drained, so the outgoing flow is no longer needed.
            final boolean drainCompletedInEarlierAttempt = isDrainComplete();
            if (!stateTransition.trySetCurrentState(ConnectorState.STOPPED, ConnectorState.DRAINING)
                    && !stateTransition.trySetCurrentState(ConnectorState.UPDATE_FAILED, ConnectorState.DRAINING)) {
                throw new IllegalStateException("Cannot prepare flow evolution for " + connector + " from state " + stateTransition.getCurrentState());
            }

            progress.set(new FlowEvolutionProgress(drainCompletedInEarlierAttempt
                    ? FlowEvolutionStatus.EVOLVE_DRAIN_COMPLETE
                    : FlowEvolutionStatus.EVOLVE_DRAIN_PENDING, null));
            final Bundle postUpgradeBundle = connector.getLoadedBundle();
            final ConnectorFlowSnapshot outgoingSnapshot = authoritativeSnapshot;
            final Bundle preUpgradeBundle = outgoingSnapshot == null || outgoingSnapshot.bundle() == null ? postUpgradeBundle : outgoingSnapshot.bundle();
            final PreUpgradeFlowContext preUpgradeFlowContext = new PreUpgradeFlowContext(connector.getActiveFlowContext(), preUpgradeBundle);
            final StandardFlowEvolutionContext context = new StandardFlowEvolutionContext(preUpgradeFlowContext, postUpgradeBundle);
            latestAttempt = new FlowEvolutionAttempt(context, drainCompletedInEarlierAttempt, scheduler);
            // Only a start request begins an evolution, so the connector is desired RUNNING while it drains and once the
            // upgraded flow is applied. A node that joins while this connector is draining inherits the flow mapped from the
            // desired state, so it also starts and enters the same evolution.
            stateTransition.setDesiredState(ConnectorState.RUNNING);
        }
    }

    boolean isInProgress() {
        synchronized (lock) {
            // Desired state cannot say this. stop() sets the desired state to STOPPED before it aborts the evolution.
            // The drain status cannot say this either, because a pending evolution reports EVOLVE_DRAIN_PENDING before its attempt
            // begins, while a user-requested drain may also be running.
            final StandardFlowEvolutionContext context = currentContext();
            return stateTransition.getCurrentState() == ConnectorState.DRAINING
                    && context != null
                    && !context.isFailed()
                    && !hasFailed();
        }
    }

    /**
     * Cancels the evolution in progress, if there is one.
     *
     * @param whileInProgress runs while holding the evolution lock, only when an evolution is in progress and before it is cancelled
     * @return whether an evolution was in progress
     */
    boolean cancelInProgress(final Runnable whileInProgress) {
        synchronized (lock) {
            if (!isInProgress()) {
                return false;
            }

            whileInProgress.run();
        }

        // Recorded before cancelling, because the coordinator wakes on the cancellation and must see the failure to stop the Connector
        publishFailure("Flow evolution was cancelled", null);
        closeCurrentAttempt();
        return true;
    }

    /**
     * Runs preflight, the Connector evolution hook, and the local evolve-drain phase on the provided executor.
     *
     * @return the future for the local evolve-drain phase of the prepared attempt
     */
    Future<Void> evolve(final ExecutorService executor) throws FlowUpdateException {
        Objects.requireNonNull(executor, "Executor Service is required");

        synchronized (lock) {
            final StandardFlowEvolutionContext context = currentContext();
            verifyContext(context);
            final FlowEvolutionAttempt attempt = latestAttempt;
            if (attempt.evolveFuture != null) {
                return attempt.evolveFuture;
            }

            attempt.evolveFuture = executor.submit(() -> {
                runEvolveDrainPhase(context);
                return null;
            });
            return attempt.evolveFuture;
        }
    }

    private void runEvolveDrainPhase(final StandardFlowEvolutionContext context) throws FlowUpdateException {
        runPreflight(context);
        if (isDrainComplete()) {
            return;
        }

        verifyContext(context);
        final CompletableFuture<Void> connectorFuture;
        try {
            connectorFuture = Objects.requireNonNull(connector.invokeFlowEvolutionHook(context), "Connector prepareForFlowEvolution must return a CompletableFuture");
        } catch (final FlowUpdateException e) {
            fail(context, e.getMessage(), e);
            throw e;
        }

        try {
            synchronized (lock) {
                verifyContext(context);
                latestAttempt.prepareFuture = connectorFuture;
            }
        } catch (final FlowUpdateException e) {
            connectorFuture.cancel(true);
            throw e;
        }

        try {
            connectorFuture.get();
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FlowUpdateException("Interrupted while waiting for flow evolution hook for " + connector, e);
        } catch (final CancellationException e) {
            throw new FlowUpdateException("Flow evolution hook was cancelled for " + connector, e);
        } catch (final ExecutionException e) {
            final Throwable cause = e.getCause() == null ? e : e.getCause();
            fail(context, cause.getMessage(), cause);
            throw new FlowUpdateException("Flow evolution hook failed for " + connector, cause);
        }

        completeEvolveDrainPhase(context);
    }

    private void runPreflight(final StandardFlowEvolutionContext context) throws FlowUpdateException {
        verifyContext(context);
        if (!isDrainComplete()) {
            refreshPreUpgradeParameterSecrets(context);
        }

        final List<ValidationResult> ghostResults = connector.validateManagedFlowComponents();
        if (!ghostResults.isEmpty()) {
            final String explanation = ghostResults.getFirst().getExplanation();
            fail(context, explanation, null);
            throw new FlowUpdateException(explanation);
        }

        verifyContext(context);
    }

    private void refreshPreUpgradeParameterSecrets(final StandardFlowEvolutionContext context) throws FlowUpdateException {
        final ConnectorFlowSnapshot snapshot = authoritativeSnapshot;
        if (snapshot == null || snapshot.parameterContext() == null) {
            verifyContext(context);
            connector.setUnresolvedParameterSecretReferences(Map.of());
            return;
        }

        final ParameterSecretReferences.Resolution resolution = connector.resolveParameterSecrets(snapshot.parameterContext(), snapshot.parameterSecretReferences());
        if (!resolution.unresolvedReferences().isEmpty()) {
            final String explanation = explainUnresolvedSecrets(resolution);
            verifyContext(context);
            connector.setUnresolvedParameterSecretReferences(Map.copyOf(resolution.unresolvedReferences()));
            fail(context, explanation, null);
            throw new FlowUpdateException(explanation);
        }

        verifyContext(context);
        connector.setUnresolvedParameterSecretReferences(Map.of());
        if (!resolution.parameterValues().isEmpty()) {
            connector.getActiveFlowContext().getParameterContext().updateParameters(resolution.parameterValues());
        }
    }

    private static String explainUnresolvedSecrets(final ParameterSecretReferences.Resolution resolution) {
        if (resolution.secretsProviderFailure() != null) {
            return "Flow evolution cannot start: the secrets provider could not be reached: %s. Restore access to the provider, then start the Connector to resume the upgrade."
                .formatted(resolution.secretsProviderFailure());
        }

        final Map.Entry<String, SecretReference> unresolved = resolution.unresolvedReferences().entrySet().stream()
            .sorted(Map.Entry.comparingByKey())
            .findFirst()
            .orElseThrow();
        final SecretReference reference = unresolved.getValue();
        final String secretName = reference.getFullyQualifiedName() == null ? reference.getSecretName() : reference.getFullyQualifiedName();
        return "Flow evolution cannot start: secret [%s] used by parameter [%s] could not be resolved. Restore the secret in its provider, then start the Connector to resume the upgrade."
            .formatted(secretName, unresolved.getKey());
    }

    private void completeEvolveDrainPhase(final StandardFlowEvolutionContext context) throws FlowUpdateException {
        // The hook may start processors so queues can empty. Those processors must be stopped before the upgraded
        // flow is verified, so no FlowFile reaches a connection the upgraded flow removes after it has been checked.
        verifyContext(context);
        if (!stopPreUpgradeFlow()) {
            fail(context, "Failed to stop the pre-upgrade flow", null);
            throw new FlowUpdateException("Failed to stop the pre-upgrade flow for " + connector);
        }

        verifyUpgradedFlow(context);

        synchronized (lock) {
            verifyContext(context);
            progress.updateAndGet(current -> current.withDrainStatus(FlowEvolutionStatus.EVOLVE_DRAIN_COMPLETE));
        }
    }

    /**
     * Fails the evolution if the upgraded flow cannot replace the active flow, for example because it removes a
     * connection that still holds FlowFiles.
     */
    private void verifyUpgradedFlow(final StandardFlowEvolutionContext context) throws FlowUpdateException {
        verifyContext(context);
        final VersionedExternalFlow proposedFlow;
        try {
            proposedFlow = connector.getUpgradedFlow();
        } catch (final Exception failure) {
            fail(context, failure.getMessage(), failure);
            throw new FlowUpdateException("Failed to determine the upgraded flow for " + connector, failure);
        }
        if (proposedFlow == null || proposedFlow.getFlowContents() == null) {
            fail(context, "Upgraded flow was not returned", null);
            throw new FlowUpdateException("Upgraded flow was not returned for " + connector);
        }

        verifyContext(context);
        try {
            connector.getActiveFlowContext().verifyUpdateFlow(proposedFlow);
        } catch (final FlowUpdateException e) {
            fail(context, e.getMessage(), e);
            throw e;
        }
    }

    private void verifyContext(final StandardFlowEvolutionContext context) throws FlowUpdateException {
        if (currentContext() != context || context == null || context.isFailed() || hasFailed() || stateTransition.getCurrentState() != ConnectorState.DRAINING) {
            throw new FlowUpdateException("Flow evolution was cancelled for " + connector);
        }
    }

    /**
     * @return the context of the current or most recent attempt, or null when no attempt has begun while the evolution is pending
     */
    private StandardFlowEvolutionContext currentContext() {
        final FlowEvolutionAttempt attempt = latestAttempt;
        return attempt == null ? null : attempt.context;
    }

    /**
     * {@link Connector#applyUpdate} must be safe to run again on a flow it has already partly updated, because a start
     * after a failed apply resumes after the hook and applies again.
     */
    void beginEvolvedFlowUpdate() throws FlowUpdateException {
        // Checked again after the drain barrier because an attempt that resumes after the hook skips the evolve-drain phase, and the
        // persisted flow is replaced once the upgraded flow is applied.
        verifyUpgradedFlow(currentContext());

        synchronized (lock) {
            if (!isInProgress() || !isDrainComplete()) {
                throw new FlowUpdateException("Cannot apply the upgraded flow for " + connector + " because the evolve-drain phase is not complete");
            }

            // Leaving DRAINING closes the evolution attempt, and prepareForUpdate follows as for any configuration update.
            // The status stays EVOLVE_DRAIN_COMPLETE until the upgraded flow is applied so a peer still waiting on the drain
            // barrier passes it. The desired state stays RUNNING, because only a start request begins an evolution.
            stateTransition.setCurrentState(ConnectorState.PREPARING_FOR_UPDATE);
        }
    }

    /**
     * Marks this attempt failed and cancels its hook after releasing the evolution lock. The hook future is copied
     * under the lock so a newer attempt is not cancelled. This does not cancel the evolve-drain task, because it runs
     * on that task and cancelling it would interrupt the task before it stops the pre-upgrade flow.
     *
     * @param expectedContext the context of the attempt to close
     * @return false when the context is already closed or has been replaced
     */
    private boolean closeAttempt(final StandardFlowEvolutionContext expectedContext) {
        final CompletableFuture<Void> prepareFuture;
        synchronized (lock) {
            final FlowEvolutionAttempt attempt = latestAttempt;
            if (attempt == null || attempt.context.isFailed() || attempt.context != expectedContext) {
                return false;
            }

            attempt.context.fail();
            prepareFuture = attempt.prepareFuture;
        }

        cancel(prepareFuture);
        return true;
    }

    /**
     * Marks the current attempt failed and cancels its hook and evolve-drain task. The futures are copied under the
     * evolution lock so a newer attempt started after this close is not cancelled.
     */
    private void closeCurrentAttempt() {
        final CompletableFuture<Void> prepareFuture;
        final Future<Void> evolveDrainTask;
        synchronized (lock) {
            final FlowEvolutionAttempt attempt = latestAttempt;
            if (attempt == null || attempt.context.isFailed()) {
                return;
            }

            attempt.context.fail();
            prepareFuture = attempt.prepareFuture;
            evolveDrainTask = attempt.evolveFuture;
        }

        cancel(evolveDrainTask);
        cancel(prepareFuture);
    }

    private static void cancel(final Future<?> future) {
        if (future != null) {
            future.cancel(true);
        }
    }

    private void fail(final StandardFlowEvolutionContext context, final String explanation, final Throwable cause) {
        if (!closeAttempt(context)) {
            return;
        }

        finishFailure(explanation, cause);
    }

    /**
     * Fails the current attempt, or finishes a failure that was already recorded, and stops the outgoing flow.
     */
    void fail(final String explanation, final Throwable cause) {
        closeCurrentAttempt();
        finishFailure(explanation, cause);
    }

    private void finishFailure(final String explanation, final Throwable cause) {
        if (!isDrainComplete() && !stopPreUpgradeFlow()) {
            publishFailure(explanation, cause);
            synchronized (lock) {
                stateTransition.setDesiredState(ConnectorState.STOPPED);
                stateTransition.setCurrentState(ConnectorState.STOPPING);
            }
            schedulePreUpgradeStopRetry();
            return;
        }

        publishFailure(explanation, cause);
        synchronized (lock) {
            stateTransition.setDesiredState(ConnectorState.STOPPED);
            stateTransition.setCurrentState(ConnectorState.STOPPED);
            // A peer may not have drained yet, so the next attempt drains again. An attempt that started after this
            // node's drain had already completed keeps that progress, because the drain barrier may already have passed
            // and the upgraded flow may have been partly applied.
            final FlowEvolutionAttempt attempt = latestAttempt;
            if (attempt == null || !attempt.drainCompletedInEarlierAttempt) {
                progress.updateAndGet(current -> current.replaceDrainStatus(FlowEvolutionStatus.EVOLVE_DRAIN_COMPLETE, FlowEvolutionStatus.EVOLVE_DRAIN_PENDING));
            }
        }
    }

    private void publishFailure(final String explanation, final Throwable cause) {
        final String concreteExplanation = explanation == null || explanation.isBlank()
                ? cause == null ? "Unknown flow evolution failure" : cause.getClass().getSimpleName()
                : explanation;
        final FlowEvolutionProgress previous = progress.getAndUpdate(current -> current.withFailureIfAbsent(concreteExplanation));
        if (!previous.hasFailed()) {
            connector.getComponentLog().error("Flow evolution failed: {}", concreteExplanation, cause);
        }
    }

    /**
     * Keeps stopping the pre-upgrade flow after {@link #finishFailure} left the connector STOPPING, using the scheduler of
     * the failed attempt. Only one retry is scheduled per attempt, although a failure may be finished more than once.
     */
    private void schedulePreUpgradeStopRetry() {
        final FlowEngine scheduler;
        synchronized (lock) {
            final FlowEvolutionAttempt attempt = latestAttempt;
            if (attempt == null || attempt.preUpgradeStopRetryScheduled) {
                return;
            }

            attempt.preUpgradeStopRetryScheduled = true;
            scheduler = attempt.scheduler;
        }

        scheduler.schedule(() -> retryPreUpgradeStop(scheduler, 0), 0, TimeUnit.SECONDS);
    }

    /**
     * Completes pending stop futures when the stop succeeds. The Connector is not started again, because the upgrade
     * is still pending and only the Connector Repository may start it.
     */
    private void retryPreUpgradeStop(final FlowEngine scheduler, final int attempt) {
        if (stateTransition.getCurrentState() != ConnectorState.STOPPING) {
            return;
        }

        if (stopPreUpgradeFlow()) {
            synchronized (lock) {
                if (stateTransition.getCurrentState() != ConnectorState.STOPPING) {
                    return;
                }
                stateTransition.setCurrentState(ConnectorState.STOPPED);
            }
            logger.info("Successfully stopped the pre-upgrade flow for {} after a failed evolution", connector);
            return;
        }

        final long delaySeconds = Math.min(60L, 1L << Math.min(attempt, 5));
        logger.warn("Failed to stop the pre-upgrade flow for {}. Will try again in {} seconds", connector, delaySeconds);
        scheduler.schedule(() -> retryPreUpgradeStop(scheduler, attempt + 1), delaySeconds, TimeUnit.SECONDS);
    }

    /**
     * @return whether the pre-upgrade flow stopped
     */
    private boolean stopPreUpgradeFlow() {
        try {
            final CompletableFuture<Void> stopped = connector.getActiveFlowContext().getRootGroup().getLifecycle().stop();
            if (stopped != null) {
                stopped.get(1, TimeUnit.MINUTES);
            }
            return true;
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("Interrupted while stopping the pre-upgrade flow for {}", connector, e);
            return false;
        } catch (final Exception e) {
            logger.warn("Failed to stop the pre-upgrade flow for {}", connector, e);
            return false;
        }
    }

    /**
     * Captures the live flow outside the evolution lock, because capturing resolves secrets, then installs it under the lock
     * after checking again that it may be installed.
     */
    void captureSnapshot() {
        verifyCanCaptureSnapshot();
        final ConnectorFlowSnapshot snapshot = connector.captureSnapshot();

        synchronized (lock) {
            verifyCanCaptureSnapshot();
            installSnapshot(snapshot);
        }
    }

    /**
     * Captures the live flow when there is no snapshot, so that a later upgrade has a pre-upgrade flow to evolve.
     */
    void ensureSnapshot() {
        if (getSnapshotFlow() != null || connector.isExtensionMissing()) {
            return;
        }

        verifyCanCaptureSnapshot();
        final ConnectorFlowSnapshot snapshot = connector.captureSnapshot();

        synchronized (lock) {
            // Another thread may have installed a snapshot, such as the pre-upgrade flow, while the live flow was being captured
            if (getSnapshotFlow() != null) {
                return;
            }

            verifyCanCaptureSnapshot();
            installSnapshot(snapshot);
        }
    }

    private void verifyCanCaptureSnapshot() {
        // Applying the upgraded flow after the evolve-drain phase records the new bundle, which is what ends the pending evolution.
        if (isPending() && !isDrainComplete()) {
            throw new IllegalStateException("Cannot capture an authoritative snapshot for " + connector + " while flow evolution is pending");
        }
    }

    /**
     * Installs the snapshot computed from the current one while holding the evolution lock.
     */
    void updateSnapshot(final UnaryOperator<ConnectorFlowSnapshot> update) {
        synchronized (lock) {
            installSnapshot(update.apply(authoritativeSnapshot));
        }
    }

    /**
     * Installs the snapshot and brings the drain status in line with whether an evolution is now pending. This is the only
     * writer of the snapshot. It must also be called, with the current snapshot, whenever the loaded bundle or whether the
     * extension is missing changes.
     */
    void installSnapshot(final ConnectorFlowSnapshot snapshot) {
        synchronized (lock) {
            authoritativeSnapshot = snapshot;

            // Once the snapshot records the loaded bundle no evolution is pending, so no attempt is open and neither its drain progress
            // nor its failure applies.
            if (!isPending()) {
                progress.set(FlowEvolutionProgress.NONE);
                latestAttempt = null;
                return;
            }

            // A node that still has the outgoing flow reports it as pending even before its own attempt begins, so a peer
            // does not apply the upgraded flow while this node may still need to drain.
            progress.updateAndGet(current -> current.replaceDrainStatus(FlowEvolutionStatus.INACTIVE, FlowEvolutionStatus.EVOLVE_DRAIN_PENDING));
        }
    }

    private static boolean sameBundle(final Bundle bundle, final Bundle other) {
        return other != null
                && Objects.equals(bundle.getGroup(), other.getGroup())
                && Objects.equals(bundle.getArtifact(), other.getArtifact())
                && Objects.equals(bundle.getVersion(), other.getVersion());
    }

    /**
     * Progress through a pending flow evolution. The drain status is kept when the evolution fails, so a retry knows whether
     * this node has already drained the outgoing flow.
     *
     * @param drainStatus {@link FlowEvolutionStatus#INACTIVE} when no evolution is pending, otherwise this node's evolve-drain progress
     * @param failureExplanation why the evolution failed, or null when it has not failed
     */
    private record FlowEvolutionProgress(FlowEvolutionStatus drainStatus, String failureExplanation) {
        private static final FlowEvolutionProgress NONE = new FlowEvolutionProgress(FlowEvolutionStatus.INACTIVE, null);

        FlowEvolutionStatus reportedStatus() {
            return hasFailed() ? FlowEvolutionStatus.FAILED : drainStatus;
        }

        boolean isDrainComplete() {
            return drainStatus == FlowEvolutionStatus.EVOLVE_DRAIN_COMPLETE;
        }

        boolean hasFailed() {
            return failureExplanation != null;
        }

        FlowEvolutionProgress withDrainStatus(final FlowEvolutionStatus status) {
            return new FlowEvolutionProgress(status, failureExplanation);
        }

        FlowEvolutionProgress replaceDrainStatus(final FlowEvolutionStatus expected, final FlowEvolutionStatus replacement) {
            return drainStatus == expected ? withDrainStatus(replacement) : this;
        }

        FlowEvolutionProgress withFailureIfAbsent(final String explanation) {
            return hasFailed() ? this : new FlowEvolutionProgress(drainStatus, explanation);
        }
    }

    /**
     * One attempt at a pending flow evolution. The futures are each set at most once per attempt, and are read and written
     * only while holding the evolution lock, so closing the attempt cancels this attempt's work and never a newer attempt's.
     */
    private static final class FlowEvolutionAttempt {
        private final StandardFlowEvolutionContext context;
        private final boolean drainCompletedInEarlierAttempt;
        private final FlowEngine scheduler;
        private CompletableFuture<Void> prepareFuture;
        private Future<Void> evolveFuture;
        private boolean preUpgradeStopRetryScheduled;

        private FlowEvolutionAttempt(final StandardFlowEvolutionContext context, final boolean drainCompletedInEarlierAttempt, final FlowEngine scheduler) {
            this.context = context;
            this.drainCompletedInEarlierAttempt = drainCompletedInEarlierAttempt;
            this.scheduler = scheduler;
        }
    }
}
