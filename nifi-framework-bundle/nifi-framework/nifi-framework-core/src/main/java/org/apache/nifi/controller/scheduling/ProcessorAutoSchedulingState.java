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
package org.apache.nifi.controller.scheduling;

import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.scheduling.auto.AutoSchedulingDiagnostics;
import org.apache.nifi.controller.scheduling.auto.ConcurrencyEvaluationState;
import org.apache.nifi.controller.scheduling.auto.ConcurrencyUpdateStatus;
import org.apache.nifi.controller.scheduling.auto.ConcurrentTaskMoveCoordinator;
import org.apache.nifi.controller.scheduling.auto.ProcessorSchedulingDecision;
import org.apache.nifi.controller.scheduling.auto.ProcessorSchedulingDecisionReason;
import org.apache.nifi.controller.scheduling.auto.ProcessorSchedulingMeasurements;
import org.apache.nifi.controller.scheduling.auto.ProcessorSchedulingSnapshot;
import org.apache.nifi.controller.scheduling.auto.StandardAutoSchedulingController;
import org.apache.nifi.controller.tasks.ConnectableTask;
import org.apache.nifi.controller.tasks.InvocationResult;
import org.apache.nifi.util.Connectables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.IntSupplier;

/**
 * Holds the automatic scheduling state of one scheduled Processor: its current scheduling settings, its workers, its concurrent task
 * slots, its measurements, and the controller that decides how many concurrent tasks it should use.
 */
final class ProcessorAutoSchedulingState implements ConcurrentTaskMoveCoordinator.Participant {
    private static final Logger logger = LoggerFactory.getLogger(ProcessorAutoSchedulingState.class);

    private static final long MAX_REPEATED_INVOCATION_NANOS = TimeUnit.MILLISECONDS.toNanos(10L);

    private final Connectable connectable;
    private final ConnectableTask connectableTask;
    private final LifecycleState lifecycleState;
    private final SchedulingGeneration generation;
    private final WaitingWorkers waitingWorkers;
    private final StandardAutoSchedulingController controller;
    private final BooleanSupplier primaryNodeCheck;
    private final IntSupplier globalConcurrentTaskLimit;
    private final Consumer<ProcessorTaskWorker> workerStarter;
    private final int maxConcurrentTasks;
    private final boolean batchingSupported;
    private final boolean sourceProcessor;
    private final ProcessorSchedulingMeasurements measurements = new ProcessorSchedulingMeasurements();
    private final AtomicReference<SchedulingSettings> schedulingSettings;
    private final AtomicInteger activeProcessorInvocations = new AtomicInteger();
    private final ConcurrentMap<Long, ProcessorTaskWorker> workers = new ConcurrentHashMap<>();
    private final AtomicLong workerSequence = new AtomicLong();
    private final AtomicLong resetSequence = new AtomicLong();
    private final SourceWorkCheck sourceWorkCheck;
    private final long evaluationIntervalNanos;

    private volatile long nextEvaluationNanos;
    private volatile long lastEvaluationNanos = System.nanoTime();
    private volatile long lastReadinessMeasurementNanos = System.nanoTime();
    private volatile long previousInputQueueSize;
    private volatile ProcessorSchedulingDecision lastDecision;
    private volatile ProcessorSchedulingDecision lastChangeDecision;
    private volatile ProcessorSchedulingSnapshot lastProcessorSchedulingSnapshot;
    private volatile boolean previousPrimaryNode;
    private volatile boolean measurementsSupported;
    private volatile double demandScore;

    /**
     * @param noWorkYieldNanos how long a Processor waits after an invocation that found no work, used as the shortest wait before a source
     * Processor that found no new data checks again
     * @param primaryNodeCheck returns whether this node is currently the primary node of the cluster
     * @param globalConcurrentTaskLimit returns the maximum number of concurrent tasks for all components combined
     * @param workerStarter starts a thread that runs the given worker
     */
    ProcessorAutoSchedulingState(final ConnectableTask connectableTask, final LifecycleState lifecycleState, final SchedulingGeneration generation,
                                 final StandardAutoSchedulingController controller, final long noWorkYieldNanos, final BooleanSupplier primaryNodeCheck,
                                 final IntSupplier globalConcurrentTaskLimit, final Consumer<ProcessorTaskWorker> workerStarter) {
        this.connectable = connectableTask.getConnectable();
        this.connectableTask = connectableTask;
        this.lifecycleState = lifecycleState;
        this.generation = generation;
        this.waitingWorkers = new WaitingWorkers(generation, this::getConcurrentTasks);
        this.controller = controller;
        this.primaryNodeCheck = primaryNodeCheck;
        this.globalConcurrentTaskLimit = globalConcurrentTaskLimit;
        this.workerStarter = workerStarter;
        this.maxConcurrentTasks = controller.getMaxConcurrentTasks();
        this.batchingSupported = connectable.isSessionBatchingSupported();
        schedulingSettings = new AtomicReference<>(new SchedulingSettings(1, batchingSupported ? TimeUnit.MILLISECONDS.toNanos(25L) : 0L));
        sourceProcessor = connectable.isTriggerWhenEmpty() || !connectable.hasIncomingConnection() || !Connectables.hasNonLoopConnection(connectable);
        this.sourceWorkCheck = new SourceWorkCheck(connectableTask, sourceProcessor, noWorkYieldNanos, waitingWorkers::signalChange);
        final double stagger = ((Math.floorMod(connectable.getIdentifier().hashCode(), 201) - 100) / 1000D);
        this.evaluationIntervalNanos = (long) (TimeUnit.SECONDS.toNanos(1L) * (1D + stagger));
        this.nextEvaluationNanos = lastEvaluationNanos + evaluationIntervalNanos;
        this.previousPrimaryNode = primaryNodeCheck.getAsBoolean();
    }

    Connectable getConnectable() {
        return connectable;
    }

    ConnectableTask getConnectableTask() {
        return connectableTask;
    }

    LifecycleState getLifecycleState() {
        return lifecycleState;
    }

    SchedulingGeneration getGeneration() {
        return generation;
    }

    WaitingWorkers getWaitingWorkers() {
        return waitingWorkers;
    }

    SourceWorkCheck getSourceWorkCheck() {
        return sourceWorkCheck;
    }

    int getMaxConcurrentTasks() {
        return maxConcurrentTasks;
    }

    boolean isBatchingSupported() {
        return batchingSupported;
    }

    boolean isSourceProcessor() {
        return sourceProcessor;
    }

    StandardAutoSchedulingController getController() {
        return controller;
    }

    ProcessorSchedulingMeasurements getMeasurements() {
        return measurements;
    }

    SchedulingSettings getSchedulingSettings() {
        return schedulingSettings.get();
    }

    ProcessorSchedulingSnapshot getLastProcessorSchedulingSnapshot() {
        return lastProcessorSchedulingSnapshot;
    }

    void setDemandScore(final double demandScore) {
        this.demandScore = demandScore;
    }

    long getResetSequence() {
        return resetSequence.get();
    }

    long incrementResetSequence() {
        return resetSequence.incrementAndGet();
    }

    /**
     * Retires workers or starts new ones so that the given number of workers are active.
     */
    void setWorkerCount(final int targetWorkerCount) {
        for (final ProcessorTaskWorker worker : resizeWorkers(targetWorkerCount)) {
            workerStarter.accept(worker);
        }
    }

    /**
     * Retires the newest workers if there are more than the target number of active workers, or creates new workers if there are fewer.
     * New workers are returned rather than started here so that their threads are started outside of this lock.
     *
     * @return the newly created workers, which the caller must start; empty if no workers needed to be created
     */
    private synchronized List<ProcessorTaskWorker> resizeWorkers(final int targetWorkerCount) {
        final List<ProcessorTaskWorker> activeWorkers = new ArrayList<>();
        for (final ProcessorTaskWorker worker : workers.values()) {
            if (!worker.isRetired()) {
                activeWorkers.add(worker);
            }
        }

        if (activeWorkers.size() > targetWorkerCount) {
            activeWorkers.sort(Comparator.comparingLong(ProcessorTaskWorker::getIdentifier));
            for (int index = targetWorkerCount; index < activeWorkers.size(); index++) {
                activeWorkers.get(index).retire();
            }

            waitingWorkers.signalChange();
            waitingWorkers.signalTaskSlotUpdate();
            return List.of();
        }

        final List<ProcessorTaskWorker> newWorkers = new ArrayList<>();
        for (int index = activeWorkers.size(); index < targetWorkerCount; index++) {
            final ProcessorTaskWorker worker = new ProcessorTaskWorker(workerSequence.getAndIncrement(), this);
            workers.put(worker.getIdentifier(), worker);
            newWorkers.add(worker);
        }

        return newWorkers;
    }

    void onWorkerStopped(final ProcessorTaskWorker worker) {
        workers.remove(worker.getIdentifier(), worker);
        waitingWorkers.signalChange();
        waitingWorkers.signalTaskSlotUpdate();
    }

    /**
     * @return <code>true</code> if the Processor is scheduled and this scheduling generation has not been stopped. Shutting down the
     * scheduling agent stops every scheduling generation.
     */
    boolean isScheduledToRun() {
        return lifecycleState.isScheduled() && !generation.isStopped();
    }

    /**
     * @return <code>true</code> if the given settings are still the Processor's current automatic scheduling settings
     */
    boolean areSchedulingSettingsCurrent(final SchedulingSettings settings) {
        return schedulingSettings.get() == settings;
    }

    boolean tryAcquireConcurrentTaskSlot(final SchedulingSettings expectedSettings) {
        while (true) {
            if (!areSchedulingSettingsCurrent(expectedSettings)) {
                return false;
            }

            final int currentActiveProcessorInvocations = activeProcessorInvocations.get();
            if (currentActiveProcessorInvocations >= expectedSettings.concurrentTasks()) {
                return false;
            }

            if (activeProcessorInvocations.compareAndSet(currentActiveProcessorInvocations, currentActiveProcessorInvocations + 1)) {
                if (areSchedulingSettingsCurrent(expectedSettings)) {
                    return true;
                }

                activeProcessorInvocations.decrementAndGet();
                waitingWorkers.signalTaskSlotUpdate();
                return false;
            }
        }
    }

    void releaseConcurrentTaskSlot() {
        activeProcessorInvocations.decrementAndGet();
        waitingWorkers.signalTaskSlotUpdate();
    }

    /**
     * @return <code>true</code> if the Processor could do useful work if invoked now. It is not ready if, for example, it is yielded, its
     * outgoing connections are full, it has no input, or it is a source Processor that recently found no new data and is not yet due to
     * be checked again.
     */
    boolean isReadyToRun() {
        if (!connectableTask.isReady()) {
            return false;
        }

        return sourceWorkCheck.isInvocationAllowed();
    }

    /**
     * Returns how long a worker may keep invoking the Processor before releasing its global permit, as long as each invocation finds
     * work to do. Each invocation uses its own Process Session and commit. Invoking a Processor that does not support session batching
     * several times in a row keeps its very short invocations from repeatedly waiting behind long-running invocations of other Processors,
     * while the time limit keeps it from holding the permit for too long. Processors that support session batching already do more work
     * in each invocation, and source Processors have no queue to drain, so neither is invoked repeatedly.
     *
     * @return the maximum time to keep invoking the Processor, or zero if it should be invoked only once per global permit
     */
    long getMaxRepeatedInvocationNanos() {
        if (batchingSupported || sourceProcessor) {
            return 0L;
        }

        return MAX_REPEATED_INVOCATION_NANOS;
    }

    void recordInvocationResult(final InvocationResult result) {
        sourceWorkCheck.recordInvocationResult(result, activeProcessorInvocations.get());
    }

    boolean isEvaluationDue(final long nowNanos) {
        if (nowNanos < nextEvaluationNanos) {
            return false;
        }

        nextEvaluationNanos = nowNanos + evaluationIntervalNanos;
        return true;
    }

    void recordReadiness(final long nowNanos) {
        final long measurementNanos = Math.max(1L, nowNanos - lastReadinessMeasurementNanos);
        lastReadinessMeasurementNanos = nowNanos;
        measurements.recordReadiness(connectableTask.getReadinessOutcome(), measurementNanos);
    }

    ProcessorSchedulingSnapshot captureProcessorSchedulingSnapshot(final long nowNanos, final boolean globalCapacityTestAllowed) {
        final SchedulingSettings currentSettings = schedulingSettings.get();
        long inputQueueSize = 0L;
        for (final Connection connection : connectable.getIncomingConnections()) {
            inputQueueSize += connection.getFlowFileQueue().getLocalQueueSize().getObjectCount();
        }

        final boolean inputQueueHasFlowFiles = connectableTask.hasLocallyConsumableInput();
        final double inputQueueGrowth = inputQueueSize - previousInputQueueSize;
        previousInputQueueSize = inputQueueSize;
        final long measurementWindowNanos = Math.max(1L, nowNanos - lastEvaluationNanos);
        lastEvaluationNanos = nowNanos;
        final boolean currentPrimaryNode = primaryNodeCheck.getAsBoolean();
        final boolean primaryNodeChanged = currentPrimaryNode != previousPrimaryNode;
        previousPrimaryNode = currentPrimaryNode;
        final boolean sourceProcessorRecentlyReportedActivity = sourceWorkCheck.hasRecentlyFoundData(nowNanos, activeProcessorInvocations.get());
        final ProcessorSchedulingSnapshot snapshot = measurements.captureSnapshot(nowNanos, measurementWindowNanos, currentSettings, connectableTask.isReady(),
                inputQueueHasFlowFiles, inputQueueSize, inputQueueGrowth, sourceProcessorRecentlyReportedActivity, primaryNodeChanged, globalCapacityTestAllowed);
        if (snapshot.committedFlowFiles() > 0L) {
            measurementsSupported = true;
        }

        lastProcessorSchedulingSnapshot = snapshot;
        return snapshot;
    }

    void resetMeasurementsAfterEvent() {
        final SchedulingSettings currentSettings = schedulingSettings.get();
        // Workers compare settings by identity so a reset also ends batches whose values have not changed.
        schedulingSettings.set(new SchedulingSettings(currentSettings.concurrentTasks(), currentSettings.runDurationNanos()));
        final long nowNanos = measurements.resetMeasurementWindow();
        lastEvaluationNanos = nowNanos;
        lastReadinessMeasurementNanos = nowNanos;
        long inputQueueSize = 0L;
        for (final Connection connection : connectable.getIncomingConnections()) {
            inputQueueSize += connection.getFlowFileQueue().getLocalQueueSize().getObjectCount();
        }

        previousInputQueueSize = inputQueueSize;
        lastProcessorSchedulingSnapshot = null;
        // A worker that read the settings before they were replaced fails to take a task slot and waits for a task slot update.
        waitingWorkers.signalChange();
        waitingWorkers.signalTaskSlotUpdate();
    }

    void applySettings(final SchedulingSettings settings) {
        final SchedulingSettings currentSettings = schedulingSettings.get();
        if (!currentSettings.equals(settings)) {
            schedulingSettings.set(new SchedulingSettings(settings.concurrentTasks(), settings.runDurationNanos()));
            waitingWorkers.signalChange();
            waitingWorkers.signalTaskSlotUpdate();
            logger.info("Automatic scheduling settings changed for {} from {} to {}", connectable, currentSettings, settings);
        }
    }

    void recordDecision(final ProcessorSchedulingDecision decision) {
        lastDecision = decision;
        final boolean comparisonEnded = switch (decision.reason()) {
            case HIGHER_CONCURRENCY_INCREASED_THROUGHPUT, HIGHER_CONCURRENCY_DID_NOT_INCREASE_THROUGHPUT, LOWER_CONCURRENCY_MAINTAINED_THROUGHPUT,
                 LOWER_CONCURRENCY_REDUCED_THROUGHPUT_OR_INCREASED_INPUT_QUEUE, COMPARISON_EXPIRED_WITHOUT_ENOUGH_MEASUREMENTS, PERFORMANCE_MEASUREMENTS_RESET -> true;
            default -> false;
        };

        if (comparisonEnded) {
            lastChangeDecision = decision;
        }
    }

    /**
     * Starts the controller's evaluation over, keeping the current scheduling settings.
     */
    void restartEvaluation() {
        controller.reset();
        lastDecision = new ProcessorSchedulingDecision(schedulingSettings.get(), ConcurrencyEvaluationState.USING_CURRENT_CONCURRENCY,
                ProcessorSchedulingDecisionReason.PERFORMANCE_MEASUREMENTS_RESET, false);
    }

    @Override
    public boolean isActive() {
        return !generation.isStopped();
    }

    @Override
    public int getConcurrentTasks() {
        return schedulingSettings.get().concurrentTasks();
    }

    @Override
    public boolean isConcurrencyComparisonActive() {
        return controller.isConcurrencyComparisonActive();
    }

    /**
     * Changes the concurrent task setting for a concurrent task move and restarts the measurements without counting a failed test.
     */
    @Override
    public boolean changeConcurrentTasks(final int change) {
        if (!isActive()) {
            return false;
        }

        final SchedulingSettings currentSettings = schedulingSettings.get();
        final int updatedConcurrentTasks = currentSettings.concurrentTasks() + change;
        final int concurrentTaskLimit = Math.min(maxConcurrentTasks, globalConcurrentTaskLimit.getAsInt());
        if (updatedConcurrentTasks < 1 || updatedConcurrentTasks > concurrentTaskLimit) {
            return false;
        }

        applySettings(new SchedulingSettings(updatedConcurrentTasks, currentSettings.runDurationNanos()));
        setWorkerCount(updatedConcurrentTasks);
        resetMeasurementsAfterEvent();
        restartEvaluation();
        return true;
    }

    @Override
    public String toString() {
        return connectable.toString();
    }

    AutoSchedulingDiagnostics getDiagnostics(final String taskMoveRole) {
        final SchedulingSettings currentSettings = schedulingSettings.get();
        final ProcessorSchedulingDecision currentDecision = lastDecision;
        final ProcessorSchedulingDecision currentChangeDecision = lastChangeDecision;
        final ProcessorSchedulingSnapshot currentSnapshot = lastProcessorSchedulingSnapshot;
        final long committedFlowFiles = currentSnapshot == null ? 0L : currentSnapshot.committedFlowFiles();
        final long measurementWindowNanos = currentSnapshot == null ? 0L : currentSnapshot.measurementWindowNanos();
        final double throughput = measurementWindowNanos == 0L ? 0D : committedFlowFiles / (measurementWindowNanos / (double) TimeUnit.SECONDS.toNanos(1L));
        final String evaluationState = currentDecision == null ? ConcurrencyEvaluationState.USING_CURRENT_CONCURRENCY.name() : currentDecision.state().name();
        final ProcessorSchedulingDecisionReason reason = currentDecision == null ? ProcessorSchedulingDecisionReason.COLLECTING_COMPARISON_MEASUREMENTS : currentDecision.reason();
        final ConcurrencyUpdateStatus concurrencyUpdateStatus = controller.getConcurrencyUpdateStatus();
        final String lastChangeReason = currentChangeDecision == null ? null : currentChangeDecision.reason().name();

        return AutoSchedulingDiagnostics.createBuilder()
                .setExecutionMode("adaptive")
                .setMaxConcurrentTasks(maxConcurrentTasks)
                .setCurrentConcurrentTasks(currentSettings.concurrentTasks())
                .setActiveProcessorInvocations(activeProcessorInvocations.get())
                .setCurrentRunDurationMillis(TimeUnit.NANOSECONDS.toMillis(currentSettings.runDurationNanos()))
                .setConcurrencyEvaluationState(evaluationState)
                .setConcurrencyUpdateReason(concurrencyUpdateStatus.reason().name())
                .setFlowFilesPerSecond(throughput)
                .setMeasurementWindowMillis(TimeUnit.NANOSECONDS.toMillis(measurementWindowNanos))
                .setLastConcurrencyUpdateReason(lastChangeReason)
                .setCollectingMeasurements(reason == ProcessorSchedulingDecisionReason.COLLECTING_COMPARISON_MEASUREMENTS)
                .setFlowFileMeasurementsAvailable(measurementsSupported)
                .setConcurrencyIncreaseExplanation(concurrencyUpdateStatus.explanation())
                .setLocalInputQueueCount(currentSnapshot == null ? 0L : currentSnapshot.localInputQueueCount())
                .setDemandScore(demandScore)
                .setTaskMoveRole(taskMoveRole)
                .build();
    }
}
