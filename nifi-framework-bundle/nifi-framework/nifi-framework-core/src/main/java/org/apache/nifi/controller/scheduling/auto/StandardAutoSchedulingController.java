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
package org.apache.nifi.controller.scheduling.auto;

import org.apache.nifi.controller.scheduling.SchedulingSettings;

import java.util.concurrent.TimeUnit;

/**
 * Chooses how many concurrent processor calls should be allowed.
 *
 * <p>The controller starts with one concurrent call. It watches completed work, available processor work,
 * how much of the time the allowed concurrent tasks are in use, back pressure, processor yields, the rate of failed calls, CPU load,
 * garbage collection, and the global concurrent task limit. It changes the setting only after collecting
 * enough measurements to make a useful comparison. Each measurement period lasts long enough for every concurrent call
 * to complete several invocations, from 6 seconds for fast processors up to 30 seconds for slow processors.</p>
 *
 * <p>When there is sustained work and the existing calls are busy, the controller tests a higher number of
 * concurrent calls. Each test adds one call, and the higher number is kept only when it produces a clear increase
 * in completed work. No increase exceeds the processor maximum or the global concurrent task limit. An unsuccessful
 * increase waits before it is tried again. While the global concurrent task limit is fully used, an increase is tested only when
 * the snapshot allows it, because the scheduling agent chose this processor to take a task from one with far less queued work.</p>
 *
 * <p>The controller also looks for a lower number of calls. If the processor is mostly idle or blocked for several
 * seconds, it removes one unused call immediately. At regular intervals it tests one fewer call even when the
 * existing calls are busy. That lower setting is kept only when completed work stays close to the reference rate and
 * the input queue does not grow. The reference rate is the rate measured at the highest setting that proved useful, so
 * a series of lower-setting tests cannot each give up a little more throughput than the one before. An unsuccessful
 * lower-setting test leaves the current setting in place and replaces the reference rate with the current rate, so that
 * a reference rate from an earlier, heavier workload does not block lower-setting tests indefinitely.</p>
 *
 * <p>The run duration is fixed when this controller is created. Batching support uses a 25 millisecond duration;
 * processors without batching use no added duration.</p>
 */
public class StandardAutoSchedulingController {
    private static final long MIN_COMPARISON_NANOS = TimeUnit.SECONDS.toNanos(6);
    private static final long MAX_COMPARISON_NANOS = TimeUnit.SECONDS.toNanos(30);
    private static final int COMPARISON_INVOCATIONS_PER_TASK = 10;
    private static final long INCREASE_COOLDOWN_NANOS = TimeUnit.SECONDS.toNanos(10);
    private static final long REDUCTION_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(30);
    private static final long UNUSED_TASK_REDUCTION_NANOS = TimeUnit.SECONDS.toNanos(3);
    private static final double THROUGHPUT_TOLERANCE = 0.05D;
    static final double MAX_FAILED_INVOCATION_FRACTION = 0.10D;
    static final double MIN_UTILIZATION_FOR_INCREASE = 0.75D;

    private final int maxConcurrentTasks;
    private final long runDurationNanos;
    private final boolean processorTriggeredSerially;
    private final boolean maxConcurrentTasksBasedOnAvailableProcessors;
    private final int availableProcessorCount;
    private ConcurrencyComparison concurrencyComparison;
    private double baselineMeasuredFlowFileCount;
    private double baselineMeasurementNanos;
    private double referenceFlowFilesPerSecond = -1D;
    private long averageInvocationNanos;
    private long nextIncreaseNanos;
    private long nextReductionNanos;
    private long concurrentTasksUnderusedNanos;
    private boolean previousIncreaseDidNotImproveThroughput;
    private volatile ConcurrencyUpdateStatus concurrencyUpdateStatus = new ConcurrencyUpdateStatus(
            ConcurrencyUpdateReason.COLLECTING_PERFORMANCE_MEASUREMENTS,
            "Gathering performance measurements before trying to increase concurrent tasks.");

    public StandardAutoSchedulingController(final int maxConcurrentTasks, final boolean batchingSupported,
                                            final boolean processorTriggeredSerially,
                                            final boolean maxConcurrentTasksBasedOnAvailableProcessors,
                                            final int availableProcessorCount) {
        this.maxConcurrentTasks = processorTriggeredSerially ? 1 : maxConcurrentTasks;
        runDurationNanos = batchingSupported ? TimeUnit.MILLISECONDS.toNanos(25) : 0L;
        this.processorTriggeredSerially = processorTriggeredSerially;
        this.maxConcurrentTasksBasedOnAvailableProcessors = maxConcurrentTasksBasedOnAvailableProcessors;
        this.availableProcessorCount = availableProcessorCount;
    }

    /**
     * Reviews the latest processor and system measurements and determines whether the current settings should change.
     *
     * @param processorMeasurements measurements for this processor
     * @param systemMeasurements measurements for the system and its global concurrent task limit
     * @return the current settings or settings to apply
     */
    public ProcessorSchedulingDecision evaluate(final ProcessorSchedulingSnapshot processorMeasurements,
                                                final SystemSchedulingSnapshot systemMeasurements) {
        final SchedulingSettings currentSettings = processorMeasurements.appliedSettings();
        final long timestampNanos = processorMeasurements.timestampNanos();
        final int maxAllowedConcurrentTasks = Math.max(1, Math.min(maxConcurrentTasks, systemMeasurements.maxGlobalConcurrentTasks()));
        if (currentSettings.concurrentTasks() > maxAllowedConcurrentTasks || currentSettings.runDurationNanos() != runDurationNanos) {
            reset();
            return createDecision(Math.min(currentSettings.concurrentTasks(), maxAllowedConcurrentTasks),
                    ProcessorSchedulingDecisionReason.CONCURRENT_TASKS_EXCEEDED_CURRENT_LIMIT, true, processorMeasurements, systemMeasurements);
        }

        if (processorMeasurements.primaryNodeChanged()) {
            reset();
        }

        if (processorMeasurements.completedInvocations() > 0L && processorMeasurements.processorInvocationNanos() > 0L) {
            averageInvocationNanos = processorMeasurements.processorInvocationNanos() / processorMeasurements.completedInvocations();
        }

        if (concurrencyComparison != null) {
            return evaluateConcurrencyComparison(processorMeasurements, systemMeasurements);
        }

        final boolean backpressureApplied = isBackpressureApplied(processorMeasurements);
        final boolean processorYielding = isProcessorYielding(processorMeasurements);

        baselineMeasuredFlowFileCount += processorMeasurements.committedFlowFiles();
        baselineMeasurementNanos += processorMeasurements.measurementWindowNanos();
        final long baselineWindowNanos = getComparisonWindowNanos();
        if (baselineMeasurementNanos > baselineWindowNanos * 2) {
            baselineMeasuredFlowFileCount *= 0.5D;
            baselineMeasurementNanos *= 0.5D;
        }

        if (nextReductionNanos == 0L) {
            nextReductionNanos = timestampNanos + REDUCTION_INTERVAL_NANOS;
        }

        final boolean concurrentTasksUnderused = processorMeasurements.concurrentTaskUtilization() < 0.5D;
        concurrentTasksUnderusedNanos = concurrentTasksUnderused || backpressureApplied || processorYielding
                ? concurrentTasksUnderusedNanos + processorMeasurements.measurementWindowNanos() : 0L;
        if (currentSettings.concurrentTasks() > 1 && concurrentTasksUnderusedNanos >= UNUSED_TASK_REDUCTION_NANOS) {
            concurrentTasksUnderusedNanos = 0L;
            referenceFlowFilesPerSecond = -1D;
            return createDecision(currentSettings.concurrentTasks() - 1, ProcessorSchedulingDecisionReason.REDUCED_UNUSED_CONCURRENT_TASK,
                    true, processorMeasurements, systemMeasurements);
        }

        if (currentSettings.concurrentTasks() > 1 && timestampNanos >= nextReductionNanos) {
            return startConcurrencyComparison(processorMeasurements, systemMeasurements, currentSettings.concurrentTasks() - 1);
        }

        final ConcurrencyUpdateReason increaseBlocker = findIncreaseBlocker(currentSettings.concurrentTasks(), processorMeasurements, systemMeasurements);
        if (increaseBlocker == null) {
            return startConcurrencyComparison(processorMeasurements, systemMeasurements, currentSettings.concurrentTasks() + 1);
        }

        final ProcessorSchedulingDecisionReason reason = switch (increaseBlocker) {
            case PROCESSOR_INVOCATION_FAILED -> ProcessorSchedulingDecisionReason.PROCESSOR_INVOCATION_FAILED;
            case CPU_LOAD_TOO_HIGH -> ProcessorSchedulingDecisionReason.CPU_LOAD_TOO_HIGH;
            case GARBAGE_COLLECTION_TIME_TOO_HIGH -> ProcessorSchedulingDecisionReason.GARBAGE_COLLECTION_TIME_TOO_HIGH;
            case GLOBAL_CAPACITY_FULL -> ProcessorSchedulingDecisionReason.GLOBAL_CAPACITY_FULL;
            case NO_WAITING_WORK -> ProcessorSchedulingDecisionReason.NO_WAITING_WORK;
            default -> ProcessorSchedulingDecisionReason.WAITING_BEFORE_ANOTHER_INCREASE;
        };
        return createDecision(currentSettings.concurrentTasks(), reason, false, processorMeasurements, systemMeasurements);
    }

    public void reset() {
        concurrencyComparison = null;
        baselineMeasuredFlowFileCount = 0D;
        baselineMeasurementNanos = 0D;
        referenceFlowFilesPerSecond = -1D;
        averageInvocationNanos = 0L;
        nextIncreaseNanos = 0L;
        nextReductionNanos = 0L;
        concurrentTasksUnderusedNanos = 0L;
        previousIncreaseDidNotImproveThroughput = false;
    }

    public ConcurrencyUpdateStatus getConcurrencyUpdateStatus() {
        return concurrencyUpdateStatus;
    }

    public boolean isConcurrencyComparisonActive() {
        return concurrencyComparison != null;
    }

    public boolean isIncreaseCoolingDown(final long timestampNanos) {
        return timestampNanos < nextIncreaseNanos;
    }

    /**
     * Chooses a measurement window long enough for each concurrent task to complete about
     * {@value #COMPARISON_INVOCATIONS_PER_TASK} invocations, so that partial invocations at the edges of the window
     * have little effect on the measured rate. The window is limited to the minimum and maximum comparison durations.
     */
    private long getComparisonWindowNanos() {
        return Math.max(MIN_COMPARISON_NANOS, Math.min(MAX_COMPARISON_NANOS, averageInvocationNanos * COMPARISON_INVOCATIONS_PER_TASK));
    }

    private ProcessorSchedulingDecision startConcurrencyComparison(final ProcessorSchedulingSnapshot processorMeasurements,
                                                                   final SystemSchedulingSnapshot systemMeasurements,
                                                                   final int comparisonConcurrentTasks) {
        final long minMeasurementNanos = getComparisonWindowNanos();
        final double baselineFlowFilesPerSecond = baselineMeasuredFlowFileCount * TimeUnit.SECONDS.toNanos(1)
                / Math.max(1D, baselineMeasurementNanos);
        final int originalConcurrentTasks = processorMeasurements.appliedSettings().concurrentTasks();
        final boolean increasingConcurrentTasks = comparisonConcurrentTasks > originalConcurrentTasks;
        final double originalFlowFilesPerSecond = increasingConcurrentTasks ? baselineFlowFilesPerSecond
                : Math.max(baselineFlowFilesPerSecond, referenceFlowFilesPerSecond);
        concurrencyComparison = new ConcurrencyComparison(originalConcurrentTasks, comparisonConcurrentTasks, originalFlowFilesPerSecond,
                baselineFlowFilesPerSecond, processorMeasurements.timestampNanos(), minMeasurementNanos);
        final ProcessorSchedulingDecisionReason reason = concurrencyComparison.isIncreasingConcurrentTasks()
                ? ProcessorSchedulingDecisionReason.TESTING_HIGHER_CONCURRENCY : ProcessorSchedulingDecisionReason.TESTING_LOWER_CONCURRENCY;
        return createDecision(comparisonConcurrentTasks, reason, true, processorMeasurements, systemMeasurements);
    }

    private ProcessorSchedulingDecision evaluateConcurrencyComparison(final ProcessorSchedulingSnapshot processorMeasurements,
                                                                      final SystemSchedulingSnapshot systemMeasurements) {
        concurrencyComparison.addMeasurements(processorMeasurements);
        if (hasHighFailureRate(processorMeasurements)) {
            return finishConcurrencyComparison(false, ProcessorSchedulingDecisionReason.PROCESSOR_INVOCATION_FAILED,
                    processorMeasurements, systemMeasurements);
        }
        if (isBackpressureApplied(processorMeasurements)) {
            return finishConcurrencyComparison(false, ProcessorSchedulingDecisionReason.PROCESSOR_BLOCKED_BY_BACK_PRESSURE,
                    processorMeasurements, systemMeasurements);
        }
        if (isProcessorYielding(processorMeasurements)) {
            return finishConcurrencyComparison(false, ProcessorSchedulingDecisionReason.PROCESSOR_YIELDING,
                    processorMeasurements, systemMeasurements);
        }
        if (!concurrencyComparison.hasMinimumMeasurements()) {
            if (concurrencyComparison.hasExpired(processorMeasurements.timestampNanos())) {
                return finishConcurrencyComparison(false, ProcessorSchedulingDecisionReason.COMPARISON_EXPIRED_WITHOUT_ENOUGH_MEASUREMENTS,
                        processorMeasurements, systemMeasurements);
            }

            return createDecision(concurrencyComparison.getComparisonConcurrentTasks(),
                    ProcessorSchedulingDecisionReason.COLLECTING_COMPARISON_MEASUREMENTS, false, processorMeasurements, systemMeasurements);
        }

        final double tolerance = Math.min(THROUGHPUT_TOLERANCE, 0.5D / concurrencyComparison.getOriginalConcurrentTasks());
        final boolean accepted = concurrencyComparison.isIncreasingConcurrentTasks()
                ? concurrencyComparison.hasIncreasedThroughput(tolerance)
                : concurrencyComparison.hasMaintainedThroughput(tolerance) && !concurrencyComparison.hasInputQueueGrown();
        final ProcessorSchedulingDecisionReason reason;
        if (concurrencyComparison.isIncreasingConcurrentTasks()) {
            reason = accepted ? ProcessorSchedulingDecisionReason.HIGHER_CONCURRENCY_INCREASED_THROUGHPUT
                    : ProcessorSchedulingDecisionReason.HIGHER_CONCURRENCY_DID_NOT_INCREASE_THROUGHPUT;
        } else {
            reason = accepted ? ProcessorSchedulingDecisionReason.LOWER_CONCURRENCY_MAINTAINED_THROUGHPUT
                    : ProcessorSchedulingDecisionReason.LOWER_CONCURRENCY_REDUCED_THROUGHPUT_OR_INCREASED_INPUT_QUEUE;
        }

        return finishConcurrencyComparison(accepted, reason, processorMeasurements, systemMeasurements);
    }

    private ProcessorSchedulingDecision finishConcurrencyComparison(final boolean accepted, final ProcessorSchedulingDecisionReason reason,
                                                                    final ProcessorSchedulingSnapshot processorMeasurements,
                                                                    final SystemSchedulingSnapshot systemMeasurements) {
        final boolean increasingConcurrentTasks = concurrencyComparison.isIncreasingConcurrentTasks();
        final int selectedConcurrentTasks = accepted ? concurrencyComparison.getComparisonConcurrentTasks()
                : concurrencyComparison.getOriginalConcurrentTasks();
        if (accepted) {
            baselineMeasuredFlowFileCount = concurrencyComparison.getMeasuredFlowFileCount();
            baselineMeasurementNanos = concurrencyComparison.getMeasurementWindowNanos();
        }

        if (increasingConcurrentTasks && accepted) {
            referenceFlowFilesPerSecond = concurrencyComparison.getMeasuredFlowFilesPerSecond();
        } else if (!increasingConcurrentTasks) {
            referenceFlowFilesPerSecond = accepted ? concurrencyComparison.getOriginalFlowFilesPerSecond()
                    : concurrencyComparison.getBaselineFlowFilesPerSecond();
        }

        final long timestampNanos = processorMeasurements.timestampNanos();
        nextIncreaseNanos = accepted && increasingConcurrentTasks ? timestampNanos : timestampNanos + INCREASE_COOLDOWN_NANOS;
        previousIncreaseDidNotImproveThroughput = !accepted && increasingConcurrentTasks;
        if (accepted || !increasingConcurrentTasks) {
            nextReductionNanos = timestampNanos
                    + (accepted && !increasingConcurrentTasks ? UNUSED_TASK_REDUCTION_NANOS : REDUCTION_INTERVAL_NANOS);
        }

        concurrencyComparison = null;
        concurrentTasksUnderusedNanos = 0L;
        return createDecision(selectedConcurrentTasks, reason,
                selectedConcurrentTasks != processorMeasurements.appliedSettings().concurrentTasks(), processorMeasurements, systemMeasurements);
    }

    private ProcessorSchedulingDecision createDecision(final int concurrentTasks, final ProcessorSchedulingDecisionReason reason,
                                                       final boolean applySettings, final ProcessorSchedulingSnapshot processorMeasurements,
                                                       final SystemSchedulingSnapshot systemMeasurements) {
        final ConcurrencyUpdateReason increaseBlocker = findIncreaseBlocker(concurrentTasks, processorMeasurements, systemMeasurements);
        concurrencyUpdateStatus = describeConcurrencyUpdate(increaseBlocker == null ? ConcurrencyUpdateReason.COLLECTING_PERFORMANCE_MEASUREMENTS : increaseBlocker,
                concurrentTasks, processorMeasurements.timestampNanos(), systemMeasurements);
        final ConcurrencyEvaluationState state = concurrencyComparison == null
                ? ConcurrencyEvaluationState.USING_CURRENT_CONCURRENCY : ConcurrencyEvaluationState.MEASURING_CONCURRENCY_CHANGE;
        return new ProcessorSchedulingDecision(new SchedulingSettings(concurrentTasks, runDurationNanos), state, reason, applySettings);
    }

    /**
     * Finds the first reason that prevents testing a higher number of concurrent tasks. The same reason determines both
     * whether an increase is tested and the explanation reported to users.
     *
     * @return the reason an increase cannot be tested, or {@code null} if an increase can be tested now
     */
    private ConcurrencyUpdateReason findIncreaseBlocker(final int concurrentTasks, final ProcessorSchedulingSnapshot processorMeasurements,
                                                        final SystemSchedulingSnapshot systemMeasurements) {
        if (processorTriggeredSerially) {
            return ConcurrencyUpdateReason.REQUIRES_SINGLE_CONCURRENT_TASK;
        }

        if (concurrentTasks >= Math.min(maxConcurrentTasks, systemMeasurements.maxGlobalConcurrentTasks())) {
            if (systemMeasurements.maxGlobalConcurrentTasks() < maxConcurrentTasks) {
                return ConcurrencyUpdateReason.CONCURRENT_TASK_LIMIT_REACHED;
            }

            return maxConcurrentTasksBasedOnAvailableProcessors ? ConcurrencyUpdateReason.CPU_LIMIT_REACHED
                    : ConcurrencyUpdateReason.CONFIGURED_MAX_CONCURRENT_TASKS_REACHED;
        }

        if (concurrencyComparison != null) {
            return concurrencyComparison.isIncreasingConcurrentTasks() ? ConcurrencyUpdateReason.TESTING_HIGHER_CONCURRENCY
                    : ConcurrencyUpdateReason.TESTING_LOWER_CONCURRENCY;
        }

        if (hasHighFailureRate(processorMeasurements)) {
            return ConcurrencyUpdateReason.PROCESSOR_INVOCATION_FAILED;
        }
        if (systemMeasurements.cpuLoadAboveLimit()) {
            return ConcurrencyUpdateReason.CPU_LOAD_TOO_HIGH;
        }
        if (systemMeasurements.garbageCollectionTimeAboveLimit()) {
            return ConcurrencyUpdateReason.GARBAGE_COLLECTION_TIME_TOO_HIGH;
        }
        if (systemMeasurements.globalCapacityFull() && !processorMeasurements.globalCapacityTestAllowed()) {
            return ConcurrencyUpdateReason.GLOBAL_CAPACITY_FULL;
        }
        if (!processorMeasurements.inputQueueHasFlowFiles() && !processorMeasurements.sourceProcessorRecentlyReportedActivity()) {
            return ConcurrencyUpdateReason.NO_WAITING_WORK;
        }
        if (!processorMeasurements.processorReady()) {
            return ConcurrencyUpdateReason.PROCESSOR_NOT_READY;
        }
        if (isBackpressureApplied(processorMeasurements)) {
            return ConcurrencyUpdateReason.DOWNSTREAM_BACK_PRESSURE;
        }
        if (isProcessorYielding(processorMeasurements)) {
            return ConcurrencyUpdateReason.PROCESSOR_YIELDING;
        }
        if (processorMeasurements.concurrentTaskUtilization() < MIN_UTILIZATION_FOR_INCREASE) {
            return ConcurrencyUpdateReason.CONCURRENT_TASKS_UNDERUSED;
        }
        if (processorMeasurements.timestampNanos() < nextIncreaseNanos) {
            return previousIncreaseDidNotImproveThroughput ? ConcurrencyUpdateReason.PREVIOUS_INCREASE_DID_NOT_IMPROVE_THROUGHPUT
                    : ConcurrencyUpdateReason.COOLDOWN;
        }
        if (concurrentTasks > 1 && baselineMeasurementNanos < getComparisonWindowNanos()) {
            return ConcurrencyUpdateReason.COLLECTING_PERFORMANCE_MEASUREMENTS;
        }

        return null;
    }

    private ConcurrencyUpdateStatus describeConcurrencyUpdate(final ConcurrencyUpdateReason reason, final int concurrentTasks, final long timestampNanos,
                                                              final SystemSchedulingSnapshot systemMeasurements) {
        final long cooldownSeconds = Math.max(1L, TimeUnit.NANOSECONDS.toSeconds(nextIncreaseNanos - timestampNanos + TimeUnit.SECONDS.toNanos(1L) - 1L));
        final String cooldownDescription = cooldownSeconds + (cooldownSeconds == 1L ? " second." : " seconds.");
        final String explanation = switch (reason) {
            case REQUIRES_SINGLE_CONCURRENT_TASK -> "Will not increase concurrent tasks because this Processor requires serial execution.";
            case CONCURRENT_TASK_LIMIT_REACHED -> "Will not increase concurrent tasks because the instance has reached its concurrent task limit of "
                    + systemMeasurements.maxGlobalConcurrentTasks() + (systemMeasurements.maxGlobalConcurrentTasks() == 1 ? " concurrent task." : " concurrent tasks.");
            case CPU_LIMIT_REACHED -> "Will not increase concurrent tasks because the Processor has reached the maximum of " + maxConcurrentTasks
                    + " concurrent tasks based on " + availableProcessorCount + (availableProcessorCount == 1 ? " available CPU core." : " available CPU cores.");
            case CONFIGURED_MAX_CONCURRENT_TASKS_REACHED -> "Will not increase concurrent tasks because the Processor has reached the configured maximum of "
                    + maxConcurrentTasks + " concurrent tasks.";
            case TESTING_HIGHER_CONCURRENCY -> "Testing whether " + concurrentTasks + " concurrent tasks improve performance before increasing further.";
            case TESTING_LOWER_CONCURRENCY -> "Will not increase concurrent tasks while testing whether fewer concurrent tasks can maintain performance.";
            case PROCESSOR_INVOCATION_FAILED -> "Will not increase concurrent tasks because too many recent Processor invocations failed.";
            case CPU_LOAD_TOO_HIGH -> "Will not increase concurrent tasks because CPU load is too high.";
            case GARBAGE_COLLECTION_TIME_TOO_HIGH -> "Will not increase concurrent tasks because garbage collection is using too much processing time.";
            case GLOBAL_CAPACITY_FULL -> "Will not increase concurrent tasks because the instance's global scheduling capacity is fully used.";
            case NO_WAITING_WORK -> "Will not increase concurrent tasks because there is not enough sustained work.";
            case PROCESSOR_NOT_READY -> "Will not increase concurrent tasks because the Processor is not currently ready to run.";
            case DOWNSTREAM_BACK_PRESSURE -> "Will not increase concurrent tasks because downstream back pressure is limiting the Processor.";
            case PROCESSOR_YIELDING -> "Will not increase concurrent tasks because the Processor is yielding.";
            case CONCURRENT_TASKS_UNDERUSED -> "Will not increase concurrent tasks because the current concurrent tasks are not busy enough.";
            case PREVIOUS_INCREASE_DID_NOT_IMPROVE_THROUGHPUT -> "Will not increase concurrent tasks because the previous increase did not improve performance; "
                    + "will try again in " + cooldownDescription;
            case COOLDOWN -> "Will not increase concurrent tasks during the cooldown; will try an increase in " + cooldownDescription;
            case COLLECTING_PERFORMANCE_MEASUREMENTS -> "Gathering performance measurements before trying to increase concurrent tasks.";
        };

        return new ConcurrencyUpdateStatus(reason, explanation);
    }

    private static boolean isBackpressureApplied(final ProcessorSchedulingSnapshot processorMeasurements) {
        return processorMeasurements.backpressureNanos() >= processorMeasurements.measurementWindowNanos() / 2;
    }

    private static boolean isProcessorYielding(final ProcessorSchedulingSnapshot processorMeasurements) {
        return processorMeasurements.processorYieldNanos() >= processorMeasurements.measurementWindowNanos() / 2;
    }

    private static boolean hasHighFailureRate(final ProcessorSchedulingSnapshot processorMeasurements) {
        final long failedInvocations = processorMeasurements.failedInvocations();
        return failedInvocations > 0L && failedInvocations >= processorMeasurements.completedInvocations() * MAX_FAILED_INVOCATION_FRACTION;
    }

    private static class ConcurrencyComparison {
        private final int originalConcurrentTasks;
        private final int comparisonConcurrentTasks;
        private final double originalFlowFilesPerSecond;
        private final double baselineFlowFilesPerSecond;
        private final long comparisonStartNanos;
        private final long minMeasurementNanos;
        private long measuredFlowFileCount;
        private long measurementWindowNanos;
        private long completedInvocations;
        private double inputQueueGrowth;

        private ConcurrencyComparison(final int originalConcurrentTasks, final int comparisonConcurrentTasks,
                                      final double originalFlowFilesPerSecond, final double baselineFlowFilesPerSecond,
                                      final long comparisonStartNanos, final long minMeasurementNanos) {
            this.originalConcurrentTasks = originalConcurrentTasks;
            this.comparisonConcurrentTasks = comparisonConcurrentTasks;
            this.originalFlowFilesPerSecond = originalFlowFilesPerSecond;
            this.baselineFlowFilesPerSecond = baselineFlowFilesPerSecond;
            this.comparisonStartNanos = comparisonStartNanos;
            this.minMeasurementNanos = minMeasurementNanos;
        }

        private void addMeasurements(final ProcessorSchedulingSnapshot processorMeasurements) {
            measuredFlowFileCount += processorMeasurements.committedFlowFiles();
            measurementWindowNanos += processorMeasurements.measurementWindowNanos();
            completedInvocations += processorMeasurements.completedInvocations();
            inputQueueGrowth += processorMeasurements.inputQueueGrowth();
        }

        private boolean isIncreasingConcurrentTasks() {
            return comparisonConcurrentTasks > originalConcurrentTasks;
        }

        private boolean hasMinimumMeasurements() {
            return measurementWindowNanos >= minMeasurementNanos && (measuredFlowFileCount > 0L || completedInvocations > 0L);
        }

        private boolean hasExpired(final long timestampNanos) {
            return timestampNanos - comparisonStartNanos >= MAX_COMPARISON_NANOS;
        }

        private double getMeasuredFlowFilesPerSecond() {
            return measuredFlowFileCount * (double) TimeUnit.SECONDS.toNanos(1) / Math.max(1L, measurementWindowNanos);
        }

        private boolean hasIncreasedThroughput(final double tolerance) {
            return getMeasuredFlowFilesPerSecond() > originalFlowFilesPerSecond * (1D + tolerance);
        }

        private boolean hasMaintainedThroughput(final double tolerance) {
            return getMeasuredFlowFilesPerSecond() >= originalFlowFilesPerSecond * (1D - tolerance);
        }

        private boolean hasInputQueueGrown() {
            return inputQueueGrowth > 0D;
        }

        private double getOriginalFlowFilesPerSecond() {
            return originalFlowFilesPerSecond;
        }

        private double getBaselineFlowFilesPerSecond() {
            return baselineFlowFilesPerSecond;
        }

        private int getOriginalConcurrentTasks() {
            return originalConcurrentTasks;
        }

        private int getComparisonConcurrentTasks() {
            return comparisonConcurrentTasks;
        }

        private long getMeasuredFlowFileCount() {
            return measuredFlowFileCount;
        }

        private long getMeasurementWindowNanos() {
            return measurementWindowNanos;
        }
    }
}
