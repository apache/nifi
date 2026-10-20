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
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.Triggerable;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.queue.QueueSchedulingRegistration;
import org.apache.nifi.controller.scheduling.auto.AutoSchedulingDiagnostics;
import org.apache.nifi.controller.scheduling.auto.AutoSchedulingResetReason;
import org.apache.nifi.controller.scheduling.auto.ConcurrencyEvaluationState;
import org.apache.nifi.controller.scheduling.auto.ConcurrencyUpdateStatus;
import org.apache.nifi.controller.scheduling.auto.ConcurrentTaskMoveCoordinator;
import org.apache.nifi.controller.scheduling.auto.ConcurrentTaskMoveSelector;
import org.apache.nifi.controller.scheduling.auto.ConcurrentTaskMoveSelector.ProcessorDemand;
import org.apache.nifi.controller.scheduling.auto.ProcessorSchedulingDecision;
import org.apache.nifi.controller.scheduling.auto.ProcessorSchedulingDecisionReason;
import org.apache.nifi.controller.scheduling.auto.ProcessorSchedulingMeasurements;
import org.apache.nifi.controller.scheduling.auto.ProcessorSchedulingSnapshot;
import org.apache.nifi.controller.scheduling.auto.StandardAutoSchedulingController;
import org.apache.nifi.controller.scheduling.auto.SystemSchedulingMetrics;
import org.apache.nifi.controller.scheduling.auto.SystemSchedulingSnapshot;
import org.apache.nifi.controller.tasks.ConnectableTask;
import org.apache.nifi.controller.tasks.InvocationObserver;
import org.apache.nifi.controller.tasks.InvocationOutcome;
import org.apache.nifi.controller.tasks.InvocationResult;
import org.apache.nifi.controller.tasks.ReportingTaskWrapper;
import org.apache.nifi.nar.NarThreadContextClassLoader;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.util.Connectables;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.support.CronExpression;

import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Scheduling agent that runs components on virtual threads. A {@link DynamicSemaphore}
 * limits the number of component invocations that can run concurrently.
 */
public class VirtualThreadSchedulingAgent implements SchedulingAgent {

    private static final Logger logger = LoggerFactory.getLogger(VirtualThreadSchedulingAgent.class);

    private static final long PERMIT_POLL_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(1L);
    private static final long MAX_REPEATED_INVOCATION_NANOS = TimeUnit.MILLISECONDS.toNanos(10L);

    private final FlowController flowController;
    private final RepositoryContextFactory contextFactory;
    private final DynamicSemaphore globalSemaphore;
    private final int autoMaxConcurrentTasks;
    private final int availableProcessorCount;
    private final boolean autoMaxConcurrentTasksBasedOnAvailableProcessors;
    private final long noWorkYieldNanos;
    private final ExecutorService executorService;
    private final ScheduledExecutorService schedulingEvaluationExecutor;
    private final SystemSchedulingMetrics systemSchedulingMetrics;
    private final ConcurrentMap<String, SchedulingGeneration> schedulingGenerations = new ConcurrentHashMap<>();
    private final ConcurrentTaskMoveCoordinator concurrentTaskMoveCoordinator = new ConcurrentTaskMoveCoordinator();
    private final LongAdder globalPermitHoldNanos = new LongAdder();
    private final LongAdder globalPermitWaitNanos = new LongAdder();
    private final LongAdder globalTaskSlotsFullyUsedSamples = new LongAdder();
    private final LongAdder globalTaskUsageSamples = new LongAdder();
    private final AtomicBoolean shutdown = new AtomicBoolean();
    private final AtomicInteger runningThreadCount = new AtomicInteger();
    private volatile SystemSchedulingSnapshot systemSchedulingSnapshot = SystemSchedulingSnapshot.createBuilder()
            .setCpuLoad(-1D)
            .setAverageCpuLoad(-1D)
            .setMaxGlobalConcurrentTasks(1)
            .build();
    private volatile long lastSystemSchedulingSnapshotNanos = System.nanoTime();
    private volatile String adminYieldDuration = "1 sec";
    private volatile long adminYieldNanos = TimeUnit.SECONDS.toNanos(1L);

    public VirtualThreadSchedulingAgent(final FlowController flowController, final RepositoryContextFactory contextFactory,
                                        final NiFiProperties nifiProperties, final int maxThreadCount) {
        this(flowController, contextFactory, nifiProperties, maxThreadCount, nifiProperties.getProcessorAutoMaxConcurrentTasks(), new SystemSchedulingMetrics());
    }

    VirtualThreadSchedulingAgent(final FlowController flowController, final RepositoryContextFactory contextFactory, final NiFiProperties nifiProperties,
                                final int maxThreadCount, final int configuredAutoMaxConcurrentTasks, final SystemSchedulingMetrics systemSchedulingMetrics) {
        this.systemSchedulingMetrics = systemSchedulingMetrics;
        this.flowController = flowController;
        this.contextFactory = contextFactory;
        this.globalSemaphore = new DynamicSemaphore(maxThreadCount);
        availableProcessorCount = Runtime.getRuntime().availableProcessors();
        final long cpuLimitedMaxConcurrentTasks = 4L * availableProcessorCount;
        autoMaxConcurrentTasks = (int) Math.min(configuredAutoMaxConcurrentTasks, cpuLimitedMaxConcurrentTasks);
        autoMaxConcurrentTasksBasedOnAvailableProcessors = configuredAutoMaxConcurrentTasks > cpuLimitedMaxConcurrentTasks;

        final String boredYieldDuration = nifiProperties.getBoredYieldDuration();
        try {
            noWorkYieldNanos = FormatUtils.getTimeDuration(boredYieldDuration, TimeUnit.NANOSECONDS);
        } catch (final IllegalArgumentException e) {
            throw new IllegalStateException("Failed to create VirtualThreadSchedulingAgent because the "
                    + NiFiProperties.BORED_YIELD_DURATION + " property is set to an invalid time duration: " + boredYieldDuration, e);
        }

        final ThreadFactory threadFactory = runnable -> {
            final Thread thread = Thread.ofVirtual().inheritInheritableThreadLocals(false).unstarted(runnable);
            thread.setContextClassLoader(NarThreadContextClassLoader.getInstance());
            return thread;
        };
        executorService = Executors.newThreadPerTaskExecutor(threadFactory);
        schedulingEvaluationExecutor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            final Thread thread = Thread.ofPlatform().daemon(true).name("Automatic Processor Scheduling Controller").unstarted(runnable);
            thread.setContextClassLoader(NarThreadContextClassLoader.getInstance());
            return thread;
        });
        schedulingEvaluationExecutor.scheduleWithFixedDelay(this::evaluateAutoScheduling, 250L, 250L, TimeUnit.MILLISECONDS);
        logger.info("VirtualThreadSchedulingAgent initialized with {} permits", maxThreadCount);
    }

    @Override
    public int getProcessContextConcurrencyLimit(final Connectable connectable) {
        if (connectable.getSchedulingStrategy() != SchedulingStrategy.AUTO) {
            return connectable.getMaxConcurrentTasks();
        }

        return connectable instanceof ProcessorNode processorNode && processorNode.isTriggeredSerially() ? 1 : autoMaxConcurrentTasks;
    }

    @Override
    public void shutdown() {
        signalShutdown(true);
        schedulingEvaluationExecutor.shutdownNow();
        executorService.shutdownNow();
    }

    public void shutdownGracefully() {
        signalShutdown(false);
        schedulingEvaluationExecutor.shutdown();
        executorService.shutdown();
    }

    private void signalShutdown(final boolean interrupt) {
        shutdown.set(true);

        for (final SchedulingGeneration generation : schedulingGenerations.values()) {
            generation.stop(interrupt);
        }
    }

    public boolean awaitTermination(final long timeout, final TimeUnit timeUnit) throws InterruptedException {
        final long deadlineNanos = System.nanoTime() + timeUnit.toNanos(timeout);
        if (!schedulingEvaluationExecutor.awaitTermination(timeout, timeUnit)) {
            return false;
        }

        final long remainingNanos = Math.max(0L, deadlineNanos - System.nanoTime());
        return executorService.awaitTermination(remainingNanos, TimeUnit.NANOSECONDS);
    }

    public boolean isTerminated() {
        return schedulingEvaluationExecutor.isTerminated() && executorService.isTerminated();
    }

    @Override
    public void schedule(final Connectable connectable, final LifecycleState lifecycleState) {
        final boolean cronDriven = connectable.getSchedulingStrategy() == SchedulingStrategy.CRON_DRIVEN;
        final CronExpression cronExpression;
        final long schedulingNanos;
        if (cronDriven) {
            final String cronSchedule = connectable.evaluateParameters(connectable.getSchedulingPeriod());
            cronExpression = parseCronExpression(cronSchedule, connectable);
            schedulingNanos = 0L;
        } else {
            cronExpression = null;
            schedulingNanos = connectable.getSchedulingPeriod(TimeUnit.NANOSECONDS);
        }

        final String componentId = connectable.getIdentifier();
        final SchedulingGeneration generation;
        synchronized (lifecycleState) {
            generation = registerSchedulingGeneration(componentId);
            lifecycleState.setScheduled(true);
        }

        try {
            final ConnectableTask connectableTask = new ConnectableTask(this, connectable, flowController, contextFactory, lifecycleState, generation::isRunning);
            if (connectable.getSchedulingStrategy() == SchedulingStrategy.AUTO) {
                final int maxConcurrentTasks = getProcessContextConcurrencyLimit(connectable);
                final ProcessorAutoSchedulingState autoSchedulingState = new ProcessorAutoSchedulingState(connectable, connectableTask, lifecycleState, generation,
                        maxConcurrentTasks, connectable.isSessionBatchingSupported(),
                        connectable instanceof ProcessorNode processorNode && processorNode.isTriggeredSerially());
                generation.setAutoSchedulingState(autoSchedulingState);
                registerQueueListeners(connectable, generation);
                startAutoWorkers(autoSchedulingState, 1);
                logger.info("Scheduled {} for automatic scheduling with a maximum of {} concurrent tasks", connectable, maxConcurrentTasks);
                return;
            }

            final int taskCount = connectable.getMaxConcurrentTasks();
            for (int i = 0; i < taskCount; i++) {
                final String threadName = buildThreadName(connectable, i);
                submitTask(threadName, generation, () -> runSchedulingLoop(connectable, connectableTask, schedulingNanos, lifecycleState, generation, cronExpression));
            }

            logger.info("Scheduled {} to run with {} virtual threads", connectable, taskCount);
        } catch (final Throwable t) {
            synchronized (lifecycleState) {
                if (stopSchedulingGeneration(componentId, generation, true)) {
                    lifecycleState.setScheduled(false);
                }
            }

            throw t;
        }
    }

    private void registerQueueListeners(final Connectable connectable, final SchedulingGeneration generation) {
        final Set<FlowFileQueue> queues = new HashSet<>();
        for (final Connection connection : connectable.getIncomingConnections()) {
            queues.add(connection.getFlowFileQueue());
        }

        for (final Connection connection : connectable.getConnections()) {
            queues.add(connection.getFlowFileQueue());
        }

        for (final FlowFileQueue queue : queues) {
            generation.addQueue(queue);
            generation.addQueueRegistration(queue.addSchedulingListener(generation::signalChange));
        }
    }

    private void startAutoWorkers(final ProcessorAutoSchedulingState state, final int desiredWorkerCount) {
        final List<ProcessorTaskWorker> workers = state.resizeWorkers(desiredWorkerCount);
        for (final ProcessorTaskWorker worker : workers) {
            final String threadName = buildThreadName(state.getConnectable(), Math.toIntExact(worker.getIdentifier()));
            submitTask(threadName, state.getGeneration(), () -> runAutoSchedulingLoop(state, worker));
        }
    }

    private void runAutoSchedulingLoop(final ProcessorAutoSchedulingState state, final ProcessorTaskWorker worker) {
        try {
            while (worker.isActive()) {
                try {
                    if (!runAutoSchedulingIteration(state, worker)) {
                        return;
                    }
                } catch (final Throwable t) {
                    if (!worker.isActive()) {
                        return;
                    }

                    try {
                        state.getConnectable().yield(adminYieldNanos, TimeUnit.NANOSECONDS);
                    } catch (final Throwable yieldError) {
                        t.addSuppressed(yieldError);
                    }

                    logger.error("Unexpected error in automatic scheduling loop for {}. Will yield for {} and continue.", state.getConnectable(), adminYieldDuration, t);
                    waitForDelay(adminYieldNanos, state.getGeneration());
                }
            }
        } finally {
            state.workerStopped(worker);
        }
    }

    /**
     * Waits for the Processor to be ready, then invokes it while holding a global permit and a concurrent task slot.
     *
     * @return <code>false</code> if a global permit could not be acquired because scheduling stopped, <code>true</code> otherwise
     */
    private boolean runAutoSchedulingIteration(final ProcessorAutoSchedulingState state, final ProcessorTaskWorker worker) {
        // Wait without taking a global permit while the Processor cannot do anything useful: for example, it is yielded, its outgoing
        // connections are full, or it has no input. A source Processor that recently found no new data also waits until it is due to check again.
        if (!state.getConnectableTask().isReady() || !state.isSourceWorkCheckAllowed()) {
            waitForAutoReadiness(state, worker);
            return true;
        }

        final SchedulingGeneration generation = state.getGeneration();
        // Read the count before trying for a task slot, so that a slot released after the attempt still wakes this worker.
        final long taskSlotUpdateCount = generation.getTaskSlotUpdateCount();
        final ProcessorSchedulingMeasurements measurements = state.getMeasurements();
        measurements.recordTaskBusy();
        if (!acquirePermitWithPolling(state.getLifecycleState(), generation)) {
            measurements.recordTaskIdle();
            return false;
        }

        final long permitHoldStartNanos = System.nanoTime();
        final boolean concurrentTaskSlotAvailable;
        try {
            concurrentTaskSlotAvailable = invokeIfConcurrentTaskSlotAvailable(state, worker);
        } finally {
            globalPermitHoldNanos.add(System.nanoTime() - permitHoldStartNanos);
            // The Processor or a stop request may have interrupted this thread. Clear the interrupt so that it does not cut short the
            // worker's next wait; the worker notices that it has been stopped by checking the scheduling state.
            Thread.interrupted();
            globalSemaphore.release();
            measurements.recordTaskIdle();
        }

        if (!concurrentTaskSlotAvailable) {
            generation.waitForTaskSlotUpdate(taskSlotUpdateCount, worker);
        }

        return true;
    }

    /**
     * Invokes the Processor if it is still ready and one of its concurrent task slots is available. A source Processor that has
     * recently found no work is invoked only if no other concurrent task is already checking it for new work. Must be called
     * while holding a global permit.
     *
     * @return <code>false</code> if no concurrent task slot was available, <code>true</code> otherwise
     */
    private boolean invokeIfConcurrentTaskSlotAvailable(final ProcessorAutoSchedulingState state, final ProcessorTaskWorker worker) {
        if (!worker.isActive() || !state.getConnectableTask().isReady()) {
            return true;
        }

        final SchedulingSettings schedulingSettings = state.getSchedulingSettings();
        if (!state.tryAcquireConcurrentTaskSlot(schedulingSettings)) {
            return false;
        }

        try {
            final boolean sourceWorkCheckRequired = state.isSourceWorkCheckRequired();
            if (sourceWorkCheckRequired && !state.reserveSourceWorkCheck()) {
                return true;
            }

            final long invocationStartNanos = System.nanoTime();
            try {
                final BooleanSupplier keepInvoking = () -> !Thread.currentThread().isInterrupted() && worker.canKeepRunning(schedulingSettings);
                invokeWhileConditionTrue(state.getMaxRepeatedInvocationNanos(), System::nanoTime, keepInvoking, () -> invokeProcessor(state, worker, schedulingSettings));
            } finally {
                state.getMeasurements().recordInvocationDuration(System.nanoTime() - invocationStartNanos);
                if (sourceWorkCheckRequired) {
                    state.releaseSourceWorkCheck();
                }
            }
        } finally {
            state.releaseConcurrentTaskSlot();
        }

        return true;
    }

    /**
     * Invokes the Processor once, recording its measurements. A batch of work within the invocation stops early if scheduling stops,
     * the worker is retired, or the automatic scheduling settings change.
     */
    private InvocationResult invokeProcessor(final ProcessorAutoSchedulingState state, final ProcessorTaskWorker worker, final SchedulingSettings schedulingSettings) {
        final BooleanSupplier continueRunning = () -> worker.canKeepRunning(schedulingSettings);
        final InvocationObserver observer = state.getMeasurements().createInvocationObserver();
        final InvocationResult result = state.getConnectableTask().invoke(schedulingSettings.runDurationNanos(), continueRunning, observer);
        state.recordInvocationResult(result);
        return result;
    }

    /**
     * Invokes the Processor at least once, then keeps invoking it while each invocation finds work to do, the maximum duration has
     * not passed, and the condition is still true. A maximum duration of zero results in a single invocation.
     */
    static InvocationResult invokeWhileConditionTrue(final long maxDurationNanos, final LongSupplier nanoTimeSupplier, final BooleanSupplier condition, final Supplier<InvocationResult> invocation) {
        final long deadlineNanos = nanoTimeSupplier.getAsLong() + maxDurationNanos;
        while (true) {
            final InvocationResult result = invocation.get();
            if (result.getOutcome() != InvocationOutcome.INVOKED_WITH_ACTIVITY) {
                return result;
            }

            if (nanoTimeSupplier.getAsLong() >= deadlineNanos) {
                return result;
            }

            if (!condition.getAsBoolean()) {
                return result;
            }
        }
    }

    private void waitForAutoReadiness(final ProcessorAutoSchedulingState state, final ProcessorTaskWorker worker) {
        final SchedulingGeneration generation = state.getGeneration();
        final long changeSequence = generation.getChangeSequence();
        if (state.getConnectableTask().isReady() && state.isSourceWorkCheckAllowed()) {
            return;
        }

        final Instant now = Instant.now();
        long delayNanos = TimeUnit.SECONDS.toNanos(1L);
        final Instant queueDeadline = generation.getNextQueueDeadline();
        if (queueDeadline.isAfter(now)) {
            delayNanos = Math.min(delayNanos, Duration.between(now, queueDeadline).toNanos());
        }

        final long sourceDelayNanos = state.getSourceWorkCheckDelayNanos();
        if (sourceDelayNanos > 0L) {
            delayNanos = Math.min(delayNanos, sourceDelayNanos);
        }

        generation.awaitChange(changeSequence, delayNanos, worker);
    }

    @Override
    public void scheduleOnce(final Connectable connectable, final LifecycleState lifecycleState, final Callable<Future<Void>> stopCallback) {
        final String componentId = connectable.getIdentifier();
        final SchedulingGeneration generation;
        synchronized (lifecycleState) {
            generation = registerSchedulingGeneration(componentId);
            lifecycleState.setScheduled(true);
        }

        try {
            final ConnectableTask connectableTask = new ConnectableTask(this, connectable, flowController, contextFactory, lifecycleState, generation::isRunning);
            final String threadName = buildThreadName(connectable, 0);

            submitTask(threadName, generation, () -> {
                try {
                    runOnce(connectable, connectableTask, stopCallback, lifecycleState, generation);
                } finally {
                    stopSchedulingGeneration(componentId, generation, false);
                }
            });
        } catch (final Throwable t) {
            synchronized (lifecycleState) {
                if (stopSchedulingGeneration(componentId, generation, true)) {
                    lifecycleState.setScheduled(false);
                }
            }

            throw t;
        }
    }

    @Override
    public void unschedule(final Connectable connectable, final LifecycleState lifecycleState) {
        synchronized (lifecycleState) {
            final SchedulingGeneration generation = schedulingGenerations.remove(connectable.getIdentifier());
            if (generation != null) {
                generation.stop(false);
            }

            lifecycleState.setScheduled(false);
        }

        logger.info("Stopped scheduling {} to run", connectable);
    }

    @Override
    public void schedule(final ReportingTaskNode taskNode, final LifecycleState lifecycleState) {
        final boolean cronDriven = taskNode.getSchedulingStrategy() == SchedulingStrategy.CRON_DRIVEN;
        final CronExpression cronExpression;
        final long schedulingNanos;
        if (cronDriven) {
            cronExpression = parseCronExpression(taskNode.getSchedulingPeriod(), taskNode);
            schedulingNanos = 0L;
        } else {
            cronExpression = null;
            schedulingNanos = taskNode.getSchedulingPeriod(TimeUnit.NANOSECONDS);
        }

        final String componentId = taskNode.getIdentifier();
        final SchedulingGeneration generation;
        synchronized (lifecycleState) {
            generation = registerSchedulingGeneration(componentId);
            lifecycleState.setScheduled(true);
        }

        try {
            final Runnable reportingTaskWrapper = new ReportingTaskWrapper(taskNode, lifecycleState, flowController.getExtensionManager(), generation::isRunning);
            final String threadName = "Reporting Task: " + taskNode.getName();

            submitTask(threadName, generation,
                    () -> runReportingTaskLoop(taskNode, reportingTaskWrapper, schedulingNanos, cronExpression, lifecycleState, generation));

            logger.info("{} started on virtual thread", taskNode.getReportingTask());
        } catch (final Throwable t) {
            synchronized (lifecycleState) {
                if (stopSchedulingGeneration(componentId, generation, true)) {
                    lifecycleState.setScheduled(false);
                }
            }

            throw t;
        }
    }

    @Override
    public void unschedule(final ReportingTaskNode taskNode, final LifecycleState lifecycleState) {
        synchronized (lifecycleState) {
            final SchedulingGeneration generation = schedulingGenerations.remove(taskNode.getIdentifier());
            if (generation != null) {
                generation.stop(false);
            }

            lifecycleState.setScheduled(false);
        }

        logger.info("Stopped scheduling {} to run", taskNode.getReportingTask());
    }

    private SchedulingGeneration registerSchedulingGeneration(final String componentId) {
        if (shutdown.get()) {
            throw new IllegalStateException("VirtualThreadSchedulingAgent has been shut down and cannot accept new work");
        }

        final SchedulingGeneration generation = new SchedulingGeneration();
        final SchedulingGeneration existingGeneration = schedulingGenerations.putIfAbsent(componentId, generation);
        if (existingGeneration != null) {
            throw new IllegalStateException("Component " + componentId + " is already scheduled");
        }

        if (shutdown.get()) {
            stopSchedulingGeneration(componentId, generation, true);
            throw new IllegalStateException("VirtualThreadSchedulingAgent has been shut down and cannot accept new work");
        }

        return generation;
    }

    private boolean stopSchedulingGeneration(final String componentId, final SchedulingGeneration generation, final boolean interrupt) {
        final boolean removed = schedulingGenerations.remove(componentId, generation);
        generation.stop(interrupt);
        return removed;
    }

    private boolean isActive(final LifecycleState lifecycleState, final SchedulingGeneration generation) {
        return !shutdown.get() && lifecycleState.isScheduled() && !generation.isStopped();
    }

    private void evaluateAutoScheduling() {
        try {
            final long nowNanos = System.nanoTime();
            final int maxGlobalConcurrentTasks = globalSemaphore.getMaxPermits();
            globalTaskUsageSamples.increment();
            // A waiting task means every permit is effectively in use, even while a permit is handed to the next waiting task.
            if (globalSemaphore.getInUsePermits() >= maxGlobalConcurrentTasks || globalSemaphore.getWaitingThreadCount() > 0) {
                globalTaskSlotsFullyUsedSamples.increment();
            }

            if (nowNanos - lastSystemSchedulingSnapshotNanos >= TimeUnit.SECONDS.toNanos(1L)) {
                final long taskUsageSamples = globalTaskUsageSamples.sumThenReset();
                final double fractionOfSamplesWithAllGlobalTaskSlotsUsed = taskUsageSamples == 0L
                        ? 0D : globalTaskSlotsFullyUsedSamples.sumThenReset() / (double) taskUsageSamples;
                final long permitWaitNanos = globalPermitWaitNanos.sumThenReset();
                final long permitHoldNanos = globalPermitHoldNanos.sumThenReset();
                final double permitUseNanos = (double) permitWaitNanos + permitHoldNanos;
                final double fractionOfGlobalTaskTimeSpentWaiting = permitUseNanos == 0D ? 0D : permitWaitNanos / permitUseNanos;
                systemSchedulingSnapshot = systemSchedulingMetrics.captureSnapshot(maxGlobalConcurrentTasks,
                        fractionOfSamplesWithAllGlobalTaskSlotsUsed, fractionOfGlobalTaskTimeSpentWaiting);
                lastSystemSchedulingSnapshotNanos = nowNanos;
                balanceConcurrentTasks(nowNanos);
            }

            for (final SchedulingGeneration generation : schedulingGenerations.values()) {
                final ProcessorAutoSchedulingState state = generation.getAutoSchedulingState();
                if (state == null || generation.isStopped()) {
                    continue;
                }

                state.recordReadiness(nowNanos);
                startAutoWorkers(state, state.getConcurrentTasks());
                if (!state.isEvaluationDue(nowNanos)) {
                    continue;
                }

                final boolean globalCapacityTestAllowed = concurrentTaskMoveCoordinator.isTestAllowed(state);
                final ProcessorSchedulingSnapshot processorMeasurements = state.captureProcessorSchedulingSnapshot(nowNanos, globalCapacityTestAllowed);
                final long resetSequence = state.getResetSequence();
                final ProcessorSchedulingDecision decision = state.getController().evaluate(processorMeasurements, systemSchedulingSnapshot);
                logger.debug("Automatic scheduling evaluation for {}: processorMeasurements={}, systemMeasurements={}, decision={}",
                        state.getConnectable(), processorMeasurements, systemSchedulingSnapshot, decision);
                if (generation.isStopped() || schedulingGenerations.get(state.getConnectable().getIdentifier()) != generation
                        || state.getResetSequence() != resetSequence) {
                    continue;
                }

                state.recordDecision(decision);
                if (decision.applySettings()) {
                    final int selectedConcurrentTasks = Math.max(1, Math.min(decision.settings().concurrentTasks(),
                            Math.min(state.getMaxConcurrentTasks(), globalSemaphore.getMaxPermits())));
                    final SchedulingSettings settings = new SchedulingSettings(selectedConcurrentTasks,
                            state.isBatchingSupported() ? Math.min(decision.settings().runDurationNanos(), TimeUnit.MILLISECONDS.toNanos(25L)) : 0L);
                    state.applySettings(settings);
                    startAutoWorkers(state, selectedConcurrentTasks);
                }

                concurrentTaskMoveCoordinator.recordDecision(state, decision.reason());
            }
        } catch (final Throwable failure) {
            logger.error("Failed to evaluate automatic Processor scheduling", failure);
        }
    }

    /**
     * Updates each Processor's demand score and lets the coordinator end or start a concurrent task move.
     */
    private void balanceConcurrentTasks(final long nowNanos) {
        final List<ProcessorDemand> demands = new ArrayList<>();
        final Map<String, ProcessorAutoSchedulingState> statesByIdentifier = new HashMap<>();
        for (final SchedulingGeneration generation : schedulingGenerations.values()) {
            final ProcessorAutoSchedulingState state = generation.getAutoSchedulingState();
            final ProcessorSchedulingSnapshot snapshot = state == null ? null : state.getLastProcessorSchedulingSnapshot();
            if (snapshot == null || generation.isStopped()) {
                continue;
            }

            final int maxConcurrentTasks = Math.min(state.getMaxConcurrentTasks(), globalSemaphore.getMaxPermits());
            final StandardAutoSchedulingController controller = state.getController();
            final ProcessorDemand demand = new ProcessorDemand(state.getConnectable().getIdentifier(), state.isSourceProcessor(), state.getConcurrentTasks(),
                    maxConcurrentTasks, controller.isConcurrencyComparisonActive(), controller.isIncreaseCoolingDown(nowNanos), snapshot);
            state.setDemandScore(ConcurrentTaskMoveSelector.calculateScore(demand));
            demands.add(demand);
            statesByIdentifier.put(demand.identifier(), state);
        }

        concurrentTaskMoveCoordinator.balance(demands, statesByIdentifier, systemSchedulingSnapshot.globalCapacityFull());
    }

    private static CronExpression parseCronExpression(final String cronSchedule, final Object component) {
        try {
            return CronExpression.parse(cronSchedule);
        } catch (final RuntimeException e) {
            throw new IllegalStateException("Cannot schedule " + component + " to run because its scheduling period is not a valid CRON expression: " + cronSchedule, e);
        }
    }

    @Override
    public void onEvent(final Connectable connectable) {
        final SchedulingGeneration generation = schedulingGenerations.get(connectable.getIdentifier());
        if (generation != null) {
            final ProcessorAutoSchedulingState state = generation.getAutoSchedulingState();
            if (state != null) {
                requestControllerReset(generation, state, AutoSchedulingResetReason.PROCESSOR_CONNECTIONS_CHANGED);
            }

            generation.signalChange();
        }
    }

    @Override
    public synchronized void setMaxThreadCount(final int maxThreads) {
        globalSemaphore.setMaxPermits(maxThreads);
        resetAutoControllersForGlobalConcurrentTaskLimitChange();
        logger.info("Global semaphore permits updated to {}", maxThreads);
    }

    @Override
    public synchronized void incrementMaxThreadCount(final int toAdd) {
        if (toAdd == 0) {
            return;
        }

        final int currentMax = globalSemaphore.getMaxPermits();
        final int newMax = currentMax + toAdd;
        if (newMax < 1) {
            throw new IllegalStateException("Cannot remove " + (-toAdd) + " permits from global semaphore because there are only " + currentMax + " permits available");
        }

        globalSemaphore.setMaxPermits(newMax);
        resetAutoControllersForGlobalConcurrentTaskLimitChange();
    }

    private void resetAutoControllersForGlobalConcurrentTaskLimitChange() {
        for (final SchedulingGeneration generation : schedulingGenerations.values()) {
            final ProcessorAutoSchedulingState state = generation.getAutoSchedulingState();
            if (state != null) {
                requestControllerReset(generation, state, AutoSchedulingResetReason.GLOBAL_CONCURRENT_TASK_LIMIT_CHANGED);
                generation.signalChange();
            }
        }
    }

    private void requestControllerReset(final SchedulingGeneration generation, final ProcessorAutoSchedulingState state,
                                        final AutoSchedulingResetReason reason) {
        final long resetSequence = state.incrementResetSequence();
        try {
            schedulingEvaluationExecutor.execute(() -> {
                if (generation.isStopped() || state.getResetSequence() != resetSequence) {
                    return;
                }

                resetController(generation, state, reason);
            });
        } catch (final RejectedExecutionException e) {
            if (!shutdown.get()) {
                throw e;
            }
        }
    }

    /**
     * Restarts a Processor's measurements after an event that makes them no longer comparable. Runs on the evaluation thread.
     */
    private void resetController(final SchedulingGeneration generation, final ProcessorAutoSchedulingState state, final AutoSchedulingResetReason reason) {
        if (reason == AutoSchedulingResetReason.PROCESSOR_CONNECTIONS_CHANGED) {
            generation.clearQueueRegistrations();
            registerQueueListeners(state.getConnectable(), generation);
        }

        state.resetMeasurementsAfterEvent();
        if (reason == AutoSchedulingResetReason.GLOBAL_CONCURRENT_TASK_LIMIT_CHANGED) {
            final SchedulingSettings currentSettings = state.getSchedulingSettings();
            final int selectedConcurrentTasks = Math.min(currentSettings.concurrentTasks(), globalSemaphore.getMaxPermits());
            final SchedulingSettings clampedSettings = new SchedulingSettings(selectedConcurrentTasks, currentSettings.runDurationNanos());
            state.applySettings(clampedSettings);
            startAutoWorkers(state, selectedConcurrentTasks);
        }

        state.restartEvaluation();
    }

    @Override
    public void setAdministrativeYieldDuration(final String duration) {
        this.adminYieldNanos = FormatUtils.getTimeDuration(duration, TimeUnit.NANOSECONDS);
        this.adminYieldDuration = duration;
    }

    @Override
    public String getAdministrativeYieldDuration() {
        return adminYieldDuration;
    }

    @Override
    public long getAdministrativeYieldDuration(final TimeUnit timeUnit) {
        return timeUnit.convert(adminYieldNanos, TimeUnit.NANOSECONDS);
    }

    DynamicSemaphore getGlobalSemaphore() {
        return globalSemaphore;
    }

    int getRunningThreadCount() {
        return runningThreadCount.get();
    }

    void requestAutoSchedulingSettings(final String componentIdentifier, final SchedulingSettings settings) {
        final SchedulingGeneration generation = schedulingGenerations.get(componentIdentifier);
        if (generation == null || generation.getAutoSchedulingState() == null) {
            throw new IllegalStateException("Component " + componentIdentifier + " is not scheduled using automatic scheduling");
        }

        schedulingEvaluationExecutor.execute(() -> {
            final ProcessorAutoSchedulingState state = generation.getAutoSchedulingState();
            if (state == null || generation.isStopped() || schedulingGenerations.get(componentIdentifier) != generation) {
                return;
            }

            state.applySettings(settings);
            startAutoWorkers(state, settings.concurrentTasks());
        });
    }

    int getAutoSchedulingWaiterCount(final String componentIdentifier) {
        final SchedulingGeneration generation = schedulingGenerations.get(componentIdentifier);
        return generation == null ? 0 : generation.getWaiterCount();
    }

    ProcessorSchedulingMeasurements getAutoSchedulingMeasurements(final String componentIdentifier) {
        final SchedulingGeneration generation = schedulingGenerations.get(componentIdentifier);
        final ProcessorAutoSchedulingState state = generation == null ? null : generation.getAutoSchedulingState();
        return state == null ? null : state.getMeasurements();
    }

    int getAutoSchedulingTaskSlotWaiterCount(final String componentIdentifier) {
        final SchedulingGeneration generation = schedulingGenerations.get(componentIdentifier);
        return generation == null ? 0 : generation.getTaskSlotWaiterCount();
    }

    long getAutoSchedulingTaskSlotUpdateCount(final String componentIdentifier) {
        final SchedulingGeneration generation = schedulingGenerations.get(componentIdentifier);
        return generation == null ? -1L : generation.getTaskSlotUpdateCount();
    }

    boolean isShutdown() {
        return shutdown.get();
    }

    /**
     * @return number of component invocations currently holding global permits
     */
    public int getActiveThreadCount() {
        return globalSemaphore.getInUsePermits();
    }

    public AutoSchedulingDiagnostics getAutoSchedulingDiagnostics(final String componentIdentifier) {
        final SchedulingGeneration generation = schedulingGenerations.get(componentIdentifier);
        final ProcessorAutoSchedulingState state = generation == null ? null : generation.getAutoSchedulingState();
        return state == null ? null : state.getDiagnostics();
    }

    private void runSchedulingLoop(final Connectable connectable, final ConnectableTask connectableTask, final long schedulingNanos,
                                   final LifecycleState lifecycleState, final SchedulingGeneration generation, final CronExpression cronExpression) {
        final boolean cronDriven = cronExpression != null;

        OffsetDateTime nextCronSchedule = null;
        if (cronDriven) {
            nextCronSchedule = getNextCronSchedule(OffsetDateTime.now(), cronExpression);
            if (nextCronSchedule == null) {
                logger.warn("CRON expression for {} has no future firings; scheduling loop will exit without invoking the component", connectable);
                return;
            }

            final long initialDelayMillis = Math.max(nextCronSchedule.toInstant().toEpochMilli() - System.currentTimeMillis(), 0L);
            if (initialDelayMillis > 0L) {
                waitForDelay(TimeUnit.MILLISECONDS.toNanos(initialDelayMillis), generation);
            }
        }

        while (true) {
            try {
                if (!acquirePermitWithPolling(lifecycleState, generation)) {
                    return;
                }

                final InvocationResult invocationResult;
                final long permitHoldStartNanos = System.nanoTime();
                try {
                    invocationResult = connectableTask.invoke();
                } finally {
                    // Interrupt status from one invocation must not carry into the scheduling loop.
                    Thread.interrupted();
                    globalPermitHoldNanos.add(System.nanoTime() - permitHoldStartNanos);
                    globalSemaphore.release();
                }

                if (cronDriven) {
                    nextCronSchedule = getNextCronSchedule(nextCronSchedule, cronExpression);
                    if (nextCronSchedule == null) {
                        logger.warn("CRON expression for {} has no further firings after the current invocation; scheduling loop is exiting", connectable);
                        return;
                    }

                    final long sleepMillis = Math.max(nextCronSchedule.toInstant().toEpochMilli() - System.currentTimeMillis(), 0L);
                    waitForDelay(TimeUnit.MILLISECONDS.toNanos(sleepMillis), generation);
                } else {
                    waitForNextInvocation(connectable, schedulingNanos, generation, invocationResult);
                }
            } catch (final Throwable t) {
                if (!isActive(lifecycleState, generation)) {
                    return;
                }

                try {
                    connectable.yield(adminYieldNanos, TimeUnit.NANOSECONDS);
                } catch (final Throwable yieldError) {
                    t.addSuppressed(yieldError);
                }

                logger.error("Unexpected error in scheduling loop for {}. Will yield for {} and continue.", connectable, adminYieldDuration, t);
                waitForDelay(adminYieldNanos, generation);
            }
        }
    }

    private void runOnce(final Connectable connectable, final ConnectableTask connectableTask, final Callable<Future<Void>> stopCallback,
                         final LifecycleState lifecycleState, final SchedulingGeneration generation) {
        try {
            if (!acquirePermitWithPolling(lifecycleState, generation)) {
                if (isActive(lifecycleState, generation)) {
                    logger.warn("Run once request for {} was not executed because permit acquisition was interrupted", connectable);
                } else {
                    logger.warn("Run once request for {} was not executed because scheduling is no longer active", connectable);
                }

                return;
            }

            final long permitHoldStartNanos = System.nanoTime();
            try {
                connectableTask.invoke();
            } finally {
                globalPermitHoldNanos.add(System.nanoTime() - permitHoldStartNanos);
                globalSemaphore.release();
            }
        } catch (final Throwable t) {
            logger.error("Unexpected error running {} once", connectable, t);
        } finally {
            try {
                stopCallback.call();
            } catch (final Throwable t) {
                logger.error("Error while stopping {} after running once", connectable, t);
            }
        }
    }

    private void runReportingTaskLoop(final ReportingTaskNode taskNode, final Runnable reportingTaskWrapper, final long schedulingNanos,
                                      final CronExpression cronExpression, final LifecycleState lifecycleState, final SchedulingGeneration generation) {
        final boolean cronDriven = cronExpression != null;

        OffsetDateTime nextCronSchedule = null;
        if (cronDriven) {
            nextCronSchedule = getNextCronSchedule(OffsetDateTime.now(), cronExpression);
            if (nextCronSchedule == null) {
                logger.warn("CRON expression for {} has no future firings; scheduling loop will exit without invoking the reporting task",
                        taskNode.getReportingTask());
                return;
            }

            final long initialDelayMillis = Math.max(nextCronSchedule.toInstant().toEpochMilli() - System.currentTimeMillis(), 0L);
            if (initialDelayMillis > 0L) {
                waitForDelay(TimeUnit.MILLISECONDS.toNanos(initialDelayMillis), generation);
            }
        }

        while (true) {
            try {
                if (!acquirePermitWithPolling(lifecycleState, generation)) {
                    return;
                }

                final long permitHoldStartNanos = System.nanoTime();
                try {
                    reportingTaskWrapper.run();
                } finally {
                    // Interrupt status from one invocation must not carry into the scheduling loop.
                    Thread.interrupted();
                    globalPermitHoldNanos.add(System.nanoTime() - permitHoldStartNanos);
                    globalSemaphore.release();
                }

                if (cronDriven) {
                    nextCronSchedule = getNextCronSchedule(nextCronSchedule, cronExpression);
                    if (nextCronSchedule == null) {
                        logger.warn("CRON expression for {} has no further firings after the current invocation; scheduling loop is exiting",
                                taskNode.getReportingTask());
                        return;
                    }

                    final long sleepMillis = Math.max(nextCronSchedule.toInstant().toEpochMilli() - System.currentTimeMillis(), 0L);
                    waitForDelay(TimeUnit.MILLISECONDS.toNanos(sleepMillis), generation);
                } else {
                    waitForDelay(schedulingNanos, generation);
                }
            } catch (final Throwable t) {
                if (!isActive(lifecycleState, generation)) {
                    return;
                }

                logger.error("Unexpected error in scheduling loop for {}. Will wait for {} and continue.", taskNode.getReportingTask(), adminYieldDuration, t);
                waitForDelay(adminYieldNanos, generation);
            }
        }
    }

    private void waitForNextInvocation(final Connectable connectable, final long schedulingNanos, final SchedulingGeneration generation,
                                       final InvocationResult invocationResult) {
        final long sleepNanos;
        final long yieldExpiration = connectable.getYieldExpiration();
        final long yieldDelayNanos;
        if (yieldExpiration == 0L) {
            yieldDelayNanos = 0L;
        } else {
            yieldDelayNanos = TimeUnit.MILLISECONDS.toNanos(Math.max(yieldExpiration - System.currentTimeMillis(), 0L));
        }

        if (yieldDelayNanos > 0L) {
            sleepNanos = Math.max(schedulingNanos, yieldDelayNanos);
        } else if (invocationResult.isYield()) {
            sleepNanos = noWorkYieldNanos > 0L ? noWorkYieldNanos : schedulingNanos;
        } else {
            sleepNanos = schedulingNanos;
        }

        waitForDelay(sleepNanos, generation);
    }

    private boolean acquirePermitWithPolling(final LifecycleState lifecycleState, final SchedulingGeneration generation) {
        final long waitStartNanos = System.nanoTime();
        while (isActive(lifecycleState, generation)) {
            try {
                if (globalSemaphore.tryAcquire(PERMIT_POLL_INTERVAL_NANOS, TimeUnit.NANOSECONDS)) {
                    if (isActive(lifecycleState, generation)) {
                        globalPermitWaitNanos.add(System.nanoTime() - waitStartNanos);
                        return true;
                    }

                    globalSemaphore.release();
                    return false;
                }
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        return false;
    }

    private void waitForDelay(final long delayNanos, final SchedulingGeneration generation) {
        if (delayNanos <= Triggerable.MINIMUM_SCHEDULING_NANOS) {
            return;
        }

        try {
            generation.awaitStop(delayNanos, TimeUnit.NANOSECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static String buildThreadName(final Connectable connectable, final int taskIndex) {
        return "%s[type=%s, id=%s, group=%s] task %d".formatted(connectable.getName(), connectable.getComponentType(), connectable.getIdentifier(),
                connectable.getProcessGroup().getName(), taskIndex);
    }

    private void submitTask(final String threadName, final SchedulingGeneration generation, final Runnable task) {
        final Runnable trackedTask = () -> {
            final Thread currentThread = Thread.currentThread();
            currentThread.setName(threadName);
            generation.addThread(currentThread);
            runningThreadCount.incrementAndGet();

            try {
                if (!shutdown.get() && !generation.isStopped()) {
                    task.run();
                }
            } finally {
                runningThreadCount.decrementAndGet();
                generation.removeThread(currentThread);
            }
        };

        executorService.execute(trackedTask);
    }

    private static OffsetDateTime getNextCronSchedule(final OffsetDateTime currentSchedule, final CronExpression cronExpression) {
        final OffsetDateTime now = OffsetDateTime.now();
        return cronExpression.next(now.isAfter(currentSchedule) ? now : currentSchedule);
    }

    private static class ProcessorTaskWorker {
        private final ProcessorAutoSchedulingState state;
        private final long identifier;
        private final AtomicBoolean retired = new AtomicBoolean();

        ProcessorTaskWorker(final ProcessorAutoSchedulingState state, final long identifier) {
            this.state = state;
            this.identifier = identifier;
        }

        long getIdentifier() {
            return identifier;
        }

        boolean isRetired() {
            return retired.get();
        }

        void retire() {
            retired.set(true);
        }

        /**
         * @return <code>true</code> if the Processor is still scheduled to run and this worker has not been retired
         */
        boolean isActive() {
            return state.isScheduledToRun() && !isRetired();
        }

        /**
         * @return <code>true</code> if this worker is still active and the Processor's automatic scheduling settings are still the given settings
         */
        boolean canKeepRunning(final SchedulingSettings schedulingSettings) {
            return isActive() && state.areSchedulingSettingsCurrent(schedulingSettings);
        }
    }

    private record WaitingProcessorTask(Thread thread, ProcessorTaskWorker worker) {
    }

    private class ProcessorAutoSchedulingState implements ConcurrentTaskMoveCoordinator.Participant {
        private final Connectable connectable;
        private final ConnectableTask connectableTask;
        private final LifecycleState lifecycleState;
        private final SchedulingGeneration generation;
        private final int maxConcurrentTasks;
        private final boolean batchingSupported;
        private final boolean sourceProcessor;
        private final StandardAutoSchedulingController controller;
        private final ProcessorSchedulingMeasurements measurements = new ProcessorSchedulingMeasurements();
        private final AtomicReference<SchedulingSettings> schedulingSettings;
        private final AtomicInteger activeProcessorInvocations = new AtomicInteger();
        private final ConcurrentMap<Long, ProcessorTaskWorker> workers = new ConcurrentHashMap<>();
        private final AtomicLong workerSequence = new AtomicLong();
        private final AtomicBoolean sourceWorkCheckInProgress = new AtomicBoolean();
        private final AtomicLong resetSequence = new AtomicLong();
        private final long evaluationIntervalNanos;
        private final long baseSourceDelayNanos;
        private final long maximumSourceDelayNanos;

        private volatile long nextEvaluationNanos;
        private volatile long lastEvaluationNanos = System.nanoTime();
        private volatile long lastReadinessMeasurementNanos = System.nanoTime();
        private volatile long previousInputQueueSize;
        private volatile long sourceDelayNanos;
        private volatile long nextSourceWorkCheckNanos;
        private volatile long lastSourceActivityNanos;
        private volatile ProcessorSchedulingDecision lastDecision;
        private volatile ProcessorSchedulingDecision lastChangeDecision;
        private volatile ProcessorSchedulingSnapshot lastProcessorSchedulingSnapshot;
        private volatile boolean previousPrimaryNode;
        private volatile boolean measurementsSupported;
        private volatile double demandScore;

        ProcessorAutoSchedulingState(final Connectable connectable, final ConnectableTask connectableTask, final LifecycleState lifecycleState, final SchedulingGeneration generation,
                                     final int maxConcurrentTasks, final boolean batchingSupported, final boolean processorTriggeredSerially) {
            this.connectable = connectable;
            this.connectableTask = connectableTask;
            this.lifecycleState = lifecycleState;
            this.generation = generation;
            this.maxConcurrentTasks = maxConcurrentTasks;
            this.batchingSupported = batchingSupported;
            schedulingSettings = new AtomicReference<>(new SchedulingSettings(1, batchingSupported ? TimeUnit.MILLISECONDS.toNanos(25L) : 0L));
            sourceProcessor = connectable.isTriggerWhenEmpty() || !connectable.hasIncomingConnection() || !Connectables.hasNonLoopConnection(connectable);
            controller = new StandardAutoSchedulingController(maxConcurrentTasks, batchingSupported, processorTriggeredSerially,
                    autoMaxConcurrentTasksBasedOnAvailableProcessors, availableProcessorCount);
            final double stagger = ((Math.floorMod(connectable.getIdentifier().hashCode(), 201) - 100) / 1000D);
            this.evaluationIntervalNanos = (long) (TimeUnit.SECONDS.toNanos(1L) * (1D + stagger));
            this.nextEvaluationNanos = lastEvaluationNanos + evaluationIntervalNanos;
            this.baseSourceDelayNanos = Math.max(noWorkYieldNanos, TimeUnit.MILLISECONDS.toNanos(1L));
            this.maximumSourceDelayNanos = Math.max(baseSourceDelayNanos, TimeUnit.MILLISECONDS.toNanos(100L));
            this.previousPrimaryNode = flowController.isPrimary();
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

        synchronized List<ProcessorTaskWorker> resizeWorkers(final int targetWorkerCount) {
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

                generation.signalChange();
                generation.signalTaskSlotUpdate();
                return List.of();
            }

            final List<ProcessorTaskWorker> newWorkers = new ArrayList<>();
            for (int index = activeWorkers.size(); index < targetWorkerCount; index++) {
                final ProcessorTaskWorker worker = new ProcessorTaskWorker(this, workerSequence.getAndIncrement());
                workers.put(worker.getIdentifier(), worker);
                newWorkers.add(worker);
            }

            return newWorkers;
        }

        void workerStopped(final ProcessorTaskWorker worker) {
            workers.remove(worker.getIdentifier(), worker);
            generation.signalChange();
            generation.signalTaskSlotUpdate();
        }

        /**
         * @return <code>true</code> if NiFi is not shutting down, the Processor is scheduled, and this scheduling generation has not been stopped
         */
        boolean isScheduledToRun() {
            return VirtualThreadSchedulingAgent.this.isActive(lifecycleState, generation);
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
                    generation.signalTaskSlotUpdate();
                    return false;
                }
            }
        }

        void releaseConcurrentTaskSlot() {
            activeProcessorInvocations.decrementAndGet();
            generation.signalTaskSlotUpdate();
        }

        /**
         * @return <code>true</code> unless this is a source Processor that recently found no new data and is either not yet due to check
         * again or is already being checked by another concurrent task
         */
        boolean isSourceWorkCheckAllowed() {
            if (!isSourceWorkCheckRequired()) {
                return true;
            }

            if (sourceWorkCheckInProgress.get()) {
                return false;
            }

            return System.nanoTime() >= nextSourceWorkCheckNanos;
        }

        boolean reserveSourceWorkCheck() {
            return sourceWorkCheckInProgress.compareAndSet(false, true);
        }

        /**
         * @return <code>true</code> if this is a source Processor that has no queued input and recently found no new data, so only one
         * concurrent task at a time should check it for new data
         */
        boolean isSourceWorkCheckRequired() {
            if (!sourceProcessor || sourceDelayNanos == 0L) {
                return false;
            }

            return !connectableTask.hasLocallyConsumableInput();
        }

        void releaseSourceWorkCheck() {
            sourceWorkCheckInProgress.set(false);
            generation.signalChange();
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

        long getSourceWorkCheckDelayNanos() {
            return Math.max(0L, nextSourceWorkCheckNanos - System.nanoTime());
        }

        void recordInvocationResult(final InvocationResult result) {
            if (!sourceProcessor) {
                return;
            }

            if (result.getOutcome() == InvocationOutcome.INVOKED_WITH_ACTIVITY) {
                sourceDelayNanos = 0L;
                nextSourceWorkCheckNanos = 0L;
                lastSourceActivityNanos = System.nanoTime();
                generation.signalChange();
            } else if (result.getOutcome() == InvocationOutcome.INVOKED_WITHOUT_ACTIVITY
                    && activeProcessorInvocations.get() <= 1
                    && !connectableTask.hasLocallyConsumableInput()) {
                sourceDelayNanos = sourceDelayNanos == 0L ? baseSourceDelayNanos : Math.min(maximumSourceDelayNanos, sourceDelayNanos * 2L);
                nextSourceWorkCheckNanos = System.nanoTime() + sourceDelayNanos;
            }
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
            final boolean currentPrimaryNode = flowController.isPrimary();
            final boolean primaryNodeChanged = currentPrimaryNode != previousPrimaryNode;
            previousPrimaryNode = currentPrimaryNode;
            final boolean sourceProcessorRecentlyReportedActivity = hasSourceProcessorRecentlyReportedActivity(nowNanos);
            final ProcessorSchedulingSnapshot snapshot = measurements.captureSnapshot(nowNanos, measurementWindowNanos, currentSettings, connectableTask.isReady(),
                    inputQueueHasFlowFiles, inputQueueSize, inputQueueGrowth, sourceProcessorRecentlyReportedActivity, primaryNodeChanged, globalCapacityTestAllowed);
            if (snapshot.committedFlowFiles() > 0L) {
                measurementsSupported = true;
            }

            lastProcessorSchedulingSnapshot = snapshot;
            return snapshot;
        }

        /**
         * @return <code>true</code> if this is a source Processor that has not come up empty since it last found new data, and it is either
         * running now or found new data within the last two seconds
         */
        private boolean hasSourceProcessorRecentlyReportedActivity(final long nowNanos) {
            if (!sourceProcessor || sourceDelayNanos > 0L) {
                return false;
            }

            if (activeProcessorInvocations.get() > 0) {
                return true;
            }

            if (lastSourceActivityNanos == 0L) {
                return false;
            }

            return nowNanos - lastSourceActivityNanos < TimeUnit.SECONDS.toNanos(2);
        }

        void resetMeasurementsAfterEvent() {
            final long nowNanos = System.nanoTime();
            final SchedulingSettings currentSettings = schedulingSettings.get();
            // Workers compare settings by identity so a reset also ends batches whose values have not changed.
            schedulingSettings.set(new SchedulingSettings(currentSettings.concurrentTasks(), currentSettings.runDurationNanos()));
            lastEvaluationNanos = nowNanos;
            lastReadinessMeasurementNanos = nowNanos;
            long inputQueueSize = 0L;
            for (final Connection connection : connectable.getIncomingConnections()) {
                inputQueueSize += connection.getFlowFileQueue().getLocalQueueSize().getObjectCount();
            }

            previousInputQueueSize = inputQueueSize;
            lastProcessorSchedulingSnapshot = null;
            generation.signalChange();
        }

        void applySettings(final SchedulingSettings settings) {
            final SchedulingSettings currentSettings = schedulingSettings.get();
            if (!currentSettings.equals(settings)) {
                schedulingSettings.set(new SchedulingSettings(settings.concurrentTasks(), settings.runDurationNanos()));
                generation.signalChange();
                generation.signalTaskSlotUpdate();
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
            return !generation.isStopped() && schedulingGenerations.get(connectable.getIdentifier()) == generation;
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
            final SchedulingSettings currentSettings = schedulingSettings.get();
            final int updatedConcurrentTasks = currentSettings.concurrentTasks() + change;
            if (!isActive() || updatedConcurrentTasks < 1 || updatedConcurrentTasks > Math.min(maxConcurrentTasks, globalSemaphore.getMaxPermits())) {
                return false;
            }

            applySettings(new SchedulingSettings(updatedConcurrentTasks, currentSettings.runDurationNanos()));
            startAutoWorkers(this, updatedConcurrentTasks);
            resetController(generation, this, AutoSchedulingResetReason.CONCURRENT_TASK_MOVED);
            return true;
        }

        @Override
        public String toString() {
            return connectable.toString();
        }

        AutoSchedulingDiagnostics getDiagnostics() {
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
                    .setTaskMoveRole(concurrentTaskMoveCoordinator.getRole(this))
                    .build();
        }
    }

    private class SchedulingGeneration {
        private final CountDownLatch stopSignal = new CountDownLatch(1);
        private final Set<Thread> threads = ConcurrentHashMap.newKeySet();
        private final Set<WaitingProcessorTask> waiters = ConcurrentHashMap.newKeySet();
        private final Set<WaitingProcessorTask> taskSlotWaiters = ConcurrentHashMap.newKeySet();
        private final Set<FlowFileQueue> queues = ConcurrentHashMap.newKeySet();
        private final List<QueueSchedulingRegistration> queueRegistrations = new ArrayList<>();
        private final AtomicBoolean interruptRequested = new AtomicBoolean();
        private final AtomicLong changeSequence = new AtomicLong();
        private final AtomicLong taskSlotUpdateCount = new AtomicLong();
        private volatile ProcessorAutoSchedulingState autoSchedulingState;

        void setAutoSchedulingState(final ProcessorAutoSchedulingState autoSchedulingState) {
            this.autoSchedulingState = autoSchedulingState;
        }

        ProcessorAutoSchedulingState getAutoSchedulingState() {
            return autoSchedulingState;
        }

        void addQueue(final FlowFileQueue queue) {
            queues.add(queue);
        }

        synchronized void addQueueRegistration(final QueueSchedulingRegistration registration) {
            if (isStopped()) {
                registration.close();
            } else {
                queueRegistrations.add(registration);
            }
        }

        synchronized void clearQueueRegistrations() {
            for (final QueueSchedulingRegistration registration : queueRegistrations) {
                registration.close();
            }

            queueRegistrations.clear();
            queues.clear();
        }

        /**
         * @return the earliest time at which the FlowFile at the front of one of the Processor's queues becomes available, or
         * {@link Instant#EPOCH} if none of the queues knows of such a time
         */
        Instant getNextQueueDeadline() {
            Instant earliestDeadline = Instant.EPOCH;
            for (final FlowFileQueue queue : queues) {
                final Instant deadline = queue.getNextFlowFileAvailabilityTime();
                if (!deadline.isAfter(Instant.EPOCH)) {
                    continue;
                }

                if (earliestDeadline.equals(Instant.EPOCH) || deadline.isBefore(earliestDeadline)) {
                    earliestDeadline = deadline;
                }
            }

            return earliestDeadline;
        }

        long getChangeSequence() {
            return changeSequence.get();
        }

        int getWaiterCount() {
            return waiters.size();
        }

        /**
         * @return a count that increases each time a concurrent task slot may have become available: a slot is released, a worker stops
         * or is retired, the scheduling settings change, or scheduling stops
         */
        long getTaskSlotUpdateCount() {
            return taskSlotUpdateCount.get();
        }

        int getTaskSlotWaiterCount() {
            return taskSlotWaiters.size();
        }

        void signalChange() {
            changeSequence.incrementAndGet();
            final ProcessorAutoSchedulingState state = autoSchedulingState;
            final int wakeLimit = state == null ? waiters.size() : state.getConcurrentTasks();
            int awakened = 0;
            for (final WaitingProcessorTask waiter : waiters) {
                if (waiter.worker().isRetired()) {
                    waiters.remove(waiter);
                    continue;
                }

                LockSupport.unpark(waiter.thread());
                if (++awakened >= wakeLimit) {
                    break;
                }
            }
        }

        void awaitChange(final long expectedSequence, final long delayNanos, final ProcessorTaskWorker worker) {
            final Thread currentThread = Thread.currentThread();
            final WaitingProcessorTask waiter = new WaitingProcessorTask(currentThread, worker);
            waiters.add(waiter);
            try {
                if (!isStopped() && !worker.isRetired() && changeSequence.get() == expectedSequence) {
                    LockSupport.parkNanos(Math.max(1L, delayNanos));
                }
            } finally {
                waiters.remove(waiter);
            }
        }

        void signalTaskSlotUpdate() {
            taskSlotUpdateCount.incrementAndGet();
            // Retired waiters are woken as well so that they observe retirement and exit.
            for (final WaitingProcessorTask waiter : taskSlotWaiters) {
                LockSupport.unpark(waiter.thread());
            }
        }

        /**
         * Waits until the task slot update count differs from the given count, the worker is retired, or scheduling stops.
         */
        void waitForTaskSlotUpdate(final long previousUpdateCount, final ProcessorTaskWorker worker) {
            final Thread currentThread = Thread.currentThread();
            final WaitingProcessorTask waiter = new WaitingProcessorTask(currentThread, worker);
            taskSlotWaiters.add(waiter);
            try {
                while (!isStopped() && !worker.isRetired() && taskSlotUpdateCount.get() == previousUpdateCount) {
                    LockSupport.park();
                }
            } finally {
                taskSlotWaiters.remove(waiter);
            }
        }

        void addThread(final Thread thread) {
            threads.add(thread);

            if (interruptRequested.get()) {
                thread.interrupt();
            }
        }

        void removeThread(final Thread thread) {
            threads.remove(thread);
        }

        void stop(final boolean interrupt) {
            if (interrupt) {
                interruptRequested.set(true);
            }

            stopSignal.countDown();
            signalChange();
            signalTaskSlotUpdate();
            clearQueueRegistrations();

            if (interruptRequested.get()) {
                for (final Thread thread : threads) {
                    thread.interrupt();
                }
            }
        }

        boolean isStopped() {
            return stopSignal.getCount() == 0L;
        }

        boolean isRunning() {
            return !isStopped();
        }

        void awaitStop(final long timeout, final TimeUnit timeUnit) throws InterruptedException {
            stopSignal.await(timeout, timeUnit);
        }
    }
}
