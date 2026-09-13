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
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.Triggerable;
import org.apache.nifi.controller.tasks.ConnectableTask;
import org.apache.nifi.controller.tasks.InvocationResult;
import org.apache.nifi.controller.tasks.ReportingTaskWrapper;
import org.apache.nifi.nar.NarThreadContextClassLoader;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.support.CronExpression;

import java.time.OffsetDateTime;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Scheduling agent that runs components on virtual threads. A {@link DynamicSemaphore}
 * limits the number of component invocations that can run concurrently.
 */
public class VirtualThreadSchedulingAgent implements SchedulingAgent {

    private static final Logger logger = LoggerFactory.getLogger(VirtualThreadSchedulingAgent.class);

    private static final long PERMIT_POLL_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(1L);

    private final FlowController flowController;
    private final RepositoryContextFactory contextFactory;
    private final DynamicSemaphore globalSemaphore;
    private final long noWorkYieldNanos;
    private final ExecutorService executorService;
    private final ConcurrentMap<String, SchedulingGeneration> schedulingGenerations = new ConcurrentHashMap<>();
    private final AtomicBoolean shutdown = new AtomicBoolean();
    private final AtomicInteger runningThreadCount = new AtomicInteger();
    private volatile String adminYieldDuration = "1 sec";
    private volatile long adminYieldNanos = TimeUnit.SECONDS.toNanos(1L);

    public VirtualThreadSchedulingAgent(final FlowController flowController, final RepositoryContextFactory contextFactory,
                                        final NiFiProperties nifiProperties, final int maxThreadCount) {
        this.flowController = flowController;
        this.contextFactory = contextFactory;
        this.globalSemaphore = new DynamicSemaphore(maxThreadCount);

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
        logger.info("VirtualThreadSchedulingAgent initialized with {} permits", maxThreadCount);
    }

    @Override
    public void shutdown() {
        signalShutdown(true);
        executorService.shutdownNow();
    }

    public void shutdownGracefully() {
        signalShutdown(false);
        executorService.shutdown();
    }

    private void signalShutdown(final boolean interrupt) {
        shutdown.set(true);

        for (final SchedulingGeneration generation : schedulingGenerations.values()) {
            generation.stop(interrupt);
        }
    }

    public boolean awaitTermination(final long timeout, final TimeUnit timeUnit) throws InterruptedException {
        return executorService.awaitTermination(timeout, timeUnit);
    }

    public boolean isTerminated() {
        return executorService.isTerminated();
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
            final ConnectableTask connectableTask = new ConnectableTask(this, connectable, flowController, contextFactory, lifecycleState);
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

    @Override
    public void scheduleOnce(final Connectable connectable, final LifecycleState lifecycleState, final Callable<Future<Void>> stopCallback) {
        final String componentId = connectable.getIdentifier();
        final SchedulingGeneration generation;
        synchronized (lifecycleState) {
            generation = registerSchedulingGeneration(componentId);
            lifecycleState.setScheduled(true);
        }

        try {
            final ConnectableTask connectableTask = new ConnectableTask(this, connectable, flowController, contextFactory, lifecycleState);
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
            final Runnable reportingTaskWrapper = new ReportingTaskWrapper(taskNode, lifecycleState, flowController.getExtensionManager());
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

    private static CronExpression parseCronExpression(final String cronSchedule, final Object component) {
        try {
            return CronExpression.parse(cronSchedule);
        } catch (final RuntimeException e) {
            throw new IllegalStateException("Cannot schedule " + component + " to run because its scheduling period is not a valid CRON expression: " + cronSchedule, e);
        }
    }

    @Override
    public void onEvent(final Connectable connectable) {
    }

    @Override
    public synchronized void setMaxThreadCount(final int maxThreads) {
        globalSemaphore.setMaxPermits(maxThreads);
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

    boolean isShutdown() {
        return shutdown.get();
    }

    /**
     * @return number of component invocations currently holding global permits
     */
    public int getActiveThreadCount() {
        return globalSemaphore.getInUsePermits();
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
                try {
                    invocationResult = connectableTask.invoke();
                } finally {
                    // Interrupt status from one invocation must not carry into the scheduling loop.
                    Thread.interrupted();
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

            try {
                connectableTask.invoke();
            } finally {
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

                try {
                    reportingTaskWrapper.run();
                } finally {
                    // Interrupt status from one invocation must not carry into the scheduling loop.
                    Thread.interrupted();
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
        while (isActive(lifecycleState, generation)) {
            try {
                if (globalSemaphore.tryAcquire(PERMIT_POLL_INTERVAL_NANOS, TimeUnit.NANOSECONDS)) {
                    if (isActive(lifecycleState, generation)) {
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
        return connectable.getName() + "[type=" + connectable.getComponentType() + ", id=" + connectable.getIdentifier()
                + ", group=" + connectable.getProcessGroup().getName() + "] task " + taskIndex;
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

    private static class SchedulingGeneration {
        private final CountDownLatch stopSignal = new CountDownLatch(1);
        private final Set<Thread> threads = ConcurrentHashMap.newKeySet();
        private final AtomicBoolean interruptRequested = new AtomicBoolean();

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

            if (interruptRequested.get()) {
                for (final Thread thread : threads) {
                    thread.interrupt();
                }
            }
        }

        boolean isStopped() {
            return stopSignal.getCount() == 0L;
        }

        void awaitStop(final long timeout, final TimeUnit timeUnit) throws InterruptedException {
            stopSignal.await(timeout, timeUnit);
        }
    }
}
