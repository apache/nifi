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
import org.apache.nifi.controller.tasks.ConnectableTask;
import org.apache.nifi.controller.tasks.InvocationResult;
import org.apache.nifi.controller.tasks.ReportingTaskWrapper;
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
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Scheduling agent that runs processors, funnels, ports, and reporting tasks on virtual
 * threads rather than on a shared thread pool. A single {@link DynamicSemaphore} bounds
 * the number of invocations that may run concurrently across the entire flow. Its permit
 * count replaces the old Timer-Driven thread pool size and is adjustable at runtime via
 * {@link #setMaxThreadCount(int)}.
 * <p>
 * The agent is used for both {@link SchedulingStrategy#TIMER_DRIVEN} and
 * {@link SchedulingStrategy#CRON_DRIVEN} connectables. Each scheduled connectable is given
 * one virtual thread per concurrent task; every iteration of the scheduling loop acquires
 * a permit, calls {@link ConnectableTask#invoke()}, releases the permit, and then sleeps
 * until the next invocation is due.
 */
public class VirtualThreadSchedulingAgent implements SchedulingAgent {

    private static final Logger logger = LoggerFactory.getLogger(VirtualThreadSchedulingAgent.class);

    /**
     * Sleeps in the scheduling loop are broken up into chunks of at most this long so that
     * a processor that is unscheduled while a scheduling thread is sleeping will exit the
     * sleep and observe the state change quickly rather than having to wait for the full
     * scheduling period to elapse.
     */
    private static final long POLL_INTERVAL_NANOS = TimeUnit.MILLISECONDS.toNanos(25L);

    private final FlowController flowController;
    private final RepositoryContextFactory contextFactory;
    private final DynamicSemaphore globalSemaphore;
    private final long noWorkYieldNanos;
    private final Set<Thread> runningThreads = ConcurrentHashMap.newKeySet();
    private volatile boolean shutdown = false;
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
            throw new RuntimeException("Failed to create VirtualThreadSchedulingAgent because the "
                    + NiFiProperties.BORED_YIELD_DURATION + " property is set to an invalid time duration: " + boredYieldDuration);
        }

        logger.info("VirtualThreadSchedulingAgent initialized with {} permits", maxThreadCount);
    }

    /**
     * Marks the agent as shut down and interrupts every virtual thread that is currently running a scheduling loop,
     * a run-once invocation, or a reporting-task loop. After {@code shutdown()} returns, subsequent calls to any of
     * the {@code schedule} methods will fail fast with {@link IllegalStateException}. This method is idempotent and
     * may be called multiple times (for example, both on the {@code kill} path in {@code FlowController.shutdown}
     * and again from {@code StandardProcessScheduler.shutdown}). Interrupts are a best-effort mechanism: processor
     * code that does not honor interruption will continue to run until it completes naturally, but the virtual
     * threads themselves are daemon threads and will be reaped when the JVM exits.
     */
    @Override
    public void shutdown() {
        shutdown = true;
        int interrupted = 0;
        for (final Thread thread : runningThreads) {
            thread.interrupt();
            interrupted++;
        }
        if (interrupted > 0) {
            logger.info("Shutdown interrupted {} virtual threads managed by the VirtualThreadSchedulingAgent", interrupted);
        }
    }

    /**
     * Schedules the given connectable to run on virtual threads. This method transitions the
     * {@link LifecycleState} to scheduled and then spawns one virtual thread per concurrent task. Each scheduling loop
     * captures {@link LifecycleState#getLastStopTime()} at startup and will exit as soon as either
     * {@link LifecycleState#isScheduled()} becomes {@code false} or the last-stop-time advances, which is how the
     * agent guards against a rapid stop/start cycle leaving prior loops running alongside newly-spawned ones. The
     * corresponding {@link #unschedule(Connectable, LifecycleState)} call performs the inverse transition. Calling
     * {@code setScheduled(true)} here is intentionally idempotent: the framework scheduler also marks the state as
     * scheduled in some code paths (for example, reporting-task startup), and double-calling is harmless because the
     * last-stop-time only advances on transitions to the stopped state.
     */
    @Override
    public void schedule(final Connectable connectable, final LifecycleState lifecycleState) {
        final CronExpression cronExpression;
        if (connectable.getSchedulingStrategy() == SchedulingStrategy.CRON_DRIVEN) {
            final String cronSchedule = connectable.evaluateParameters(connectable.getSchedulingPeriod());
            cronExpression = parseCronExpression(cronSchedule, connectable);
        } else {
            cronExpression = null;
        }

        // setScheduled(true) is idempotent with the state the framework already set before calling into the agent.
        // It is done here too so that the agent is self-consistent even for callers that do not follow the framework
        // contract, but it also means that if anything below fails we MUST roll it back: otherwise the component
        // would be left flagged as scheduled with zero (or a partial set of) scheduling loops actually running.
        // Any loops that did start before the failure will exit on their next isActive() check when the rollback
        // flips the flag back to false.
        lifecycleState.setScheduled(true);
        try {
            final long startStopTime = lifecycleState.getLastStopTime();
            final ConnectableTask connectableTask = new ConnectableTask(this, connectable, flowController, contextFactory, lifecycleState);
            final int taskCount = connectable.getMaxConcurrentTasks();

            for (int i = 0; i < taskCount; i++) {
                final String threadName = buildThreadName(connectable, i);
                startTrackedVirtualThread(threadName, () -> runSchedulingLoop(connectable, connectableTask, lifecycleState, startStopTime, cronExpression));
            }

            logger.info("Scheduled {} to run with {} Virtual Threads", connectable, taskCount);
        } catch (final Throwable t) {
            lifecycleState.setScheduled(false);
            throw t;
        }
    }

    @Override
    public void scheduleOnce(final Connectable connectable, final LifecycleState lifecycleState, final Callable<Future<Void>> stopCallback) {
        lifecycleState.setScheduled(true);
        try {
            final ConnectableTask connectableTask = new ConnectableTask(this, connectable, flowController, contextFactory, lifecycleState);
            final String threadName = buildThreadName(connectable, 0);

            startTrackedVirtualThread(threadName, () -> runOnce(connectable, connectableTask, stopCallback));
        } catch (final Throwable t) {
            lifecycleState.setScheduled(false);
            throw t;
        }
    }

    @Override
    public void unschedule(final Connectable connectable, final LifecycleState lifecycleState) {
        lifecycleState.setScheduled(false);
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

        lifecycleState.setScheduled(true);
        try {
            final long startStopTime = lifecycleState.getLastStopTime();
            final Runnable reportingTaskWrapper = new ReportingTaskWrapper(taskNode, lifecycleState, flowController.getExtensionManager());
            final String threadName = "Reporting Task: " + taskNode.getName();

            startTrackedVirtualThread(threadName,
                    () -> runReportingTaskLoop(taskNode, reportingTaskWrapper, schedulingNanos, cronExpression, lifecycleState, startStopTime));

            logger.info("{} started on virtual thread", taskNode.getReportingTask());
        } catch (final Throwable t) {
            lifecycleState.setScheduled(false);
            throw t;
        }
    }

    @Override
    public void unschedule(final ReportingTaskNode taskNode, final LifecycleState lifecycleState) {
        lifecycleState.setScheduled(false);
        logger.info("Stopped scheduling {} to run", taskNode.getReportingTask());
    }

    /**
     * @return {@code true} if {@code lifecycleState} is still scheduled and its last-stop-time has not changed since
     * the scheduling loop was spawned (meaning the loop is still running against its original scheduling generation).
     * Used by the scheduling loops and their polling helpers to detect both the normal stop path (isScheduled flips to
     * false) and the rapid stop/start race (a stop increments lastStopTime even if a quick restart flips isScheduled
     * back to true before the old loop has observed the stop).
     */
    private boolean isActive(final LifecycleState lifecycleState, final long startStopTime) {
        return lifecycleState.isScheduled() && lifecycleState.getLastStopTime() == startStopTime;
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
        return runningThreads.size();
    }

    boolean isShutdown() {
        return shutdown;
    }

    /**
     * @return the number of virtual threads that are currently executing a processor or
     * reporting-task invocation. A thread counts as active when it is holding a permit on
     * the global semaphore, which mirrors the old Timer-Driven engine's active-count
     * semantics used by the cluster heartbeat and UI active-thread counter.
     */
    public int getActiveThreadCount() {
        return globalSemaphore.getInUsePermits();
    }

    /**
     * Runs the scheduling loop for a {@link Connectable}. Each iteration acquires a permit from the global semaphore,
     * invokes the connectable, releases the permit, and sleeps until the next invocation is due. The entire body of
     * the loop is wrapped in a {@code try/catch(Throwable)} so that no exception or error -- including
     * {@link Error} subclasses or bugs in the scheduling logic itself -- can cause the virtual thread to terminate
     * silently. A processor is expected to continue being triggered as long as it is scheduled, so on any unexpected
     * {@link Throwable} the error is logged, an administrative yield is applied to prevent tight-looping on a broken
     * task, and the loop continues on its next iteration.
     */
    private void runSchedulingLoop(final Connectable connectable, final ConnectableTask connectableTask,
                                   final LifecycleState lifecycleState, final long startStopTime, final CronExpression cronExpression) {

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
                sleepWithPolling(TimeUnit.MILLISECONDS.toNanos(initialDelayMillis), lifecycleState, startStopTime);
            }
        }

        while (isActive(lifecycleState, startStopTime)) {
            try {
                if (!acquirePermitWithPolling(lifecycleState, startStopTime)) {
                    return;
                }

                final InvocationResult invocationResult;
                try {
                    invocationResult = connectableTask.invoke();
                } finally {
                    globalSemaphore.release();
                }

                if (cronDriven) {
                    nextCronSchedule = getNextCronSchedule(nextCronSchedule, cronExpression);
                    if (nextCronSchedule == null) {
                        logger.warn("CRON expression for {} has no further firings after the current invocation; scheduling loop is exiting", connectable);
                        return;
                    }
                    final long sleepMillis = Math.max(nextCronSchedule.toInstant().toEpochMilli() - System.currentTimeMillis(), 0L);
                    sleepWithPolling(TimeUnit.MILLISECONDS.toNanos(sleepMillis), lifecycleState, startStopTime);
                } else {
                    sleepForSchedulingPeriod(connectable, lifecycleState, startStopTime, invocationResult);
                }
            } catch (final Throwable t) {
                // Nothing in the loop body is expected to throw (ConnectableTask.invoke() catches Throwable itself, and
                // acquirePermitWithPolling handles InterruptedException). If anything does escape to here, it must not
                // be allowed to kill the scheduling virtual thread: as long as the component remains scheduled we will
                // keep triggering it. Log the error, apply an administrative yield to avoid tight-looping on a broken
                // invocation, and continue with the next iteration.
                try {
                    connectable.yield(adminYieldNanos, TimeUnit.NANOSECONDS);
                } catch (final Throwable yieldError) {
                    t.addSuppressed(yieldError);
                }

                logger.error("Unexpected error in scheduling loop for {}. Will yield for {} and continue.", connectable, adminYieldDuration, t);
            }
        }
    }

    private void runOnce(final Connectable connectable, final ConnectableTask connectableTask, final Callable<Future<Void>> stopCallback) {
        try {
            try {
                globalSemaphore.acquire();
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
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

    /**
     * Runs the reporting-task scheduling loop. As with {@link #runSchedulingLoop}, the loop body is wrapped in a
     * {@code try/catch(Throwable)} so that no unexpected failure can cause the virtual thread to exit. A reporting
     * task is expected to continue running until it is unscheduled. When {@code cronExpression} is non-null the loop
     * sleeps until the next CRON firing; otherwise it sleeps for {@code schedulingNanos} after each run.
     */
    private void runReportingTaskLoop(final ReportingTaskNode taskNode, final Runnable reportingTaskWrapper, final long schedulingNanos,
                                      final CronExpression cronExpression, final LifecycleState lifecycleState, final long startStopTime) {
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
                sleepWithPolling(TimeUnit.MILLISECONDS.toNanos(initialDelayMillis), lifecycleState, startStopTime);
            }
        }

        while (isActive(lifecycleState, startStopTime)) {
            try {
                if (!acquirePermitWithPolling(lifecycleState, startStopTime)) {
                    return;
                }

                try {
                    reportingTaskWrapper.run();
                } finally {
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
                    sleepWithPolling(TimeUnit.MILLISECONDS.toNanos(sleepMillis), lifecycleState, startStopTime);
                } else {
                    sleepWithPolling(schedulingNanos, lifecycleState, startStopTime);
                }
            } catch (final Throwable t) {
                // ReportingTaskWrapper.run() is expected to catch Throwable itself, so reaching this handler indicates
                // an unexpected framework-level failure. Log and continue so that one bad invocation does not
                // permanently kill the scheduling loop for this reporting task.
                logger.error("Unexpected error in scheduling loop for {}. Continuing on next scheduled interval.", taskNode.getReportingTask(), t);
            }
        }
    }

    private void sleepForSchedulingPeriod(final Connectable connectable, final LifecycleState lifecycleState, final long startStopTime,
                                          final InvocationResult invocationResult) {
        final long sleepNanos;
        final long yieldExpiration = connectable.getYieldExpiration();
        final long now = System.currentTimeMillis();
        if (yieldExpiration > now) {
            sleepNanos = TimeUnit.MILLISECONDS.toNanos(yieldExpiration - now);
        } else if (invocationResult.isYield()) {
            sleepNanos = noWorkYieldNanos;
        } else {
            sleepNanos = connectable.getSchedulingPeriod(TimeUnit.NANOSECONDS);
        }

        sleepWithPolling(sleepNanos, lifecycleState, startStopTime);
    }

    /**
     * Attempts to acquire a permit from the global semaphore, waking up periodically to re-check whether the
     * scheduling generation this loop belongs to is still active. This prevents a scheduling thread from blocking
     * indefinitely on {@link DynamicSemaphore#acquire()} when the flow is heavily loaded and the component has been
     * unscheduled; without this, stopping a processor while all global permits were held elsewhere would have to wait
     * for one of those other processors to release a permit before the stop could take effect.
     *
     * @return {@code true} if a permit was acquired (the caller MUST release it), {@code false} if the scheduling
     *         generation ended (i.e., the component was stopped) or the thread was interrupted before a permit could be acquired
     */
    private boolean acquirePermitWithPolling(final LifecycleState lifecycleState, final long startStopTime) {
        while (isActive(lifecycleState, startStopTime)) {
            try {
                if (globalSemaphore.tryAcquire(POLL_INTERVAL_NANOS, TimeUnit.NANOSECONDS)) {
                    return true;
                }
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        return false;
    }

    /**
     * Sleeps for approximately {@code sleepNanos}, waking up periodically to check whether the scheduling generation
     * this loop belongs to is still active. This allows a stop/unschedule request to take effect promptly even when
     * the scheduling period is long: rather than calling {@code Thread.sleep(sleepNanos)} and forcing the caller to
     * wait out the entire delay, the sleep is broken into chunks of at most {@link #POLL_INTERVAL_NANOS} and the
     * generation is re-checked between chunks. Returns immediately if {@code sleepNanos <= 0} so that a zero
     * scheduling period (run-as-fast-as-possible) does not incur any artificial delay.
     */
    private void sleepWithPolling(final long sleepNanos, final LifecycleState lifecycleState, final long startStopTime) {
        if (sleepNanos <= 0L) {
            return;
        }

        final long sleepExpiration = System.nanoTime() + sleepNanos;
        while (isActive(lifecycleState, startStopTime)) {
            final long remainingNanos = sleepExpiration - System.nanoTime();
            if (remainingNanos <= 0L) {
                return;
            }

            final long chunkNanos = Math.min(remainingNanos, POLL_INTERVAL_NANOS);
            try {
                TimeUnit.NANOSECONDS.sleep(chunkNanos);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private static String buildThreadName(final Connectable connectable, final int taskIndex) {
        return connectable.getName() + "[type=" + connectable.getComponentType() + ", id=" + connectable.getIdentifier()
                + ", group=" + connectable.getProcessGroup().getName() + "] task " + taskIndex;
    }

    /**
     * Starts a virtual thread running {@code task} under the given thread name and tracks it in {@link #runningThreads}
     * so that {@link #shutdown()} can interrupt it if the agent is torn down while the task is still running. The thread
     * removes itself from the tracking set in a {@code finally} block when the task completes so the set does not grow
     * unbounded as processors and reporting tasks are scheduled and unscheduled over the agent's lifetime.
     * <p>
     * The {@code shutdown} flag is re-checked inside the wrapped runnable because there is a small window between
     * {@link Thread#start()} and the first line of the virtual thread's body during which a concurrent call to
     * {@link #shutdown()} could miss the newly-started thread. The double-check ensures that a task is not started
     * (and therefore cannot acquire the global semaphore) after shutdown has been signaled.
     *
     * @throws IllegalStateException if {@link #shutdown()} has already been called
     */
    private void startTrackedVirtualThread(final String threadName, final Runnable task) {
        if (shutdown) {
            throw new IllegalStateException("VirtualThreadSchedulingAgent has been shut down and cannot accept new work");
        }

        final Runnable trackedTask = () -> {
            final Thread self = Thread.currentThread();
            runningThreads.add(self);
            try {
                if (shutdown) {
                    return;
                }
                task.run();
            } finally {
                runningThreads.remove(self);
            }
        };

        Thread.ofVirtual().name(threadName).start(trackedTask);
    }

    /**
     * Returns the next firing time for the given CRON expression after {@code currentSchedule}.
     * Callers MUST handle a {@code null} return: {@link CronExpression#next(java.time.temporal.Temporal)}
     * returns {@code null} when no future firing is reachable within the expression's search horizon,
     * for example for a physically impossible expression such as {@code "0 0 0 30 2 ?"} (February 30).
     */
    private static OffsetDateTime getNextCronSchedule(final OffsetDateTime currentSchedule, final CronExpression cronExpression) {
        // Clock resolution is not millisecond-precise, so ensure the next scheduled time is strictly after the time
        // this invocation was supposed to run, otherwise the same moment could be scheduled twice.
        final OffsetDateTime now = OffsetDateTime.now();
        return cronExpression.next(now.isAfter(currentSchedule) ? now : currentSchedule);
    }
}
