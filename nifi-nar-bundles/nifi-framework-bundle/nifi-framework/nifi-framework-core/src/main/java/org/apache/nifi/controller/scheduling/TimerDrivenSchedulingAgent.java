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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.tasks.ConnectableTask;
import org.apache.nifi.controller.tasks.InvocationResult;
import org.apache.nifi.controller.tasks.ReportingTaskWrapper;
import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimerDrivenSchedulingAgent extends AbstractSchedulingAgent {

    private static final Logger logger = LoggerFactory.getLogger(TimerDrivenSchedulingAgent.class);
    private final long noWorkYieldNanos;

    private final FlowController flowController;
    private final RepositoryContextFactory contextFactory;
    private final StringEncryptor encryptor;

    private volatile String adminYieldDuration = "1 sec";

    public TimerDrivenSchedulingAgent(final FlowController flowController, final FlowEngine flowEngine, final RepositoryContextFactory contextFactory,
            final StringEncryptor encryptor, final NiFiProperties nifiProperties) {
        super(flowEngine);
        this.flowController = flowController;
        this.contextFactory = contextFactory;
        this.encryptor = encryptor;

        final String boredYieldDuration = nifiProperties.getBoredYieldDuration();
        try {
            noWorkYieldNanos = FormatUtils.getTimeDuration(boredYieldDuration, TimeUnit.NANOSECONDS);
        } catch (final IllegalArgumentException e) {
            throw new RuntimeException("Failed to create SchedulingAgent because the " + NiFiProperties.BORED_YIELD_DURATION + " property is set to an invalid time duration: " + boredYieldDuration);
        }
    }

    @Override
    public void shutdown() {
        flowEngine.shutdown();
    }

    @Override
    public void doSchedule(final ReportingTaskNode taskNode, final LifecycleState scheduleState) {
        final Runnable reportingTaskWrapper = new ReportingTaskWrapper(taskNode, scheduleState, flowController.getExtensionManager());
        final long schedulingNanos = taskNode.getSchedulingPeriod(TimeUnit.NANOSECONDS);

        final ScheduledFuture<?> future = flowEngine.scheduleWithFixedDelay(reportingTaskWrapper, 0L, schedulingNanos, TimeUnit.NANOSECONDS);
        final List<ScheduledFuture<?>> futures = new ArrayList<>(1);
        futures.add(future);
        scheduleState.setFutures(futures);

        logger.info("{} started.", taskNode.getReportingTask());
    }

    @Override
    public void doSchedule(final Connectable connectable, final LifecycleState scheduleState) {
        final List<ScheduledFuture<?>> futures = new ArrayList<>();
        final ConnectableTask connectableTask = new ConnectableTask(this, connectable, flowController, contextFactory, scheduleState, encryptor);

        for (int i = 0; i < connectable.getMaxConcurrentTasks(); i++) {
            // Determine the task to run and create it.
            final AtomicReference<ScheduledFuture<?>> futureRef = new AtomicReference<>();

            final Runnable trigger = createTrigger(connectableTask, scheduleState, futureRef);

            // Schedule the task to run
            final ScheduledFuture<?> future = flowEngine.scheduleWithFixedDelay(trigger, 0L,
                connectable.getSchedulingPeriod(TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS);

            // now that we have the future, set the atomic reference so that if the component is yielded we
            // are able to then cancel this future.
            futureRef.set(future);

            // Keep track of the futures so that we can update the ScheduleState.
            futures.add(future);
        }

        scheduleState.setFutures(futures);
        logger.info("Scheduled {} to run with {} threads", connectable, connectable.getMaxConcurrentTasks());
    }


    private Runnable createTrigger(final ConnectableTask connectableTask, final LifecycleState scheduleState, final AtomicReference<ScheduledFuture<?>> futureRef) {
        final Connectable connectable = connectableTask.getConnectable();
        final Runnable yieldDetectionRunnable = new Runnable() {
            @Override
            public void run() {
                // Call the task. It will return a boolean indicating whether or not we should yield
                // based on a lack of work for to do for the component.
                final InvocationResult invocationResult = connectableTask.invoke();
                if (invocationResult.isYield()) {
                    logger.debug("Yielding {} due to {}", connectable, invocationResult.getYieldExplanation());
                }

                // If the component is yielded, cancel its future and re-submit it to run again
                // after the yield has expired.
                final long newYieldExpiration = connectable.getYieldExpiration();
                final long now = System.currentTimeMillis();
                if (newYieldExpiration > now) {
                    final long yieldMillis = newYieldExpiration - now;
                    final long scheduleMillis = connectable.getSchedulingPeriod(TimeUnit.MILLISECONDS);
                    final ScheduledFuture<?> scheduledFuture = futureRef.get();
                    if (scheduledFuture == null) {
                        return;
                    }

                    // If we are able to cancel the future, create a new one and update the ScheduleState so that it has
                    // an accurate accounting of which futures are outstanding; we must then also update the futureRef
                    // so that we can do this again the next time that the component is yielded.
                    if (scheduledFuture.cancel(false)) {
                        final long yieldNanos = Math.max(TimeUnit.MILLISECONDS.toNanos(scheduleMillis), TimeUnit.MILLISECONDS.toNanos(yieldMillis));

                        synchronized (scheduleState) {
                            if (scheduleState.isScheduled()) {
                                final long schedulingNanos = connectable.getSchedulingPeriod(TimeUnit.NANOSECONDS);
                                final ScheduledFuture<?> newFuture = flowEngine.scheduleWithFixedDelay(this, yieldNanos, schedulingNanos, TimeUnit.NANOSECONDS);

                                scheduleState.replaceFuture(scheduledFuture, newFuture);
                                futureRef.set(newFuture);
                            }
                        }
                    }
                } else if (noWorkYieldNanos > 0L && invocationResult.isYield()) {
                    // Component itself didn't yield but there was no work to do, so the framework will choose
                    // to yield the component automatically for a short period of time.
                    final ScheduledFuture<?> scheduledFuture = futureRef.get();
                    if (scheduledFuture == null) {
                        return;
                    }

                    // If we are able to cancel the future, create a new one and update the ScheduleState so that it has
                    // an accurate accounting of which futures are outstanding; we must then also update the futureRef
                    // so that we can do this again the next time that the component is yielded.
                    if (scheduledFuture.cancel(false)) {
                        synchronized (scheduleState) {
                            if (scheduleState.isScheduled()) {
                                final ScheduledFuture<?> newFuture = flowEngine.scheduleWithFixedDelay(this, noWorkYieldNanos,
                                    connectable.getSchedulingPeriod(TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS);

                                scheduleState.replaceFuture(scheduledFuture, newFuture);
                                futureRef.set(newFuture);
                            }
                        }
                    }
                }
            }
        };

        return yieldDetectionRunnable;
    }

    @Override
    public void doUnschedule(final Connectable connectable, final LifecycleState scheduleState) {
        for (final ScheduledFuture<?> future : scheduleState.getFutures()) {
            // stop scheduling to run but do not interrupt currently running tasks.
            future.cancel(false);
        }

        logger.info("Stopped scheduling {} to run", connectable);
    }

    @Override
    public void doUnschedule(final ReportingTaskNode taskNode, final LifecycleState scheduleState) {
        for (final ScheduledFuture<?> future : scheduleState.getFutures()) {
            // stop scheduling to run but do not interrupt currently running tasks.
            future.cancel(false);
        }

        logger.info("Stopped scheduling {} to run", taskNode.getReportingTask());
    }

    @Override
    public void setAdministrativeYieldDuration(final String yieldDuration) {
        this.adminYieldDuration = yieldDuration;
    }

    @Override
    public String getAdministrativeYieldDuration() {
        return adminYieldDuration;
    }

    @Override
    public long getAdministrativeYieldDuration(final TimeUnit timeUnit) {
        return FormatUtils.getTimeDuration(adminYieldDuration, timeUnit);
    }

    @Override
    public void onEvent(final Connectable connectable) {
    }

    @Override
    public void setMaxThreadCount(final int maxThreads) {
    }

    @Override
    public void incrementMaxThreadCount(int toAdd) {
        final int corePoolSize = flowEngine.getCorePoolSize();
        if (toAdd < 0 && corePoolSize + toAdd < 1) {
            throw new IllegalStateException("Cannot remove " + (-toAdd) + " threads from pool because there are only " + corePoolSize + " threads in the pool");
        }

        flowEngine.setCorePoolSize(corePoolSize + toAdd);
    }
}
