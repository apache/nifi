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
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.tasks.ContinuallyRunConnectableTask;
import org.apache.nifi.controller.tasks.ContinuallyRunProcessorTask;
import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.StandardProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This is similar to the TimerDrivenSchedulingAgent but modified to
 * stop after the first successful execution of a processor.
 */
public class RunOnceSchedulingAgent extends AbstractSchedulingAgent {

    private static final Logger logger = LoggerFactory.getLogger(RunOnceSchedulingAgent.class);
    private final long noWorkYieldNanos;

    private final FlowController flowController;
    private final ProcessContextFactory contextFactory;
    private final StringEncryptor encryptor;
    private final VariableRegistry variableRegistry;

    private volatile String adminYieldDuration = "1 sec";

    public RunOnceSchedulingAgent(
            final FlowController flowController,
            final FlowEngine flowEngine,
            final ProcessContextFactory contextFactory,
            final StringEncryptor encryptor,
            final VariableRegistry variableRegistry,
            final NiFiProperties nifiProperties) {
        super(flowEngine);
        this.flowController = flowController;
        this.contextFactory = contextFactory;
        this.encryptor = encryptor;
        this.variableRegistry = variableRegistry;

        final String boredYieldDuration = nifiProperties.getBoredYieldDuration();
        try {
            noWorkYieldNanos = FormatUtils.getTimeDuration(boredYieldDuration, TimeUnit.NANOSECONDS);
        } catch (final IllegalArgumentException e) {
            throw new RuntimeException("Failed to create SchedulingAgent because the " + NiFiProperties.BORED_YIELD_DURATION + " property is set to an invalid time duration: " + boredYieldDuration);
        }
    }

    private StateManager getStateManager(final String componentId) {
        return flowController.getStateManagerProvider().getStateManager(componentId);
    }

    @Override
    public void shutdown() {
        flowEngine.shutdown();
    }

    @Override
    public void doSchedule(final ReportingTaskNode taskNode, final ScheduleState scheduleState) {
        throw new UnsupportedOperationException("ReportingTasks cannot be scheduled in RUN_ONCE Mode");
    }

    /**
     * This is based on {@link TimerDrivenSchedulingAgent#doSchedule(Connectable, ScheduleState)} but modified to
     * stop after the first successful execution.
     */
    @Override
    public void doSchedule(final Connectable connectable, final ScheduleState scheduleState) {
        final List<ScheduledFuture<?>> futures = new ArrayList<>();
        for (int i = 0; i < connectable.getMaxConcurrentTasks(); i++) {
            final Callable<Boolean> continuallyRunTask;
            final ProcessContext processContext;
            final ContinuallyRunProcessorTask continuallyRunProcessorTask;

            // Determine the task to run and create it.
            if (connectable.getConnectableType() == ConnectableType.PROCESSOR) {
                final ProcessorNode procNode = (ProcessorNode) connectable;
                final StandardProcessContext standardProcContext = new StandardProcessContext(procNode, flowController, encryptor, getStateManager(connectable.getIdentifier()),
                        variableRegistry);
                final ContinuallyRunProcessorTask runnableTask = new ContinuallyRunProcessorTask(this, procNode, flowController, contextFactory, scheduleState,
                        standardProcContext);

                continuallyRunTask = runnableTask;
                processContext = standardProcContext;
                continuallyRunProcessorTask = runnableTask;
            } else {
                processContext = new ConnectableProcessContext(connectable, encryptor, getStateManager(connectable.getIdentifier()));
                continuallyRunTask = new ContinuallyRunConnectableTask(contextFactory, connectable, scheduleState, processContext);
                continuallyRunProcessorTask = null;
            }

            final AtomicReference<ScheduledFuture<?>> futureRef = new AtomicReference<>();

            final Runnable yieldDetectionRunnable = new Runnable() {
                @Override
                public void run() {
                    // Call the continually run task. It will return a boolean
                    // indicating whether or not we should yield
                    // based on a lack of work for to do for the component.
                    final boolean shouldYield;
                    boolean shouldStop = false;
                    try {
                        if (continuallyRunProcessorTask != null) {
                            int invocationCount = continuallyRunProcessorTask.getInvocationCount();
                            shouldYield = continuallyRunTask.call();
                            if (continuallyRunProcessorTask.getInvocationCount() > invocationCount) {
                                shouldStop = true;
                            }
                        } else {
                            shouldYield = continuallyRunTask.call();
                        }
                    } catch (final RuntimeException re) {
                        throw re;
                    } catch (final Exception e) {
                        throw new ProcessException(e);
                    }

                    if (shouldStop) {
                        final ScheduledFuture<?> scheduledFuture = futureRef.get();
                        if (scheduledFuture != null) {
                            scheduledFuture.cancel(false);
                        }
                        flowController.getProcessScheduler().stopProcessor((ProcessorNode) connectable);
                        return;
                    }

                    // If the component is yielded, cancel its future and
                    // re-submit it to run again
                    // after the yield has expired.
                    final long newYieldExpiration = connectable.getYieldExpiration();
                    if (newYieldExpiration > System.currentTimeMillis()) {
                        final long yieldMillis = newYieldExpiration - System.currentTimeMillis();
                        final ScheduledFuture<?> scheduledFuture = futureRef.get();
                        if (scheduledFuture == null) {
                            return;
                        }

                        // If we are able to cancel the future, create a new one
                        // and update the ScheduleState so that it has
                        // an accurate accounting of which futures are
                        // outstanding; we must then also update the futureRef
                        // so that we can do this again the next time that the
                        // component is yielded.
                        if (scheduledFuture.cancel(false)) {
                            final long yieldNanos = TimeUnit.MILLISECONDS.toNanos(yieldMillis);

                            synchronized (scheduleState) {
                                if (scheduleState.isScheduled()) {
                                    final ScheduledFuture<?> newFuture = flowEngine.scheduleWithFixedDelay(this, yieldNanos, connectable.getSchedulingPeriod(TimeUnit.NANOSECONDS),
                                            TimeUnit.NANOSECONDS);

                                    scheduleState.replaceFuture(scheduledFuture, newFuture);
                                    futureRef.set(newFuture);
                                }
                            }
                        }
                    } else if (noWorkYieldNanos > 0L && shouldYield) {
                        // Component itself didn't yield but there was no work
                        // to do, so the framework will choose
                        // to yield the component automatically for a short
                        // period of time.
                        final ScheduledFuture<?> scheduledFuture = futureRef.get();
                        if (scheduledFuture == null) {
                            return;
                        }

                        // If we are able to cancel the future, create a new one
                        // and update the ScheduleState so that it has
                        // an accurate accounting of which futures are
                        // outstanding; we must then also update the futureRef
                        // so that we can do this again the next time that the
                        // component is yielded.
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

            // Schedule the task to run
            final ScheduledFuture<?> future = flowEngine.scheduleWithFixedDelay(yieldDetectionRunnable, 0L, connectable.getSchedulingPeriod(TimeUnit.NANOSECONDS),
                    TimeUnit.NANOSECONDS);

            // now that we have the future, set the atomic reference so that if
            // the component is yielded we
            // are able to then cancel this future.
            futureRef.set(future);

            // Keep track of the futures so that we can update the
            // ScheduleState.
            futures.add(future);
        }

        scheduleState.setFutures(futures);
        logger.info("Scheduled {} to run once with {} threads", connectable, connectable.getMaxConcurrentTasks());
    }

    @Override
    public void doUnschedule(final Connectable connectable, final ScheduleState scheduleState) {
        for (final ScheduledFuture<?> future : scheduleState.getFutures()) {
            // stop scheduling to run but do not interrupt currently running tasks.
            future.cancel(false);
        }

        logger.info("Stopped scheduling {} to run", connectable);
    }

    @Override
    public void doUnschedule(final ReportingTaskNode taskNode, final ScheduleState scheduleState) {
        throw new UnsupportedOperationException("ReportingTasks cannot be scheduled in RUN_ONCE Mode");
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

}
