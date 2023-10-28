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
import org.apache.nifi.controller.tasks.ReportingTaskWrapper;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.processor.exception.ProcessException;
import org.springframework.scheduling.support.CronExpression;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class CronSchedulingAgent extends AbstractTimeBasedSchedulingAgent {
    private final Map<Object, Map<Integer, ScheduledFuture<?>>> scheduledFutures = new HashMap<>();

    public CronSchedulingAgent(final FlowController flowController, final FlowEngine flowEngine, final RepositoryContextFactory contextFactory) {
        super(flowEngine, flowController, contextFactory);
    }

    @Override
    public void shutdown() {
        scheduledFutures.values().forEach(map -> map.values().forEach(future -> {
            if (!future.isCancelled()) {
                // stop scheduling to run and interrupt currently running tasks.
                future.cancel(true);
            }
        }));
        flowEngine.shutdown();
    }

    @Override
    public void doSchedule(final ReportingTaskNode taskNode, final LifecycleState scheduleState) {
        final Map<Integer, ScheduledFuture<?>> componentFuturesMap = scheduledFutures.computeIfAbsent(taskNode, k -> new HashMap<>());
        if (!componentFuturesMap.values().isEmpty()) {
            throw new IllegalStateException("Cannot schedule " + taskNode.getReportingTask().getIdentifier() + " because it is already scheduled to run");
        }

        final String cronSchedule = taskNode.getSchedulingPeriod();
        final CronExpression cronExpression;
        try {
            cronExpression = CronExpression.parse(cronSchedule);
        } catch (final Exception pe) {
            throw new IllegalStateException("Cannot schedule Reporting Task " + taskNode.getReportingTask().getIdentifier() + " to run because its scheduling period is not valid", pe);
        }

        final ReportingTaskWrapper taskWrapper = new ReportingTaskWrapper(taskNode, scheduleState, flowController.getExtensionManager());

        final OffsetDateTime initialDate = getInitialDate(cronExpression);
        final long initialDelay = getInitialDelay(initialDate);

        final Runnable command = new Runnable() {

            private OffsetDateTime nextSchedule = initialDate;

            @Override
            public void run() {
                taskWrapper.run();

                nextSchedule = getNextSchedule(nextSchedule, cronExpression);
                final long delay = getDelay(nextSchedule);

                logger.debug("Finished running Reporting Task {}; next scheduled time is at {} after a delay of {} milliseconds", taskNode, nextSchedule, delay);
                final ScheduledFuture<?> newFuture = flowEngine.schedule(this, delay, TimeUnit.MILLISECONDS);
                final ScheduledFuture<?> oldFuture = componentFuturesMap.put(0, newFuture);
                scheduleState.replaceFuture(oldFuture, newFuture);
            }
        };

        final ScheduledFuture<?> future = flowEngine.schedule(command, initialDelay, TimeUnit.MILLISECONDS);
        componentFuturesMap.put(0, future);

        scheduleState.setScheduled(true);
        scheduleState.setFutures(componentFuturesMap.values());
        logger.info("Scheduled Reporting Task {} to run threads on schedule {}", taskNode, cronSchedule);
    }

    @Override
    public synchronized void doSchedule(final Connectable connectable, final LifecycleState scheduleState) {
        final Map<Integer, ScheduledFuture<?>> componentFuturesMap = scheduledFutures.computeIfAbsent(connectable, k -> new HashMap<>());
        if (!componentFuturesMap.values().isEmpty()) {
            throw new IllegalStateException("Cannot schedule " + connectable + " because it is already scheduled to run");
        }

        final String cronSchedule = connectable.evaluateParameters(connectable.getSchedulingPeriod());

        final CronExpression cronExpression;
        try {
            cronExpression = CronExpression.parse(cronSchedule);
        } catch (final Exception pe) {
            throw new IllegalStateException("Cannot schedule " + connectable + " to run because its scheduling period is not valid", pe);
        }

        for (int i = 0; i < connectable.getMaxConcurrentTasks(); i++) {
            final ConnectableTask continuallyRunTask = new ConnectableTask(this, connectable, flowController, contextFactory, scheduleState);

            final AtomicInteger taskNumber = new AtomicInteger(i);

            final OffsetDateTime initialDate = getInitialDate(cronExpression);
            final long initialDelay = getInitialDelay(initialDate);

            final Runnable command = new Runnable() {

                private OffsetDateTime nextSchedule = initialDate;

                @Override
                public void run() {
                    try {
                        continuallyRunTask.invoke();
                    } catch (final RuntimeException re) {
                        throw re;
                    } catch (final Exception e) {
                        throw new ProcessException(e);
                    }

                    nextSchedule = getNextSchedule(nextSchedule, cronExpression);
                    final long delay = getDelay(nextSchedule);

                    logger.debug("Finished task for {}; next scheduled time is at {} after a delay of {} milliseconds", connectable, nextSchedule, delay);
                    final ScheduledFuture<?> newFuture = flowEngine.schedule(this, delay, TimeUnit.MILLISECONDS);
                    final ScheduledFuture<?> oldFuture = componentFuturesMap.put(taskNumber.get(), newFuture);
                    scheduleState.replaceFuture(oldFuture, newFuture);
                }
            };

            final ScheduledFuture<?> future = flowEngine.schedule(command, initialDelay, TimeUnit.MILLISECONDS);
            componentFuturesMap.put(taskNumber.get(), future);
        }

        scheduleState.setFutures(componentFuturesMap.values());
        logger.info("Scheduled {} to run with {} threads on schedule {}", connectable, connectable.getMaxConcurrentTasks(), cronSchedule);
    }

    @Override
    public synchronized void doUnschedule(final Connectable connectable, final LifecycleState scheduleState) {
        unschedule((Object) connectable, scheduleState);
    }

    @Override
    public synchronized void doUnschedule(final ReportingTaskNode taskNode, final LifecycleState scheduleState) {
        unschedule((Object) taskNode, scheduleState);
    }

    private void unschedule(final Object scheduled, final LifecycleState scheduleState) {
        scheduledFutures.remove(scheduled);
        scheduleState.getFutures().forEach(future -> {
            if (!future.isCancelled()) {
                // stop scheduling to run but do not interrupt currently running tasks.
                future.cancel(false);
            }
        });

        scheduleState.setScheduled(false);
        logger.info("Stopped scheduling {} to run", scheduled);
    }

    @Override
    public void onEvent(final Connectable connectable) {
    }

    @Override
    public void setMaxThreadCount(final int maxThreads) {
    }

    private OffsetDateTime getInitialDate(final CronExpression cronExpression) {
        final OffsetDateTime now = OffsetDateTime.now();
        final OffsetDateTime initialDate = cronExpression.next(now);
        return initialDate == null ? now : initialDate;
    }

    private long getInitialDelay(final OffsetDateTime initialDate) {
        return initialDate.toInstant().toEpochMilli() - System.currentTimeMillis();
    }

    private static OffsetDateTime getNextSchedule(final OffsetDateTime currentSchedule, final CronExpression cronExpression) {
        // Since the clock has not a millisecond precision, we have to check that we
        // schedule the next time after the time this was supposed to run, otherwise
        // we might end up with running the same task twice
        final OffsetDateTime now = OffsetDateTime.now();
        return cronExpression.next(now.isAfter(currentSchedule) ? now : currentSchedule);
    }

    private static long getDelay(final OffsetDateTime nextSchedule) {
        return Math.max(nextSchedule.toInstant().toEpochMilli() - System.currentTimeMillis(), 0L);
    }
}
