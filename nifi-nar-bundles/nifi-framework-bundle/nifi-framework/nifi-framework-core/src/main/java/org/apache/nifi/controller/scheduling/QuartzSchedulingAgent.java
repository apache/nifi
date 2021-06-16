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
import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.FormatUtils;
import org.quartz.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class QuartzSchedulingAgent extends AbstractSchedulingAgent {

    private final Logger logger = LoggerFactory.getLogger(QuartzSchedulingAgent.class);

    private final FlowController flowController;
    private final RepositoryContextFactory contextFactory;
    private final StringEncryptor encryptor;

    private volatile String adminYieldDuration = "1 sec";
    private final Map<Object, List<AtomicBoolean>> canceledTriggers = new HashMap<>();

    public QuartzSchedulingAgent(final FlowController flowController, final FlowEngine flowEngine, final RepositoryContextFactory contextFactory, final StringEncryptor enryptor) {
        super(flowEngine);
        this.flowController = flowController;
        this.contextFactory = contextFactory;
        this.encryptor = enryptor;
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void doSchedule(final ReportingTaskNode taskNode, final LifecycleState scheduleState) {
        final List<AtomicBoolean> existingTriggers = canceledTriggers.get(taskNode);
        if (existingTriggers != null) {
            throw new IllegalStateException("Cannot schedule " + taskNode.getReportingTask().getIdentifier() + " because it is already scheduled to run");
        }

        final String cronSchedule = taskNode.getSchedulingPeriod();
        final CronExpression cronExpression;
        try {
            cronExpression = new CronExpression(cronSchedule);
        } catch (final Exception pe) {
            throw new IllegalStateException("Cannot schedule Reporting Task " + taskNode.getReportingTask().getIdentifier() + " to run because its scheduling period is not valid");
        }

        final ReportingTaskWrapper taskWrapper = new ReportingTaskWrapper(taskNode, scheduleState, flowController.getExtensionManager());

        final AtomicBoolean canceled = new AtomicBoolean(false);
        final Date initialDate = cronExpression.getTimeAfter(new Date());
        final long initialDelay = initialDate.getTime() - System.currentTimeMillis();

        final Runnable command = new Runnable() {

            private Date nextSchedule = initialDate;

            @Override
            public void run() {
                if (canceled.get()) {
                    return;
                }

                taskWrapper.run();

                if (canceled.get()) {
                    return;
                }

                nextSchedule = getNextSchedule(nextSchedule, cronExpression);
                final long delay = getDelay(nextSchedule);

                logger.debug("Finished running Reporting Task {}; next scheduled time is at {} after a delay of {} milliseconds", taskNode, nextSchedule, delay);
                flowEngine.schedule(this, delay, TimeUnit.MILLISECONDS);
            }
        };

        final List<AtomicBoolean> triggers = new ArrayList<>(1);
        triggers.add(canceled);
        canceledTriggers.put(taskNode, triggers);

        flowEngine.schedule(command, initialDelay, TimeUnit.MILLISECONDS);
        scheduleState.setScheduled(true);
        logger.info("Scheduled Reporting Task {} to run threads on schedule {}", taskNode, cronSchedule);
    }

    @Override
    public synchronized void doSchedule(final Connectable connectable, final LifecycleState scheduleState) {
        final List<AtomicBoolean> existingTriggers = canceledTriggers.get(connectable);
        if (existingTriggers != null) {
            throw new IllegalStateException("Cannot schedule " + connectable + " because it is already scheduled to run");
        }

        final String cronSchedule = connectable.getSchedulingPeriod();
        final CronExpression cronExpression;
        try {
            cronExpression = new CronExpression(cronSchedule);
        } catch (final Exception pe) {
            throw new IllegalStateException("Cannot schedule " + connectable + " to run because its scheduling period is not valid");
        }

        final List<AtomicBoolean> triggers = new ArrayList<>();
        for (int i = 0; i < connectable.getMaxConcurrentTasks(); i++) {
            final ConnectableTask continuallyRunTask = new ConnectableTask(this, connectable, flowController, contextFactory, scheduleState, encryptor);

            final AtomicBoolean canceled = new AtomicBoolean(false);

            final Date initialDate = cronExpression.getTimeAfter(new Date());
            final long initialDelay = initialDate.getTime() - System.currentTimeMillis();

            final Runnable command = new Runnable() {

                private Date nextSchedule = initialDate;

                @Override
                public void run() {
                    if (canceled.get()) {
                        return;
                    }

                    try {
                        continuallyRunTask.invoke();
                    } catch (final RuntimeException re) {
                        throw re;
                    } catch (final Exception e) {
                        throw new ProcessException(e);
                    }

                    if (canceled.get()) {
                        return;
                    }

                    nextSchedule = getNextSchedule(nextSchedule, cronExpression);
                    final long delay = getDelay(nextSchedule);

                    logger.debug("Finished task for {}; next scheduled time is at {} after a delay of {} milliseconds", connectable, nextSchedule, delay);
                    flowEngine.schedule(this, delay, TimeUnit.MILLISECONDS);
                }
            };


            flowEngine.schedule(command, initialDelay, TimeUnit.MILLISECONDS);
            triggers.add(canceled);
        }

        canceledTriggers.put(connectable, triggers);
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
        final List<AtomicBoolean> triggers = canceledTriggers.remove(scheduled);
        if (triggers == null) {
            throw new IllegalStateException("Cannot unschedule " + scheduled + " because it was not scheduled to run");
        }

        for (final AtomicBoolean trigger : triggers) {
            trigger.set(true);
        }

        scheduleState.setScheduled(false);
        logger.info("Stopped scheduling {} to run", scheduled);
    }

    @Override
    public void onEvent(final Connectable connectable) {
    }

    @Override
    public void setMaxThreadCount(final int maxThreads) {
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
    public void incrementMaxThreadCount(int toAdd) {
        final int corePoolSize = flowEngine.getCorePoolSize();
        if (toAdd < 0 && corePoolSize + toAdd < 1) {
            throw new IllegalStateException("Cannot remove " + (-toAdd) + " threads from pool because there are only " + corePoolSize + " threads in the pool");
        }

        flowEngine.setCorePoolSize(corePoolSize + toAdd);
    }

    private static Date getNextSchedule(final Date currentSchedule, final CronExpression cronExpression) {
        // Since the clock has not a millisecond precision, we have to check that we
        // schedule the next time after the time this was supposed to run, otherwise
        // we might end up with running the same task twice
        final Date now = new Date();
        return cronExpression.getTimeAfter(now.after(currentSchedule) ? now : currentSchedule);
    }

    private static long getDelay(Date nextSchedule) {
        return Math.max(nextSchedule.getTime() - System.currentTimeMillis(), 0L);
    }
}
