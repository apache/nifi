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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.tasks.ContinuallyRunConnectableTask;
import org.apache.nifi.controller.tasks.ContinuallyRunProcessorTask;
import org.apache.nifi.controller.tasks.ReportingTaskWrapper;
import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.engine.FlowEngine;
import org.apache.nifi.processor.StandardProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.FormatUtils;
import org.quartz.CronExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QuartzSchedulingAgent extends AbstractSchedulingAgent {

    private final Logger logger = LoggerFactory.getLogger(QuartzSchedulingAgent.class);

    private final FlowController flowController;
    private final ProcessContextFactory contextFactory;
    private final StringEncryptor encryptor;

    private volatile String adminYieldDuration = "1 sec";
    private final Map<Object, List<AtomicBoolean>> canceledTriggers = new HashMap<>();

    public QuartzSchedulingAgent(final FlowController flowController, final FlowEngine flowEngine, final ProcessContextFactory contextFactory, final StringEncryptor enryptor) {
        super(flowEngine);
        this.flowController = flowController;
        this.contextFactory = contextFactory;
        this.encryptor = enryptor;
    }

    private StateManager getStateManager(final String componentId) {
        return flowController.getStateManagerProvider().getStateManager(componentId);
    }

    @Override
    public void shutdown() {
    }

    @Override
    public void doSchedule(final ReportingTaskNode taskNode, final ScheduleState scheduleState) {
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

        final ReportingTaskWrapper taskWrapper = new ReportingTaskWrapper(taskNode, scheduleState);

        final AtomicBoolean canceled = new AtomicBoolean(false);
        final Runnable command = new Runnable() {
            @Override
            public void run() {
                if (canceled.get()) {
                    return;
                }

                taskWrapper.run();

                if (canceled.get()) {
                    return;
                }

                final Date date = cronExpression.getTimeAfter(new Date());
                final long delay = date.getTime() - System.currentTimeMillis();

                logger.debug("Finished running Reporting Task {}; next scheduled time is at {} after a delay of {} milliseconds", taskNode, date, delay);
                flowEngine.schedule(this, delay, TimeUnit.MILLISECONDS);
            }
        };

        final List<AtomicBoolean> triggers = new ArrayList<>(1);
        triggers.add(canceled);
        canceledTriggers.put(taskNode, triggers);

        final Date initialDate = cronExpression.getTimeAfter(new Date());
        final long initialDelay = initialDate.getTime() - System.currentTimeMillis();
        flowEngine.schedule(command, initialDelay, TimeUnit.MILLISECONDS);
        scheduleState.setScheduled(true);
        logger.info("Scheduled Reporting Task {} to run threads on schedule {}", taskNode, cronSchedule);
    }

    @Override
    public synchronized void doSchedule(final Connectable connectable, final ScheduleState scheduleState) {
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
            final Callable<Boolean> continuallyRunTask;

            if (connectable.getConnectableType() == ConnectableType.PROCESSOR) {
                final ProcessorNode procNode = (ProcessorNode) connectable;

                final StandardProcessContext standardProcContext = new StandardProcessContext(procNode, flowController, encryptor, getStateManager(connectable.getIdentifier()));
                ContinuallyRunProcessorTask runnableTask = new ContinuallyRunProcessorTask(this, procNode, flowController, contextFactory, scheduleState, standardProcContext);
                continuallyRunTask = runnableTask;
            } else {
                final ConnectableProcessContext connProcContext = new ConnectableProcessContext(connectable, encryptor, getStateManager(connectable.getIdentifier()));
                continuallyRunTask = new ContinuallyRunConnectableTask(contextFactory, connectable, scheduleState, connProcContext);
            }

            final AtomicBoolean canceled = new AtomicBoolean(false);
            final Runnable command = new Runnable() {
                @Override
                public void run() {
                    if (canceled.get()) {
                        return;
                    }

                    try {
                        continuallyRunTask.call();
                    } catch (final RuntimeException re) {
                        throw re;
                    } catch (final Exception e) {
                        throw new ProcessException(e);
                    }

                    if (canceled.get()) {
                        return;
                    }

                    final Date date = cronExpression.getTimeAfter(new Date());
                    final long delay = date.getTime() - System.currentTimeMillis();

                    logger.debug("Finished task for {}; next scheduled time is at {} after a delay of {} milliseconds", connectable, date, delay);
                    flowEngine.schedule(this, delay, TimeUnit.MILLISECONDS);
                }
            };

            final Date initialDate = cronExpression.getTimeAfter(new Date());
            final long initialDelay = initialDate.getTime() - System.currentTimeMillis();
            flowEngine.schedule(command, initialDelay, TimeUnit.MILLISECONDS);
            triggers.add(canceled);
        }

        canceledTriggers.put(connectable, triggers);
        logger.info("Scheduled {} to run with {} threads on schedule {}", connectable, connectable.getMaxConcurrentTasks(), cronSchedule);
    }

    @Override
    public synchronized void doUnschedule(final Connectable connectable, final ScheduleState scheduleState) {
        unschedule((Object) connectable, scheduleState);
    }

    @Override
    public synchronized void doUnschedule(final ReportingTaskNode taskNode, final ScheduleState scheduleState) {
        unschedule((Object) taskNode, scheduleState);
    }

    private void unschedule(final Object scheduled, final ScheduleState scheduleState) {
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
}
