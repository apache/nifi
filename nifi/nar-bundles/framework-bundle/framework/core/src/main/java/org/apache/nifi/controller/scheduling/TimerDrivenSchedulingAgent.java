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
import org.apache.nifi.util.FormatUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimerDrivenSchedulingAgent implements SchedulingAgent {

    private static final Logger logger = LoggerFactory.getLogger(TimerDrivenSchedulingAgent.class);

    private final FlowController flowController;
    private final FlowEngine flowEngine;
    private final ProcessContextFactory contextFactory;
    private final StringEncryptor encryptor;

    private volatile String adminYieldDuration = "1 sec";

    public TimerDrivenSchedulingAgent(final FlowController flowController, final FlowEngine flowEngine, final ProcessContextFactory contextFactory, final StringEncryptor encryptor) {
        this.flowController = flowController;
        this.flowEngine = flowEngine;
        this.contextFactory = contextFactory;
        this.encryptor = encryptor;
    }

    @Override
    public void shutdown() {
        flowEngine.shutdown();
    }

    @Override
    public void schedule(final ReportingTaskNode taskNode, final ScheduleState scheduleState) {
        final Runnable reportingTaskWrapper = new ReportingTaskWrapper(taskNode, scheduleState);
        final long schedulingNanos = taskNode.getSchedulingPeriod(TimeUnit.NANOSECONDS);

        final ScheduledFuture<?> future = flowEngine.scheduleWithFixedDelay(reportingTaskWrapper, 0L, schedulingNanos, TimeUnit.NANOSECONDS);
        final List<ScheduledFuture<?>> futures = new ArrayList<>(1);
        futures.add(future);
        scheduleState.setFutures(futures);

        logger.info("{} started.", taskNode.getReportingTask());
    }

    @Override
    public void schedule(final Connectable connectable, final ScheduleState scheduleState) {
        final Runnable runnable;
        if (connectable.getConnectableType() == ConnectableType.PROCESSOR) {
            final ProcessorNode procNode = (ProcessorNode) connectable;
            ContinuallyRunProcessorTask runnableTask = new ContinuallyRunProcessorTask(this, procNode, flowController, contextFactory, scheduleState, encryptor);
            runnable = runnableTask;
        } else {
            runnable = new ContinuallyRunConnectableTask(contextFactory, connectable, scheduleState, encryptor);
        }

        final List<ScheduledFuture<?>> futures = new ArrayList<>();
        for (int i = 0; i < connectable.getMaxConcurrentTasks(); i++) {
            final ScheduledFuture<?> future = flowEngine.scheduleWithFixedDelay(runnable, 0L, connectable.getSchedulingPeriod(TimeUnit.NANOSECONDS), TimeUnit.NANOSECONDS);
            futures.add(future);
        }

        scheduleState.setFutures(futures);
        logger.info("Scheduled {} to run with {} threads", connectable, connectable.getMaxConcurrentTasks());
    }

    @Override
    public void unschedule(final Connectable connectable, final ScheduleState scheduleState) {
        for (final ScheduledFuture<?> future : scheduleState.getFutures()) {
            // stop scheduling to run but do not interrupt currently running tasks.
            future.cancel(false);
        }

        logger.info("Stopped scheduling {} to run", connectable);
    }

    @Override
    public void unschedule(final ReportingTaskNode taskNode, final ScheduleState scheduleState) {
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

}
