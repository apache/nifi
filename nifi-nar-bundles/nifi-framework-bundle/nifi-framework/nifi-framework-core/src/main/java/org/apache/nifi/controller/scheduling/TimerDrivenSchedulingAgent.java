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
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.StandardProcessContext;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimerDrivenSchedulingAgent extends AbstractSchedulingAgent {

    private static final Logger logger = LoggerFactory.getLogger(TimerDrivenSchedulingAgent.class);
    private final long noWorkYieldNanos;

    private final FlowController flowController;
    private final ProcessContextFactory contextFactory;
    private final StringEncryptor encryptor;
    private final VariableRegistry variableRegistry;

    private volatile boolean connectableUnscheduled;

    private volatile String adminYieldDuration = "1 sec";

    public TimerDrivenSchedulingAgent(
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
        final Runnable reportingTaskWrapper = new ReportingTaskWrapper(taskNode, scheduleState);
        final long schedulingNanos = taskNode.getSchedulingPeriod(TimeUnit.NANOSECONDS);

        final ScheduledFuture<?> future = flowEngine.scheduleWithFixedDelay(reportingTaskWrapper, 0L, schedulingNanos, TimeUnit.NANOSECONDS);
        final List<ScheduledFuture<?>> futures = new ArrayList<>(1);
        futures.add(future);
        scheduleState.setFutures(futures);

        logger.info("{} started.", taskNode.getReportingTask());
    }

    @Override
    public void doSchedule(final Connectable connectable, final ScheduleState scheduleState) {
        this.connectableUnscheduled = false;
        final Callable<Boolean> continuallyRunTask;
        final ProcessContext processContext;

        // Determine the task to run and create it.
        if (connectable.getConnectableType() == ConnectableType.PROCESSOR) {
            final ProcessorNode procNode = (ProcessorNode) connectable;
            processContext = new StandardProcessContext(procNode, flowController, encryptor, getStateManager(connectable.getIdentifier()), variableRegistry);
            continuallyRunTask = new ContinuallyRunProcessorTask(this, procNode, flowController, contextFactory, scheduleState, (StandardProcessContext)processContext);
        } else {
            processContext = new ConnectableProcessContext(connectable, encryptor, getStateManager(connectable.getIdentifier()));
            continuallyRunTask = new ContinuallyRunConnectableTask(contextFactory, connectable, scheduleState, processContext);
        }

        Runnable continuallyRunTaskDelegate = new Runnable() {    
            @Override
            public void run() {
                try {
                    if (!connectableUnscheduled) {
                        boolean shouldYield = continuallyRunTask.call();
                        long resubmissionDelay = connectable.getYieldExpiration() > 0 ? (connectable.getYieldExpiration() - System.currentTimeMillis())
                                : (noWorkYieldNanos > 0L && shouldYield ? noWorkYieldNanos : connectable.getSchedulingPeriod(TimeUnit.MILLISECONDS));
                        flowEngine.schedule(this, resubmissionDelay, TimeUnit.MILLISECONDS);
                    }
                } catch (Exception e) {
                    // should never happen since any exceptions raised from 'continuallyRunTask' will be handled there
                    throw new ProcessException(e);
                }
            }
        };
               
        for (int i = 0; i < connectable.getMaxConcurrentTasks(); i++) {
            this.flowEngine.execute(continuallyRunTaskDelegate);
        }

        logger.info("Scheduled {} to run with {} threads", connectable, connectable.getMaxConcurrentTasks());
    }

    @Override
    public void doUnschedule(final Connectable connectable, final ScheduleState scheduleState) {
        this.connectableUnscheduled = true;
        logger.info("Stopped scheduling {} to run", connectable);
    }

    @Override
    public void doUnschedule(final ReportingTaskNode taskNode, final ScheduleState scheduleState) {
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
