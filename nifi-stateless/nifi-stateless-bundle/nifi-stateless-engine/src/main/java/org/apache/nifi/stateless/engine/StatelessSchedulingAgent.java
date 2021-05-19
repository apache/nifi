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

package org.apache.nifi.stateless.engine;

import org.apache.nifi.connectable.Connectable;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.scheduling.LifecycleState;
import org.apache.nifi.controller.scheduling.SchedulingAgent;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.processor.SimpleProcessLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class StatelessSchedulingAgent implements SchedulingAgent {
    private static final Logger logger = LoggerFactory.getLogger(StatelessSchedulingAgent.class);
    private final ExtensionManager extensionManager;

    public StatelessSchedulingAgent(final ExtensionManager extensionManager) {
        this.extensionManager = extensionManager;
    }

    @Override
    public void schedule(final Connectable connectable, final LifecycleState scheduleState) {
    }

    @Override
    public void scheduleOnce(Connectable connectable, LifecycleState scheduleState, Callable<Future<Void>> stopCallback) {
    }

    @Override
    public void unschedule(final Connectable connectable, final LifecycleState scheduleState) {
    }

    @Override
    public void onEvent(final Connectable connectable) {
    }

    @Override
    public void schedule(final ReportingTaskNode taskNode, final LifecycleState scheduleState) {
        final long schedulingMillis = taskNode.getSchedulingPeriod(TimeUnit.MILLISECONDS);
        final Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Thread.sleep(schedulingMillis);
                    } catch (final InterruptedException e) {
                        logger.info("Interrupted while waiting to trigger {}. Will no longer trigger Reporting Task to run", taskNode);
                        return;
                    }

                    triggerReportingTask(taskNode, scheduleState);
                }
            }
        });

        thread.setName("Trigger Reporting Task " + taskNode.getName());
        thread.setDaemon(true);
        thread.start();
    }

    private void triggerReportingTask(final ReportingTaskNode taskNode, final LifecycleState scheduleState) {
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(extensionManager, taskNode.getReportingTask().getClass(), taskNode.getIdentifier())) {
            logger.debug("Triggering {} to run", taskNode);
            scheduleState.incrementActiveThreadCount(null);

            try {
                taskNode.getReportingTask().onTrigger(taskNode.getReportingContext());
            } finally {
                scheduleState.decrementActiveThreadCount(null);
            }

        } catch (final Throwable t) {
            final ComponentLog componentLog = new SimpleProcessLogger(taskNode.getIdentifier(), taskNode.getReportingTask());
            componentLog.error("Error running task {} due to {}", new Object[]{taskNode.getReportingTask(), t.toString()});
            if (componentLog.isDebugEnabled()) {
                componentLog.error("", t);
            }
        }
    }

    @Override
    public void unschedule(final ReportingTaskNode taskNode, final LifecycleState scheduleState) {
    }

    @Override
    public void setMaxThreadCount(final int maxThreads) {
    }

    @Override
    public void incrementMaxThreadCount(final int toAdd) {
    }

    @Override
    public void setAdministrativeYieldDuration(final String duration) {
    }

    @Override
    public String getAdministrativeYieldDuration() {
        return "1 sec";
    }

    @Override
    public long getAdministrativeYieldDuration(final TimeUnit timeUnit) {
        return timeUnit.convert(1, TimeUnit.SECONDS);
    }

    @Override
    public void shutdown() {
    }
}
