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
package org.apache.nifi.controller.tasks;

import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.scheduling.ScheduleState;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReportingTaskWrapper implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(ReportingTaskWrapper.class);

    private final ReportingTaskNode taskNode;
    private final ScheduleState scheduleState;

    public ReportingTaskWrapper(final ReportingTaskNode taskNode, final ScheduleState scheduleState) {
        this.taskNode = taskNode;
        this.scheduleState = scheduleState;
    }

    @SuppressWarnings("deprecation")
    @Override
    public synchronized void run() {
        scheduleState.incrementActiveThreadCount();
        try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
            taskNode.getReportingTask().onTrigger(taskNode.getReportingContext());
        } catch (final Throwable t) {
            logger.error("Error running task {} due to {}", taskNode.getReportingTask(), t.toString());
            if (logger.isDebugEnabled()) {
                logger.error("", t);
            }
        } finally {
            // if the processor is no longer scheduled to run and this is the last thread,
            // invoke the OnStopped methods
            if (!scheduleState.isScheduled() && scheduleState.getActiveThreadCount() == 1 && scheduleState.mustCallOnStoppedMethods()) {
                try (final NarCloseable x = NarCloseable.withNarLoader()) {
                    ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnStopped.class, org.apache.nifi.processor.annotation.OnStopped.class, taskNode.getReportingTask(), taskNode.getReportingContext());
                }
            }

            scheduleState.decrementActiveThreadCount();
        }
    }

}
