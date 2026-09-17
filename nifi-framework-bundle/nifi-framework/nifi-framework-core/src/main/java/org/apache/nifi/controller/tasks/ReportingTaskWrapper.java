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
import org.apache.nifi.controller.scheduling.LifecycleState;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.logging.StandardLoggingContext;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.processor.StandardComponentLog;
import org.apache.nifi.util.ReflectionUtils;

import java.util.function.BooleanSupplier;

public class ReportingTaskWrapper implements Runnable {

    private static final BooleanSupplier SCHEDULING_GENERATION_ALWAYS_ACTIVE = () -> true;

    private final ReportingTaskNode taskNode;
    private final LifecycleState lifecycleState;
    private final ExtensionManager extensionManager;
    private final BooleanSupplier schedulingGenerationActive;

    public ReportingTaskWrapper(final ReportingTaskNode taskNode, final LifecycleState lifecycleState, final ExtensionManager extensionManager) {
        this(taskNode, lifecycleState, extensionManager, SCHEDULING_GENERATION_ALWAYS_ACTIVE);
    }

    public ReportingTaskWrapper(final ReportingTaskNode taskNode, final LifecycleState lifecycleState, final ExtensionManager extensionManager,
                                final BooleanSupplier schedulingGenerationActive) {
        this.taskNode = taskNode;
        this.lifecycleState = lifecycleState;
        this.extensionManager = extensionManager;
        this.schedulingGenerationActive = schedulingGenerationActive;
    }

    @Override
    public void run() {
        final boolean activeThreadCountIncremented;
        synchronized (lifecycleState) {
            activeThreadCountIncremented = schedulingGenerationActive.getAsBoolean() && lifecycleState.tryIncrementActiveThreadCount(null);
        }
        if (!activeThreadCountIncremented) {
            return;
        }

        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, taskNode.getReportingTask().getClass(), taskNode.getIdentifier())) {
            taskNode.getReportingTask().onTrigger(taskNode.getReportingContext());
        } catch (final Throwable t) {
            final ComponentLog componentLog = new StandardComponentLog(taskNode.getIdentifier(), taskNode.getReportingTask(), new StandardLoggingContext());
            componentLog.error("Error running task {}", taskNode.getReportingTask(), t);
            if (componentLog.isDebugEnabled()) {
                componentLog.error("", t);
            }
        } finally {
            try {
                // if the reporting task is no longer scheduled to run and this is the last thread,
                // invoke the OnStopped methods
                if (!lifecycleState.isScheduled() && lifecycleState.getActiveThreadCount() == 1 && lifecycleState.mustCallOnStoppedMethods()) {
                    try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(extensionManager, taskNode.getReportingTask().getClass(), taskNode.getIdentifier())) {
                        ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnStopped.class, taskNode.getReportingTask(), taskNode.getConfigurationContext());
                    }
                }
            } finally {
                lifecycleState.decrementActiveThreadCount();
            }
        }
    }

}
