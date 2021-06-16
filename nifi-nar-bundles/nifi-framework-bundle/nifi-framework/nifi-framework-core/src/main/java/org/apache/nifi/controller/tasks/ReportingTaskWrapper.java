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
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.processor.SimpleProcessLogger;
import org.apache.nifi.util.ReflectionUtils;

public class ReportingTaskWrapper implements Runnable {

    private final ReportingTaskNode taskNode;
    private final LifecycleState lifecycleState;
    private final ExtensionManager extensionManager;

    public ReportingTaskWrapper(final ReportingTaskNode taskNode, final LifecycleState lifecycleState, final ExtensionManager extensionManager) {
        this.taskNode = taskNode;
        this.lifecycleState = lifecycleState;
        this.extensionManager = extensionManager;
    }

    @Override
    public synchronized void run() {
        lifecycleState.incrementActiveThreadCount(null);
        try (final NarCloseable narCloseable = NarCloseable.withComponentNarLoader(extensionManager, taskNode.getReportingTask().getClass(), taskNode.getIdentifier())) {
            taskNode.getReportingTask().onTrigger(taskNode.getReportingContext());
        } catch (final Throwable t) {
            final ComponentLog componentLog = new SimpleProcessLogger(taskNode.getIdentifier(), taskNode.getReportingTask());
            componentLog.error("Error running task {} due to {}", new Object[]{taskNode.getReportingTask(), t.toString()});
            if (componentLog.isDebugEnabled()) {
                componentLog.error("", t);
            }
        } finally {
            try {
                // if the reporting task is no longer scheduled to run and this is the last thread,
                // invoke the OnStopped methods
                if (!lifecycleState.isScheduled() && lifecycleState.getActiveThreadCount() == 1 && lifecycleState.mustCallOnStoppedMethods()) {
                    try (final NarCloseable x = NarCloseable.withComponentNarLoader(extensionManager, taskNode.getReportingTask().getClass(), taskNode.getIdentifier())) {
                        ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnStopped.class, taskNode.getReportingTask(), taskNode.getConfigurationContext());
                    }
                }
            } finally {
                lifecycleState.decrementActiveThreadCount(null);
            }
        }
    }

}
