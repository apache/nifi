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
package org.apache.nifi.components.monitor;

import com.google.common.annotations.VisibleForTesting;
import org.apache.nifi.controller.ActiveThreadInfo;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ThreadDetails;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.reporting.Severity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.NumberFormat;
import java.util.List;

public class LongRunningTaskMonitor implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(LongRunningTaskMonitor.class);

    private final FlowManager flowManager;
    private final EventReporter eventReporter;
    private final long thresholdMillis;

    public LongRunningTaskMonitor(FlowManager flowManager, EventReporter eventReporter, long thresholdMillis) {
        this.flowManager = flowManager;
        this.eventReporter = eventReporter;
        this.thresholdMillis = thresholdMillis;
    }

    @Override
    public void run() {
        getLogger().debug("Checking long running processor tasks...");
        final long start = System.nanoTime();

        int activeThreadCount = 0;
        int longRunningThreadCount = 0;

        ThreadDetails threadDetails = captureThreadDetails();

        for (ProcessorNode processorNode : flowManager.getRootGroup().findAllProcessors()) {
            List<ActiveThreadInfo> activeThreads = processorNode.getActiveThreads(threadDetails);
            activeThreadCount += activeThreads.size();

            for (ActiveThreadInfo activeThread : activeThreads) {
                if (activeThread.getActiveMillis() > thresholdMillis) {
                    longRunningThreadCount++;

                    String taskSeconds = String.format("%,d seconds", activeThread.getActiveMillis() / 1000);

                    getLogger().warn(String.format("Long running task detected on processor [id=%s, name=%s, type=%s]. Task time: %s. Stack trace:\n%s",
                            processorNode.getIdentifier(), processorNode.getName(), processorNode.getComponentType(), taskSeconds, activeThread.getStackTrace()));

                    eventReporter.reportEvent(Severity.WARNING, "Long Running Task", String.format("Processor with ID %s, Name %s and Type %s has a task that has been running for %s " +
                            "(thread name: %s).", processorNode.getIdentifier(), processorNode.getName(), processorNode.getComponentType(), taskSeconds, activeThread.getThreadName()));

                    processorNode.getLogger().warn(String.format("The processor has a task that has been running for %s (thread name: %s).",
                            taskSeconds, activeThread.getThreadName()));
                }
            }
        }

        final long nanos = System.nanoTime() - start;
        getLogger().info("Active threads: {}; Long running threads: {}; time to check: {} nanos", activeThreadCount, longRunningThreadCount, NumberFormat.getInstance().format(nanos));
    }

    @VisibleForTesting
    protected Logger getLogger() {
        return LOGGER;
    }

    @VisibleForTesting
    protected ThreadDetails captureThreadDetails() {
        return ThreadDetails.capture();
    }
}
