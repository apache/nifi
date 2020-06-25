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
package org.apache.nifi.diagnostics.bootstrap.tasks;

import org.apache.nifi.controller.ActiveThreadInfo;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ThreadDetails;
import org.apache.nifi.diagnostics.DiagnosticTask;
import org.apache.nifi.diagnostics.DiagnosticsDumpElement;
import org.apache.nifi.diagnostics.StandardDiagnosticsDumpElement;
import org.apache.nifi.util.FormatUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class LongRunningProcessorTask implements DiagnosticTask {
    private static final long MIN_ACTIVE_MILLIS = 30_000L;

    private final FlowController flowController;

    public LongRunningProcessorTask(final FlowController flowController) {
        this.flowController = flowController;
    }

    @Override
    public DiagnosticsDumpElement captureDump(final boolean verbose) {
        final List<String> details = new ArrayList<>();
        final ThreadDetails threadDetails = ThreadDetails.capture();

        for (final ProcessorNode processorNode : flowController.getFlowManager().getRootGroup().findAllProcessors()) {
            final List<ActiveThreadInfo> activeThreads = processorNode.getActiveThreads(threadDetails);

            for (final ActiveThreadInfo activeThread : activeThreads) {
                if (activeThread.getActiveMillis() > MIN_ACTIVE_MILLIS) {
                    String threadName = activeThread.getThreadName();
                    if (activeThread.isTerminated()) {
                        threadName = threadName + " (Terminated)";
                    }

                    details.add(processorNode + " - " + threadName + " has been active for " + FormatUtils.formatMinutesSeconds(activeThread.getActiveMillis(), TimeUnit.MILLISECONDS) + " minutes");
                }
            }
        }

        if (details.isEmpty()) {
            details.add("No long-running tasks identified");
        }

        return new StandardDiagnosticsDumpElement("Long-Running Processor Tasks", details);
    }
}
