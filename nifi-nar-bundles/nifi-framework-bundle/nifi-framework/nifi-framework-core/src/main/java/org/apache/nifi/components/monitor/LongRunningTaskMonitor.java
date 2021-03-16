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

import org.apache.nifi.controller.ActiveThreadInfo;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ThreadDetails;
import org.apache.nifi.controller.flow.FlowManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class LongRunningTaskMonitor implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(LongRunningTaskMonitor.class);

    private final FlowManager flowManager;
    private final long thresholdMillis;

    public LongRunningTaskMonitor(FlowManager flowManager, long thresholdMillis) {
        this.flowManager = flowManager;
        this.thresholdMillis = thresholdMillis;
    }

    @Override
    public void run() {
        LOGGER.debug("Checking long running processor tasks...");

        int activeThreadCount = 0;
        int longRunningThreadCount = 0;

        ThreadDetails threadDetails = ThreadDetails.capture();

        for (ProcessorNode processorNode : flowManager.getRootGroup().findAllProcessors()) {
            List<ActiveThreadInfo> activeThreads = processorNode.getActiveThreads(threadDetails);
            activeThreadCount += activeThreads.size();

            for (ActiveThreadInfo activeThread : activeThreads) {
                if (activeThread.getActiveMillis() > thresholdMillis) {
                    longRunningThreadCount++;

                    LOGGER.warn(String.format("Long running task detected on processor [id=%s, type=%s, name=%s]. Thread name: %s; Active time: %,d; Stack trace:\n%s",
                            processorNode.getIdentifier(), processorNode.getComponentType(), processorNode.getName(),
                            activeThread.getThreadName(), activeThread.getActiveMillis(), activeThread.getStackTrace()));

                    processorNode.getLogger().warn(String.format("Long running task detected on the processor [thread name: %s; active time: %,d].",
                            activeThread.getThreadName(), activeThread.getActiveMillis()));
                }
            }
        }

        LOGGER.info("Active threads: {}; Long running threads: {}", activeThreadCount, longRunningThreadCount);
    }
}
