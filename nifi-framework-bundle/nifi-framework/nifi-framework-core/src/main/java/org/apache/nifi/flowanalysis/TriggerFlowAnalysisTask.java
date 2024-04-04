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
package org.apache.nifi.flowanalysis;

import org.apache.nifi.controller.flowanalysis.FlowAnalyzer;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

public class TriggerFlowAnalysisTask implements Runnable {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private final FlowAnalyzer flowAnalyzer;
    private final Supplier<VersionedProcessGroup> rootProcessGroupSupplier;

    public TriggerFlowAnalysisTask(FlowAnalyzer flowAnalyzer, Supplier<VersionedProcessGroup> rootProcessGroupSupplier) {
        this.flowAnalyzer = flowAnalyzer;
        this.rootProcessGroupSupplier = rootProcessGroupSupplier;
    }

    @Override
    public void run() {
        if (flowAnalyzer.isFlowAnalysisRequired()) {
            logger.debug("Triggering analysis of entire flow");
            try {
                flowAnalyzer.analyzeProcessGroup(rootProcessGroupSupplier.get());
                flowAnalyzer.setFlowAnalysisRequired(false);
            } catch (final Throwable t) {
                logger.error("Encountered unexpected error when attempting to analyze flow", t);
            }
        } else {
            logger.trace("Flow hasn't changed, flow analysis is put on hold");
        }
    }
}
