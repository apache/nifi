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
package org.apache.nifi.controller.flowanalysis;

import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.flow.VersionedProcessGroup;

/**
 * Analyzes components, parts or the entirety of the flow.
 */
public interface FlowAnalyzer {

    /**
     * Returns whether flow analysis should be scheduled
     * @return true if flow analysis should be scheduled, false otherwise
     */
    boolean isFlowAnalysisRequired();

    /**
     * Sets whether flow analysis should be scheduled
     */
    void setFlowAnalysisRequired(boolean flowAnalysisRequired);

    /**
     * Analyzes a processor
     *
     * @param processorNode the processor (as a node) to be analyzed
     */
    void analyzeProcessor(ProcessorNode processorNode);

    /**
     * Analyzes a controller service
     *
     * @param controllerServiceNode the controller service (as a node) to be analyzed
     */
    void analyzeControllerService(ControllerServiceNode controllerServiceNode);

    /**
     * Analyze the flow or a part of it
     *
     * @param processGroup The process group (as a {@link org.apache.nifi.flow.VersionedComponent VersionedComponent})
     *                     representing (a part of) the flow to be analyzed. Recursive - all child process groups will
     *                     be analyzed as well.
     */
    void analyzeProcessGroup(VersionedProcessGroup processGroup);
}
