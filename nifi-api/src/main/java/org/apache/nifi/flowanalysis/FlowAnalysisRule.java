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

import org.apache.nifi.components.ConfigurableComponent;
import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.reporting.InitializationException;

import java.util.Collection;
import java.util.Collections;

/**
 * A single rule that can analyze components or a flow (represented by a process group)
 */
public interface FlowAnalysisRule extends ConfigurableComponent {
    /**
     * Provides the Flow Analysis Rule with access to objects that may be of use
     * throughout its lifecycle
     *
     * @param context see {@link FlowAnalysisRuleInitializationContext}
     * @throws org.apache.nifi.reporting.InitializationException if unable to initialize
     */
    void initialize(FlowAnalysisRuleInitializationContext context) throws InitializationException;

    /**
     * Analyze a component provided by the framework.
     * This is a callback method invoked by the framework.
     * It should be expected that this method will be called with any and all available components.
     *
     * @param component the component to be analyzed
     * @param context   see {@link FlowAnalysisRuleContext}
     * @return a collection of {@link ComponentAnalysisResult} as the result of the analysis of the given component
     */
    default Collection<ComponentAnalysisResult> analyzeComponent(VersionedComponent component, FlowAnalysisRuleContext context) {
        return Collections.emptySet();
    }

    /**
     * Analyze a flow or a part of it, represented by a process group.
     * This is a callback method invoked by the framework.
     * It should be expected that this method will be called by the root process group and all of its child process groups.
     * In case a flow analysis is requested for a particular process group this method will be called for all it's child
     * process groups as well.
     *
     * @param processGroup the process group to be analyzed
     * @param context      see {@link FlowAnalysisRuleContext}
     * @return a collection of {@link GroupAnalysisResult} as the result of the analysis.
     * One {@link GroupAnalysisResult} in the collection can either refer to a component within the analyzed process group,
     * to a child process group or the entirety of the process group
     */
    default Collection<GroupAnalysisResult> analyzeProcessGroup(VersionedProcessGroup processGroup, FlowAnalysisRuleContext context) {
        return Collections.emptySet();
    }
}
