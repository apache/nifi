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

package org.apache.nifi.registry.flow.mapping;

import org.apache.nifi.flow.VersionedFlowAnalysisRule;
import org.apache.nifi.flow.VersionedFlowRegistryClient;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedParameterProvider;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedReportingTask;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.registry.flow.diff.ComparableDataFlow;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class StandardComparableDataFlow implements ComparableDataFlow {
    private final String name;
    private final VersionedProcessGroup contents;
    private final Set<VersionedControllerService> controllerLevelServices;
    private final Set<VersionedReportingTask> reportingTasks;
    private final Set<VersionedFlowAnalysisRule> flowAnalysisRules;
    private final Set<VersionedParameterContext> parameterContexts;
    private final Set<VersionedParameterProvider> parameterProviders;
    private final Set<VersionedFlowRegistryClient> flowRegistryClients;

    public StandardComparableDataFlow(final String name, final VersionedProcessGroup contents) {
        this(name, contents, Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), Collections.emptySet());
    }

    public StandardComparableDataFlow(
            final String name,
            final VersionedProcessGroup contents,
            final Set<VersionedControllerService> controllerLevelServices,
            final Set<VersionedReportingTask> reportingTasks,
            final Set<VersionedFlowAnalysisRule> flowAnalysisRules,
            final Set<VersionedParameterContext> parameterContexts,
            final Set<VersionedParameterProvider> parameterProviders,
            final Set<VersionedFlowRegistryClient> flowRegistryClients
    ) {
        this.name = name;
        this.contents = contents;
        this.controllerLevelServices = controllerLevelServices == null ? Collections.emptySet() : new HashSet<>(controllerLevelServices);
        this.reportingTasks = reportingTasks == null ? Collections.emptySet() : new HashSet<>(reportingTasks);
        this.flowAnalysisRules = flowAnalysisRules == null ? Collections.emptySet() : new HashSet<>(flowAnalysisRules);
        this.parameterContexts = parameterContexts == null ? Collections.emptySet() : new HashSet<>(parameterContexts);
        this.parameterProviders = parameterProviders == null ? Collections.emptySet() : new HashSet<>(parameterProviders);
        this.flowRegistryClients = flowRegistryClients == null ? Collections.emptySet() : new HashSet<>(flowRegistryClients);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public VersionedProcessGroup getContents() {
        return contents;
    }

    @Override
    public Set<VersionedControllerService> getControllerLevelServices() {
        return controllerLevelServices;
    }

    @Override
    public Set<VersionedReportingTask> getReportingTasks() {
        return reportingTasks;
    }

    @Override
    public Set<VersionedFlowAnalysisRule> getFlowAnalysisRules() {
        return flowAnalysisRules;
    }

    @Override
    public Set<VersionedParameterContext> getParameterContexts() {
        return parameterContexts;
    }

    @Override
    public Set<VersionedParameterProvider> getParameterProviders() {
        return parameterProviders;
    }

    @Override
    public Set<VersionedFlowRegistryClient> getFlowRegistryClients() {
        return flowRegistryClients;
    }
}
