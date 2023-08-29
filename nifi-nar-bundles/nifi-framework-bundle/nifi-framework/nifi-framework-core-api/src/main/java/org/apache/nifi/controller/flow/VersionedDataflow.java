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

package org.apache.nifi.controller.flow;

import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedFlowAnalysisRule;
import org.apache.nifi.flow.VersionedFlowRegistryClient;
import org.apache.nifi.flow.VersionedParameterProvider;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedReportingTask;
import org.apache.nifi.flow.VersionedParameterContext;

import java.util.List;

public class VersionedDataflow {
    private VersionedFlowEncodingVersion encodingVersion;
    private int maxTimerDrivenThreadCount;
    private List<VersionedFlowRegistryClient> registries;
    private List<VersionedParameterContext> parameterContexts;
    private List<VersionedParameterProvider> parameterProviders;
    private List<VersionedControllerService> controllerServices;
    private List<VersionedReportingTask> reportingTasks;
    private List<VersionedFlowAnalysisRule> flowAnalysisRules;
    private VersionedProcessGroup rootGroup;

    public VersionedFlowEncodingVersion getEncodingVersion() {
        return encodingVersion;
    }

    public void setEncodingVersion(final VersionedFlowEncodingVersion encodingVersion) {
        this.encodingVersion = encodingVersion;
    }

    public int getMaxTimerDrivenThreadCount() {
        return maxTimerDrivenThreadCount;
    }

    public void setMaxTimerDrivenThreadCount(final int maxTimerDrivenThreadCount) {
        this.maxTimerDrivenThreadCount = maxTimerDrivenThreadCount;
    }

    public List<VersionedFlowRegistryClient> getRegistries() {
        return registries;
    }

    public void setRegistries(final List<VersionedFlowRegistryClient> registries) {
        this.registries = registries;
    }

    public List<VersionedParameterContext> getParameterContexts() {
        return parameterContexts;
    }

    public void setParameterContexts(final List<VersionedParameterContext> parameterContexts) {
        this.parameterContexts = parameterContexts;
    }

    public List<VersionedControllerService> getControllerServices() {
        return controllerServices;
    }

    public void setControllerServices(final List<VersionedControllerService> controllerServices) {
        this.controllerServices = controllerServices;
    }

    public List<VersionedReportingTask> getReportingTasks() {
        return reportingTasks;
    }

    public void setReportingTasks(final List<VersionedReportingTask> reportingTasks) {
        this.reportingTasks = reportingTasks;
    }

    public List<VersionedFlowAnalysisRule> getFlowAnalysisRules() {
        return flowAnalysisRules;
    }

    public void setFlowAnalysisRules(List<VersionedFlowAnalysisRule> flowAnalysisRules) {
        this.flowAnalysisRules = flowAnalysisRules;
    }

    public List<VersionedParameterProvider> getParameterProviders() {
        return parameterProviders;
    }

    public void setParameterProviders(final List<VersionedParameterProvider> parameterProviders) {
        this.parameterProviders = parameterProviders;
    }

    public VersionedProcessGroup getRootGroup() {
        return rootGroup;
    }

    public void setRootGroup(final VersionedProcessGroup rootGroup) {
        this.rootGroup = rootGroup;
    }
}
