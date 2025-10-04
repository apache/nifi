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

package org.apache.nifi.c2.protocol.component.api;

import io.swagger.v3.oas.annotations.media.Schema;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

public class ComponentManifest implements Serializable {
    private static final long serialVersionUID = 1L;

    private List<DefinedType> apis;
    private List<ControllerServiceDefinition> controllerServices;
    private List<ProcessorDefinition> processors;
    private List<ReportingTaskDefinition> reportingTasks;
    private List<ParameterProviderDefinition> parameterProviders;
    private List<FlowRegistryClientDefinition> flowRegistryClients;
    private List<FlowAnalysisRuleDefinition> flowAnalysisRules;

    @Schema(description = "Public interfaces defined in this bundle")
    public List<DefinedType> getApis() {
        return (apis != null ? Collections.unmodifiableList(apis) : null);
    }

    public void setApis(List<DefinedType> apis) {
        this.apis = apis;
    }

    @Schema(description = "Controller Services provided in this bundle")
    public List<ControllerServiceDefinition> getControllerServices() {
        return (controllerServices != null ? Collections.unmodifiableList(controllerServices) : null);
    }

    public void setControllerServices(List<ControllerServiceDefinition> controllerServices) {
        this.controllerServices = controllerServices;
    }

    @Schema(description = "Processors provided in this bundle")
    public List<ProcessorDefinition> getProcessors() {
        return (processors != null ? Collections.unmodifiableList(processors) : null);
    }

    public void setProcessors(List<ProcessorDefinition> processors) {
        this.processors = processors;
    }

    @Schema(description = "Reporting Tasks provided in this bundle")
    public List<ReportingTaskDefinition> getReportingTasks() {
        return (reportingTasks != null ? Collections.unmodifiableList(reportingTasks) : null);
    }

    public void setReportingTasks(List<ReportingTaskDefinition> reportingTasks) {
        this.reportingTasks = reportingTasks;
    }

    @Schema(description = "Parameter Providers provided in this bundle")
    public List<ParameterProviderDefinition> getParameterProviders() {
        return (parameterProviders != null ? Collections.unmodifiableList(parameterProviders) : null);
    }

    public void setParameterProviders(List<ParameterProviderDefinition> parameterProviders) {
        this.parameterProviders = parameterProviders;
    }

    @Schema(description = "Flow Analysis Rules provided in this bundle")
    public List<FlowAnalysisRuleDefinition> getFlowAnalysisRules() {
        return (flowAnalysisRules != null ? Collections.unmodifiableList(flowAnalysisRules) : null);
    }

    public void setFlowAnalysisRules(List<FlowAnalysisRuleDefinition> flowAnalysisRules) {
        this.flowAnalysisRules = flowAnalysisRules;
    }

    @Schema(description = "Flow Registry Clients provided in this bundle")
    public List<FlowRegistryClientDefinition> getFlowRegistryClients() {
        return (flowRegistryClients != null ? Collections.unmodifiableList(flowRegistryClients) : null);
    }

    public void setFlowRegistryClients(List<FlowRegistryClientDefinition> flowRegistryClients) {
        this.flowRegistryClients = flowRegistryClients;
    }

}
