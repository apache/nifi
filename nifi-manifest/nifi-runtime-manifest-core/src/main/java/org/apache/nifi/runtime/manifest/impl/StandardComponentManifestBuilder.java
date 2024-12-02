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
package org.apache.nifi.runtime.manifest.impl;

import org.apache.nifi.c2.protocol.component.api.ComponentManifest;
import org.apache.nifi.c2.protocol.component.api.ControllerServiceDefinition;
import org.apache.nifi.c2.protocol.component.api.FlowAnalysisRuleDefinition;
import org.apache.nifi.c2.protocol.component.api.ParameterProviderDefinition;
import org.apache.nifi.c2.protocol.component.api.ProcessorDefinition;
import org.apache.nifi.c2.protocol.component.api.ReportingTaskDefinition;
import org.apache.nifi.runtime.manifest.ComponentManifestBuilder;

import java.util.ArrayList;
import java.util.List;

/**
 * Standard implementation of ComponentManifestBuilder.
 */
public class StandardComponentManifestBuilder implements ComponentManifestBuilder {

    private final List<ProcessorDefinition> processors = new ArrayList<>();
    private final List<ControllerServiceDefinition> controllerServices = new ArrayList<>();
    private final List<ReportingTaskDefinition> reportingTasks = new ArrayList<>();
    private final List<ParameterProviderDefinition> parameterProviders = new ArrayList<>();
    private final List<FlowAnalysisRuleDefinition> flowAnalysisRules = new ArrayList<>();

    @Override
    public ComponentManifestBuilder addProcessor(final ProcessorDefinition processorDefinition) {
        if (processorDefinition == null) {
            throw new IllegalArgumentException("Processor definition cannot be null");
        }
        processors.add(processorDefinition);
        return this;
    }

    @Override
    public ComponentManifestBuilder addControllerService(final ControllerServiceDefinition controllerServiceDefinition) {
        if (controllerServiceDefinition == null) {
            throw new IllegalArgumentException("Controller Service definition cannot be null");
        }
        controllerServices.add(controllerServiceDefinition);
        return this;
    }

    @Override
    public ComponentManifestBuilder addReportingTask(final ReportingTaskDefinition reportingTaskDefinition) {
        if (reportingTaskDefinition == null) {
            throw new IllegalArgumentException("Reporting task definition cannot be null");
        }
        reportingTasks.add(reportingTaskDefinition);
        return this;
    }

    @Override
    public ComponentManifestBuilder addParameterProvider(ParameterProviderDefinition parameterProviderDefinition) {
        if (parameterProviderDefinition == null) {
            throw new IllegalArgumentException("Parameter Provider definition cannot be null");
        }
        parameterProviders.add(parameterProviderDefinition);
        return this;
    }

    @Override
    public ComponentManifestBuilder addFlowAnalysisRule(FlowAnalysisRuleDefinition flowAnalysisRuleDefinition) {
        if (flowAnalysisRuleDefinition == null) {
            throw new IllegalArgumentException("Flow Analysis Rule definition cannot be null");
        }
        flowAnalysisRules.add(flowAnalysisRuleDefinition);
        return this;
    }

    @Override
    public ComponentManifest build() {
        final ComponentManifest componentManifest = new ComponentManifest();
        componentManifest.setProcessors(new ArrayList<>(processors));
        componentManifest.setControllerServices(new ArrayList<>(controllerServices));
        componentManifest.setReportingTasks(new ArrayList<>(reportingTasks));
        componentManifest.setParameterProviders(new ArrayList<>(parameterProviders));
        componentManifest.setFlowAnalysisRules(new ArrayList<>(flowAnalysisRules));
        return componentManifest;
    }

}
