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
package org.apache.nifi.runtime.manifest;

import org.apache.nifi.c2.protocol.component.api.ComponentManifest;
import org.apache.nifi.c2.protocol.component.api.ControllerServiceDefinition;
import org.apache.nifi.c2.protocol.component.api.FlowAnalysisRuleDefinition;
import org.apache.nifi.c2.protocol.component.api.FlowRegistryClientDefinition;
import org.apache.nifi.c2.protocol.component.api.ParameterProviderDefinition;
import org.apache.nifi.c2.protocol.component.api.ProcessorDefinition;
import org.apache.nifi.c2.protocol.component.api.ReportingTaskDefinition;

/**
 * Builder for creating a ComponentManifest.
 */
public interface ComponentManifestBuilder {

    /**
     * @param processorDefinition a processor definition to add
     * @return the builder
     */
    ComponentManifestBuilder addProcessor(ProcessorDefinition processorDefinition);

    /**
     * @param controllerServiceDefinition a controller service definition to add
     * @return the builder
     */
    ComponentManifestBuilder addControllerService(ControllerServiceDefinition controllerServiceDefinition);

    /**
     * @param reportingTaskDefinition a reporting task definition to add
     * @return the builder
     */
    ComponentManifestBuilder addReportingTask(ReportingTaskDefinition reportingTaskDefinition);

    /**
     * @param parameterProviderDefinition a parameter provider definition to add
     * @return the builder
     */
    ComponentManifestBuilder addParameterProvider(ParameterProviderDefinition parameterProviderDefinition);

    /**
     * @param flowAnalysisRuleDefinition a flow analysis rule definition to add
     * @return the builder
     */
    ComponentManifestBuilder addFlowAnalysisRule(FlowAnalysisRuleDefinition flowAnalysisRuleDefinition);

    /**
     * @param flowRegistryClientDefinition a flow registry client definition to add
     * @return the builder
     */
    ComponentManifestBuilder addFlowRegistryClient(FlowRegistryClientDefinition flowRegistryClientDefinition);

    /**
     * @return a component manifest containing all the added definitions
     */
    ComponentManifest build();

}
