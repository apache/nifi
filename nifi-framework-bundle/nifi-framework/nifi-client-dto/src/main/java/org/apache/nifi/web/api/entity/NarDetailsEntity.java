/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.web.api.entity;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.xml.bind.annotation.XmlType;
import org.apache.nifi.web.api.dto.DocumentedTypeDTO;
import org.apache.nifi.web.api.dto.NarCoordinateDTO;
import org.apache.nifi.web.api.dto.NarSummaryDTO;

import java.util.Set;

@XmlType(name = "narDetailsEntity")
public class NarDetailsEntity extends Entity {

    private NarSummaryDTO narSummary;
    private Set<NarCoordinateDTO> dependentCoordinates;
    private Set<DocumentedTypeDTO> processorTypes;
    private Set<DocumentedTypeDTO> controllerServiceTypes;
    private Set<DocumentedTypeDTO> reportingTaskTypes;
    private Set<DocumentedTypeDTO> parameterProviderTypes;
    private Set<DocumentedTypeDTO> flowRegistryClientTypes;
    private Set<DocumentedTypeDTO> flowAnalysisRuleTypes;

    @Schema(description = "The NAR summary")
    public NarSummaryDTO getNarSummary() {
        return narSummary;
    }

    public void setNarSummary(final NarSummaryDTO narSummary) {
        this.narSummary = narSummary;
    }

    @Schema(description = "The coordinates of NARs that depend on this NAR")
    public Set<NarCoordinateDTO> getDependentCoordinates() {
        return dependentCoordinates;
    }

    public void setDependentCoordinates(final Set<NarCoordinateDTO> dependentCoordinates) {
        this.dependentCoordinates = dependentCoordinates;
    }

    @Schema(description = "The Processor types contained in the NAR")
    public Set<DocumentedTypeDTO> getProcessorTypes() {
        return processorTypes;
    }

    public void setProcessorTypes(final Set<DocumentedTypeDTO> processorTypes) {
        this.processorTypes = processorTypes;
    }

    @Schema(description = "The ControllerService types contained in the NAR")
    public Set<DocumentedTypeDTO> getControllerServiceTypes() {
        return controllerServiceTypes;
    }

    public void setControllerServiceTypes(final Set<DocumentedTypeDTO> controllerServiceTypes) {
        this.controllerServiceTypes = controllerServiceTypes;
    }

    @Schema(description = "The ReportingTask types contained in the NAR")
    public Set<DocumentedTypeDTO> getReportingTaskTypes() {
        return reportingTaskTypes;
    }

    public void setReportingTaskTypes(final Set<DocumentedTypeDTO> reportingTaskTypes) {
        this.reportingTaskTypes = reportingTaskTypes;
    }

    @Schema(description = "The ParameterProvider types contained in the NAR")
    public Set<DocumentedTypeDTO> getParameterProviderTypes() {
        return parameterProviderTypes;
    }

    public void setParameterProviderTypes(final Set<DocumentedTypeDTO> parameterProviderTypes) {
        this.parameterProviderTypes = parameterProviderTypes;
    }

    @Schema(description = "The FlowRegistryClient types contained in the NAR")
    public Set<DocumentedTypeDTO> getFlowRegistryClientTypes() {
        return flowRegistryClientTypes;
    }

    public void setFlowRegistryClientTypes(final Set<DocumentedTypeDTO> flowRegistryClientTypes) {
        this.flowRegistryClientTypes = flowRegistryClientTypes;
    }

    public Set<DocumentedTypeDTO> getFlowAnalysisRuleTypes() {
        return flowAnalysisRuleTypes;
    }

    @Schema(description = "The FlowAnalysisRule types contained in the NAR")
    public void setFlowAnalysisRuleTypes(final Set<DocumentedTypeDTO> flowAnalysisRuleTypes) {
        this.flowAnalysisRuleTypes = flowAnalysisRuleTypes;
    }
}
