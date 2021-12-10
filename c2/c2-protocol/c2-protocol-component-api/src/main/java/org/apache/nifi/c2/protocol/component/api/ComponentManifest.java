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

import io.swagger.annotations.ApiModel;
import io.swagger.annotations.ApiModelProperty;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;

@ApiModel
public class ComponentManifest implements Serializable {
    private static final long serialVersionUID = 1L;

    private List<DefinedType> apis;
    private List<ControllerServiceDefinition> controllerServices;
    private List<ProcessorDefinition> processors;
    private List<ReportingTaskDefinition> reportingTasks;

    @ApiModelProperty("Public interfaces defined in this bundle")
    public List<DefinedType> getApis() {
        return (apis != null ? Collections.unmodifiableList(apis) : null);
    }

    public void setApis(List<DefinedType> apis) {
        this.apis = apis;
    }

    @ApiModelProperty("Controller Services provided in this bundle")
    public List<ControllerServiceDefinition> getControllerServices() {
        return (controllerServices != null ? Collections.unmodifiableList(controllerServices) : null);
    }

    public void setControllerServices(List<ControllerServiceDefinition> controllerServices) {
        this.controllerServices = controllerServices;
    }

    @ApiModelProperty("Processors provided in this bundle")
    public List<ProcessorDefinition> getProcessors() {
        return (processors != null ? Collections.unmodifiableList(processors) : null);
    }

    public void setProcessors(List<ProcessorDefinition> processors) {
        this.processors = processors;
    }

    @ApiModelProperty("Reporting Tasks provided in this bundle")
    public List<ReportingTaskDefinition> getReportingTasks() {
        return (reportingTasks != null ? Collections.unmodifiableList(reportingTasks) : null);
    }

    public void setReportingTasks(List<ReportingTaskDefinition> reportingTasks) {
        this.reportingTasks = reportingTasks;
    }

}
