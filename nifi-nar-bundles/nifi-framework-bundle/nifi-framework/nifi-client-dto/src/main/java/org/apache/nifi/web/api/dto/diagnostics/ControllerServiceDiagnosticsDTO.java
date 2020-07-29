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

package org.apache.nifi.web.api.dto.diagnostics;

import javax.xml.bind.annotation.XmlType;

import org.apache.nifi.web.api.entity.ControllerServiceEntity;

import io.swagger.annotations.ApiModelProperty;

@XmlType(name = "controllerServiceDiagnostics")
public class ControllerServiceDiagnosticsDTO {
    private ControllerServiceEntity controllerService;
    private ClassLoaderDiagnosticsDTO classLoaderDiagnostics;

    public void setControllerService(final ControllerServiceEntity controllerService) {
        this.controllerService = controllerService;
    }

    @ApiModelProperty("The Controller Service")
    public ControllerServiceEntity getControllerService() {
        return controllerService;
    }

    public void setClassLoaderDiagnostics(ClassLoaderDiagnosticsDTO classLoaderDiagnostics) {
        this.classLoaderDiagnostics = classLoaderDiagnostics;
    }

    @ApiModelProperty("Information about the Controller Service's Class Loader")
    public ClassLoaderDiagnosticsDTO getClassLoaderDiagnostics() {
        return classLoaderDiagnostics;
    }
}
