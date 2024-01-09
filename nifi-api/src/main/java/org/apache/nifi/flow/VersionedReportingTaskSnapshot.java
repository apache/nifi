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
package org.apache.nifi.flow;

import io.swagger.v3.oas.annotations.media.Schema;

import java.util.List;

@Schema
public class VersionedReportingTaskSnapshot {

    private List<VersionedReportingTask> reportingTasks;
    private List<VersionedControllerService> controllerServices;

    @Schema(description = "The controller services")
    public List<VersionedControllerService> getControllerServices() {
        return controllerServices;
    }

    public void setControllerServices(List<VersionedControllerService> controllerServices) {
        this.controllerServices = controllerServices;
    }

    @Schema(description = "The reporting tasks")
    public List<VersionedReportingTask> getReportingTasks() {
        return reportingTasks;
    }

    public void setReportingTasks(List<VersionedReportingTask> reportingTasks) {
        this.reportingTasks = reportingTasks;
    }

}
