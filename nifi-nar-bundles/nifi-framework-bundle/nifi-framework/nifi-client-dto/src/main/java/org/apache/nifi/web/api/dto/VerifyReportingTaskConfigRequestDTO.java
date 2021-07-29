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

package org.apache.nifi.web.api.dto;

import io.swagger.annotations.ApiModelProperty;
import io.swagger.annotations.ApiModelProperty.AccessMode;

import javax.xml.bind.annotation.XmlType;
import java.util.List;

@XmlType(name = "verifyReportingTaskConfigRequest")
public class VerifyReportingTaskConfigRequestDTO extends AsynchronousRequestDTO<VerifyConfigUpdateStepDTO> {
    private String reportingTaskId;
    private ReportingTaskDTO reportingTask;
    private List<ConfigVerificationResultDTO> results;

    @ApiModelProperty("The ID of the Controller Service whose configuration was verified")
    public String getReportingTaskId() {
        return reportingTaskId;
    }

    public void setReportingTaskId(final String reportingTaskId) {
        this.reportingTaskId = reportingTaskId;
    }

    @ApiModelProperty("The Controller Service")
    public ReportingTaskDTO getReportingTask() {
        return reportingTask;
    }

    public void setReportingTask(final ReportingTaskDTO reportingTask) {
        this.reportingTask = reportingTask;
    }

    @ApiModelProperty(value="The Results of the verification", accessMode = AccessMode.READ_ONLY)
    public List<ConfigVerificationResultDTO> getResults() {
        return results;
    }

    public void setResults(final List<ConfigVerificationResultDTO> results) {
        this.results = results;
    }
}
