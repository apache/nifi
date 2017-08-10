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

import java.util.Date;
import java.util.List;

import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;

import org.apache.nifi.web.api.dto.util.TimestampAdapter;

import com.wordnik.swagger.annotations.ApiModelProperty;

@XmlType(name = "variableRegistryUpdateRequest")
public class VariableRegistryUpdateRequestDTO {
    private String requestId;
    private String processGroupId;
    private String uri;
    private Date submissionTime = new Date();
    private Date lastUpdated = new Date();
    private boolean complete = false;
    private String failureReason;
    private List<VariableRegistryUpdateStepDTO> updateSteps;


    @ApiModelProperty("The unique ID of the Process Group that the variable registry belongs to")
    public String getProcessGroupId() {
        return processGroupId;
    }

    public void setProcessGroupId(String processGroupId) {
        this.processGroupId = processGroupId;
    }

    @ApiModelProperty(value = "The unique ID of this request.", readOnly = true)
    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    @ApiModelProperty(value = "The URI for future requests to this drop request.", readOnly = true)
    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    @XmlJavaTypeAdapter(TimestampAdapter.class)
    @ApiModelProperty(value = "The time at which this request was submitted.", dataType = "string", readOnly = true)
    public Date getSubmissionTime() {
        return submissionTime;
    }

    public void setSubmissionTime(Date submissionTime) {
        this.submissionTime = submissionTime;
    }

    @XmlJavaTypeAdapter(TimestampAdapter.class)
    @ApiModelProperty(value = "The last time this request was updated.", dataType = "string", readOnly = true)
    public Date getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(Date lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    @ApiModelProperty(value = "The steps that are required in order to complete the request, along with the status of each", readOnly = true)
    public List<VariableRegistryUpdateStepDTO> getUpdateSteps() {
        return updateSteps;
    }

    public void setUpdateSteps(List<VariableRegistryUpdateStepDTO> updateSteps) {
        this.updateSteps = updateSteps;
    }

    @ApiModelProperty(value = "Whether or not this request has completed", readOnly = true)
    public boolean isComplete() {
        return complete;
    }

    public void setComplete(boolean complete) {
        this.complete = complete;
    }

    @ApiModelProperty(value = "An explanation of why this request failed, or null if this request has not failed", readOnly = true)
    public String getFailureReason() {
        return failureReason;
    }

    public void setFailureReason(String reason) {
        this.failureReason = reason;
    }
}
