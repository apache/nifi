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
import org.apache.nifi.web.api.dto.util.TimestampAdapter;

import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.util.Date;
import java.util.List;

public abstract class AsynchronousRequestDTO<T extends UpdateStepDTO> {
    private String requestId;
    private String uri;
    private Date submissionTime;
    private Date lastUpdated;
    private boolean complete = false;
    private String failureReason;
    private int percentCompleted;
    private String state;
    private List<T> updateSteps;


    @ApiModelProperty(value = "The ID of the request", accessMode = ApiModelProperty.AccessMode.READ_ONLY)
    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(final String requestId) {
        this.requestId = requestId;
    }

    @ApiModelProperty(value = "The URI for the request", accessMode = ApiModelProperty.AccessMode.READ_ONLY)
    public String getUri() {
        return uri;
    }

    public void setUri(final String uri) {
        this.uri = uri;
    }

    @XmlJavaTypeAdapter(TimestampAdapter.class)
    @ApiModelProperty(value = "The timestamp of when the request was submitted", accessMode = ApiModelProperty.AccessMode.READ_ONLY)
    public Date getSubmissionTime() {
        return submissionTime;
    }

    public void setSubmissionTime(final Date submissionTime) {
        this.submissionTime = submissionTime;
    }

    @XmlJavaTypeAdapter(TimestampAdapter.class)
    @ApiModelProperty(value = "The timestamp of when the request was last updated", accessMode = ApiModelProperty.AccessMode.READ_ONLY)
    public Date getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(final Date lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    @ApiModelProperty(value = "Whether or not the request is completed", accessMode = ApiModelProperty.AccessMode.READ_ONLY)
    public boolean isComplete() {
        return complete;
    }

    public void setComplete(final boolean complete) {
        this.complete = complete;
    }

    @ApiModelProperty(value = "The reason for the request failing, or null if the request has not failed", accessMode = ApiModelProperty.AccessMode.READ_ONLY)
    public String getFailureReason() {
        return failureReason;
    }

    public void setFailureReason(final String failureReason) {
        this.failureReason = failureReason;
    }

    @ApiModelProperty(value = "A value between 0 and 100 (inclusive) indicating how close the request is to completion", accessMode = ApiModelProperty.AccessMode.READ_ONLY)
    public int getPercentCompleted() {
        return percentCompleted;
    }

    public void setPercentCompleted(final int percentCompleted) {
        this.percentCompleted = percentCompleted;
    }

    @ApiModelProperty(value = "A description of the current state of the request", accessMode = ApiModelProperty.AccessMode.READ_ONLY)
    public String getState() {
        return state;
    }

    public void setState(final String state) {
        this.state = state;
    }

    @ApiModelProperty(value = "The steps that are required in order to complete the request, along with the status of each", accessMode = ApiModelProperty.AccessMode.READ_ONLY)
    public List<T> getUpdateSteps() {
        return updateSteps;
    }

    public void setUpdateSteps(List<T> updateSteps) {
        this.updateSteps = updateSteps;
    }
}
