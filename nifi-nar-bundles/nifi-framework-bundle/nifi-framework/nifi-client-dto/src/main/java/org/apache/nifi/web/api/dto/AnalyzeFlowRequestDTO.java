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

import javax.xml.bind.annotation.XmlType;
import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.util.Date;

/**
 * A request to analyze (a part) of the flow.
 */
@XmlType(name = "analyzeFlowRequest")
public class AnalyzeFlowRequestDTO {

    private String processGroupId;
    private String uri;

    private Date submissionTime;
    private Date lastUpdated;

    private Boolean finished;
    private String failureReason;

    private String state;

    /**
     * The id of the process group representing (a part of) the flow to be analyzed.
     *
     * @return The id
     */
    @ApiModelProperty("The id of the process group representing (a part of) the flow to be analyzed.")
    public String getProcessGroupId() {
        return this.processGroupId;
    }

    public void setProcessGroupId(final String processGroupId) {
        this.processGroupId = processGroupId;
    }

    /**
     * The uri for linking to this flow analysis request in this NiFi.
     *
     * @return The uri
     */
    @ApiModelProperty("The URI for future requests to this flow analysis request.")
    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    /**
     * @return time the request was submitted
     */
    @XmlJavaTypeAdapter(TimestampAdapter.class)
    @ApiModelProperty(
            value = "The timestamp when the request was submitted.",
            dataType = "string"
    )
    public Date getSubmissionTime() {
        return submissionTime;
    }

    public void setSubmissionTime(Date submissionTime) {
        this.submissionTime = submissionTime;
    }

    /**
     * @return whether the analysis has finished
     */
    @ApiModelProperty("Whether the analysis has finished (no matter how).")
    public Boolean isFinished() {
        return finished;
    }

    public void setFinished(Boolean finished) {
        this.finished = finished;
    }

    /**
     * @return the reason, if any, that this flow analysis request failed
     */
    @ApiModelProperty("The reason, if any, that this flow analysis request failed.")
    public String getFailureReason() {
        return failureReason;
    }

    public void setFailureReason(String failureReason) {
        this.failureReason = failureReason;
    }

    /**
     * @return the time this request was last updated
     */
    @XmlJavaTypeAdapter(TimestampAdapter.class)
    @ApiModelProperty(
            value = "The last time this flow analysis request was updated.",
            dataType = "string"
    )
    public Date getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(Date lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    /**
     * @return the current state of the flow analysis request.
     */
    @ApiModelProperty("The current state of the flow analysis request.")
    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

}
