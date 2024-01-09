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

import io.swagger.v3.oas.annotations.media.Schema;
import org.apache.nifi.web.api.dto.util.TimestampAdapter;

import jakarta.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.util.Date;

public abstract class FlowUpdateRequestDTO {
    protected String requestId;
    protected String processGroupId;
    protected String uri;
    protected Date lastUpdated;
    protected boolean complete = false;
    protected String failureReason;
    protected int percentCompleted;
    protected String state;

    @Schema(description = "The unique ID of the Process Group being updated")
    public String getProcessGroupId() {
        return processGroupId;
    }

    public void setProcessGroupId(String processGroupId) {
        this.processGroupId = processGroupId;
    }

    @Schema(description = "The unique ID of this request.", accessMode = Schema.AccessMode.READ_ONLY)
    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    @Schema(description = "The URI for future requests to this drop request.", accessMode = Schema.AccessMode.READ_ONLY)
    public String getUri() {
        return uri;
    }

    public void setUri(String uri) {
        this.uri = uri;
    }

    @XmlJavaTypeAdapter(TimestampAdapter.class)
    @Schema(description = "The last time this request was updated.", type = "string", accessMode = Schema.AccessMode.READ_ONLY)
    public Date getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(Date lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    @Schema(description = "Whether or not this request has completed", accessMode = Schema.AccessMode.READ_ONLY)
    public boolean isComplete() {
        return complete;
    }

    public void setComplete(boolean complete) {
        this.complete = complete;
    }

    @Schema(description = "An explanation of why this request failed, or null if this request has not failed", accessMode = Schema.AccessMode.READ_ONLY)
    public String getFailureReason() {
        return failureReason;
    }

    public void setFailureReason(String reason) {
        this.failureReason = reason;
    }

    @Schema(description = "The state of the request", accessMode = Schema.AccessMode.READ_ONLY)
    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    @Schema(description = "The percentage complete for the request, between 0 and 100", accessMode = Schema.AccessMode.READ_ONLY)
    public int getPercentCompleted() {
        return percentCompleted;
    }

    public void setPercentCompleted(int percentCompleted) {
        this.percentCompleted = percentCompleted;
    }
}
