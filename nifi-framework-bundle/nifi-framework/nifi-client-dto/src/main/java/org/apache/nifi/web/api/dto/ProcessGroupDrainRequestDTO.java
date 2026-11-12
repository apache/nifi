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
import jakarta.xml.bind.annotation.XmlType;

import java.util.List;

/**
 * A request to drain a Process Group. While the drain is active, the components that bring new data into the group are kept stopped
 * and the other components are kept running, so that the data already in the group can flow out of it.
 */
@XmlType(name = "processGroupDrainRequest")
public class ProcessGroupDrainRequestDTO {

    private String id;
    private String processGroupId;
    private String uri;

    private String state;
    private String failureReason;
    private Boolean prepared;

    private Integer originalCount;
    private Long originalSize;
    private Integer currentCount;
    private Long currentSize;

    private List<DrainBlockingComponentDTO> blockingComponents;

    @Schema(description = "The identifier of the drain request.")
    public String getId() {
        return id;
    }

    public void setId(final String id) {
        this.id = id;
    }

    @Schema(description = "The identifier of the Process Group that is being drained.")
    public String getProcessGroupId() {
        return processGroupId;
    }

    public void setProcessGroupId(final String processGroupId) {
        this.processGroupId = processGroupId;
    }

    @Schema(description = "The URI for future requests to this drain request.")
    public String getUri() {
        return uri;
    }

    public void setUri(final String uri) {
        this.uri = uri;
    }

    @Schema(description = "The state of the drain. Only a DRAINING drain is active.", allowableValues = {"DRAINING", "FAILED", "ENDED"})
    public String getState() {
        return state;
    }

    public void setState(final String state) {
        this.state = state;
    }

    @Schema(description = "The reason that the drain failed, if it failed.")
    public String getFailureReason() {
        return failureReason;
    }

    public void setFailureReason(final String failureReason) {
        this.failureReason = failureReason;
    }

    @Schema(description = "Whether the components that bring data into the Process Group have stopped and the other components have been started.")
    public Boolean getPrepared() {
        return prepared;
    }

    public void setPrepared(final Boolean prepared) {
        this.prepared = prepared;
    }

    @Schema(description = "The number of FlowFiles that were queued in the Process Group once the components that bring data into it had stopped.")
    public Integer getOriginalCount() {
        return originalCount;
    }

    public void setOriginalCount(final Integer originalCount) {
        this.originalCount = originalCount;
    }

    @Schema(description = "The size, in bytes, of the FlowFiles that were queued in the Process Group once the components that bring data into it had stopped.")
    public Long getOriginalSize() {
        return originalSize;
    }

    public void setOriginalSize(final Long originalSize) {
        this.originalSize = originalSize;
    }

    @Schema(description = "The number of FlowFiles that are queued in the Process Group now.")
    public Integer getCurrentCount() {
        return currentCount;
    }

    public void setCurrentCount(final Integer currentCount) {
        this.currentCount = currentCount;
    }

    @Schema(description = "The size, in bytes, of the FlowFiles that are queued in the Process Group now.")
    public Long getCurrentSize() {
        return currentSize;
    }

    public void setCurrentSize(final Long currentSize) {
        this.currentSize = currentSize;
    }

    @Schema(description = "The components that have FlowFiles queued but cannot process them because they are disabled, or stopped and invalid.")
    public List<DrainBlockingComponentDTO> getBlockingComponents() {
        return blockingComponents;
    }

    public void setBlockingComponents(final List<DrainBlockingComponentDTO> blockingComponents) {
        this.blockingComponents = blockingComponents;
    }
}
