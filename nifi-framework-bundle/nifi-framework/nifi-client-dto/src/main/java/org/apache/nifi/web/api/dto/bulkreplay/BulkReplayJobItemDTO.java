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
package org.apache.nifi.web.api.dto.bulkreplay;

import io.swagger.v3.oas.annotations.media.Schema;
import jakarta.xml.bind.annotation.XmlType;

/**
 * A single item in a bulk replay job. On submission, only the provenance-related fields are
 * required. Server-assigned fields (itemId, itemIndex, status, times) are populated in responses.
 */
@XmlType(name = "bulkReplayJobItem")
public class BulkReplayJobItemDTO {

    // --- Server-assigned ---
    private String itemId;
    private Integer itemIndex;
    private BulkReplayItemStatus status;
    private String errorMessage;
    private String startTime;
    private String endTime;
    private String lastUpdated;

    // --- Submitted by client ---
    private Long provenanceEventId;
    private String clusterNodeId;
    private String flowFileUuid;
    private String eventType;
    private String eventTime;
    private String componentName;

    private Long fileSizeBytes;

    @Schema(description = "Server-assigned unique id of this item.")
    public String getItemId() {
        return itemId;
    }

    public void setItemId(String itemId) {
        this.itemId = itemId;
    }

    @Schema(description = "Zero-based index of this item within the job.")
    public Integer getItemIndex() {
        return itemIndex;
    }

    public void setItemIndex(Integer itemIndex) {
        this.itemIndex = itemIndex;
    }

    @Schema(description = "Current status of this item.")
    public BulkReplayItemStatus getStatus() {
        return status;
    }

    public void setStatus(BulkReplayItemStatus status) {
        this.status = status;
    }

    @Schema(description = "Error message if replay failed, null otherwise.")
    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    @Schema(description = "Time at which replay of this item began.", type = "string")
    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    @Schema(description = "Time at which replay of this item completed.", type = "string")
    public String getEndTime() {
        return endTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

    @Schema(description = "Time at which this record was last updated.", type = "string")
    public String getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(String lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    @Schema(description = "The numeric id of the provenance event to replay.")
    public Long getProvenanceEventId() {
        return provenanceEventId;
    }

    public void setProvenanceEventId(Long provenanceEventId) {
        this.provenanceEventId = provenanceEventId;
    }

    @Schema(description = "The cluster node id that owns the provenance event. Null in standalone mode.")
    public String getClusterNodeId() {
        return clusterNodeId;
    }

    public void setClusterNodeId(String clusterNodeId) {
        this.clusterNodeId = clusterNodeId;
    }

    @Schema(description = "The UUID of the FlowFile associated with this event.")
    public String getFlowFileUuid() {
        return flowFileUuid;
    }

    public void setFlowFileUuid(String flowFileUuid) {
        this.flowFileUuid = flowFileUuid;
    }

    @Schema(description = "The type of the provenance event.")
    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    @Schema(description = "The timestamp of the provenance event.")
    public String getEventTime() {
        return eventTime;
    }

    public void setEventTime(String eventTime) {
        this.eventTime = eventTime;
    }

    @Schema(description = "The name of the component that generated the provenance event.")
    public String getComponentName() {
        return componentName;
    }

    public void setComponentName(String componentName) {
        this.componentName = componentName;
    }

    @Schema(description = "The size of the FlowFile content in bytes. Used to determine if replay can proceed when a cluster node is disconnected.")
    public Long getFileSizeBytes() {
        return fileSizeBytes;
    }

    public void setFileSizeBytes(Long fileSizeBytes) {
        this.fileSizeBytes = fileSizeBytes;
    }
}
