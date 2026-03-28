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
 * Summary of a bulk replay job. Returned by list and get-by-id endpoints.
 * Also used as the POST request body (with {@code items} populated via {@link BulkReplayJobDetailDTO}).
 */
@XmlType(name = "bulkReplayJobSummary")
public class BulkReplayJobSummaryDTO {

    private String jobId;
    private String jobName;
    private String processorId;
    private String processorName;
    private String processorType;
    private String groupId;
    private String submittedBy;
    private String submissionTime;
    private String startTime;
    private String endTime;
    private String lastUpdated;
    private BulkReplayJobStatus status;
    private String statusMessage;
    private Integer totalItems;
    private Integer processedItems;
    private Integer succeededItems;
    private Integer failedItems;
    private Integer percentComplete;
    private String disconnectWaitDeadline;

    @Schema(description = "The unique id of this bulk replay job.")
    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    @Schema(description = "Human-readable name for this job. Defaults to the processor name and submission timestamp if not provided.")
    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    @Schema(description = "The id of the processor whose provenance events are being replayed.")
    public String getProcessorId() {
        return processorId;
    }

    public void setProcessorId(String processorId) {
        this.processorId = processorId;
    }

    @Schema(description = "The name of the processor.")
    public String getProcessorName() {
        return processorName;
    }

    public void setProcessorName(String processorName) {
        this.processorName = processorName;
    }

    @Schema(description = "The type of the processor.")
    public String getProcessorType() {
        return processorType;
    }

    public void setProcessorType(String processorType) {
        this.processorType = processorType;
    }

    @Schema(description = "The id of the process group containing the processor.")
    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    @Schema(description = "The identity of the user who submitted this job.")
    public String getSubmittedBy() {
        return submittedBy;
    }

    public void setSubmittedBy(String submittedBy) {
        this.submittedBy = submittedBy;
    }

    @Schema(description = "The time at which this job was submitted.", type = "string")
    public String getSubmissionTime() {
        return submissionTime;
    }

    public void setSubmissionTime(String submissionTime) {
        this.submissionTime = submissionTime;
    }

    @Schema(description = "The time at which execution began.", type = "string")
    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    @Schema(description = "The time at which execution completed.", type = "string")
    public String getEndTime() {
        return endTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

    @Schema(description = "The time at which this record was last updated.", type = "string")
    public String getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(String lastUpdated) {
        this.lastUpdated = lastUpdated;
    }

    @Schema(description = "The current status of this job.")
    public BulkReplayJobStatus getStatus() {
        return status;
    }

    public void setStatus(BulkReplayJobStatus status) {
        this.status = status;
    }

    @Schema(description = "A human-readable description of the current status.")
    public String getStatusMessage() {
        return statusMessage;
    }

    public void setStatusMessage(String statusMessage) {
        this.statusMessage = statusMessage;
    }

    @Schema(description = "The total number of items selected for replay.")
    public Integer getTotalItems() {
        return totalItems;
    }

    public void setTotalItems(Integer totalItems) {
        this.totalItems = totalItems;
    }

    @Schema(description = "The number of items that have been processed (succeeded + failed).")
    public Integer getProcessedItems() {
        return processedItems;
    }

    public void setProcessedItems(Integer processedItems) {
        this.processedItems = processedItems;
    }

    @Schema(description = "The number of items that were successfully replayed.")
    public Integer getSucceededItems() {
        return succeededItems;
    }

    public void setSucceededItems(Integer succeededItems) {
        this.succeededItems = succeededItems;
    }

    @Schema(description = "The number of items whose replay failed.")
    public Integer getFailedItems() {
        return failedItems;
    }

    public void setFailedItems(Integer failedItems) {
        this.failedItems = failedItems;
    }

    @Schema(description = "Percentage of items processed, 0-100.")
    public Integer getPercentComplete() {
        return percentComplete;
    }

    public void setPercentComplete(Integer percentComplete) {
        this.percentComplete = percentComplete;
    }

    @Schema(description = "ISO-8601 deadline for the current disconnect wait. Null when not waiting for a disconnected node.", type = "string")
    public String getDisconnectWaitDeadline() {
        return disconnectWaitDeadline;
    }

    public void setDisconnectWaitDeadline(String disconnectWaitDeadline) {
        this.disconnectWaitDeadline = disconnectWaitDeadline;
    }
}
