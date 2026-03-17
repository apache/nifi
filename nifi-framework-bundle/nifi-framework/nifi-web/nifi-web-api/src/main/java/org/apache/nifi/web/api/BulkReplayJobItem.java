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
package org.apache.nifi.web.api;

import org.apache.nifi.web.api.dto.bulkreplay.BulkReplayItemStatus;

import java.time.Instant;

/**
 * Thread-safe in-memory representation of a single item within a bulk replay job.
 * Only the owner node creates and holds {@code BulkReplayJobItem} instances.
 * Status fields use {@code volatile} so the worker thread and API threads see
 * consistent values without locking.
 */
public class BulkReplayJobItem {

    // Immutable fields set at creation
    private final String itemId;
    private final String jobId;
    private final int itemIndex;
    private final Long provenanceEventId;
    private final String clusterNodeId;
    private final String flowFileUuid;
    private final String eventType;
    private final String eventTime;
    private final String componentName;
    private final long fileSizeBytes;

    // Mutable status fields — written by worker, read by API threads
    private volatile BulkReplayItemStatus status = BulkReplayItemStatus.QUEUED;
    private volatile String errorMessage;
    private volatile Instant startTime;
    private volatile Instant endTime;
    private volatile Instant lastUpdated;

    public BulkReplayJobItem(final String itemId,
                              final String jobId,
                              final int itemIndex,
                              final Long provenanceEventId,
                              final String clusterNodeId,
                              final String flowFileUuid,
                              final String eventType,
                              final String eventTime,
                              final String componentName,
                              final long fileSizeBytes) {
        this.itemId = itemId;
        this.jobId = jobId;
        this.itemIndex = itemIndex;
        this.provenanceEventId = provenanceEventId;
        this.clusterNodeId = clusterNodeId;
        this.flowFileUuid = flowFileUuid;
        this.eventType = eventType;
        this.eventTime = eventTime;
        this.componentName = componentName;
        this.fileSizeBytes = fileSizeBytes;
        this.lastUpdated = Instant.now();
    }

    public String getItemId() {
        return itemId;
    }

    public String getJobId() {
        return jobId;
    }

    public int getItemIndex() {
        return itemIndex;
    }

    public Long getProvenanceEventId() {
        return provenanceEventId;
    }

    public String getClusterNodeId() {
        return clusterNodeId;
    }

    public String getFlowFileUuid() {
        return flowFileUuid;
    }

    public String getEventType() {
        return eventType;
    }

    public String getEventTime() {
        return eventTime;
    }

    public String getComponentName() {
        return componentName;
    }

    public long getFileSizeBytes() {
        return fileSizeBytes;
    }

    public BulkReplayItemStatus getStatus() {
        return status;
    }

    public void setStatus(final BulkReplayItemStatus status) {
        this.status = status;
        touch();
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(final String errorMessage) {
        this.errorMessage = errorMessage;
        touch();
    }

    public Instant getStartTime() {
        return startTime;
    }

    public void setStartTime(final Instant startTime) {
        this.startTime = startTime;
        touch();
    }

    public Instant getEndTime() {
        return endTime;
    }

    public void setEndTime(final Instant endTime) {
        this.endTime = endTime;
        touch();
    }

    public Instant getLastUpdated() {
        return lastUpdated;
    }

    private void touch() {
        lastUpdated = Instant.now();
    }
}
