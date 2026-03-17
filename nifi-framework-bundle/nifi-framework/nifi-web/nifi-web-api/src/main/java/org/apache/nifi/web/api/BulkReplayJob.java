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
import org.apache.nifi.web.api.dto.bulkreplay.BulkReplayJobStatus;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread-safe in-memory representation of a bulk replay job. Only the owner node creates and
 * holds a {@code BulkReplayJob}. Progress fields use {@link AtomicInteger} and {@code volatile}
 * so the worker thread and API threads see consistent values without locking.
 */
public class BulkReplayJob {

    // Immutable identity fields
    private final String jobId;
    private final String jobName;
    private final String submittedBy;
    private final Instant submissionTime;
    private final String processorId;
    private final String processorName;
    private final String processorType;
    private final String groupId;
    private final List<BulkReplayJobItem> items;

    // Mutable — written by worker, read by API threads
    private volatile BulkReplayJobStatus status = BulkReplayJobStatus.QUEUED;
    private volatile String statusMessage = "Queued";
    private volatile Instant startTime;
    private volatile Instant endTime;
    private volatile Instant lastUpdated;
    private volatile boolean cancelRequested = false;
    private volatile boolean deleteRequested = false;
    private final AtomicBoolean submitted = new AtomicBoolean(false);

    private volatile Instant disconnectWaitDeadline;

    private final AtomicInteger processedItems = new AtomicInteger(0);
    private final AtomicInteger succeededItems = new AtomicInteger(0);
    private final AtomicInteger failedItems = new AtomicInteger(0);

    public BulkReplayJob(final String jobId,
                         final String jobName,
                         final String submittedBy,
                         final Instant submissionTime,
                         final String processorId,
                         final String processorName,
                         final String processorType,
                         final String groupId,
                         final List<BulkReplayJobItem> items) {
        this.jobId = jobId;
        this.jobName = jobName;
        this.submittedBy = submittedBy;
        this.submissionTime = submissionTime;
        this.processorId = processorId;
        this.processorName = processorName;
        this.processorType = processorType;
        this.groupId = groupId;
        this.items = Collections.unmodifiableList(items);
        this.lastUpdated = Instant.now();
    }

    public String getJobId() {
        return jobId;
    }

    public String getJobName() {
        return jobName;
    }

    public String getSubmittedBy() {
        return submittedBy;
    }

    public Instant getSubmissionTime() {
        return submissionTime;
    }

    public String getProcessorId() {
        return processorId;
    }

    public String getProcessorName() {
        return processorName;
    }

    public String getProcessorType() {
        return processorType;
    }

    public String getGroupId() {
        return groupId;
    }

    public List<BulkReplayJobItem> getItems() {
        return items;
    }

    public int getTotalItems() {
        return items.size();
    }

    public BulkReplayJobStatus getStatus() {
        return status;
    }

    public void setStatus(final BulkReplayJobStatus status) {
        this.status = status;
        touch();
    }

    public String getStatusMessage() {
        return statusMessage;
    }

    public void setStatusMessage(final String statusMessage) {
        this.statusMessage = statusMessage;
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

    public boolean isCancelRequested() {
        return cancelRequested;
    }

    public void setCancelRequested(final boolean cancelRequested) {
        this.cancelRequested = cancelRequested;
        touch();
    }

    public boolean isDeleteRequested() {
        return deleteRequested;
    }

    public void setDeleteRequested(final boolean deleteRequested) {
        this.deleteRequested = deleteRequested;
        touch();
    }

    public boolean isSubmitted() {
        return submitted.get();
    }

    public void setSubmitted(final boolean val) {
        submitted.set(val);
    }

    public boolean tryMarkSubmitted() {
        return submitted.compareAndSet(false, true);
    }

    public int getProcessedItems() {
        return processedItems.get();
    }

    public int getSucceededItems() {
        return succeededItems.get();
    }

    public int getFailedItems() {
        return failedItems.get();
    }

    /** Called by the worker after each item completes (succeeded or failed). */
    public void recordItemProcessed(final boolean succeeded) {
        processedItems.incrementAndGet();
        if (succeeded) {
            succeededItems.incrementAndGet();
        } else {
            failedItems.incrementAndGet();
        }
        touch();
    }

    public Instant getDisconnectWaitDeadline() {
        return disconnectWaitDeadline;
    }

    public void setDisconnectWaitDeadline(final Instant disconnectWaitDeadline) {
        this.disconnectWaitDeadline = disconnectWaitDeadline;
        touch();
    }

    public int getPercentComplete() {
        final int total = getTotalItems();
        if (total == 0) {
            return 100;
        }
        return processedItems.get() * 100 / total;
    }

    /**
     * Recalculates the processedItems, succeededItems, and failedItems counters from the
     * current item statuses. Used when resuming a job after failover.
     */
    public void initializeCountersFromItems() {
        int processed = 0;
        int succeeded = 0;
        int failed = 0;
        for (final BulkReplayJobItem item : items) {
            final BulkReplayItemStatus itemStatus = item.getStatus();
            if (itemStatus == BulkReplayItemStatus.SUCCEEDED) {
                processed++;
                succeeded++;
            } else if (itemStatus == BulkReplayItemStatus.FAILED) {
                processed++;
                failed++;
            }
        }
        processedItems.set(processed);
        succeededItems.set(succeeded);
        failedItems.set(failed);
    }

    private void touch() {
        lastUpdated = Instant.now();
    }
}
