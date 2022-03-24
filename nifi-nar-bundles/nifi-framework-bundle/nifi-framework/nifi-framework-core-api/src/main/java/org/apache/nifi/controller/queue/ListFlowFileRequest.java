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

package org.apache.nifi.controller.queue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ListFlowFileRequest implements ListFlowFileStatus {
    private final String requestId;
    private final int maxResults;
    private final QueueSize queueSize;
    private final long submissionTime = System.currentTimeMillis();
    private final List<FlowFileSummary> flowFileSummaries = new ArrayList<>();

    private ListFlowFileState state = ListFlowFileState.WAITING_FOR_LOCK;
    private String failureReason;
    private long lastUpdated = System.currentTimeMillis();

    public ListFlowFileRequest(final String requestId, final int maxResults, final QueueSize queueSize) {
        this.requestId = requestId;
        this.maxResults = maxResults;
        this.queueSize = queueSize;
    }

    @Override
    public String getRequestIdentifier() {
        return requestId;
    }

    @Override
    public long getRequestSubmissionTime() {
        return submissionTime;
    }

    @Override
    public synchronized long getLastUpdated() {
        return lastUpdated;
    }

    @Override
    public synchronized ListFlowFileState getState() {
        return state;
    }

    @Override
    public synchronized String getFailureReason() {
        return failureReason;
    }

    public synchronized void setState(final ListFlowFileState state) {
        this.state = state;
        this.lastUpdated = System.currentTimeMillis();
    }

    public synchronized void setFailure(final String explanation) {
        this.state = ListFlowFileState.FAILURE;
        this.failureReason = explanation;
        this.lastUpdated = System.currentTimeMillis();
    }

    @Override
    public synchronized List<FlowFileSummary> getFlowFileSummaries() {
        return Collections.unmodifiableList(flowFileSummaries);
    }

    public synchronized void setFlowFileSummaries(final List<FlowFileSummary> summaries) {
        this.flowFileSummaries.clear();
        this.flowFileSummaries.addAll(summaries);
        lastUpdated = System.currentTimeMillis();
    }

    @Override
    public QueueSize getQueueSize() {
        return queueSize;
    }

    public synchronized boolean cancel() {
        if (this.state == ListFlowFileState.COMPLETE || this.state == ListFlowFileState.CANCELED) {
            return false;
        }

        this.state = ListFlowFileState.CANCELED;
        return true;
    }

    @Override
    public synchronized int getCompletionPercentage() {
        return state == ListFlowFileState.COMPLETE ? 100 : 0;
    }

    @Override
    public int getMaxResults() {
        return maxResults;
    }
}
