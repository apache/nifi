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

package org.apache.nifi.controller.flowanalysis;

import org.apache.nifi.flowanalysis.AnalyzeFlowState;
import org.apache.nifi.flowanalysis.AnalyzeFlowStatus;

public class AnalyzeFlowRequest implements AnalyzeFlowStatus {
    private final String processGroupId;

    private final long submissionTime = System.currentTimeMillis();
    private volatile long lastUpdated = System.currentTimeMillis();

    private AnalyzeFlowState state = AnalyzeFlowState.WAITING;
    private volatile String failureReason;

    public AnalyzeFlowRequest(final String processGroupId) {
        this.processGroupId = processGroupId;
    }

    @Override
    public String getProcessGroupId() {
        return processGroupId;
    }

    @Override
    public long getRequestSubmissionTime() {
        return submissionTime;
    }

    @Override
    public long getLastUpdated() {
        return lastUpdated;
    }

    @Override
    public synchronized AnalyzeFlowState getState() {
        return state;
    }

    @Override
    public String getFailureReason() {
        return failureReason;
    }

    public synchronized void setState(AnalyzeFlowState state) {
        setState(state, null);
    }

    public synchronized void setState(AnalyzeFlowState state, String explanation) {
        this.state = state;
        this.failureReason = explanation;
        this.lastUpdated = System.currentTimeMillis();
    }

    public synchronized boolean cancel() {
        boolean cancelled;

        if (this.state == AnalyzeFlowState.WAITING) {
            setState(AnalyzeFlowState.CANCELED);

            cancelled = true;
        } else {
            cancelled = false;
        }

        return cancelled;
    }
}
