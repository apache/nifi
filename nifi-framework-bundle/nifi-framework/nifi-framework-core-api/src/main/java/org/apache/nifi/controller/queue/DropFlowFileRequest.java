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

public class DropFlowFileRequest implements DropFlowFileStatus {
    private final String identifier;
    private final long submissionTime = System.currentTimeMillis();

    private volatile QueueSize originalSize;
    private volatile QueueSize currentSize;
    private volatile QueueSize droppedSize = new QueueSize(0, 0L);
    private volatile long lastUpdated = System.currentTimeMillis();
    private volatile String failureReason;

    private DropFlowFileState state = DropFlowFileState.WAITING_FOR_LOCK;


    public DropFlowFileRequest(final String identifier) {
        this.identifier = identifier;
    }

    @Override
    public String getRequestIdentifier() {
        return identifier;
    }

    @Override
    public long getRequestSubmissionTime() {
        return submissionTime;
    }

    @Override
    public QueueSize getOriginalSize() {
        return originalSize;
    }

    public void setOriginalSize(final QueueSize originalSize) {
        this.originalSize = originalSize;
    }

    @Override
    public QueueSize getCurrentSize() {
        return currentSize;
    }

    public void setCurrentSize(final QueueSize queueSize) {
        this.currentSize = queueSize;
    }

    @Override
    public QueueSize getDroppedSize() {
        return droppedSize;
    }

    public void setDroppedSize(final QueueSize droppedSize) {
        this.droppedSize = droppedSize;
    }

    @Override
    public synchronized DropFlowFileState getState() {
        return state;
    }

    @Override
    public long getLastUpdated() {
        return lastUpdated;
    }

    @Override
    public String getFailureReason() {
        return failureReason;
    }

    public synchronized void setState(final DropFlowFileState state) {
        setState(state, null);
    }

    public synchronized void setState(final DropFlowFileState state, final String explanation) {
        this.state = state;
        this.failureReason = explanation;
        this.lastUpdated = System.currentTimeMillis();
    }

    public synchronized boolean cancel() {
        if (this.state == DropFlowFileState.COMPLETE || this.state == DropFlowFileState.CANCELED) {
            return false;
        }

        this.state = DropFlowFileState.CANCELED;
        return true;
    }
}
