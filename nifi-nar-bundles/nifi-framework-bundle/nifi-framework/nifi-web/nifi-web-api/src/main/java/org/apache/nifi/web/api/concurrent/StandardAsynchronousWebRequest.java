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

package org.apache.nifi.web.api.concurrent;

import java.util.Date;
import java.util.Objects;

import org.apache.nifi.authorization.user.NiFiUser;

public class StandardAsynchronousWebRequest<T> implements AsynchronousWebRequest<T> {
    private final String id;
    private final String processGroupId;
    private final NiFiUser user;

    private volatile boolean complete = false;
    private volatile Date lastUpdated = new Date();
    private volatile String state;
    private volatile int percentComplete;
    private volatile String failureReason;
    private volatile boolean cancelled;
    private volatile T results;

    public StandardAsynchronousWebRequest(final String requestId, final String processGroupId, final NiFiUser user, final String state) {
        this.id = requestId;
        this.processGroupId = processGroupId;
        this.user = user;
        this.state = state;
    }

    public String getRequestId() {
        return id;
    }

    @Override
    public boolean isComplete() {
        return complete;
    }

    @Override
    public String getProcessGroupId() {
        return processGroupId;
    }

    @Override
    public void markComplete(final T results) {
        this.complete = true;
        this.results = results;
        this.lastUpdated = new Date();
        this.percentComplete = 100;
        this.state = "Complete";
    }

    @Override
    public Date getLastUpdated() {
        return lastUpdated;
    }

    @Override
    public String getState() {
        return state;
    }

    @Override
    public int getPercentComplete() {
        return percentComplete;
    }

    @Override
    public void update(Date date, String state, int percentComplete) {
        if (percentComplete < 0 || percentComplete > 100) {
            throw new IllegalArgumentException("Cannot set percent complete to a value of " + percentComplete + "; it must be between 0 and 100.");
        }

        if (isCancelled()) {
            throw new IllegalStateException("Cannot update state because request has already been cancelled by user");
        }

        if (isComplete()) {
            final String failure = getFailureReason();
            final String explanation = failure == null ? "successfully" : "with failure reason: " + failure;
            throw new IllegalStateException("Cannot update state to '" + state + "' because request is already completed " + explanation);
        }

        this.lastUpdated = date;
        this.state = state;
        this.percentComplete = percentComplete;
    }

    @Override
    public NiFiUser getUser() {
        return user;
    }

    @Override
    public void setFailureReason(final String explanation) {
        this.failureReason = Objects.requireNonNull(explanation);
        this.complete = true;
        this.results = null;
        this.lastUpdated = new Date();
    }

    @Override
    public String getFailureReason() {
        return failureReason;
    }

    @Override
    public T getResults() {
        return results;
    }

    @Override
    public void cancel() {
        this.cancelled = true;
        percentComplete = 100;
        state = "Canceled by user";
        setFailureReason("Request cancelled by user");
    }

    @Override
    public boolean isCancelled() {
        return cancelled;
    }
}
