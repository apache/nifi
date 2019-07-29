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

import org.apache.nifi.authorization.user.NiFiUser;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Objects;

public class StandardAsynchronousWebRequest<R, T> implements AsynchronousWebRequest<R, T> {
    private final String id;
    private final String componentId;
    private final NiFiUser user;
    private final List<UpdateStep> updateSteps;
    private final R request;

    private volatile boolean complete = false;
    private volatile Date lastUpdated = new Date();
    private volatile int percentComplete;
    private volatile String failureReason;
    private volatile boolean cancelled;
    private volatile T results;
    private volatile Runnable cancelCallback;

    private int currentStepIndex = 0;

    public StandardAsynchronousWebRequest(final String requestId, final R request, final String componentId, final NiFiUser user, final List<UpdateStep> updateSteps) {
        this.id = requestId;
        this.componentId = componentId;
        this.user = user;
        this.updateSteps = updateSteps;
        this.request = request;
    }

    public synchronized UpdateStep getCurrentStep() {
        return (updateSteps == null || updateSteps.size() <= currentStepIndex) ? null : updateSteps.get(currentStepIndex);
    }

    public R getRequest() {
        return request;
    }

    public String getRequestId() {
        return id;
    }

    @Override
    public boolean isComplete() {
        return complete;
    }

    @Override
    public String getComponentId() {
        return componentId;
    }

    @Override
    public void setCancelCallback(final Runnable runnable) {
        this.cancelCallback = runnable;
    }

    @Override
    public void markStepComplete() {
        markStepComplete(this.results);
    }

    @Override
    public synchronized void markStepComplete(final T results) {
        if (isCancelled() || isComplete()) {
            return;
        }

        final UpdateStep currentStep = getCurrentStep();
        if (currentStep != null) {
            currentStep.markCompleted();
        }

        currentStepIndex++;
        this.complete = currentStepIndex >= updateSteps.size();
        this.results = results;
        this.lastUpdated = new Date();
        this.percentComplete = currentStepIndex  * 100 / updateSteps.size();
    }

    @Override
    public synchronized String getState() {
        if (isComplete()) {
            return "Complete";
        }

        String failureReason = getFailureReason();
        if (failureReason != null) {
            return "Failed: " + failureReason;
        }

        return getCurrentStep().getDescription();
    }

    @Override
    public Date getLastUpdated() {
        return lastUpdated;
    }

    @Override
    public List<UpdateStep> getUpdateSteps() {
        return Collections.unmodifiableList(updateSteps);
    }

    @Override
    public int getPercentComplete() {
        return percentComplete;
    }

    @Override
    public NiFiUser getUser() {
        return user;
    }

    @Override
    public synchronized void fail(final String explanation) {
        this.failureReason = Objects.requireNonNull(explanation);
        this.complete = true;
        this.results = null;
        this.lastUpdated = new Date();

        getCurrentStep().fail(explanation);
    }

    @Override
    public synchronized String getFailureReason() {
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
        fail("Request cancelled by user");
        cancelCallback.run();
    }

    @Override
    public boolean isCancelled() {
        return cancelled;
    }
}
