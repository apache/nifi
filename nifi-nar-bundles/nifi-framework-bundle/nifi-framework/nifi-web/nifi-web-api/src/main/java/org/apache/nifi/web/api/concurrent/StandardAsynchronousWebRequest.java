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
    private volatile String failureReason;
    private volatile T results;

    public StandardAsynchronousWebRequest(final String requestId, final String processGroupId, final NiFiUser user) {
        this.id = requestId;
        this.processGroupId = processGroupId;
        this.user = user;
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
    }

    @Override
    public Date getLastUpdated() {
        return lastUpdated;
    }

    @Override
    public void setLastUpdated(final Date date) {
        this.lastUpdated = lastUpdated;
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
    }

    @Override
    public String getFailureReason() {
        return failureReason;
    }

    @Override
    public T getResults() {
        return results;
    }
}
