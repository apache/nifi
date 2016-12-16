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

package org.apache.nifi.controller.repository;

import java.util.List;

import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.repository.claim.ContentClaim;

/**
 * A simple RepositoryRecord that represents a Set of Content Claims that need to be cleaned up
 */
public class TransientClaimRepositoryRecord implements RepositoryRecord {
    private final List<ContentClaim> claimsToCleanUp;

    public TransientClaimRepositoryRecord(final List<ContentClaim> claimsToCleanUp) {
        this.claimsToCleanUp = claimsToCleanUp;
    }

    @Override
    public FlowFileQueue getDestination() {
        return null;
    }

    @Override
    public FlowFileQueue getOriginalQueue() {
        return null;
    }

    @Override
    public RepositoryRecordType getType() {
        return RepositoryRecordType.CLEANUP_TRANSIENT_CLAIMS;
    }

    @Override
    public ContentClaim getCurrentClaim() {
        return null;
    }

    @Override
    public ContentClaim getOriginalClaim() {
        return null;
    }

    @Override
    public long getCurrentClaimOffset() {
        return 0;
    }

    @Override
    public FlowFileRecord getCurrent() {
        return null;
    }

    @Override
    public boolean isAttributesChanged() {
        return false;
    }

    @Override
    public boolean isMarkedForAbort() {
        return false;
    }

    @Override
    public String getSwapLocation() {
        return null;
    }

    @Override
    public List<ContentClaim> getTransientClaims() {
        return claimsToCleanUp;
    }
}
