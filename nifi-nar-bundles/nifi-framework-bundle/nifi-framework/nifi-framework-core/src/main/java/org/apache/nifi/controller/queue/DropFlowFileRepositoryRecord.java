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

import java.util.Collections;
import java.util.List;

import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.RepositoryRecord;
import org.apache.nifi.controller.repository.RepositoryRecordType;
import org.apache.nifi.controller.repository.claim.ContentClaim;

public class DropFlowFileRepositoryRecord implements RepositoryRecord {
    private final FlowFileQueue queue;
    private final FlowFileRecord flowFile;

    public DropFlowFileRepositoryRecord(final FlowFileQueue queue, final FlowFileRecord flowFile) {
        this.queue = queue;
        this.flowFile = flowFile;
    }

    @Override
    public FlowFileQueue getDestination() {
        return null;
    }

    @Override
    public FlowFileQueue getOriginalQueue() {
        return queue;
    }

    @Override
    public RepositoryRecordType getType() {
        return RepositoryRecordType.DELETE;
    }

    @Override
    public ContentClaim getCurrentClaim() {
        return flowFile.getContentClaim();
    }

    @Override
    public ContentClaim getOriginalClaim() {
        return flowFile.getContentClaim();
    }

    @Override
    public long getCurrentClaimOffset() {
        return flowFile.getContentClaimOffset();
    }

    @Override
    public FlowFileRecord getCurrent() {
        return flowFile;
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
        return Collections.emptyList();
    }
}
