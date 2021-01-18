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

import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.flowfile.attributes.CoreAttributes;

public class LiveSerializedRepositoryRecord implements SerializedRepositoryRecord {
    private final RepositoryRecord record;

    public LiveSerializedRepositoryRecord(final RepositoryRecord repositoryRecord) {
        this.record = repositoryRecord;
    }

    @Override
    public String getQueueIdentifier() {
        final FlowFileQueue destination = record.getDestination();
        final FlowFileQueue queue = destination == null ? record.getOriginalQueue() : destination;
        return queue == null ? null : queue.getIdentifier();
    }

    @Override
    public RepositoryRecordType getType() {
        return record.getType();
    }

    @Override
    public ContentClaim getContentClaim() {
        return record.getCurrentClaim();
    }

    @Override
    public long getClaimOffset() {
        return record.getCurrentClaimOffset();
    }

    @Override
    public String getSwapLocation() {
        return record.getSwapLocation();
    }

    @Override
    public FlowFileRecord getFlowFileRecord() {
        return record.getCurrent();
    }

    @Override
    public boolean isMarkedForAbort() {
        return record.isMarkedForAbort();
    }

    @Override
    public boolean isAttributesChanged() {
        return record.isAttributesChanged();
    }

    @Override
    public String toString() {
        return "LiveSerializedRepositoryRecord[recordType=" + record.getType() + ", queueId=" + (record.getDestination() == null ? null : record.getDestination().getIdentifier())
            + ", flowFileUuid=" + record.getCurrent().getAttribute(CoreAttributes.UUID.key()) + ", attributesChanged=" + isAttributesChanged() + "]";
    }


}
