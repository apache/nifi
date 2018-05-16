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

package org.apache.nifi.provenance.util;

import java.util.List;
import java.util.Map;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.provenance.serialization.StorageSummary;

public class StorageSummaryEvent implements ProvenanceEventRecord {
    private final ProvenanceEventRecord event;
    private final StorageSummary storageSummary;

    public StorageSummaryEvent(final ProvenanceEventRecord event, final StorageSummary storageSummary) {
        this.event = event;
        this.storageSummary = storageSummary;
    }

    @Override
    public long getEventId() {
        return storageSummary.getEventId();
    }

    @Override
    public long getEventTime() {
        return event.getEventTime();
    }

    @Override
    public long getFlowFileEntryDate() {
        return event.getFlowFileEntryDate();
    }

    @Override
    public long getLineageStartDate() {
        return event.getLineageStartDate();
    }

    @Override
    public long getFileSize() {
        return event.getFileSize();
    }

    @Override
    public Long getPreviousFileSize() {
        return event.getPreviousFileSize();
    }

    @Override
    public long getEventDuration() {
        return event.getEventDuration();
    }

    @Override
    public ProvenanceEventType getEventType() {
        return event.getEventType();
    }

    @Override
    public Map<String, String> getAttributes() {
        return event.getAttributes();
    }

    @Override
    public Map<String, String> getPreviousAttributes() {
        return event.getPreviousAttributes();
    }

    @Override
    public Map<String, String> getUpdatedAttributes() {
        return event.getUpdatedAttributes();
    }

    @Override
    public String getComponentId() {
        return event.getComponentId();
    }

    @Override
    public String getComponentType() {
        return event.getComponentType();
    }

    @Override
    public String getTransitUri() {
        return event.getTransitUri();
    }

    @Override
    public String getSourceSystemFlowFileIdentifier() {
        return event.getSourceSystemFlowFileIdentifier();
    }

    @Override
    public String getFlowFileUuid() {
        return event.getFlowFileUuid();
    }

    @Override
    public List<String> getParentUuids() {
        return event.getParentUuids();
    }

    @Override
    public List<String> getChildUuids() {
        return event.getChildUuids();
    }

    @Override
    public String getAlternateIdentifierUri() {
        return event.getAlternateIdentifierUri();
    }

    @Override
    public String getDetails() {
        return event.getDetails();
    }

    @Override
    public String getRelationship() {
        return event.getRelationship();
    }

    @Override
    public String getSourceQueueIdentifier() {
        return event.getSourceQueueIdentifier();
    }

    @Override
    public String getContentClaimSection() {
        return event.getContentClaimSection();
    }

    @Override
    public String getPreviousContentClaimSection() {
        return event.getPreviousContentClaimSection();
    }

    @Override
    public String getContentClaimContainer() {
        return event.getContentClaimContainer();
    }

    @Override
    public String getPreviousContentClaimContainer() {
        return event.getPreviousContentClaimContainer();
    }

    @Override
    public String getContentClaimIdentifier() {
        return event.getContentClaimIdentifier();
    }

    @Override
    public String getPreviousContentClaimIdentifier() {
        return event.getPreviousContentClaimIdentifier();
    }

    @Override
    public Long getContentClaimOffset() {
        return event.getContentClaimOffset();
    }

    @Override
    public Long getPreviousContentClaimOffset() {
        return event.getPreviousContentClaimOffset();
    }

    /**
     * Returns the best event identifier for this event (eventId if available, descriptive identifier if not yet persisted to allow for traceability).
     *
     * @return a descriptive event ID to allow tracing
     */
    @Override
    public String getBestEventIdentifier() {
        return Long.toString(getEventId());
    }
}
