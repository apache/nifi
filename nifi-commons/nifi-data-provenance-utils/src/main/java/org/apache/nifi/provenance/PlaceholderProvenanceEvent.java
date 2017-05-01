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

package org.apache.nifi.provenance;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A Provenance Event that is used to replace another Provenance Event when authorizations
 * are not granted for the original Provenance Event
 */
public class PlaceholderProvenanceEvent implements ProvenanceEventRecord {
    private final String componentId;
    private final long eventId;
    private final long eventTime;
    private final String flowFileUuid;

    public PlaceholderProvenanceEvent(final ProvenanceEventRecord original) {
        this.componentId = original.getComponentId();
        this.eventId = original.getEventId();
        this.eventTime = original.getEventTime();
        this.flowFileUuid = original.getFlowFileUuid();
    }

    @Override
    public long getEventId() {
        return eventId;
    }

    @Override
    public long getEventTime() {
        return eventTime;
    }

    @Override
    public long getFlowFileEntryDate() {
        return 0;
    }

    @Override
    public long getLineageStartDate() {
        return 0;
    }

    @Override
    public long getFileSize() {
        return -1L;
    }

    @Override
    public Long getPreviousFileSize() {
        return -1L;
    }

    @Override
    public long getEventDuration() {
        return -1L;
    }

    @Override
    public ProvenanceEventType getEventType() {
        return ProvenanceEventType.UNKNOWN;
    }

    @Override
    public Map<String, String> getAttributes() {
        return Collections.emptyMap();
    }

    @Override
    public Map<String, String> getPreviousAttributes() {
        return Collections.emptyMap();
    }

    @Override
    public Map<String, String> getUpdatedAttributes() {
        return Collections.emptyMap();
    }

    @Override
    public String getComponentId() {
        return componentId;
    }

    @Override
    public String getComponentType() {
        return null;
    }

    @Override
    public String getTransitUri() {
        return null;
    }

    @Override
    public String getSourceSystemFlowFileIdentifier() {
        return null;
    }

    @Override
    public String getFlowFileUuid() {
        return flowFileUuid;
    }

    @Override
    public List<String> getParentUuids() {
        return null;
    }

    @Override
    public List<String> getChildUuids() {
        return null;
    }

    @Override
    public String getAlternateIdentifierUri() {
        return null;
    }

    @Override
    public String getDetails() {
        return null;
    }

    @Override
    public String getRelationship() {
        return null;
    }

    @Override
    public String getSourceQueueIdentifier() {
        return null;
    }

    @Override
    public String getContentClaimSection() {
        return null;
    }

    @Override
    public String getPreviousContentClaimSection() {
        return null;
    }

    @Override
    public String getContentClaimContainer() {
        return null;
    }

    @Override
    public String getPreviousContentClaimContainer() {
        return null;
    }

    @Override
    public String getContentClaimIdentifier() {
        return null;
    }

    @Override
    public String getPreviousContentClaimIdentifier() {
        return null;
    }

    @Override
    public Long getContentClaimOffset() {
        return null;
    }

    @Override
    public Long getPreviousContentClaimOffset() {
        return null;
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
