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

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A ProvenanceEventRecord that contains some other ProvenanceEventRecord, which
 * it uses as its basic source of data, possibly transforming the data along the
 * way or providing additional functionality. The class
 * FilterProvenanceEventRecord itself simply overrides all methods of
 * ProvenanceEventRecord with versions that pass all requests to the contained
 * ProvenanceEventRecord. Subclasses of FilterProvenanceEventRecord may further
 * override some of these methods and may also provide additional methods and
 * fields.
 */
public abstract class FilterProvenanceEventRecord implements ProvenanceEventRecord {

    /** The ProvenanceEventRecord to be filtered. */
    final protected ProvenanceEventRecord record;

    protected FilterProvenanceEventRecord(final ProvenanceEventRecord record) {
        this.record = record;
    }

    @Override
    public long getEventId() {
        return record.getEventId();
    }

    @Override
    public long getEventTime() {
        return record.getEventTime();
    }

    @Override
    public long getFlowFileEntryDate() {
        return record.getFlowFileEntryDate();
    }

    @Override
    public long getLineageStartDate() {
        return record.getLineageStartDate();
    }

    @Override
    public Set<String> getLineageIdentifiers() {
        return record.getLineageIdentifiers();
    }

    @Override
    public long getFileSize() {
        return record.getFileSize();
    }

    @Override
    public Long getPreviousFileSize() {
        return record.getPreviousFileSize();
    }

    @Override
    public long getEventDuration() {
        return record.getEventDuration();
    }

    @Override
    public ProvenanceEventType getEventType() {
        return record.getEventType();
    }

    @Override
    public Map<String, String> getAttributes() {
        return record.getAttributes();
    }

    @Override
    public Map<String, String> getPreviousAttributes() {
        return record.getPreviousAttributes();
    }

    @Override
    public Map<String, String> getUpdatedAttributes() {
        return record.getUpdatedAttributes();
    }

    @Override
    public String getComponentId() {
        return record.getComponentId();
    }

    @Override
    public String getComponentType() {
        return record.getComponentType();
    }

    @Override
    public String getTransitUri() {
        return record.getTransitUri();
    }

    @Override
    public String getSourceSystemFlowFileIdentifier() {
        return record.getSourceSystemFlowFileIdentifier();
    }

    @Override
    public String getFlowFileUuid() {
        return record.getFlowFileUuid();
    }

    @Override
    public String getAlternateIdentifierUri() {
        return record.getAlternateIdentifierUri();
    }

    @Override
    public String getDetails() {
        return record.getDetails();
    }

    @Override
    public String getRelationship() {
        return record.getRelationship();
    }

    @Override
    public String getSourceQueueIdentifier() {
        return record.getSourceQueueIdentifier();
    }

    @Override
    public String getContentClaimSection() {
        return record.getContentClaimSection();
    }

    @Override
    public String getPreviousContentClaimSection() {
        return record.getPreviousContentClaimSection();
    }

    @Override
    public String getContentClaimContainer() {
        return record.getContentClaimContainer();
    }

    @Override
    public String getPreviousContentClaimContainer() {
        return record.getPreviousContentClaimContainer();
    }

    @Override
    public String getContentClaimIdentifier() {
        return record.getContentClaimIdentifier();
    }

    @Override
    public String getPreviousContentClaimIdentifier() {
        return record.getPreviousContentClaimIdentifier();
    }

    @Override
    public Long getContentClaimOffset() {
        return record.getContentClaimOffset();
    }

    @Override
    public Long getPreviousContentClaimOffset() {
        return record.getPreviousContentClaimOffset();
    }

    @Override
    public List<String> getParentUuids() {
        return record.getParentUuids();
    }

    @Override
    public List<String> getChildUuids() {
        return record.getChildUuids();
    }
}
