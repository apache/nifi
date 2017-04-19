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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.Relationship;

/**
 * Holder for provenance relevant information
 */
public class StandardProvenanceEventRecord implements ProvenanceEventRecord {

    private final long eventTime;
    private final long entryDate;
    private final ProvenanceEventType eventType;
    private final long lineageStartDate;
    private final String componentId;
    private final String componentType;
    private final String transitUri;
    private final String sourceSystemFlowFileIdentifier;
    private final String uuid;
    private final List<String> parentUuids;
    private final List<String> childrenUuids;
    private final String alternateIdentifierUri;
    private final String details;
    private final String relationship;
    private final long storageByteOffset;
    private final String storageFilename;
    private final long eventDuration;

    private final String contentClaimSection;
    private final String contentClaimContainer;
    private final String contentClaimIdentifier;
    private final Long contentClaimOffset;
    private final long contentSize;

    private final String previousClaimSection;
    private final String previousClaimContainer;
    private final String previousClaimIdentifier;
    private final Long previousClaimOffset;
    private final Long previousSize;

    private final String sourceQueueIdentifier;

    private final Map<String, String> previousAttributes;
    private final Map<String, String> updatedAttributes;

    private volatile long eventId = -1L;

    StandardProvenanceEventRecord(final Builder builder) {
        this.eventTime = builder.eventTime;
        this.entryDate = builder.entryDate;
        this.eventType = builder.eventType;
        this.componentId = builder.componentId;
        this.componentType = builder.componentType;
        this.transitUri = builder.transitUri;
        this.sourceSystemFlowFileIdentifier = builder.sourceSystemFlowFileIdentifier;
        this.uuid = builder.uuid;
        this.parentUuids = builder.parentUuids;
        this.childrenUuids = builder.childrenUuids;
        this.alternateIdentifierUri = builder.alternateIdentifierUri;
        this.details = builder.details;
        this.relationship = builder.relationship;
        this.storageByteOffset = builder.storageByteOffset;
        this.storageFilename = builder.storageFilename;
        this.eventDuration = builder.eventDuration;
        this.lineageStartDate = builder.lineageStartDate;

        previousClaimSection = builder.previousClaimSection;
        previousClaimContainer = builder.previousClaimContainer;
        previousClaimIdentifier = builder.previousClaimIdentifier;
        previousClaimOffset = builder.previousClaimOffset;
        previousSize = builder.previousSize;

        contentClaimSection = builder.contentClaimSection;
        contentClaimContainer = builder.contentClaimContainer;
        contentClaimIdentifier = builder.contentClaimIdentifier;
        contentClaimOffset = builder.contentClaimOffset;
        contentSize = builder.contentSize;

        previousAttributes = builder.previousAttributes == null ? Collections.emptyMap() : Collections.unmodifiableMap(builder.previousAttributes);
        updatedAttributes = builder.updatedAttributes == null ? Collections.emptyMap() : Collections.unmodifiableMap(builder.updatedAttributes);

        sourceQueueIdentifier = builder.sourceQueueIdentifier;

        if (builder.eventId != null) {
            eventId = builder.eventId;
        }
    }

    public static StandardProvenanceEventRecord copy(StandardProvenanceEventRecord other) {
        Builder builder = new Builder().fromEvent(other);
        return new StandardProvenanceEventRecord(builder);
    }

    public String getStorageFilename() {
        return storageFilename;
    }

    public long getStorageByteOffset() {
        return storageByteOffset;
    }

    void setEventId(final long eventId) {
        this.eventId = eventId;
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
    public long getLineageStartDate() {
        return lineageStartDate;
    }

    @Override
    public long getFileSize() {
        return contentSize;
    }

    @Override
    public Long getPreviousFileSize() {
        return previousSize;
    }

    @Override
    public ProvenanceEventType getEventType() {
        return eventType;
    }

    @Override
    public Map<String, String> getAttributes() {
        final Map<String, String> allAttrs = new HashMap<>(previousAttributes.size() + updatedAttributes.size());
        allAttrs.putAll(previousAttributes);
        for (final Map.Entry<String, String> entry : updatedAttributes.entrySet()) {
            if (entry.getValue() != null) {
                allAttrs.put(entry.getKey(), entry.getValue());
            }
        }
        return allAttrs;
    }

    public String getAttribute(final String attributeName) {
        if (updatedAttributes.containsKey(attributeName)) {
            return updatedAttributes.get(attributeName);
        }

        return previousAttributes.get(attributeName);
    }

    @Override
    public String getComponentId() {
        return componentId;
    }

    @Override
    public String getComponentType() {
        return componentType;
    }

    @Override
    public String getTransitUri() {
        return transitUri;
    }

    @Override
    public String getSourceSystemFlowFileIdentifier() {
        return sourceSystemFlowFileIdentifier;
    }

    @Override
    public String getFlowFileUuid() {
        return uuid;
    }

    @Override
    public List<String> getParentUuids() {
        return parentUuids == null ? Collections.emptyList() : parentUuids;
    }

    @Override
    public List<String> getChildUuids() {
        return childrenUuids == null ? Collections.emptyList() : childrenUuids;
    }

    @Override
    public String getAlternateIdentifierUri() {
        return alternateIdentifierUri;
    }

    @Override
    public long getEventDuration() {
        return eventDuration;
    }

    @Override
    public String getDetails() {
        return details;
    }

    @Override
    public String getRelationship() {
        return relationship;
    }

    @Override
    public long getFlowFileEntryDate() {
        return entryDate;
    }

    @Override
    public String getContentClaimSection() {
        return contentClaimSection;
    }

    @Override
    public String getContentClaimContainer() {
        return contentClaimContainer;
    }

    @Override
    public String getContentClaimIdentifier() {
        return contentClaimIdentifier;
    }

    @Override
    public Long getContentClaimOffset() {
        return contentClaimOffset;
    }

    @Override
    public String getSourceQueueIdentifier() {
        return sourceQueueIdentifier;
    }

    @Override
    public Map<String, String> getPreviousAttributes() {
        return previousAttributes;
    }

    @Override
    public String getPreviousContentClaimContainer() {
        return previousClaimContainer;
    }

    @Override
    public String getPreviousContentClaimIdentifier() {
        return previousClaimIdentifier;
    }

    @Override
    public Long getPreviousContentClaimOffset() {
        return previousClaimOffset;
    }

    @Override
    public String getPreviousContentClaimSection() {
        return previousClaimSection;
    }

    @Override
    public Map<String, String> getUpdatedAttributes() {
        return updatedAttributes;
    }

    @Override
    public int hashCode() {
        final int eventTypeCode;
        if (eventType == ProvenanceEventType.CLONE || eventType == ProvenanceEventType.JOIN || eventType == ProvenanceEventType.FORK) {
            eventTypeCode = 1472;
        } else if (eventType == ProvenanceEventType.REPLAY) {
            eventTypeCode = 21479 + (int) (0x7FFFFFFF & eventTime); // use lower bits of event time.
        } else {
            eventTypeCode = 4812 + eventType.hashCode() + 4 * uuid.hashCode();
        }

        return -37423 + 3 * componentId.hashCode() + (transitUri == null ? 0 : 41 * transitUri.hashCode())
                + (relationship == null ? 0 : 47 * relationship.hashCode()) + 44 * eventTypeCode
                + 47 * getChildUuids().hashCode() + 47 * getParentUuids().hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof StandardProvenanceEventRecord)) {
            return false;
        }

        final StandardProvenanceEventRecord other = (StandardProvenanceEventRecord) obj;
        // If event ID's are populated and not equal, return false. If they have not yet been populated, do not
        // use them in the comparison.
        if (eventId > 0L && other.getEventId() > 0L && eventId != other.getEventId()) {
            return false;
        }
        if (eventType != other.eventType) {
            return false;
        }

        if (!componentId.equals(other.componentId)) {
            return false;
        }

        if (different(parentUuids, other.parentUuids)) {
            return false;
        }

        if (different(childrenUuids, other.childrenUuids)) {
            return false;
        }

        // SPAWN had issues indicating which should be the event's FlowFileUUID in the case that there is 1 parent and 1 child.
        if (!uuid.equals(other.uuid)) {
            return false;
        }

        if (different(transitUri, other.transitUri)) {
            return false;
        }

        if (different(relationship, other.relationship)) {
            return false;
        }

        return !(eventType == ProvenanceEventType.REPLAY && eventTime != other.getEventTime());
    }

    private boolean different(final Object a, final Object b) {
        if (a == null && b == null) {
            return false;
        }
        if (a == null || b == null) {
            return true;
        }

        return !a.equals(b);
    }

    private boolean different(final List<String> a, final List<String> b) {
        if (a == null && b == null) {
            return false;
        }

        if (a == null && b != null && !b.isEmpty()) {
            return true;
        }

        if (a == null && b.isEmpty()) {
            return false;
        }

        if (a != null && !a.isEmpty() && b == null) {
            return true;
        }

        if (a.isEmpty() && b == null) {
            return false;
        }

        if (a.size() != b.size()) {
            return true;
        }

        final List<String> sortedA = new ArrayList<>(a);
        final List<String> sortedB = new ArrayList<>(b);

        Collections.sort(sortedA);
        Collections.sort(sortedB);

        for (int i = 0; i < sortedA.size(); i++) {
            if (!sortedA.get(i).equals(sortedB.get(i))) {
                return true;
            }
        }

        return false;
    }

    @Override
    public String toString() {
        return "ProvenanceEventRecord ["
                + "eventId=" + eventId
                + ", eventType=" + eventType
                + ", eventTime=" + new Date(eventTime)
                + ", uuid=" + uuid
                + ", fileSize=" + contentSize
                + ", componentId=" + componentId
                + ", transitUri=" + transitUri
                + ", sourceSystemFlowFileIdentifier=" + sourceSystemFlowFileIdentifier
                + ", parentUuids=" + parentUuids
                + ", alternateIdentifierUri=" + alternateIdentifierUri + "]";
    }

    /**
     * Returns a unique identifier for the record. By default, it uses
     * {@link ProvenanceEventRecord#getEventId()} but if it has not been persisted to the
     * repository, this is {@code -1}, so it constructs a String of the format
     * {@code <event type>_on_<flowfile UUID>_by_<component UUID>_at_<event time>}.
     *
     * @return a String identifying the record for later analysis
     */
    @Override
    public String getBestEventIdentifier() {
        if (getEventId() != -1) {
            return Long.toString(getEventId());
        } else {
            return getEventType().name() + "_on_" + getFlowFileUuid() + "_by_" + getComponentId() + "_at_" + getEventTime();
        }
    }

    public static class Builder implements ProvenanceEventBuilder {

        private long eventTime = System.currentTimeMillis();
        private long entryDate;
        private long lineageStartDate;
        private ProvenanceEventType eventType = null;
        private String componentId = null;
        private String componentType = null;
        private String sourceSystemFlowFileIdentifier = null;
        private String transitUri = null;
        private String uuid = null;
        private List<String> parentUuids = null;
        private List<String> childrenUuids = null;
        private String alternateIdentifierUri = null;
        private String details = null;
        private String relationship = null;
        private long storageByteOffset = -1L;
        private long eventDuration = -1L;
        private String storageFilename;
        private Long eventId;

        private String contentClaimSection;
        private String contentClaimContainer;
        private String contentClaimIdentifier;
        private Long contentClaimOffset;
        private Long contentSize;

        private String previousClaimSection;
        private String previousClaimContainer;
        private String previousClaimIdentifier;
        private Long previousClaimOffset;
        private Long previousSize;

        private String sourceQueueIdentifier;

        private Map<String, String> previousAttributes;
        private Map<String, String> updatedAttributes;

        @Override
        public Builder fromEvent(final ProvenanceEventRecord event) {
            eventTime = event.getEventTime();
            entryDate = event.getFlowFileEntryDate();
            lineageStartDate = event.getLineageStartDate();
            eventType = event.getEventType();
            componentId = event.getComponentId();
            componentType = event.getComponentType();
            transitUri = event.getTransitUri();
            sourceSystemFlowFileIdentifier = event.getSourceSystemFlowFileIdentifier();
            uuid = event.getFlowFileUuid();
            parentUuids = event.getParentUuids();
            childrenUuids = event.getChildUuids();
            alternateIdentifierUri = event.getAlternateIdentifierUri();
            eventDuration = event.getEventDuration();
            previousAttributes = event.getPreviousAttributes();
            updatedAttributes = event.getUpdatedAttributes();
            details = event.getDetails();
            relationship = event.getRelationship();

            contentClaimSection = event.getContentClaimSection();
            contentClaimContainer = event.getContentClaimContainer();
            contentClaimIdentifier = event.getContentClaimIdentifier();
            contentClaimOffset = event.getContentClaimOffset();
            contentSize = event.getFileSize();

            previousClaimSection = event.getPreviousContentClaimSection();
            previousClaimContainer = event.getPreviousContentClaimContainer();
            previousClaimIdentifier = event.getPreviousContentClaimIdentifier();
            previousClaimOffset = event.getPreviousContentClaimOffset();
            previousSize = event.getPreviousFileSize();

            sourceQueueIdentifier = event.getSourceQueueIdentifier();

            if (event instanceof StandardProvenanceEventRecord) {
                final StandardProvenanceEventRecord standardProvEvent = (StandardProvenanceEventRecord) event;
                storageByteOffset = standardProvEvent.storageByteOffset;
                storageFilename = standardProvEvent.storageFilename;
            }

            return this;
        }

        public Builder setEventId(final long eventId) {
            this.eventId = eventId;
            return this;
        }

        @Override
        public ProvenanceEventBuilder copy() {
            final Builder copy = new Builder();
            copy.eventTime = eventTime;
            copy.entryDate = entryDate;
            copy.lineageStartDate = lineageStartDate;
            copy.eventType = eventType;
            copy.componentId = componentId;
            copy.componentType = componentType;
            copy.transitUri = transitUri;
            copy.sourceSystemFlowFileIdentifier = sourceSystemFlowFileIdentifier;
            copy.uuid = uuid;
            if (parentUuids != null) {
                copy.parentUuids = new ArrayList<>(parentUuids);
            }
            if (childrenUuids != null) {
                copy.childrenUuids = new ArrayList<>(childrenUuids);
            }
            copy.alternateIdentifierUri = alternateIdentifierUri;
            copy.eventDuration = eventDuration;
            if (previousAttributes != null) {
                copy.previousAttributes = new HashMap<>(previousAttributes);
            }
            if (updatedAttributes != null) {
                copy.updatedAttributes = new HashMap<>(updatedAttributes);
            }
            copy.details = details;
            copy.relationship = relationship;

            copy.contentClaimContainer = contentClaimContainer;
            copy.contentClaimSection = contentClaimSection;
            copy.contentClaimIdentifier = contentClaimIdentifier;
            copy.contentClaimOffset = contentClaimOffset;
            copy.contentSize = contentSize;

            copy.previousClaimContainer = previousClaimContainer;
            copy.previousClaimSection = previousClaimSection;
            copy.previousClaimIdentifier = previousClaimIdentifier;
            copy.previousClaimOffset = previousClaimOffset;
            copy.previousSize = previousSize;

            copy.sourceQueueIdentifier = sourceQueueIdentifier;
            copy.storageByteOffset = storageByteOffset;
            copy.storageFilename = storageFilename;

            return copy;
        }


        @Override
        public Builder setFlowFileEntryDate(final long entryDate) {
            this.entryDate = entryDate;
            return this;
        }

        @Override
        public Builder setAttributes(final Map<String, String> previousAttributes, final Map<String, String> updatedAttributes) {
            this.previousAttributes = previousAttributes;
            this.updatedAttributes = updatedAttributes;
            return this;
        }

        public Builder setPreviousAttributes(final Map<String, String> previousAttributes) {
            this.previousAttributes = previousAttributes;
            return this;
        }

        public Builder setUpdatedAttributes(final Map<String, String> updatedAttributes) {
            this.updatedAttributes = updatedAttributes;
            return this;
        }

        @Override
        public Builder setFlowFileUUID(final String uuid) {
            this.uuid = uuid;
            return this;
        }

        public Builder setStorageLocation(final String filename, final long offset) {
            this.storageFilename = filename;
            this.storageByteOffset = offset;
            return this;
        }

        @Override
        public Builder setEventTime(long eventTime) {
            this.eventTime = eventTime;
            return this;
        }

        @Override
        public Builder setEventDuration(final long millis) {
            this.eventDuration = millis;
            return this;
        }

        @Override
        public Builder setLineageStartDate(final long startDate) {
            this.lineageStartDate = startDate;
            return this;
        }

        @Override
        public Builder setEventType(ProvenanceEventType eventType) {
            this.eventType = eventType;
            return this;
        }

        @Override
        public Builder setComponentId(String componentId) {
            this.componentId = componentId;
            return this;
        }

        @Override
        public Builder setComponentType(String componentType) {
            this.componentType = componentType;
            return this;
        }

        @Override
        public Builder setSourceSystemFlowFileIdentifier(String sourceSystemFlowFileIdentifier) {
            this.sourceSystemFlowFileIdentifier = sourceSystemFlowFileIdentifier;
            return this;
        }

        @Override
        public Builder setTransitUri(String transitUri) {
            this.transitUri = transitUri;
            return this;
        }

        @Override
        public Builder addParentFlowFile(final FlowFile parentFlowFile) {
            if (this.parentUuids == null) {
                this.parentUuids = new ArrayList<>();
            }
            this.parentUuids.add(parentFlowFile.getAttribute(CoreAttributes.UUID.key()));
            return this;
        }

        public Builder addParentUuid(final String uuid) {
            if (this.parentUuids == null) {
                this.parentUuids = new ArrayList<>();
            }
            this.parentUuids.add(uuid);
            return this;
        }

        @Override
        public Builder removeParentFlowFile(final FlowFile parentFlowFile) {
            if (this.parentUuids == null) {
                return this;
            }

            parentUuids.remove(parentFlowFile.getAttribute(CoreAttributes.UUID.key()));
            return this;
        }

        @Override
        public Builder addChildFlowFile(final FlowFile childFlowFile) {
            return addChildFlowFile(childFlowFile.getAttribute(CoreAttributes.UUID.key()));
        }

        @Override
        public Builder addChildFlowFile(final String childId) {
            if (this.childrenUuids == null) {
                this.childrenUuids = new ArrayList<>();
            }
            this.childrenUuids.add(childId);
            return this;
        }

        public Builder addChildUuid(final String uuid) {
            if (this.childrenUuids == null) {
                this.childrenUuids = new ArrayList<>();
            }
            this.childrenUuids.add(uuid);
            return this;
        }

        public Builder setChildUuids(final List<String> uuids) {
            this.childrenUuids = uuids;
            return this;
        }

        public Builder setParentUuids(final List<String> uuids) {
            this.parentUuids = uuids;
            return this;
        }

        @Override
        public Builder removeChildFlowFile(final FlowFile childFlowFile) {
            if (this.childrenUuids == null) {
                return this;
            }

            childrenUuids.remove(childFlowFile.getAttribute(CoreAttributes.UUID.key()));
            return this;
        }

        @Override
        public Builder setAlternateIdentifierUri(String alternateIdentifierUri) {
            this.alternateIdentifierUri = alternateIdentifierUri;
            return this;
        }

        @Override
        public Builder setDetails(String details) {
            this.details = details;
            return this;
        }

        @Override
        public Builder setRelationship(Relationship relationship) {
            this.relationship = relationship.getName();
            return this;
        }

        public Builder setRelationship(final String relationship) {
            this.relationship = relationship;
            return this;
        }

        @Override
        public ProvenanceEventBuilder fromFlowFile(final FlowFile flowFile) {
            setFlowFileEntryDate(flowFile.getEntryDate());
            setLineageStartDate(flowFile.getLineageStartDate());
            setAttributes(Collections.emptyMap(), flowFile.getAttributes());
            uuid = flowFile.getAttribute(CoreAttributes.UUID.key());
            this.contentSize = flowFile.getSize();
            return this;
        }

        @Override
        public Builder setPreviousContentClaim(final String container, final String section, final String identifier, final Long offset, final long size) {
            previousClaimSection = section;
            previousClaimContainer = container;
            previousClaimIdentifier = identifier;
            previousClaimOffset = offset;
            previousSize = size;
            return this;
        }

        @Override
        public Builder setCurrentContentClaim(final String container, final String section, final String identifier, final Long offset, final long size) {
            contentClaimSection = section;
            contentClaimContainer = container;
            contentClaimIdentifier = identifier;
            contentClaimOffset = offset;
            contentSize = size;
            return this;
        }

        @Override
        public Builder setSourceQueueIdentifier(final String identifier) {
            sourceQueueIdentifier = identifier;
            return this;
        }

        private void assertSet(final Object value, final String name) {
            if (value == null) {
                throw new IllegalStateException("Cannot create Provenance Event Record because " + name + " is not set");
            }
        }

        public ProvenanceEventType getEventType() {
            return eventType;
        }

        public List<String> getChildUuids() {
            return Collections.unmodifiableList(childrenUuids);
        }

        public List<String> getParentUuids() {
            return Collections.unmodifiableList(parentUuids);
        }

        @Override
        public StandardProvenanceEventRecord build() {
            assertSet(eventType, "Event Type");
            assertSet(componentId, "Component ID");
            assertSet(componentType, "Component Type");
            assertSet(uuid, "FlowFile UUID");
            assertSet(contentSize, "FlowFile Size");

            switch (eventType) {
                case ADDINFO:
                    if (alternateIdentifierUri == null) {
                        throw new IllegalStateException("Cannot create Provenance Event Record of type " + eventType + " because no alternate identifiers have been set");
                    }
                    break;
                case RECEIVE:
                case FETCH:
                case SEND:
                    assertSet(transitUri, "Transit URI");
                    break;
                case ROUTE:
                    assertSet(relationship, "Relationship");
                    break;
                case CLONE:
                case FORK:
                case JOIN:
                    if ((parentUuids == null || parentUuids.isEmpty()) && (childrenUuids == null || childrenUuids.isEmpty())) {
                        throw new IllegalStateException("Cannot create Provenance Event Record of type " + eventType + " because no Parent UUIDs or Children UUIDs have been set");
                    }
                    break;
                default:
                    break;
            }

            return new StandardProvenanceEventRecord(this);
        }

        @Override
        public List<String> getChildFlowFileIds() {
            return childrenUuids;
        }

        @Override
        public List<String> getParentFlowFileIds() {
            return parentUuids;
        }

        @Override
        public String getFlowFileId() {
            return uuid;
        }
    }
}
