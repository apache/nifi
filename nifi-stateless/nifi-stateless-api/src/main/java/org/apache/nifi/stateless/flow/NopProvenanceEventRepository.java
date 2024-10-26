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

package org.apache.nifi.stateless.flow;

import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.provenance.ProvenanceEventBuilder;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.apache.nifi.provenance.ProvenanceEventType;

import java.util.List;
import java.util.Map;

class NopProvenanceEventRepository implements ProvenanceEventRepository {
    @Override
    public ProvenanceEventBuilder eventBuilder() {
        return new EventBuilder();
    }

    @Override
    public void registerEvent(final ProvenanceEventRecord event) {
    }

    @Override
    public void registerEvents(final Iterable<ProvenanceEventRecord> events) {
    }

    @Override
    public List<ProvenanceEventRecord> getEvents(final long minId, final int maxEvents) {
        return List.of();
    }

    @Override
    public Long getMaxEventId() {
        return 0L;
    }

    @Override
    public ProvenanceEventRecord getEvent(final long eventId) {
        return null;
    }

    @Override
    public void close() {
    }



    private static class EventBuilder implements ProvenanceEventBuilder {

        @Override
        public ProvenanceEventBuilder setEventType(final ProvenanceEventType provenanceEventType) {
            return this;
        }

        @Override
        public ProvenanceEventBuilder fromEvent(final ProvenanceEventRecord provenanceEventRecord) {
            return this;
        }

        @Override
        public ProvenanceEventBuilder setFlowFileEntryDate(final long l) {
            return this;
        }

        @Override
        public ProvenanceEventBuilder setPreviousContentClaim(final String s, final String s1, final String s2, final Long aLong, final long l) {
            return this;
        }

        @Override
        public ProvenanceEventBuilder setCurrentContentClaim(final String s, final String s1, final String s2, final Long aLong, final long l) {
            return this;
        }

        @Override
        public ProvenanceEventBuilder setSourceQueueIdentifier(final String s) {
            return this;
        }

        @Override
        public ProvenanceEventBuilder setAttributes(final Map<String, String> map, final Map<String, String> map1) {
            return this;
        }

        @Override
        public ProvenanceEventBuilder setFlowFileUUID(final String s) {
            return this;
        }

        @Override
        public ProvenanceEventBuilder setEventTime(final long l) {
            return this;
        }

        @Override
        public ProvenanceEventBuilder setEventDuration(final long l) {
            return this;
        }

        @Override
        public ProvenanceEventBuilder setLineageStartDate(final long l) {
            return this;
        }

        @Override
        public ProvenanceEventBuilder setComponentId(final String s) {
            return this;
        }

        @Override
        public ProvenanceEventBuilder setComponentType(final String s) {
            return this;
        }

        @Override
        public ProvenanceEventBuilder setSourceSystemFlowFileIdentifier(final String s) {
            return this;
        }

        @Override
        public ProvenanceEventBuilder setTransitUri(final String s) {
            return this;
        }

        @Override
        public ProvenanceEventBuilder addParentFlowFile(final FlowFile flowFile) {
            return this;
        }

        @Override
        public ProvenanceEventBuilder removeParentFlowFile(final FlowFile flowFile) {
            return this;
        }

        @Override
        public ProvenanceEventBuilder addChildFlowFile(final FlowFile flowFile) {
            return this;
        }

        @Override
        public ProvenanceEventBuilder addChildFlowFile(final String s) {
            return this;
        }

        @Override
        public ProvenanceEventBuilder removeChildFlowFile(final FlowFile flowFile) {
            return this;
        }

        @Override
        public ProvenanceEventBuilder setAlternateIdentifierUri(final String s) {
            return this;
        }

        @Override
        public ProvenanceEventBuilder setDetails(final String s) {
            return this;
        }

        @Override
        public ProvenanceEventBuilder setRelationship(final Relationship relationship) {
            return this;
        }

        @Override
        public ProvenanceEventBuilder fromFlowFile(final FlowFile flowFile) {
            return this;
        }

        @Override
        public ProvenanceEventRecord build() {
            return new NopProvenanceEventRecord();
        }

        @Override
        public List<String> getChildFlowFileIds() {
            return List.of();
        }

        @Override
        public List<String> getParentFlowFileIds() {
            return List.of();
        }

        @Override
        public String getFlowFileId() {
            return "";
        }

        @Override
        public ProvenanceEventBuilder copy() {
            return this;
        }
    }

    private static class NopProvenanceEventRecord implements ProvenanceEventRecord {

        @Override
        public long getEventId() {
            return 0;
        }

        @Override
        public long getEventTime() {
            return 0;
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
            return 0;
        }

        @Override
        public Long getPreviousFileSize() {
            return 0L;
        }

        @Override
        public long getEventDuration() {
            return 0;
        }

        @Override
        public ProvenanceEventType getEventType() {
            return ProvenanceEventType.UNKNOWN;
        }

        @Override
        public Map<String, String> getAttributes() {
            return Map.of();
        }

        @Override
        public Map<String, String> getPreviousAttributes() {
            return Map.of();
        }

        @Override
        public Map<String, String> getUpdatedAttributes() {
            return Map.of();
        }

        @Override
        public String getComponentId() {
            return "";
        }

        @Override
        public String getComponentType() {
            return "";
        }

        @Override
        public String getTransitUri() {
            return "";
        }

        @Override
        public String getSourceSystemFlowFileIdentifier() {
            return "";
        }

        @Override
        public String getFlowFileUuid() {
            return "";
        }

        @Override
        public List<String> getParentUuids() {
            return List.of();
        }

        @Override
        public List<String> getChildUuids() {
            return List.of();
        }

        @Override
        public String getAlternateIdentifierUri() {
            return "";
        }

        @Override
        public String getDetails() {
            return "";
        }

        @Override
        public String getRelationship() {
            return "";
        }

        @Override
        public String getSourceQueueIdentifier() {
            return "";
        }

        @Override
        public String getContentClaimSection() {
            return "";
        }

        @Override
        public String getPreviousContentClaimSection() {
            return "";
        }

        @Override
        public String getContentClaimContainer() {
            return "";
        }

        @Override
        public String getPreviousContentClaimContainer() {
            return "";
        }

        @Override
        public String getContentClaimIdentifier() {
            return "";
        }

        @Override
        public String getPreviousContentClaimIdentifier() {
            return "";
        }

        @Override
        public Long getContentClaimOffset() {
            return 0L;
        }

        @Override
        public Long getPreviousContentClaimOffset() {
            return 0L;
        }

        @Override
        public String getBestEventIdentifier() {
            return "";
        }
    }
}
