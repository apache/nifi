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
package org.apache.nifi.stateless.repository;

import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.provenance.AsyncLineageSubmission;
import org.apache.nifi.provenance.IdentifierLookup;
import org.apache.nifi.provenance.ProvenanceAuthorizableFactory;
import org.apache.nifi.provenance.ProvenanceEventBuilder;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.apache.nifi.provenance.ProvenanceEventType;
import org.apache.nifi.provenance.ProvenanceRepository;
import org.apache.nifi.provenance.StandardProvenanceEventRecord;
import org.apache.nifi.provenance.lineage.ComputeLineageSubmission;
import org.apache.nifi.provenance.search.Query;
import org.apache.nifi.provenance.search.QuerySubmission;
import org.apache.nifi.provenance.search.SearchableField;
import org.apache.nifi.util.RingBuffer;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class StatelessProvenanceRepository implements ProvenanceRepository {

    public static String CONTAINER_NAME = "in-memory";

    private final RingBuffer<ProvenanceEventRecord> ringBuffer;
    private final int maxSize;

    private final AtomicLong idGenerator = new AtomicLong(0L);

    public StatelessProvenanceRepository(final int maxEvents) {
        maxSize = maxEvents;
        ringBuffer = new RingBuffer<>(maxSize);
    }

    @Override
    public void initialize(final EventReporter eventReporter, final Authorizer authorizer, final ProvenanceAuthorizableFactory resourceFactory,
                           final IdentifierLookup idLookup) throws IOException {

    }

    @Override
    public ProvenanceEventRepository getProvenanceEventRepository() {
        return this;
    }

    @Override
    public ProvenanceEventBuilder eventBuilder() {
        return new StandardProvenanceEventRecord.Builder();
    }

    @Override
    public void registerEvent(final ProvenanceEventRecord event) {
        final long id = idGenerator.getAndIncrement();
        ringBuffer.add(new IdEnrichedProvEvent(event, id));
    }

    @Override
    public void registerEvents(final Iterable<ProvenanceEventRecord> events) {
        for (final ProvenanceEventRecord event : events) {
            registerEvent(event);
        }
    }

    @Override
    public List<ProvenanceEventRecord> getEvents(final long firstRecordId, final int maxRecords) throws IOException {
        return getEvents(firstRecordId, maxRecords, null);
    }

    @Override
    public List<ProvenanceEventRecord> getEvents(final long firstRecordId, final int maxRecords, final NiFiUser user) throws IOException {
        return ringBuffer.getSelectedElements(new RingBuffer.Filter<ProvenanceEventRecord>() {
            @Override
            public boolean select(final ProvenanceEventRecord value) {
                return value.getEventId() >= firstRecordId;
            }
        }, maxRecords);
    }

    @Override
    public Long getMaxEventId() {
        final ProvenanceEventRecord newest = ringBuffer.getNewestElement();
        return (newest == null) ? null : newest.getEventId();
    }

    public ProvenanceEventRecord getEvent(final String identifier) throws IOException {
        final List<ProvenanceEventRecord> records = ringBuffer.getSelectedElements(new RingBuffer.Filter<ProvenanceEventRecord>() {
            @Override
            public boolean select(final ProvenanceEventRecord event) {
                return identifier.equals(event.getFlowFileUuid());
            }
        }, 1);
        return records.isEmpty() ? null : records.get(0);
    }

    @Override
    public ProvenanceEventRecord getEvent(final long id) {
        final List<ProvenanceEventRecord> records = ringBuffer.getSelectedElements(new RingBuffer.Filter<ProvenanceEventRecord>() {
            @Override
            public boolean select(final ProvenanceEventRecord event) {
                return event.getEventId() == id;
            }
        }, 1);

        return records.isEmpty() ? null : records.get(0);
    }

    @Override
    public ProvenanceEventRecord getEvent(final long id, final NiFiUser user) {
        return getEvent(id);
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public List<SearchableField> getSearchableFields() {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<SearchableField> getSearchableAttributes() {
        throw new UnsupportedOperationException();
    }

    @Override
    public QuerySubmission submitQuery(final Query query, final NiFiUser user) {
        throw new UnsupportedOperationException();
    }

    @Override
    public QuerySubmission retrieveQuerySubmission(final String queryIdentifier, final NiFiUser user) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ComputeLineageSubmission submitLineageComputation(final long eventId, final NiFiUser user) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AsyncLineageSubmission submitLineageComputation(final String flowFileUuid, final NiFiUser user) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ComputeLineageSubmission retrieveLineageSubmission(String lineageIdentifier, final NiFiUser user) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ComputeLineageSubmission submitExpandParents(final long eventId, final NiFiUser user) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ComputeLineageSubmission submitExpandChildren(final long eventId, final NiFiUser user) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getContainerCapacity(final String containerName) throws IOException {
        return maxSize;
    }

    @Override
    public Set<String> getContainerNames() {
        return Collections.singleton(CONTAINER_NAME);
    }

    @Override
    public long getContainerUsableSpace(String containerName) throws IOException {
        return maxSize - ringBuffer.getSize();
    }

    @Override
    public String getContainerFileStoreName(String containerName) {
        return null;
    }

    private static class IdEnrichedProvEvent implements ProvenanceEventRecord {

        private final ProvenanceEventRecord record;
        private final long id;

        public IdEnrichedProvEvent(final ProvenanceEventRecord record, final long id) {
            this.record = record;
            this.id = id;
        }

        @Override
        public long getEventId() {
            return id;
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
        public List<String> getParentUuids() {
            return record.getParentUuids();
        }

        @Override
        public List<String> getChildUuids() {
            return record.getChildUuids();
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

}
