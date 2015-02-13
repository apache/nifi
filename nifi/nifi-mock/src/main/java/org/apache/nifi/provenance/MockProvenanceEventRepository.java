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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.events.EventReporter;
import org.apache.nifi.provenance.lineage.ComputeLineageSubmission;
import org.apache.nifi.provenance.search.Query;
import org.apache.nifi.provenance.search.QuerySubmission;
import org.apache.nifi.provenance.search.SearchableField;

public class MockProvenanceEventRepository implements ProvenanceEventRepository {

    private final List<StoredProvenanceEvent> records = new ArrayList<>();
    private final AtomicLong idGenerator = new AtomicLong(0L);

    @Override
    public void registerEvents(final Collection<ProvenanceEventRecord> events) {
        for (final ProvenanceEventRecord event : events) {
            registerEvent(event);
        }
    }

    @Override
    public void registerEvent(final ProvenanceEventRecord event) {
        final StandardProvenanceEventRecord newRecord;
        if (event instanceof StandardProvenanceEventRecord) {
            newRecord = (StandardProvenanceEventRecord) event;
        } else {
            newRecord = new StandardProvenanceEventRecord.Builder().fromEvent(event).build();
        }
        newRecord.setEventId(idGenerator.getAndIncrement());

        records.add(new IdEnrichedProvenanceEvent(newRecord));
    }

    @Override
    public void initialize(EventReporter reporter) throws IOException {
    }

    @Override
    public List<StoredProvenanceEvent> getEvents(long firstRecordId, int maxRecords) throws IOException {
        if (firstRecordId > records.size()) {
            return Collections.emptyList();
        }

        return records.subList((int) firstRecordId, Math.min(records.size(), (int) (firstRecordId + maxRecords)));
    }

    @Override
    public Long getMaxEventId() {
        return Long.valueOf(records.size() - 1);
    }

    @Override
    public QuerySubmission submitQuery(Query query) {
        throw new UnsupportedOperationException("MockProvenanceEventRepository does not support querying");
    }

    @Override
    public QuerySubmission retrieveQuerySubmission(String queryIdentifier) {
        throw new UnsupportedOperationException("MockProvenanceEventRepository does not support querying");
    }

    @Override
    public ComputeLineageSubmission submitLineageComputation(String flowFileUuid) {
        throw new UnsupportedOperationException("MockProvenanceEventRepository does not support Lineage Computation");
    }

    @Override
    public ComputeLineageSubmission retrieveLineageSubmission(String lineageIdentifier) {
        throw new UnsupportedOperationException("MockProvenanceEventRepository does not support Lineage Computation");
    }

    @Override
    public StoredProvenanceEvent getEvent(long id) throws IOException {
        if (id > records.size()) {
            return null;
        }

        return records.get((int) id);
    }

    @Override
    public ComputeLineageSubmission submitExpandParents(long eventId) {
        throw new UnsupportedOperationException("MockProvenanceEventRepository does not support Lineage Computation");
    }

    @Override
    public ComputeLineageSubmission submitExpandChildren(long eventId) {
        throw new UnsupportedOperationException("MockProvenanceEventRepository does not support Lineage Computation");
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public List<SearchableField> getSearchableFields() {
        return Collections.emptyList();
    }

    @Override
    public List<SearchableField> getSearchableAttributes() {
        return Collections.emptyList();
    }

    @Override
    public ProvenanceEventBuilder eventBuilder() {
        return new StandardProvenanceEventRecord.Builder();
    }
    
    @Override
    public Long getEarliestEventTime() throws IOException {
        final StoredProvenanceEvent event = getEvent(0);
        if ( event == null ) {
            return null;
        }
        
        return event.getEventTime();
    }
    
    @Override
    public StoredProvenanceEvent getEvent(final StorageLocation location) throws IOException {
        if ( location instanceof EventIdLocation ) {
            return getEvent( ((EventIdLocation) location).getId() );
        }
        throw new IllegalArgumentException("Invalid StorageLocation");
    }
    
    @Override
    public List<StoredProvenanceEvent> getEvents(final List<StorageLocation> storageLocations) throws IOException {
        final List<StoredProvenanceEvent> events = new ArrayList<>(storageLocations.size());
        for ( final StorageLocation location : storageLocations ) {
            final StoredProvenanceEvent event = getEvent(location);
            if ( event != null ) {
                events.add(event);
            }
        }
        return events;
    }
}
