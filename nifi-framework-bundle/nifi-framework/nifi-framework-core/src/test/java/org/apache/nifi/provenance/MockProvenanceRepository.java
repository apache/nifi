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

import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.provenance.lineage.ComputeLineageSubmission;
import org.apache.nifi.provenance.search.Query;
import org.apache.nifi.provenance.search.QuerySubmission;
import org.apache.nifi.provenance.search.SearchableField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

public class MockProvenanceRepository implements ProvenanceRepository {
    private final List<ProvenanceEventRecord> records = new ArrayList<>();
    private final AtomicLong idGenerator = new AtomicLong(0L);

    @Override
    public void registerEvents(final Iterable<ProvenanceEventRecord> events) {
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

        records.add(newRecord);
    }

    @Override
    public void initialize(final EventReporter eventReporter, final Authorizer authorizer, final ProvenanceAuthorizableFactory resourceFactory, final IdentifierLookup idLookup) throws IOException {

    }

    @Override
    public List<ProvenanceEventRecord> getEvents(final long firstRecordId, final int maxRecords) throws IOException {
        if (firstRecordId > records.size()) {
            return Collections.emptyList();
        }

        return records.subList((int) firstRecordId, Math.min(records.size(), (int) (firstRecordId + maxRecords)));
    }

    @Override
    public List<ProvenanceEventRecord> getEvents(final long firstRecordId, final int maxRecords, final NiFiUser user) throws IOException {
        return getEvents(firstRecordId, maxRecords);
    }

    @Override
    public Long getMaxEventId() {
        return (long) (records.size() - 1);
    }

    @Override
    public ProvenanceEventRecord getEvent(final long id) throws IOException {
        return null;
    }

    @Override
    public QuerySubmission submitQuery(final Query query, final NiFiUser user) {
        throw new UnsupportedOperationException("MockProvenanceRepository does not support querying");
    }

    @Override
    public List<ProvenanceEventRecord> getLatestCachedEvents(final String componentId, final int eventLimit) {
        return List.of();
    }

    @Override
    public QuerySubmission retrieveQuerySubmission(final String queryIdentifier, final NiFiUser user) {
        throw new UnsupportedOperationException("MockProvenanceRepository does not support querying");
    }

    @Override
    public ComputeLineageSubmission submitLineageComputation(final String flowFileUuid, final NiFiUser user) {
        throw new UnsupportedOperationException("MockProvenanceRepository does not support Lineage Computation");
    }

    @Override
    public ComputeLineageSubmission submitLineageComputation(final long eventId, final NiFiUser user) {
        throw new UnsupportedOperationException("MockProvenanceRepository does not support Lineage Computation");
    }

    @Override
    public ComputeLineageSubmission retrieveLineageSubmission(final String lineageIdentifier, final NiFiUser user) {
        throw new UnsupportedOperationException("MockProvenanceRepository does not support Lineage Computation");
    }

    @Override
    public ProvenanceEventRecord getEvent(final long id, final NiFiUser user) throws IOException {
        if (id > records.size()) {
            return null;
        }

        return records.get((int) id);
    }

    @Override
    public ComputeLineageSubmission submitExpandParents(final long eventId, final NiFiUser user) {
        throw new UnsupportedOperationException("MockProvenanceRepository does not support Lineage Computation");
    }

    @Override
    public ComputeLineageSubmission submitExpandChildren(final long eventId, final NiFiUser user) {
        throw new UnsupportedOperationException("MockProvenanceRepository does not support Lineage Computation");
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
    public ProvenanceEventRepository getProvenanceEventRepository() {
        return this;
    }

    @Override
    public long getContainerCapacity(final String containerName) throws IOException {
        return 0;
    }

    @Override
    public String getContainerFileStoreName(final String containerName) {
        return null;
    }

    @Override
    public Set<String> getContainerNames() {
        return Set.of();
    }

    @Override
    public long getContainerUsableSpace(final String containerName) throws IOException {
        return 0;
    }
}
