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
import org.apache.nifi.provenance.lineage.LineageComputationType;
import org.apache.nifi.provenance.search.Query;
import org.apache.nifi.provenance.search.QueryResult;
import org.apache.nifi.provenance.search.QuerySubmission;
import org.apache.nifi.provenance.search.SearchableField;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.util.NiFiProperties;

import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;

/**
 * NoOpProvenanceRepository is an implementation of {@link ProvenanceRepository} that does not store events.
 * This is useful for testing or disabling provenance tracking.
 * <p>
 * All query and lineage methods return empty results or no-ops.
 * The repository logs initialization info via the EventReporter.
 */
public class NoOpProvenanceRepository implements ProvenanceRepository {

    /**
     * Default constructor for service loading only.
     */
    public NoOpProvenanceRepository() {
    }

    /**
     * Constructs a NoOpProvenanceRepository with provided properties.
     *
     * @param ignored NiFi properties, ignored in this implementation.
     */
    public NoOpProvenanceRepository(final NiFiProperties ignored) {
    }

    /**
     * Initializes the repository but does not actually store events.
     */
    @Override
    public void initialize(EventReporter eventReporter, Authorizer authorizer,
                           ProvenanceAuthorizableFactory factory, IdentifierLookup identifierLookup) throws IOException {
        eventReporter.reportEvent(Severity.INFO, "Provenance", "Initialized NoOpProvenanceRepository - Provenance tracking is disabled");
    }

    /**
     * Closes the repository. No-op.
     */
    @Override
    public void close() throws IOException {

    }

    /**
     * @return a new builder for creating provenance event records
     */
    @Override
    public ProvenanceEventBuilder eventBuilder() {
        return new StandardProvenanceEventRecord.Builder();
    }

    /**
     * Returns null since no events are stored.
     */
    @Override
    public ProvenanceEventRecord getEvent(long id) {
        return null;
    }

    /**
     * @return null since no events are stored.
     */
    @Override
    public ProvenanceEventRecord getEvent(final long id, final NiFiUser user) {
        return null;
    }

    /**
     * @return an empty list since no events are stored
     */
    @Override
    public List<ProvenanceEventRecord> getEvents(long firstRecordId, int maxRecords) {
        return emptyList();
    }

    /**
     * @return an empty list since no events are stored
     */
    @Override
    public List<ProvenanceEventRecord> getEvents(long firstRecordId, int maxRecords, NiFiUser user) {
        return emptyList();
    }

    /**
     * @return 0 since no events are stored
     */
    @Override
    public Long getMaxEventId() {
        // No events are stored in this implementation
        return 0L;
    }

    /**
     * No-op. Events are not stored.
     */
    @Override
    public void registerEvent(ProvenanceEventRecord records) {

    }

    /**
     * No-op. Events are not stored.
     */
    @Override
    public void registerEvents(Iterable<ProvenanceEventRecord> records) {

    }

    @Override
    public ProvenanceEventRepository getProvenanceEventRepository() {
        return this;
    }

    /**
     * @return a stub query submission that returns an empty result
     */
    @Override
    public QuerySubmission submitQuery(Query query, NiFiUser user) {
        return new NoOpQuerySubmission(query, user.getIdentity());
    }

    /**
     * @return an empty list
     */
    @Override
    public List<ProvenanceEventRecord> getLatestCachedEvents(final String componentId, final int eventLimit) {
        return emptyList();
    }

    /**
     * @return a stub query submission with an empty result
     */
    @Override
    public QuerySubmission retrieveQuerySubmission(String queryIdentifier, NiFiUser user) {
        return new NoOpQuerySubmission(queryIdentifier, user.getIdentity());
    }

    /**
     * @return a stub lineage computation result with no events
     */
    @Override
    public ComputeLineageSubmission submitLineageComputation(String s, NiFiUser user) {
        return new NoOpComputeLineageSubmission(LineageComputationType.FLOWFILE_LINEAGE, null, singleton(s), user.getIdentity());
    }

    /**
     * @return a stub lineage computation result with an error message
     */
    @Override
    public ComputeLineageSubmission submitLineageComputation(long eventId, NiFiUser user) {
        final NoOpComputeLineageSubmission result = new NoOpComputeLineageSubmission(LineageComputationType.FLOWFILE_LINEAGE, eventId, emptySet(), user.getIdentity());
        result.getResult().setError("Could not find event with ID " + eventId);
        return result;
    }

    /**
     * @return a stub lineage submission with no event
     */
    @Override
    public AsyncLineageSubmission retrieveLineageSubmission(final String lineageIdentifier, final NiFiUser user) {
        return new NoOpComputeLineageSubmission(LineageComputationType.FLOWFILE_LINEAGE, null, emptyList(), user.getIdentity());
    }

    /**
     * @return an empty list of searchable fields
     */
    @Override
    public List<SearchableField> getSearchableFields() {
        return emptyList();
    }

    /**
     * @return an empty list of searchable attributes
     */
    @Override
    public List<SearchableField> getSearchableAttributes() {
        return emptyList();
    }

    /**
     * @return a singleton set containing the string "noOp"
     */
    @Override
    public Set<String> getContainerNames() {
        return singleton("noOp");
    }

    /**
     * @return 0 as this implementation does not store data
     */
    @Override
    public long getContainerCapacity(String s) {
        return 0;
    }

    /**
     * @return "noOp" as the file store name
     */
    @Override
    public String getContainerFileStoreName(String containerName) {
        return "noOp";
    }

    /**
     * @return 0 as this implementation does not store data
     */
    @Override
    public long getContainerUsableSpace(String containerName) {
        return 0;
    }

    /**
     * @return a no-op stub lineage submission for expanding parents
     */
    @Override
    public AsyncLineageSubmission submitExpandParents(final long eventId, final NiFiUser user) {
        return new NoOpComputeLineageSubmission(LineageComputationType.EXPAND_PARENTS, null, emptyList(), user.getIdentity());
    }

    /**
     * @return a stub no-op lineage submission for expanding children
     */
    @Override
    public AsyncLineageSubmission submitExpandChildren(final long eventId, final NiFiUser user) {
        return new NoOpComputeLineageSubmission(LineageComputationType.EXPAND_CHILDREN, null, emptyList(), user.getIdentity());
    }

    private static class NoOpComputeLineageSubmission extends AsyncLineageSubmission {

        public NoOpComputeLineageSubmission(LineageComputationType computationType, Long eventId, Collection<String> lineageFlowFileUuids, String submitterId) {
            super(computationType, eventId, lineageFlowFileUuids, 1, submitterId);
        }

        @Override
        public boolean isCanceled() {
            return true;
        }

    }

    private static final QueryResult noOpQueryResult = new QueryResult() {
        @Override
        public List<ProvenanceEventRecord> getMatchingEvents() {
            return emptyList();
        }

        @Override
        public long getTotalHitCount() {
            return 0;
        }

        @Override
        public long getQueryTime() {
            return 0;
        }

        @Override
        public Date getExpiration() {
            return new Date(0);
        }

        @Override
        public String getError() {
            return "";
        }

        @Override
        public int getPercentComplete() {
            return 100;
        }

        @Override
        public boolean isFinished() {
            return true;
        }

        @Override
        public boolean awaitCompletion(long time, TimeUnit unit) {
            return false;
        }
    };

    private static class NoOpQuerySubmission implements QuerySubmission {
        private final Query query;
        private final String submitterId;
        private final Date submissionTime = new Date();

        private NoOpQuerySubmission(final Query query, final String id) {
            this.query = query;
            this.submitterId = id;
        }

        private NoOpQuerySubmission(final String queryId, final String id) {
            this.query = new Query(queryId);
            this.submitterId = id;
        }

        @Override
        public Query getQuery() {
            return query;
        }

        @Override
        public QueryResult getResult() {
            return noOpQueryResult;
        }

        @Override
        public Date getSubmissionTime() {
            return submissionTime;
        }

        @Override
        public String getQueryIdentifier() {
            return query.getIdentifier();
        }

        @Override
        public void cancel() {

        }

        @Override
        public boolean isCanceled() {
            return true;
        }

        @Override
        public String getSubmitterIdentity() {
            return submitterId;
        }
    }
}
