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
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.Predicate;
import org.apache.commons.collections4.functors.AllPredicate;
import org.apache.commons.collections4.queue.CircularFifoQueue;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.provenance.lineage.ComputeLineageSubmission;
import org.apache.nifi.provenance.lineage.LineageComputationType;
import org.apache.nifi.provenance.search.Query;
import org.apache.nifi.provenance.search.QuerySubmission;
import org.apache.nifi.provenance.search.SearchTerm;
import org.apache.nifi.provenance.search.SearchableField;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.StringUtils;

/**
 * This class represents a repository of provenance events that can be queried.
 * The storage mechanism is a simply array and is not persistent. This
 * repository is for development, prototyping and testing only. It is not meant
 * for production.
 * 
 * This implementation uses a circular FIFO queue to store provenance record
 * events. The queue is a first-in first-out queue with a fixed size that
 * replaces its oldest element if full.
 * 
 * None of the calls are actually asynchronous. Search methods block until the
 * search is complete and the results are made immediately available.
 * 
 * <pre>
 * Attributes:
 * 
 * nifi.provenance.repository.buffer.size = The maximum number of events to hold in the repository
 * </pre>
 */
public final class VolatileProvenanceRepository implements ProvenanceEventRepository {

    private static final String BUFFER_SIZE = "nifi.provenance.repository.buffer.size";

    private static final int DEFAULT_BUFFER_SIZE = 10000;

    private final CircularFifoQueue<ProvenanceEventRecord> queue;

    private final List<SearchableField> searchableFields;
    private final List<SearchableField> searchableAttributes;

    private final ConcurrentMap<String, AsyncQuerySubmission> querySubmissionMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, AsyncLineageSubmission> lineageSubmissionMap = new ConcurrentHashMap<>();

    private long lastEventId = 0l;

    public VolatileProvenanceRepository() {
        final NiFiProperties properties = NiFiProperties.getInstance();

        final int bufferSize = properties.getIntegerProperty(BUFFER_SIZE, DEFAULT_BUFFER_SIZE);
        this.queue = new CircularFifoQueue<ProvenanceEventRecord>(bufferSize);

        final String indexedFieldString = properties
                .getProperty(NiFiProperties.PROVENANCE_INDEXED_FIELDS);

        final String indexedAttrString = properties
                .getProperty(NiFiProperties.PROVENANCE_INDEXED_ATTRIBUTES);

        this.searchableFields = Collections.unmodifiableList(SearchableFieldParser
                .extractSearchableFields(indexedFieldString, true));

        this.searchableAttributes = Collections.unmodifiableList(SearchableFieldParser
                .extractSearchableFields(indexedAttrString, false));
    }

    @Override
    public void initialize(final EventReporter eventReporter) {
    }

    @Override
    public ProvenanceEventBuilder eventBuilder() {
        return new StandardProvenanceEventRecord.Builder();
    }

    @Override
    public void registerEvent(final ProvenanceEventRecord event) {

        final IdEnrichedProvenanceEvent wrappedEvent = new IdEnrichedProvenanceEvent(event);

        synchronized (this.queue) {
            final long id = ++lastEventId;
            wrappedEvent.setEventId(id);
            this.queue.add(wrappedEvent);
        }
    }

    @Override
    public void registerEvents(final Iterable<ProvenanceEventRecord> events) {
        for (final ProvenanceEventRecord event : events) {
            registerEvent(event);
        }
    }

    @Override
    public List<ProvenanceEventRecord> getEvents(final long firstRecordId, final int maxRecords)
            throws IOException {

        final List<ProvenanceEventRecord> results = new ArrayList<>();

        synchronized (this.queue) {
            final Iterator<ProvenanceEventRecord> it = queue.iterator();
            while (it.hasNext() && results.size() < maxRecords) {
                final ProvenanceEventRecord record = it.next();
                if (record.getEventId() >= firstRecordId) {
                    results.add(record);
                }
            }
        }

        return results;
    }

    @Override
    public Long getMaxEventId() {
        synchronized (this.queue) {
            if (!this.queue.isEmpty()) {
                return this.lastEventId;
            }
        }
        return null;
    }

    @Override
    public ProvenanceEventRecord getEvent(final long id) {
        synchronized (queue) {
            final Iterator<ProvenanceEventRecord> it = queue.iterator();
            while (it.hasNext()) {
                final ProvenanceEventRecord record = it.next();
                if (id == record.getEventId()) {
                    return record;
                }
            }
        }

        return null;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public List<SearchableField> getSearchableFields() {
        return searchableFields;
    }

    @Override
    public List<SearchableField> getSearchableAttributes() {
        return searchableAttributes;
    }

    /**
     * Transform a query object into a series of predicates
     * 
     * @param query
     *            The query to transform
     * @return The final predicate to search with
     */
    private Predicate<ProvenanceEventRecord> createFilter(final Query query) {

        final List<Predicate<ProvenanceEventRecord>> criteria = new ArrayList<>();

        if (query.getStartDate() != null) {
            final Predicate<ProvenanceEventRecord> searchCriteria = new Predicate<ProvenanceEventRecord>() {
                public boolean evaluate(final ProvenanceEventRecord arg0) {
                    return arg0.getEventTime() >= query.getStartDate().getTime();
                };
            };
            criteria.add(searchCriteria);
        }

        if (query.getEndDate() != null) {
            final Predicate<ProvenanceEventRecord> searchCriteria = new Predicate<ProvenanceEventRecord>() {
                public boolean evaluate(final ProvenanceEventRecord arg0) {
                    return arg0.getEventTime() <= query.getEndDate().getTime();
                };
            };
            criteria.add(searchCriteria);
        }
        if (query.getMaxFileSize() != null) {
            final long maxFileSize = DataUnit.parseDataSize(query.getMaxFileSize(), DataUnit.B)
                    .longValue();
            final Predicate<ProvenanceEventRecord> searchCriteria = new Predicate<ProvenanceEventRecord>() {
                public boolean evaluate(final ProvenanceEventRecord arg0) {
                    return arg0.getFileSize() <= maxFileSize;
                };
            };
            criteria.add(searchCriteria);
        }
        if (query.getMinFileSize() != null) {
            final long minFileSize = DataUnit.parseDataSize(query.getMinFileSize(), DataUnit.B)
                    .longValue();
            final Predicate<ProvenanceEventRecord> searchCriteria = new Predicate<ProvenanceEventRecord>() {
                public boolean evaluate(final ProvenanceEventRecord arg0) {
                    return arg0.getFileSize() >= minFileSize;
                };
            };
            criteria.add(searchCriteria);
        }

        for (final SearchTerm searchTerm : query.getSearchTerms()) {
            final SearchableField searchableField = searchTerm.getSearchableField();
            final String searchValue = searchTerm.getValue();

            final String regex = searchValue.replace("?", ".{1}").replace("*", ".*");
            final Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);

            if (searchableField.isAttribute()) {
                final String attributeName = searchableField.getIdentifier();

                final Predicate<ProvenanceEventRecord> searchCriteria = new Predicate<ProvenanceEventRecord>() {
                    public boolean evaluate(final ProvenanceEventRecord arg0) {
                        final String eventAttributeValue = arg0.getAttributes().get(attributeName);
                        if (!StringUtils.isEmpty(eventAttributeValue)) {
                            return pattern.matcher(eventAttributeValue).matches();
                        }
                        return false;
                    };
                };

                criteria.add(searchCriteria);

            } else if (SearchableFields.FlowFileUUID.equals(searchableField)) {
                // if FlowFileUUID, search parent & child UUID's also.

                final Predicate<ProvenanceEventRecord> searchCriteria = new Predicate<ProvenanceEventRecord>() {
                    public boolean evaluate(final ProvenanceEventRecord arg0) {
                        if (pattern.matcher(arg0.getFlowFileUuid()).matches()) {
                            return true;
                        }

                        for (final String uuid : arg0.getParentUuids()) {
                            if (pattern.matcher(uuid).matches()) {
                                return true;
                            }
                        }

                        for (final String uuid : arg0.getChildUuids()) {
                            if (pattern.matcher(uuid).matches()) {
                                return true;
                            }
                        }

                        return false;
                    };
                };

                criteria.add(searchCriteria);

            } else {

                final Predicate<ProvenanceEventRecord> searchCriteria = new Predicate<ProvenanceEventRecord>() {
                    public boolean evaluate(final ProvenanceEventRecord arg0) {
                        final Object fieldValue = getFieldValue(arg0, searchableField);
                        if (fieldValue == null) {
                            return false;
                        }
                        if (pattern.matcher(String.valueOf(fieldValue)).matches()) {
                            return true;
                        }
                        return false;
                    };
                };
                criteria.add(searchCriteria);
            }
        }

        return AllPredicate.allPredicate(criteria);
    }

    /**
     * Given an event record and a search field, return the fields value for the
     * object
     * 
     * @param record
     * @param field
     * @return
     */
    private Object getFieldValue(final ProvenanceEventRecord record, final SearchableField field) {
        if (SearchableFields.AlternateIdentifierURI.equals(field)) {
            return record.getAlternateIdentifierUri();
        }
        if (SearchableFields.ComponentID.equals(field)) {
            return record.getComponentId();
        }
        if (SearchableFields.Details.equals(field)) {
            return record.getDetails();
        }
        if (SearchableFields.EventTime.equals(field)) {
            return record.getEventTime();
        }
        if (SearchableFields.EventType.equals(field)) {
            return record.getEventType();
        }
        if (SearchableFields.Filename.equals(field)) {
            return record.getAttributes().get(CoreAttributes.FILENAME.key());
        }
        if (SearchableFields.FileSize.equals(field)) {
            return record.getFileSize();
        }
        if (SearchableFields.FlowFileUUID.equals(field)) {
            return record.getFlowFileUuid();
        }
        if (SearchableFields.LineageStartDate.equals(field)) {
            return record.getLineageStartDate();
        }
        if (SearchableFields.Relationship.equals(field)) {
            return record.getRelationship();
        }
        if (SearchableFields.TransitURI.equals(field)) {
            return record.getTransitUri();
        }

        return null;
    }

    /**
     * Not actually an asynchronous call. Blocks until the query is complete and
     * makes the results immediately available.
     */
    @Override
    public QuerySubmission submitQuery(final Query query) {
        if (query.getEndDate() != null && query.getStartDate() != null
                && query.getStartDate().getTime() > query.getEndDate().getTime()) {
            throw new IllegalArgumentException("Query End Time cannot be before Query Start Time");
        }

        final AsyncQuerySubmission querySubmission = new AsyncQuerySubmission(query, 1);

        final Predicate<ProvenanceEventRecord> filter = createFilter(query);
        final int maxResults = querySubmission.getQuery().getMaxResults();
        final List<ProvenanceEventRecord> results = new ArrayList<>();

        synchronized (this.queue) {
            final Iterator<ProvenanceEventRecord> it = this.queue.iterator();
            while (results.size() < maxResults && it.hasNext()) {
                final ProvenanceEventRecord record = it.next();
                if (filter.evaluate(record)) {
                    results.add(record);
                }
            }
        }

        querySubmission.getResult().update(results, results.size());

        this.querySubmissionMap.put(querySubmission.getQueryIdentifier(), querySubmission);

        return querySubmission;
    }

    @Override
    public QuerySubmission retrieveQuerySubmission(final String queryIdentifier) {
        return this.querySubmissionMap.remove(queryIdentifier);
    }

    /**
     * Not actually an asynchronous call. Blocks until the query is complete and
     * makes the results immediately available.
     */
    @Override
    public AsyncLineageSubmission submitLineageComputation(final String flowFileUuid) {
        return submitLineageComputation(Collections.singleton(flowFileUuid),
                LineageComputationType.FLOWFILE_LINEAGE, null);
    }

    @Override
    public ComputeLineageSubmission retrieveLineageSubmission(final String lineageIdentifier) {
        return this.lineageSubmissionMap.remove(lineageIdentifier);
    }

    @Override
    public ComputeLineageSubmission submitExpandParents(final long eventId) {
        final ProvenanceEventRecord event = getEvent(eventId);
        if (event == null) {
            final AsyncLineageSubmission submission = new AsyncLineageSubmission(
                    LineageComputationType.EXPAND_PARENTS, eventId,
                    Collections.<String> emptyList(), 1);
            this.lineageSubmissionMap.put(submission.getLineageIdentifier(), submission);
            submission.getResult().update(Collections.<ProvenanceEventRecord> emptyList());
            return submission;
        }

        switch (event.getEventType()) {
        case JOIN:
        case FORK:
        case REPLAY:
        case CLONE:
            return submitLineageComputation(event.getParentUuids(),
                    LineageComputationType.EXPAND_PARENTS, eventId);
        default: {
            final AsyncLineageSubmission submission = new AsyncLineageSubmission(
                    LineageComputationType.EXPAND_PARENTS, eventId,
                    Collections.<String> emptyList(), 1);
            this.lineageSubmissionMap.put(submission.getLineageIdentifier(), submission);
            submission.getResult().setError(
                    "Event ID " + eventId + " indicates an event of type " + event.getEventType()
                            + " so its parents cannot be expanded");
            return submission;
        }
        }
    }

    @Override
    public ComputeLineageSubmission submitExpandChildren(final long eventId) {
        final ProvenanceEventRecord event = getEvent(eventId);
        if (event == null) {
            final AsyncLineageSubmission submission = new AsyncLineageSubmission(
                    LineageComputationType.EXPAND_CHILDREN, eventId,
                    Collections.<String> emptyList(), 1);
            lineageSubmissionMap.put(submission.getLineageIdentifier(), submission);
            submission.getResult().update(Collections.<ProvenanceEventRecord> emptyList());
            return submission;
        }

        switch (event.getEventType()) {
        case JOIN:
        case FORK:
        case REPLAY:
        case CLONE:
            return submitLineageComputation(event.getChildUuids(),
                    LineageComputationType.EXPAND_CHILDREN, eventId);
        default: {
            final AsyncLineageSubmission submission = new AsyncLineageSubmission(
                    LineageComputationType.EXPAND_CHILDREN, eventId,
                    Collections.<String> emptyList(), 1);
            lineageSubmissionMap.put(submission.getLineageIdentifier(), submission);
            submission.getResult().setError(
                    "Event ID " + eventId + " indicates an event of type " + event.getEventType()
                            + " so its children cannot be expanded");
            return submission;
        }
        }
    }

    private AsyncLineageSubmission submitLineageComputation(final Collection<String> flowFileUuids,
            final LineageComputationType computationType, final Long eventId) {

        final AsyncLineageSubmission lineageSubmission = new AsyncLineageSubmission(
                computationType, eventId, flowFileUuids, 1);

        final Predicate<ProvenanceEventRecord> filter = new Predicate<ProvenanceEventRecord>() {
            @Override
            public boolean evaluate(final ProvenanceEventRecord arg0) {
                if (flowFileUuids.contains(arg0.getFlowFileUuid())) {
                    return true;
                }
                if (!Collections.disjoint(flowFileUuids, arg0.getParentUuids())) {
                    return true;
                }
                if (!Collections.disjoint(flowFileUuids, arg0.getChildUuids())) {
                    return true;
                }
                return false;
            }
        };

        final Collection<ProvenanceEventRecord> results = new ArrayList<>(0);

        synchronized (this.queue) {
            CollectionUtils.select(this.queue, filter, results);
        }

        lineageSubmission.getResult().update(results);

        this.lineageSubmissionMap.put(lineageSubmission.getLineageIdentifier(), lineageSubmission);

        return lineageSubmission;
    }
}
