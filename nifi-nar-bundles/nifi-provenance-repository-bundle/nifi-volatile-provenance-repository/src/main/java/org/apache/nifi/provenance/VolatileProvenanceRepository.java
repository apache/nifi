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

import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.authorization.AuthorizationResult;
import org.apache.nifi.authorization.AuthorizationResult.Result;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.authorization.RequestAction;
import org.apache.nifi.authorization.resource.Authorizable;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.DataUnit;
import org.apache.nifi.provenance.lineage.ComputeLineageSubmission;
import org.apache.nifi.provenance.lineage.FlowFileLineage;
import org.apache.nifi.provenance.lineage.Lineage;
import org.apache.nifi.provenance.lineage.LineageComputationType;
import org.apache.nifi.provenance.search.Query;
import org.apache.nifi.provenance.search.QueryResult;
import org.apache.nifi.provenance.search.QuerySubmission;
import org.apache.nifi.provenance.search.SearchTerm;
import org.apache.nifi.provenance.search.SearchableField;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.RingBuffer;
import org.apache.nifi.util.RingBuffer.Filter;
import org.apache.nifi.util.RingBuffer.ForEachEvaluator;
import org.apache.nifi.util.RingBuffer.IterationDirection;
import org.apache.nifi.web.ResourceNotFoundException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

public class VolatileProvenanceRepository implements ProvenanceRepository {

    // properties
    public static final String BUFFER_SIZE = "nifi.provenance.repository.buffer.size";

    // default property values
    public static final int DEFAULT_BUFFER_SIZE = 10000;

    public static String CONTAINER_NAME = "in-memory";

    private final RingBuffer<ProvenanceEventRecord> ringBuffer;
    private final int maxSize;
    private final List<SearchableField> searchableFields;
    private final List<SearchableField> searchableAttributes;
    private final ExecutorService queryExecService;
    private final ScheduledExecutorService scheduledExecService;

    private final ConcurrentMap<String, AsyncQuerySubmission> querySubmissionMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, AsyncLineageSubmission> lineageSubmissionMap = new ConcurrentHashMap<>();
    private final AtomicLong idGenerator = new AtomicLong(0L);
    private final AtomicBoolean initialized = new AtomicBoolean(false);

    private Authorizer authorizer;  // effectively final
    private ProvenanceAuthorizableFactory resourceFactory;  // effectively final

    /**
     * Default no args constructor for service loading only
     */
    public VolatileProvenanceRepository() {
        ringBuffer = null;
        searchableFields = null;
        searchableAttributes = null;
        queryExecService = null;
        scheduledExecService = null;
        authorizer = null;
        resourceFactory = null;
        maxSize = DEFAULT_BUFFER_SIZE;
    }

    public VolatileProvenanceRepository(final NiFiProperties nifiProperties) {

        maxSize = nifiProperties.getIntegerProperty(BUFFER_SIZE, DEFAULT_BUFFER_SIZE);
        ringBuffer = new RingBuffer<>(maxSize);

        final String indexedFieldString = nifiProperties.getProperty(NiFiProperties.PROVENANCE_INDEXED_FIELDS);
        final String indexedAttrString = nifiProperties.getProperty(NiFiProperties.PROVENANCE_INDEXED_ATTRIBUTES);

        searchableFields = Collections.unmodifiableList(SearchableFieldParser.extractSearchableFields(indexedFieldString, true));
        searchableAttributes = Collections.unmodifiableList(SearchableFieldParser.extractSearchableFields(indexedAttrString, false));

        final ThreadFactory defaultThreadFactory = Executors.defaultThreadFactory();
        queryExecService = Executors.newFixedThreadPool(2, new ThreadFactory() {
            private final AtomicInteger counter = new AtomicInteger(0);

            @Override
            public Thread newThread(final Runnable r) {
                final Thread thread = defaultThreadFactory.newThread(r);
                thread.setName("Provenance Query Thread-" + counter.incrementAndGet());
                return thread;
            }
        });

        scheduledExecService = Executors.newScheduledThreadPool(2);
    }

    @Override
    public void initialize(final EventReporter eventReporter, final Authorizer authorizer, final ProvenanceAuthorizableFactory resourceFactory,
        final IdentifierLookup idLookup) throws IOException {
        if (initialized.getAndSet(true)) {
            return;
        }

        this.authorizer = authorizer;
        this.resourceFactory = resourceFactory;

        scheduledExecService.scheduleWithFixedDelay(new RemoveExpiredQueryResults(), 30L, 30L, TimeUnit.SECONDS);
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
        return ringBuffer.getSelectedElements(new Filter<ProvenanceEventRecord>() {
            @Override
            public boolean select(final ProvenanceEventRecord value) {
                if (user != null && !isAuthorized(value, user)) {
                    return false;
                }

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
        final List<ProvenanceEventRecord> records = ringBuffer.getSelectedElements(new Filter<ProvenanceEventRecord>() {
            @Override
            public boolean select(final ProvenanceEventRecord event) {
                return identifier.equals(event.getFlowFileUuid());
            }
        }, 1);
        return records.isEmpty() ? null : records.get(0);
    }

    @Override
    public ProvenanceEventRecord getEvent(final long id) {
        final List<ProvenanceEventRecord> records = ringBuffer.getSelectedElements(new Filter<ProvenanceEventRecord>() {
            @Override
            public boolean select(final ProvenanceEventRecord event) {
                return event.getEventId() == id;
            }
        }, 1);

        return records.isEmpty() ? null : records.get(0);
    }

    @Override
    public ProvenanceEventRecord getEvent(final long id, final NiFiUser user) {
        final ProvenanceEventRecord event = getEvent(id);
        if (event == null) {
            return null;
        }

        authorize(event, user);
        return event;
    }

    @Override
    public void close() throws IOException {
        queryExecService.shutdownNow();
        scheduledExecService.shutdown();
    }

    @Override
    public List<SearchableField> getSearchableFields() {
        return searchableFields;
    }

    @Override
    public List<SearchableField> getSearchableAttributes() {
        return searchableAttributes;
    }

    public QueryResult queryEvents(final Query query, final NiFiUser user) throws IOException {
        final QuerySubmission submission = submitQuery(query, user);
        final QueryResult result = submission.getResult();
        while (!result.isFinished()) {
            try {
                Thread.sleep(100L);
            } catch (final InterruptedException ie) {
            }
        }

        if (result.getError() != null) {
            throw new IOException(result.getError());
        }

        return result;
    }

    public boolean isAuthorized(final ProvenanceEventRecord event, final NiFiUser user) {
        if (authorizer == null) {
            return true;
        }

        final Authorizable eventAuthorizable;
        try {
            if (event.isRemotePortType()) {
                eventAuthorizable = resourceFactory.createRemoteDataAuthorizable(event.getComponentId());
            } else {
                eventAuthorizable = resourceFactory.createLocalDataAuthorizable(event.getComponentId());
            }
        } catch (final ResourceNotFoundException rnfe) {
            return false;
        }

        final AuthorizationResult result = eventAuthorizable.checkAuthorization(authorizer, RequestAction.READ, user, event.getAttributes());
        return Result.Approved.equals(result.getResult());
    }

    protected void authorize(final ProvenanceEventRecord event, final NiFiUser user) {
        if (authorizer == null) {
            return;
        }

        final Authorizable eventAuthorizable;
        if (event.isRemotePortType()) {
            eventAuthorizable = resourceFactory.createRemoteDataAuthorizable(event.getComponentId());
        } else {
            eventAuthorizable = resourceFactory.createLocalDataAuthorizable(event.getComponentId());
        }
        eventAuthorizable.authorize(authorizer, RequestAction.READ, user, event.getAttributes());
    }

    private Filter<ProvenanceEventRecord> createFilter(final Query query, final NiFiUser user) {
        return new Filter<ProvenanceEventRecord>() {
            @Override
            public boolean select(final ProvenanceEventRecord event) {
                if (!isAuthorized(event, user)) {
                    return false;
                }

                if (query.getStartDate() != null && query.getStartDate().getTime() > event.getEventTime()) {
                    return false;
                }

                if (query.getEndDate() != null && query.getEndDate().getTime() < event.getEventTime()) {
                    return false;
                }

                if (query.getMaxFileSize() != null) {
                    final long maxFileSize = DataUnit.parseDataSize(query.getMaxFileSize(), DataUnit.B).longValue();
                    if (event.getFileSize() > maxFileSize) {
                        return false;
                    }
                }

                if (query.getMinFileSize() != null) {
                    final long minFileSize = DataUnit.parseDataSize(query.getMinFileSize(), DataUnit.B).longValue();
                    if (event.getFileSize() < minFileSize) {
                        return false;
                    }
                }

                for (final SearchTerm searchTerm : query.getSearchTerms()) {
                    final SearchableField searchableField = searchTerm.getSearchableField();
                    final String searchValue = searchTerm.getValue();

                    if (searchableField.isAttribute()) {
                        final String attributeName = searchableField.getIdentifier();

                        final String eventAttributeValue = event.getAttributes().get(attributeName);

                        if (searchValue.contains("?") || searchValue.contains("*")) {
                            if (eventAttributeValue == null || eventAttributeValue.isEmpty()) {
                                return false;
                            }

                            final String regex = searchValue.replace("?", ".").replace("*", ".*");
                            final Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
                            if (!pattern.matcher(eventAttributeValue).matches()) {
                                return false;
                            }
                        } else if (!searchValue.equalsIgnoreCase(eventAttributeValue)) {
                            return false;
                        }
                    } else {
                        // if FlowFileUUID, search parent & child UUID's also.
                        if (searchableField.equals(SearchableFields.FlowFileUUID)) {
                            if (searchValue.contains("?") || searchValue.contains("*")) {
                                final String regex = searchValue.replace("?", ".").replace("*", ".*");
                                final Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
                                if (pattern.matcher(event.getFlowFileUuid()).matches()) {
                                    continue;
                                }

                                boolean found = false;
                                for (final String uuid : event.getParentUuids()) {
                                    if (pattern.matcher(uuid).matches()) {
                                        found = true;
                                        break;
                                    }
                                }

                                for (final String uuid : event.getChildUuids()) {
                                    if (pattern.matcher(uuid).matches()) {
                                        found = true;
                                        break;
                                    }
                                }

                                if (found) {
                                    continue;
                                }
                            } else if (event.getFlowFileUuid().equals(searchValue) || event.getParentUuids().contains(searchValue) || event.getChildUuids().contains(searchValue)) {
                                continue;
                            }

                            return false;
                        }

                        final Object fieldValue = getFieldValue(event, searchableField);
                        if (fieldValue == null) {
                            return false;
                        }

                        if (searchValue.contains("?") || searchValue.contains("*")) {
                            final String regex = searchValue.replace("?", ".").replace("*", ".*");
                            final Pattern pattern = Pattern.compile(regex, Pattern.CASE_INSENSITIVE);
                            if (!pattern.matcher(String.valueOf(fieldValue)).matches()) {
                                return false;
                            }
                        } else if (!searchValue.equalsIgnoreCase(String.valueOf(fieldValue))) {
                            return false;
                        }
                    }
                }

                return true;
            }
        };
    }

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

    @Override
    public QuerySubmission submitQuery(final Query query, final NiFiUser user) {
        if (query.getEndDate() != null && query.getStartDate() != null && query.getStartDate().getTime() > query.getEndDate().getTime()) {
            throw new IllegalArgumentException("Query End Time cannot be before Query Start Time");
        }

        if (query.getSearchTerms().isEmpty() && query.getStartDate() == null && query.getEndDate() == null) {
            final AsyncQuerySubmission result = new AsyncQuerySubmission(query, 1, user.getIdentity());
            queryExecService.submit(new QueryRunnable(ringBuffer, createFilter(query, user), query.getMaxResults(), result));
            querySubmissionMap.put(query.getIdentifier(), result);
            return result;
        }

        final AsyncQuerySubmission result = new AsyncQuerySubmission(query, 1, user.getIdentity());
        querySubmissionMap.put(query.getIdentifier(), result);
        queryExecService.submit(new QueryRunnable(ringBuffer, createFilter(query, user), query.getMaxResults(), result));

        return result;
    }

    @Override
    public QuerySubmission retrieveQuerySubmission(final String queryIdentifier, final NiFiUser user) {
        final QuerySubmission submission = querySubmissionMap.get(queryIdentifier);
        final String userId = submission.getSubmitterIdentity();

        if (user == null && userId == null) {
            return submission;
        }

        if (user == null) {
            throw new AccessDeniedException("Cannot retrieve Provenance Query Submission because no user id was provided in the provenance request.");
        }

        if (userId == null || userId.equals(user.getIdentity())) {
            return submission;
        }

        throw new AccessDeniedException("Cannot retrieve Provenance Query Submission because " + user.getIdentity() + " is not the user who submitted the request.");
    }

    public Lineage computeLineage(final String flowFileUUID, final NiFiUser user) throws IOException {
        return computeLineage(Collections.singleton(flowFileUUID), user, LineageComputationType.FLOWFILE_LINEAGE, null);
    }

    private Lineage computeLineage(final Collection<String> flowFileUuids, final NiFiUser user, final LineageComputationType computationType, final Long eventId) throws IOException {
        final AsyncLineageSubmission submission = submitLineageComputation(flowFileUuids, user, computationType, eventId);
        final StandardLineageResult result = submission.getResult();
        while (!result.isFinished()) {
            try {
                Thread.sleep(100L);
            } catch (final InterruptedException ie) {
            }
        }

        if (result.getError() != null) {
            throw new IOException(result.getError());
        }

        return new FlowFileLineage(result.getNodes(), result.getEdges());
    }

    @Override
    public ComputeLineageSubmission submitLineageComputation(final long eventId, final NiFiUser user) {
        final ProvenanceEventRecord event = getEvent(eventId);
        if (event == null) {
            final String userId = user.getIdentity();
            final AsyncLineageSubmission result = new AsyncLineageSubmission(LineageComputationType.FLOWFILE_LINEAGE, eventId, Collections.emptySet(), 1, userId);
            result.getResult().setError("Could not find event with ID " + eventId);
            lineageSubmissionMap.put(result.getLineageIdentifier(), result);
            return result;
        }

        return submitLineageComputation(Collections.singleton(event.getFlowFileUuid()), user, LineageComputationType.FLOWFILE_LINEAGE, eventId);
    }

    @Override
    public AsyncLineageSubmission submitLineageComputation(final String flowFileUuid, final NiFiUser user) {
        return submitLineageComputation(Collections.singleton(flowFileUuid), user, LineageComputationType.FLOWFILE_LINEAGE, null);
    }

    @Override
    public ComputeLineageSubmission retrieveLineageSubmission(String lineageIdentifier, final NiFiUser user) {
        final ComputeLineageSubmission submission = lineageSubmissionMap.get(lineageIdentifier);
        final String userId = submission.getSubmitterIdentity();

        if (user == null && userId == null) {
            return submission;
        }

        if (user == null) {
            throw new AccessDeniedException("Cannot retrieve Provenance Lineage Submission because no user id was provided in the lineage request.");
        }

        if (userId == null || userId.equals(user.getIdentity())) {
            return submission;
        }

        throw new AccessDeniedException("Cannot retrieve Provenance Lineage Submission because " + user.getIdentity() + " is not the user who submitted the request.");
    }

    public Lineage expandSpawnEventParents(String identifier) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ComputeLineageSubmission submitExpandParents(final long eventId, final NiFiUser user) {
        final String userId = user.getIdentity();

        final ProvenanceEventRecord event = getEvent(eventId, user);
        if (event == null) {
            final AsyncLineageSubmission submission = new AsyncLineageSubmission(LineageComputationType.EXPAND_PARENTS, eventId, Collections.emptyList(), 1, userId);
            lineageSubmissionMap.put(submission.getLineageIdentifier(), submission);
            submission.getResult().update(Collections.emptyList(), 0L);
            return submission;
        }

        switch (event.getEventType()) {
            case JOIN:
            case FORK:
            case REPLAY:
            case CLONE:
                return submitLineageComputation(event.getParentUuids(), user, LineageComputationType.EXPAND_PARENTS, eventId);
            default: {
                final AsyncLineageSubmission submission = new AsyncLineageSubmission(LineageComputationType.EXPAND_PARENTS, eventId, Collections.emptyList(), 1, userId);
                lineageSubmissionMap.put(submission.getLineageIdentifier(), submission);
                submission.getResult().setError("Event ID " + eventId + " indicates an event of type " + event.getEventType() + " so its parents cannot be expanded");
                return submission;
            }
        }
    }

    public Lineage expandSpawnEventChildren(final String identifier) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ComputeLineageSubmission submitExpandChildren(final long eventId, final NiFiUser user) {
        final String userId = user.getIdentity();

        final ProvenanceEventRecord event = getEvent(eventId, user);
        if (event == null) {
            final AsyncLineageSubmission submission = new AsyncLineageSubmission(LineageComputationType.EXPAND_CHILDREN, eventId, Collections.emptyList(), 1, userId);
            lineageSubmissionMap.put(submission.getLineageIdentifier(), submission);
            submission.getResult().update(Collections.emptyList(), 0L);
            return submission;
        }

        switch (event.getEventType()) {
            case JOIN:
            case FORK:
            case REPLAY:
            case CLONE:
                return submitLineageComputation(event.getChildUuids(), user, LineageComputationType.EXPAND_CHILDREN, eventId);
            default: {
                final AsyncLineageSubmission submission = new AsyncLineageSubmission(LineageComputationType.EXPAND_CHILDREN, eventId, Collections.emptyList(), 1, userId);
                lineageSubmissionMap.put(submission.getLineageIdentifier(), submission);
                submission.getResult().setError("Event ID " + eventId + " indicates an event of type " + event.getEventType() + " so its children cannot be expanded");
                return submission;
            }
        }
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

    private AsyncLineageSubmission submitLineageComputation(final Collection<String> flowFileUuids, final NiFiUser user, final LineageComputationType computationType, final Long eventId) {
        final String userId = user.getIdentity();
        final AsyncLineageSubmission result = new AsyncLineageSubmission(computationType, eventId, flowFileUuids, 1, userId);
        lineageSubmissionMap.put(result.getLineageIdentifier(), result);

        final Filter<ProvenanceEventRecord> filter = new Filter<ProvenanceEventRecord>() {
            @Override
            public boolean select(final ProvenanceEventRecord event) {
                if (user != null && !isAuthorized(event, user)) {
                    return false;
                }

                if (flowFileUuids.contains(event.getFlowFileUuid())) {
                    return true;
                }

                for (final String parentId : event.getParentUuids()) {
                    if (flowFileUuids.contains(parentId)) {
                        return true;
                    }
                }

                for (final String childId : event.getChildUuids()) {
                    if (flowFileUuids.contains(childId)) {
                        return true;
                    }
                }

                return false;
            }
        };

        queryExecService.submit(new ComputeLineageRunnable(ringBuffer, filter, result));

        return result;
    }

    private static class QueryRunnable implements Runnable {

        private final RingBuffer<ProvenanceEventRecord> ringBuffer;
        private final Filter<ProvenanceEventRecord> filter;
        private final AsyncQuerySubmission submission;
        private final int maxRecords;

        public QueryRunnable(final RingBuffer<ProvenanceEventRecord> ringBuffer, final Filter<ProvenanceEventRecord> filter, final int maxRecords, final AsyncQuerySubmission submission) {
            this.ringBuffer = ringBuffer;
            this.filter = filter;
            this.submission = submission;
            this.maxRecords = maxRecords;
        }

        @Override
        public void run() {
            // Retrieve the most recent results and count the total number of matches
            final AtomicInteger matchingCount = new AtomicInteger(0);
            final List<ProvenanceEventRecord> matchingRecords = new ArrayList<>(maxRecords);
            ringBuffer.forEach(new ForEachEvaluator<ProvenanceEventRecord>() {
                @Override
                public boolean evaluate(final ProvenanceEventRecord record) {
                    if (filter.select(record)) {
                        if (matchingCount.incrementAndGet() <= maxRecords) {
                            matchingRecords.add(record);
                        }
                    }

                    return true;
                }

            }, IterationDirection.BACKWARD);

            submission.getResult().update(matchingRecords, matchingCount.get());
        }
    }

    private static class ComputeLineageRunnable implements Runnable {

        private final RingBuffer<ProvenanceEventRecord> ringBuffer;
        private final Filter<ProvenanceEventRecord> filter;
        private final AsyncLineageSubmission submission;

        public ComputeLineageRunnable(final RingBuffer<ProvenanceEventRecord> ringBuffer, final Filter<ProvenanceEventRecord> filter, final AsyncLineageSubmission submission) {
            this.ringBuffer = ringBuffer;
            this.filter = filter;
            this.submission = submission;
        }

        @Override
        public void run() {
            final List<ProvenanceEventRecord> records = ringBuffer.getSelectedElements(filter);
            submission.getResult().update(records, records.size());
        }
    }

    private class RemoveExpiredQueryResults implements Runnable {

        @Override
        public void run() {
            final Date now = new Date();

            final Iterator<Map.Entry<String, AsyncQuerySubmission>> queryIterator = querySubmissionMap.entrySet().iterator();
            while (queryIterator.hasNext()) {
                final Map.Entry<String, AsyncQuerySubmission> entry = queryIterator.next();

                final StandardQueryResult result = entry.getValue().getResult();
                if (result.isFinished() && result.getExpiration().before(now)) {
                    querySubmissionMap.remove(entry.getKey());
                }
            }

            final Iterator<Map.Entry<String, AsyncLineageSubmission>> lineageIterator = lineageSubmissionMap.entrySet().iterator();
            while (lineageIterator.hasNext()) {
                final Map.Entry<String, AsyncLineageSubmission> entry = lineageIterator.next();

                final StandardLineageResult result = entry.getValue().getResult();
                if (result.isFinished() && result.getExpiration().before(now)) {
                    querySubmissionMap.remove(entry.getKey());
                }
            }
        }
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
