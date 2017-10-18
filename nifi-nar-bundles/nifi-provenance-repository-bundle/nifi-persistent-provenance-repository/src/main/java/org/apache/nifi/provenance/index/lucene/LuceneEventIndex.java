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

package org.apache.nifi.provenance.index.lucene;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.nifi.authorization.AccessDeniedException;
import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.provenance.AsyncLineageSubmission;
import org.apache.nifi.provenance.AsyncQuerySubmission;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.RepositoryConfiguration;
import org.apache.nifi.provenance.SearchableFields;
import org.apache.nifi.provenance.StandardLineageResult;
import org.apache.nifi.provenance.StandardQueryResult;
import org.apache.nifi.provenance.authorization.EventAuthorizer;
import org.apache.nifi.provenance.authorization.EventTransformer;
import org.apache.nifi.provenance.index.EventIndex;
import org.apache.nifi.provenance.index.EventIndexSearcher;
import org.apache.nifi.provenance.index.EventIndexWriter;
import org.apache.nifi.provenance.lineage.ComputeLineageSubmission;
import org.apache.nifi.provenance.lineage.LineageComputationType;
import org.apache.nifi.provenance.lucene.IndexManager;
import org.apache.nifi.provenance.lucene.LuceneUtil;
import org.apache.nifi.provenance.search.Query;
import org.apache.nifi.provenance.search.QuerySubmission;
import org.apache.nifi.provenance.serialization.StorageSummary;
import org.apache.nifi.provenance.store.EventStore;
import org.apache.nifi.provenance.util.DirectoryUtils;
import org.apache.nifi.provenance.util.NamedThreadFactory;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.util.file.FileUtils;
import org.apache.nifi.util.timebuffer.LongEntityAccess;
import org.apache.nifi.util.timebuffer.TimedBuffer;
import org.apache.nifi.util.timebuffer.TimestampedLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class LuceneEventIndex implements EventIndex {
    private static final Logger logger = LoggerFactory.getLogger(LuceneEventIndex.class);
    private static final String EVENT_CATEGORY = "Provenance Repository";

    public static final int MAX_UNDELETED_QUERY_RESULTS = 10;
    public static final int MAX_DELETE_INDEX_WAIT_SECONDS = 30;
    public static final int MAX_LINEAGE_NODES = 1000;
    public static final int MAX_INDEX_THREADS = 100;

    private final ConcurrentMap<String, AsyncQuerySubmission> querySubmissionMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, AsyncLineageSubmission> lineageSubmissionMap = new ConcurrentHashMap<>();
    private final BlockingQueue<StoredDocument> documentQueue = new LinkedBlockingQueue<>(1000);
    private final List<EventIndexTask> indexTasks = Collections.synchronizedList(new ArrayList<>());
    private final ExecutorService queryExecutor;
    private final ExecutorService indexExecutor;
    private final RepositoryConfiguration config;
    private final IndexManager indexManager;
    private final ConvertEventToLuceneDocument eventConverter;
    private final IndexDirectoryManager directoryManager;
    private volatile boolean closed = false;

    private final TimedBuffer<TimestampedLong> queuePauseNanos = new TimedBuffer<>(TimeUnit.SECONDS, 300, new LongEntityAccess());
    private final TimedBuffer<TimestampedLong> eventsIndexed = new TimedBuffer<>(TimeUnit.SECONDS, 300, new LongEntityAccess());
    private final AtomicLong eventCount = new AtomicLong(0L);
    private final EventReporter eventReporter;

    private final List<CachedQuery> cachedQueries = new ArrayList<>();

    private ScheduledExecutorService maintenanceExecutor; // effectively final
    private ScheduledExecutorService cacheWarmerExecutor;
    private EventStore eventStore;

    public LuceneEventIndex(final RepositoryConfiguration config, final IndexManager indexManager, final EventReporter eventReporter) {
        this(config, indexManager, EventIndexTask.DEFAULT_MAX_EVENTS_PER_COMMIT, eventReporter);
    }

    public LuceneEventIndex(final RepositoryConfiguration config, final IndexManager indexManager, final int maxEventsPerCommit, final EventReporter eventReporter) {
        this.eventReporter = eventReporter;
        queryExecutor = Executors.newFixedThreadPool(config.getQueryThreadPoolSize(), new NamedThreadFactory("Provenance Query"));
        indexExecutor = Executors.newFixedThreadPool(config.getIndexThreadPoolSize(), new NamedThreadFactory("Index Provenance Events"));
        cacheWarmerExecutor = Executors.newScheduledThreadPool(config.getStorageDirectories().size(), new NamedThreadFactory("Warm Lucene Index", true));
        directoryManager = new IndexDirectoryManager(config);

        // Limit number of indexing threads to 100. When we restore the repository on restart,
        // we have to re-index up to MAX_THREADS * MAX_DOCUMENTS_PER_THREADS events prior to
        // the last event that the index holds. This is done because we could have that many
        // events 'in flight', waiting to be indexed when the last index writer was committed,
        // so even though the index says the largest event ID is 1,000,000 for instance, Event
        // with ID 999,999 may still not have been indexed because another thread was in the
        // process of writing the event to the index.
        final int configuredIndexPoolSize = config.getIndexThreadPoolSize();
        final int numIndexThreads;
        if (configuredIndexPoolSize > MAX_INDEX_THREADS) {
            logger.warn("The Provenance Repository is configured to perform indexing of events using {} threads. This number exceeds the maximum allowable number of threads, which is {}. "
                + "Will proceed using {} threads. This value is limited because the performance of indexing will decrease and startup times will increase when setting this value too high.",
                configuredIndexPoolSize, MAX_INDEX_THREADS, MAX_INDEX_THREADS);
            numIndexThreads = MAX_INDEX_THREADS;
        } else {
            numIndexThreads = configuredIndexPoolSize;
        }

        for (int i = 0; i < numIndexThreads; i++) {
            final EventIndexTask task = new EventIndexTask(documentQueue, config, indexManager, directoryManager, maxEventsPerCommit, eventReporter);
            indexTasks.add(task);
            indexExecutor.submit(task);
        }
        this.config = config;
        this.indexManager = indexManager;
        this.eventConverter = new ConvertEventToLuceneDocument(config.getSearchableFields(), config.getSearchableAttributes());
    }

    @Override
    public void initialize(final EventStore eventStore) {
        this.eventStore = eventStore;
        directoryManager.initialize();

        maintenanceExecutor = Executors.newScheduledThreadPool(1, new NamedThreadFactory("Provenance Repository Maintenance"));
        maintenanceExecutor.scheduleWithFixedDelay(() -> performMaintenance(), 1, 1, TimeUnit.MINUTES);
        maintenanceExecutor.scheduleWithFixedDelay(this::purgeObsoleteQueries, 30, 30, TimeUnit.SECONDS);

        cachedQueries.add(new LatestEventsQuery());
        cachedQueries.add(new LatestEventsPerProcessorQuery());

        final Optional<Integer> warmCacheMinutesOption = config.getWarmCacheFrequencyMinutes();
        if (warmCacheMinutesOption.isPresent() && warmCacheMinutesOption.get() > 0) {
            for (final File storageDir : config.getStorageDirectories().values()) {
                final int minutes = warmCacheMinutesOption.get();
                cacheWarmerExecutor.scheduleWithFixedDelay(new LuceneCacheWarmer(storageDir, indexManager), 1, minutes, TimeUnit.MINUTES);
            }
        }
    }

    @Override
    public long getMinimumEventIdToReindex(final String partitionName) {
        return Math.max(0, getMaxEventId(partitionName) - EventIndexTask.MAX_DOCUMENTS_PER_THREAD * LuceneEventIndex.MAX_INDEX_THREADS);
    }

    protected IndexDirectoryManager getDirectoryManager() {
        return directoryManager;
    }

    @Override
    public void close() throws IOException {
        closed = true;
        queryExecutor.shutdownNow();
        indexExecutor.shutdown();
        cacheWarmerExecutor.shutdown();

        if (maintenanceExecutor != null) {
            maintenanceExecutor.shutdown();
        }

        for (final EventIndexTask task : indexTasks) {
            task.shutdown();
        }
    }

    long getMaxEventId(final String partitionName) {
        final List<File> allDirectories = getDirectoryManager().getDirectories(0L, Long.MAX_VALUE, partitionName);
        if (allDirectories.isEmpty()) {
            return -1L;
        }

        Collections.sort(allDirectories, DirectoryUtils.NEWEST_INDEX_FIRST);

        for (final File directory : allDirectories) {
            final EventIndexSearcher searcher;
            try {
                searcher = indexManager.borrowIndexSearcher(directory);
            } catch (final IOException ioe) {
                logger.warn("Unable to read from Index Directory {}. Will assume that the index is incomplete and not consider this index when determining max event ID", directory);
                continue;
            }

            try {
                final IndexReader reader = searcher.getIndexSearcher().getIndexReader();
                final int maxDocId = reader.maxDoc() - 1;
                final Document document = reader.document(maxDocId);
                final long eventId = document.getField(SearchableFields.Identifier.getSearchableFieldName()).numericValue().longValue();
                logger.info("Determined that Max Event ID indexed for Partition {} is approximately {} based on index {}", partitionName, eventId, directory);
                return eventId;
            } catch (final IOException ioe) {
                logger.warn("Unable to search Index Directory {}. Will assume that the index is incomplete and not consider this index when determining max event ID", directory, ioe);
            } finally {
                indexManager.returnIndexSearcher(searcher);
            }
        }

        return -1L;
    }

    @Override
    public void reindexEvents(final Map<ProvenanceEventRecord, StorageSummary> events) {
        final EventIndexTask indexTask = new EventIndexTask(documentQueue, config, indexManager, directoryManager, EventIndexTask.DEFAULT_MAX_EVENTS_PER_COMMIT, eventReporter);

        File lastIndexDir = null;
        long lastEventTime = -2L;

        final List<IndexableDocument> indexableDocs = new ArrayList<>(events.size());
        for (final Map.Entry<ProvenanceEventRecord, StorageSummary> entry : events.entrySet()) {
            final ProvenanceEventRecord event = entry.getKey();
            final StorageSummary summary = entry.getValue();

            for (final CachedQuery cachedQuery : cachedQueries) {
                cachedQuery.update(event, summary);
            }

            final Document document = eventConverter.convert(event, summary);
            if (document == null) {
                logger.debug("Received Provenance Event {} to index but it contained no information that should be indexed, so skipping it", event.getEventId());
            } else {
                final File indexDir;
                if (event.getEventTime() == lastEventTime) {
                    indexDir = lastIndexDir;
                } else {
                    final List<File> files = getDirectoryManager().getDirectories(event.getEventTime(), null);
                    indexDir = files.isEmpty() ? null : files.get(0);
                    lastIndexDir = indexDir;
                }

                final IndexableDocument doc = new IndexableDocument(document, summary, indexDir);
                indexableDocs.add(doc);
            }
        }

        try {
            indexTask.reIndex(indexableDocs, CommitPreference.PREVENT_COMMIT);
        } catch (final IOException ioe) {
            logger.error("Failed to reindex some Provenance Events", ioe);
            eventReporter.reportEvent(Severity.ERROR, EVENT_CATEGORY, "Failed to re-index some Provenance Events. "
                + "Some Provenance Events may not be available for querying. See logs for more information.");
        }
    }

    @Override
    public void commitChanges(final String partitionName) throws IOException {
        final Optional<File> indexDir = directoryManager.getActiveIndexDirectory(partitionName);
        if (indexDir.isPresent()) {
            final EventIndexWriter eventIndexWriter = indexManager.borrowIndexWriter(indexDir.get());
            try {
                eventIndexWriter.commit();
            } finally {
                indexManager.returnIndexWriter(eventIndexWriter, false, false);
            }
        }
    }

    protected void addEvent(final ProvenanceEventRecord event, final StorageSummary location) {
        for (final CachedQuery cachedQuery : cachedQueries) {
            cachedQuery.update(event, location);
        }

        final Document document = eventConverter.convert(event, location);
        if (document == null) {
            logger.debug("Received Provenance Event {} to index but it contained no information that should be indexed, so skipping it", event.getEventId());
        } else {
            final StoredDocument doc = new StoredDocument(document, location);
            boolean added = false;
            while (!added && !closed) {

                added = documentQueue.offer(doc);
                if (!added) {
                    final long start = System.nanoTime();
                    try {
                        added = documentQueue.offer(doc, 1, TimeUnit.SECONDS);
                    } catch (final InterruptedException e) {
                        Thread.currentThread().interrupt();
                        logger.warn("Interrupted while attempting to enqueue Provenance Event for indexing; this event will not be indexed");
                        return;
                    }
                    final long nanos = System.nanoTime() - start;
                    queuePauseNanos.add(new TimestampedLong(nanos));
                }

                if (added) {
                    final long totalEventCount = eventCount.incrementAndGet();
                    if (totalEventCount % 1_000_000 == 0 && logger.isDebugEnabled()) {
                        incrementAndReportStats();
                    }
                }
            }
        }
    }

    private void incrementAndReportStats() {
        final long fiveMinutesAgo = System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(5);
        final TimestampedLong nanosLastFive = queuePauseNanos.getAggregateValue(fiveMinutesAgo);
        if (nanosLastFive == null) {
            return;
        }

        final TimestampedLong eventsLast5 = eventsIndexed.getAggregateValue(fiveMinutesAgo);
        if (eventsLast5 == null) {
            return;
        }

        final long numEventsLast5 = eventsLast5.getValue();

        final long millis = TimeUnit.NANOSECONDS.toMillis(nanosLastFive.getValue());
        logger.debug("In the last 5 minutes, have spent {} CPU-millis waiting to enqueue events for indexing and have indexed {} events ({} since NiFi started)",
            millis, numEventsLast5, eventCount.get());
    }

    @Override
    public void addEvents(final Map<ProvenanceEventRecord, StorageSummary> events) {
        eventsIndexed.add(new TimestampedLong((long) events.size()));

        for (final Map.Entry<ProvenanceEventRecord, StorageSummary> entry : events.entrySet()) {
            addEvent(entry.getKey(), entry.getValue());
        }
    }


    @Override
    public ComputeLineageSubmission submitLineageComputation(final long eventId, final NiFiUser user, final EventAuthorizer eventAuthorizer) {
        final Optional<ProvenanceEventRecord> eventOption;
        try {
            eventOption = eventStore.getEvent(eventId);
        } catch (final Exception e) {
            logger.error("Failed to retrieve Provenance Event with ID " + eventId + " to calculate data lineage due to: " + e, e);
            final AsyncLineageSubmission result = new AsyncLineageSubmission(LineageComputationType.FLOWFILE_LINEAGE, eventId, Collections.emptySet(), 1, user.getIdentity());
            result.getResult().setError("Failed to retrieve Provenance Event with ID " + eventId + ". See logs for more information.");
            return result;
        }

        if (!eventOption.isPresent()) {
            final AsyncLineageSubmission result = new AsyncLineageSubmission(LineageComputationType.FLOWFILE_LINEAGE, eventId, Collections.emptySet(), 1, user.getIdentity());
            result.getResult().setError("Could not find Provenance Event with ID " + eventId);
            lineageSubmissionMap.put(result.getLineageIdentifier(), result);
            return result;
        }

        final ProvenanceEventRecord event = eventOption.get();
        return submitLineageComputation(Collections.singleton(event.getFlowFileUuid()), user, eventAuthorizer, LineageComputationType.FLOWFILE_LINEAGE,
            eventId, event.getLineageStartDate(), Long.MAX_VALUE);
    }


    private ComputeLineageSubmission submitLineageComputation(final Collection<String> flowFileUuids, final NiFiUser user, final EventAuthorizer eventAuthorizer,
        final LineageComputationType computationType, final Long eventId, final long startTimestamp, final long endTimestamp) {

        final List<File> indexDirs = directoryManager.getDirectories(startTimestamp, endTimestamp);
        final AsyncLineageSubmission submission = new AsyncLineageSubmission(computationType, eventId, flowFileUuids, indexDirs.size(), user.getIdentity());
        lineageSubmissionMap.put(submission.getLineageIdentifier(), submission);

        final BooleanQuery lineageQuery = buildLineageQuery(flowFileUuids);
        final List<File> indexDirectories = directoryManager.getDirectories(startTimestamp, endTimestamp);
        if (indexDirectories.isEmpty()) {
            submission.getResult().update(Collections.emptyList(), 0L);
        } else {
            Collections.sort(indexDirectories, DirectoryUtils.OLDEST_INDEX_FIRST);

            for (final File indexDir : indexDirectories) {
                queryExecutor.submit(new QueryTask(lineageQuery, submission.getResult(), MAX_LINEAGE_NODES, indexManager, indexDir,
                    eventStore, eventAuthorizer, EventTransformer.PLACEHOLDER_TRANSFORMER));
            }
        }

        // Some computations will complete very quickly. In this case, we don't want to wait
        // for the client to submit a second query to obtain the result. Instead, we want to just
        // wait some short period of time for the computation to complete before returning the submission.
        try {
            submission.getResult().awaitCompletion(500, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException ie) {
            Thread.currentThread().interrupt();
        }

        return submission;
    }

    private BooleanQuery buildLineageQuery(final Collection<String> flowFileUuids) {
        // Create a query for all Events related to the FlowFiles of interest. We do this by adding all ID's as
        // "SHOULD" clauses and then setting the minimum required to 1.
        final BooleanQuery lineageQuery;
        if (flowFileUuids == null || flowFileUuids.isEmpty()) {
            lineageQuery = null;
        } else {
            lineageQuery = new BooleanQuery();
            for (final String flowFileUuid : flowFileUuids) {
                lineageQuery.add(new TermQuery(new Term(SearchableFields.FlowFileUUID.getSearchableFieldName(), flowFileUuid)), Occur.SHOULD);
            }
            lineageQuery.setMinimumNumberShouldMatch(1);
        }

        return lineageQuery;
    }

    @Override
    public QuerySubmission submitQuery(final Query query, final EventAuthorizer authorizer, final String userId) {
        validate(query);

        // Check if we have any cached queries first that can give us the answer
        for (final CachedQuery cachedQuery : cachedQueries) {
            final Optional<List<Long>> eventIdListOption = cachedQuery.evaluate(query);
            if (eventIdListOption.isPresent()) {
                final AsyncQuerySubmission submission = new AsyncQuerySubmission(query, 1, userId);
                querySubmissionMap.put(query.getIdentifier(), submission);

                final List<Long> eventIds = eventIdListOption.get();

                queryExecutor.submit(() -> {
                    List<ProvenanceEventRecord> events;
                    try {
                        events = eventStore.getEvents(eventIds, authorizer, EventTransformer.EMPTY_TRANSFORMER);
                        submission.getResult().update(events, eventIds.size());
                    } catch (final Exception e) {
                        submission.getResult().setError("Failed to retrieve Provenance Events from store; see logs for more details");
                        logger.error("Failed to retrieve Provenance Events from store", e);
                    }
                });

                // There are some queries that are optimized and will complete very quickly. As a result,
                // we don't want to wait for the client to issue a second request, so we will give the query
                // up to 500 milliseconds to complete before running.
                try {
                    submission.getResult().awaitCompletion(500, TimeUnit.MILLISECONDS);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                }

                return submission;
            }
        }

        final List<File> indexDirectories = directoryManager.getDirectories(
            query.getStartDate() == null ? null : query.getStartDate().getTime(),
            query.getEndDate() == null ? null : query.getEndDate().getTime());

        final AsyncQuerySubmission submission = new AsyncQuerySubmission(query, indexDirectories.size(), userId);
        querySubmissionMap.put(query.getIdentifier(), submission);

        final org.apache.lucene.search.Query luceneQuery = LuceneUtil.convertQuery(query);
        logger.debug("Submitting query {} with identifier {} against index directories {}", luceneQuery, query.getIdentifier(), indexDirectories);

        if (indexDirectories.isEmpty()) {
            submission.getResult().update(Collections.emptyList(), 0L);
        } else {
            Collections.sort(indexDirectories, DirectoryUtils.NEWEST_INDEX_FIRST);

            for (final File indexDir : indexDirectories) {
                queryExecutor.submit(new QueryTask(luceneQuery, submission.getResult(), query.getMaxResults(), indexManager, indexDir,
                    eventStore, authorizer, EventTransformer.EMPTY_TRANSFORMER));
            }
        }

        // There are some queries that are optimized and will complete very quickly. As a result,
        // we don't want to wait for the client to issue a second request, so we will give the query
        // up to 500 milliseconds to complete before running.
        try {
            submission.getResult().awaitCompletion(500, TimeUnit.MILLISECONDS);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        return submission;
    }


    @Override
    public ComputeLineageSubmission submitLineageComputation(final String flowFileUuid, final NiFiUser user, final EventAuthorizer eventAuthorizer) {
        return submitLineageComputation(Collections.singleton(flowFileUuid), user, eventAuthorizer, LineageComputationType.FLOWFILE_LINEAGE, null, 0L, Long.MAX_VALUE);
    }

    @Override
    public ComputeLineageSubmission submitExpandChildren(final long eventId, final NiFiUser user, final EventAuthorizer authorizer) {
        final String userId = user.getIdentity();

        try {
            final Optional<ProvenanceEventRecord> eventOption = eventStore.getEvent(eventId);
            if (!eventOption.isPresent()) {
                final AsyncLineageSubmission submission = new AsyncLineageSubmission(LineageComputationType.EXPAND_CHILDREN, eventId, Collections.emptyList(), 1, userId);
                lineageSubmissionMap.put(submission.getLineageIdentifier(), submission);
                submission.getResult().update(Collections.emptyList(), 0L);
                return submission;
            }

            final ProvenanceEventRecord event = eventOption.get();
            switch (event.getEventType()) {
                case CLONE:
                case FORK:
                case JOIN:
                case REPLAY: {
                    return submitLineageComputation(event.getChildUuids(), user, authorizer, LineageComputationType.EXPAND_CHILDREN,
                        eventId, event.getEventTime(), Long.MAX_VALUE);
                }
                default: {
                    final AsyncLineageSubmission submission = new AsyncLineageSubmission(LineageComputationType.EXPAND_CHILDREN,
                        eventId, Collections.emptyList(), 1, userId);

                    lineageSubmissionMap.put(submission.getLineageIdentifier(), submission);
                    submission.getResult().setError("Event ID " + eventId + " indicates an event of type " + event.getEventType() + " so its children cannot be expanded");
                    return submission;
                }
            }
        } catch (final Exception e) {
            final AsyncLineageSubmission submission = new AsyncLineageSubmission(LineageComputationType.EXPAND_CHILDREN,
                eventId, Collections.emptyList(), 1, userId);
            lineageSubmissionMap.put(submission.getLineageIdentifier(), submission);
            submission.getResult().setError("Failed to expand children for lineage of event with ID " + eventId + " due to: " + e);
            return submission;
        }
    }

    @Override
    public ComputeLineageSubmission submitExpandParents(final long eventId, final NiFiUser user, final EventAuthorizer authorizer) {
        final String userId = user.getIdentity();

        try {
            final Optional<ProvenanceEventRecord> eventOption = eventStore.getEvent(eventId);
            if (!eventOption.isPresent()) {
                final AsyncLineageSubmission submission = new AsyncLineageSubmission(LineageComputationType.EXPAND_PARENTS, eventId, Collections.emptyList(), 1, userId);
                lineageSubmissionMap.put(submission.getLineageIdentifier(), submission);
                submission.getResult().update(Collections.emptyList(), 0L);
                return submission;
            }

            final ProvenanceEventRecord event = eventOption.get();
            switch (event.getEventType()) {
                case JOIN:
                case FORK:
                case CLONE:
                case REPLAY: {
                    return submitLineageComputation(event.getParentUuids(), user, authorizer, LineageComputationType.EXPAND_PARENTS,
                        eventId, event.getLineageStartDate(), event.getEventTime());
                }
                default: {
                    final AsyncLineageSubmission submission = new AsyncLineageSubmission(LineageComputationType.EXPAND_PARENTS,
                        eventId, Collections.emptyList(), 1, userId);

                    lineageSubmissionMap.put(submission.getLineageIdentifier(), submission);
                    submission.getResult().setError("Event ID " + eventId + " indicates an event of type " + event.getEventType() + " so its parents cannot be expanded");
                    return submission;
                }
            }
        } catch (final Exception e) {
            final AsyncLineageSubmission submission = new AsyncLineageSubmission(LineageComputationType.EXPAND_PARENTS,
                eventId, Collections.emptyList(), 1, userId);
            lineageSubmissionMap.put(submission.getLineageIdentifier(), submission);

            submission.getResult().setError("Failed to expand parents for lineage of event with ID " + eventId + " due to: " + e);
            return submission;
        }
    }

    @Override
    public AsyncLineageSubmission retrieveLineageSubmission(final String lineageIdentifier, final NiFiUser user) {
        final AsyncLineageSubmission submission = lineageSubmissionMap.get(lineageIdentifier);
        final String userId = submission.getSubmitterIdentity();

        if (user == null && userId == null) {
            return submission;
        }

        if (user == null) {
            throw new AccessDeniedException("Cannot retrieve Provenance Lineage Submission because no user id was provided");
        }

        if (userId == null || userId.equals(user.getIdentity())) {
            return submission;
        }

        throw new AccessDeniedException("Cannot retrieve Provenance Lineage Submission because " + user.getIdentity() + " is not the user who submitted the request");
    }

    @Override
    public QuerySubmission retrieveQuerySubmission(final String queryIdentifier, final NiFiUser user) {
        final QuerySubmission submission = querySubmissionMap.get(queryIdentifier);

        final String userId = submission.getSubmitterIdentity();

        if (user == null && userId == null) {
            return submission;
        }

        if (user == null) {
            throw new AccessDeniedException("Cannot retrieve Provenance Query Submission because no user id was provided");
        }

        if (userId == null || userId.equals(user.getIdentity())) {
            return submission;
        }

        throw new AccessDeniedException("Cannot retrieve Provenance Query Submission because " + user.getIdentity() + " is not the user who submitted the request");
    }

    @Override
    public long getSize() {
        long total = 0;
        for (final File file : directoryManager.getDirectories(null, null)) {
            total += DirectoryUtils.getSize(file);
        }
        return total;
    }

    private void validate(final Query query) {
        final int numQueries = querySubmissionMap.size();
        if (numQueries > MAX_UNDELETED_QUERY_RESULTS) {
            purgeObsoleteQueries();
            if (querySubmissionMap.size() > MAX_UNDELETED_QUERY_RESULTS) {
                throw new IllegalStateException("Cannot process query because there are currently " + numQueries + " queries whose results have not "
                    + "been deleted due to poorly behaving clients not issuing DELETE requests. Please try again later.");
            }
        }

        if (query.getEndDate() != null && query.getStartDate() != null && query.getStartDate().getTime() > query.getEndDate().getTime()) {
            throw new IllegalArgumentException("Query End Time cannot be before Query Start Time");
        }
    }

    void performMaintenance() {
        try {
            final List<ProvenanceEventRecord> firstEvents = eventStore.getEvents(0, 1);

            final long earliestEventTime;
            if (firstEvents.isEmpty()) {
                earliestEventTime = System.currentTimeMillis();
                logger.debug("Found no events in the Provenance Repository. In order to perform maintenace of the indices, "
                    + "will assume that the first event time is now ({})", System.currentTimeMillis());
            } else {
                final ProvenanceEventRecord firstEvent = firstEvents.get(0);
                earliestEventTime = firstEvent.getEventTime();
                logger.debug("First Event Time is {} ({}) with Event ID {}; will delete any Lucene Index that is older than this",
                    earliestEventTime, new Date(earliestEventTime), firstEvent.getEventId());
            }

            final List<File> indicesBeforeEarliestEvent = directoryManager.getDirectoriesBefore(earliestEventTime);

            for (final File index : indicesBeforeEarliestEvent) {
                logger.debug("Index directory {} is now expired. Attempting to remove index", index);
                tryDeleteIndex(index);
            }
        } catch (final Exception e) {
            logger.error("Failed to perform background maintenance procedures", e);
            eventReporter.reportEvent(Severity.ERROR, EVENT_CATEGORY, "Failed to perform maintenance of Provenance Repository. See logs for more information.");
        }
    }

    protected boolean tryDeleteIndex(final File indexDirectory) {
        final long startNanos = System.nanoTime();
        boolean removed = false;
        while (!removed && System.nanoTime() - startNanos < TimeUnit.SECONDS.toNanos(MAX_DELETE_INDEX_WAIT_SECONDS)) {
            removed = indexManager.removeIndex(indexDirectory);

            if (!removed) {
                try {
                    Thread.sleep(5000L);
                } catch (final InterruptedException ie) {
                    logger.debug("Interrupted when trying to remove index {} from IndexManager; will not remove index", indexDirectory);
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
        }

        if (removed) {
            try {
                FileUtils.deleteFile(indexDirectory, true);
                logger.debug("Successfully deleted directory {}", indexDirectory);
            } catch (final IOException e) {
                logger.warn("The Lucene Index located at " + indexDirectory + " has expired and contains no Provenance Events that still exist in the respository. "
                    + "However, the directory could not be deleted.", e);
            }

            directoryManager.deleteDirectory(indexDirectory);
            logger.info("Successfully removed expired Lucene Index {}", indexDirectory);
        } else {
            logger.warn("The Lucene Index located at {} has expired and contains no Provenance Events that still exist in the respository. "
                + "However, the directory could not be deleted because it is still actively being used. Will continue to try to delete "
                + "in a subsequent maintenance cycle.", indexDirectory);
        }

        return removed;
    }

    private void purgeObsoleteQueries() {
        try {
            final Date now = new Date();

            final Iterator<Map.Entry<String, AsyncQuerySubmission>> queryIterator = querySubmissionMap.entrySet().iterator();
            while (queryIterator.hasNext()) {
                final Map.Entry<String, AsyncQuerySubmission> entry = queryIterator.next();

                final StandardQueryResult result = entry.getValue().getResult();
                if (entry.getValue().isCanceled() || result.isFinished() && result.getExpiration().before(now)) {
                    queryIterator.remove();
                }
            }

            final Iterator<Map.Entry<String, AsyncLineageSubmission>> lineageIterator = lineageSubmissionMap.entrySet().iterator();
            while (lineageIterator.hasNext()) {
                final Map.Entry<String, AsyncLineageSubmission> entry = lineageIterator.next();

                final StandardLineageResult result = entry.getValue().getResult();
                if (entry.getValue().isCanceled() || result.isFinished() && result.getExpiration().before(now)) {
                    lineageIterator.remove();
                }
            }
        } catch (final Exception e) {
            logger.error("Failed to expire Provenance Query Results due to {}", e.toString());
            logger.error("", e);
        }
    }
}
