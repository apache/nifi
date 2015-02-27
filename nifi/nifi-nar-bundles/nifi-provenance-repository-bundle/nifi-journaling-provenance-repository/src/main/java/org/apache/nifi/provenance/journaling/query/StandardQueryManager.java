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
package org.apache.nifi.provenance.journaling.query;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.nifi.provenance.AsyncLineageSubmission;
import org.apache.nifi.provenance.AsyncQuerySubmission;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.ProvenanceEventRepository;
import org.apache.nifi.provenance.StoredProvenanceEvent;
import org.apache.nifi.provenance.journaling.JournaledProvenanceEvent;
import org.apache.nifi.provenance.journaling.JournaledStorageLocation;
import org.apache.nifi.provenance.journaling.config.JournalingRepositoryConfig;
import org.apache.nifi.provenance.journaling.index.EventIndexSearcher;
import org.apache.nifi.provenance.journaling.index.IndexAction;
import org.apache.nifi.provenance.journaling.index.IndexManager;
import org.apache.nifi.provenance.journaling.index.QueryUtils;
import org.apache.nifi.provenance.journaling.index.SearchResult;
import org.apache.nifi.provenance.journaling.index.VoidIndexAction;
import org.apache.nifi.provenance.journaling.journals.JournalReader;
import org.apache.nifi.provenance.journaling.journals.StandardJournalReader;
import org.apache.nifi.provenance.journaling.toc.StandardTocReader;
import org.apache.nifi.provenance.journaling.toc.TocReader;
import org.apache.nifi.provenance.lineage.ComputeLineageSubmission;
import org.apache.nifi.provenance.lineage.LineageComputationType;
import org.apache.nifi.provenance.search.Query;
import org.apache.nifi.provenance.search.QuerySubmission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StandardQueryManager implements QueryManager {
    private static final Logger logger = LoggerFactory.getLogger(StandardQueryManager.class);
    
    private final int maxConcurrentQueries;
    private final IndexManager indexManager;
    private final ExecutorService executor;
    private final JournalingRepositoryConfig config;
    private final ConcurrentMap<String, AsyncQuerySubmission> querySubmissionMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, AsyncLineageSubmission> lineageSubmissionMap = new ConcurrentHashMap<>();
    
    public StandardQueryManager(final IndexManager indexManager, final ExecutorService executor, final JournalingRepositoryConfig config, final int maxConcurrentQueries) {
        this.config = config;
        this.maxConcurrentQueries = maxConcurrentQueries;
        this.indexManager = indexManager;
        this.executor = executor;
    }
    
    @Override
    public QuerySubmission submitQuery(final Query query) {
        final int numQueries = querySubmissionMap.size();
        if (numQueries > maxConcurrentQueries) {
            throw new IllegalStateException("Cannot process query because there are currently " + numQueries + " queries whose results have not been deleted (likely due to poorly behaving clients not issuing DELETE requests). Please try again later.");
        }

        if (query.getEndDate() != null && query.getStartDate() != null && query.getStartDate().getTime() > query.getEndDate().getTime()) {
            throw new IllegalArgumentException("Query End Time cannot be before Query Start Time");
        }

        if (query.getSearchTerms().isEmpty() && query.getStartDate() == null && query.getEndDate() == null) {
            final AsyncQuerySubmission result = new AsyncQuerySubmission(query, 1);

            // empty query. Just get the latest events.
            final Runnable runnable = new Runnable() {
                @Override
                public void run() {
                    try {
                        logger.debug("Fetching latest events from Provenance repo");
                        final long indexStartNanos = System.nanoTime();
                        
                        // Query each index for the latest events.
                        final Set<List<JournaledStorageLocation>> locationSet = indexManager.withEachIndex(new IndexAction<List<JournaledStorageLocation>>() {
                            @Override
                            public List<JournaledStorageLocation> perform(final EventIndexSearcher searcher) throws IOException {
                                return searcher.getLatestEvents(query.getMaxResults());
                            }
                        });
                        final long indexMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - indexStartNanos);
                        final long retrievalStartNanos = System.nanoTime();
                        
                        final List<JournaledStorageLocation> orderedLocations = new ArrayList<>();
                        for ( final List<JournaledStorageLocation> locations : locationSet ) {
                            orderedLocations.addAll(locations);
                        }
                        
                        Collections.sort(orderedLocations, new Comparator<JournaledStorageLocation>() {
                            @Override
                            public int compare(final JournaledStorageLocation o1, final JournaledStorageLocation o2) {
                                return Long.compare(o1.getEventId(), o2.getEventId());
                            }
                        });

                        final List<JournaledStorageLocation> locationsToKeep;
                        if ( orderedLocations.size() > query.getMaxResults() ) {
                            locationsToKeep = orderedLocations.subList(0, query.getMaxResults());
                        } else {
                            locationsToKeep = orderedLocations;
                        }
                        
                        final List<StoredProvenanceEvent> matchingRecords = getEvents(locationsToKeep, new AtomicInteger(locationsToKeep.size()));
                        
                        final long totalNumEvents = indexManager.getNumberOfEvents();
                        final long retrievalMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - retrievalStartNanos);
                        logger.debug("Updated query result with {} matching records; total number of events = {}; index search took {} millis, event retrieval took {} millis", matchingRecords.size(), totalNumEvents, indexMillis, retrievalMillis);
                        result.getResult().update(matchingRecords, totalNumEvents);
                    } catch (final Exception e) {
                        result.getResult().setError("Failed to obtain latest events in repository due to " + e);
                        logger.error("Failed to obtain latest events in repository due to {}", e.toString());
                        if ( logger.isDebugEnabled() ) {
                            logger.error("", e);
                        }
                    }
                }
            };
            
            executor.submit(runnable);
            querySubmissionMap.put(query.getIdentifier(), result);
            return result;
        }

        final AtomicInteger retrievalCount = new AtomicInteger(query.getMaxResults());
        final AsyncQuerySubmission submission = new AsyncQuerySubmission(query, indexManager.getNumberOfIndices()) {
            @Override
            public void cancel() {
                super.cancel();
                querySubmissionMap.remove(query.getIdentifier());
            }
        };
        
        querySubmissionMap.put(query.getIdentifier(), submission);

        try {
            indexManager.withEachIndex(new VoidIndexAction() {
                @Override
                public void perform(final EventIndexSearcher searcher) throws IOException {
                    try {
                        logger.debug("Running {} against {}", query, searcher);
                        
                        final long indexStart = System.nanoTime();
                        final SearchResult searchResult = searcher.search(query);
                        final long indexMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - indexStart);
                        logger.debug("{} has {} hits against {} over {} files", query, searchResult.getTotalCount(), searcher, searchResult.getLocations().size());
                        
                        final long retrievalStart = System.nanoTime();
                        final List<StoredProvenanceEvent> matchingRecords = getEvents(searchResult.getLocations(), retrievalCount);
                        final long retrievalMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - retrievalStart);
                        
                        logger.debug("Finished executing {} against {}; found {} total matches, retrieved {} of them; index search took {} millis, record retrieval took {} millis", 
                                query, searcher, searchResult.getTotalCount(), matchingRecords.size(), indexMillis, retrievalMillis);
                        submission.getResult().update(matchingRecords, searchResult.getTotalCount());
                    } catch (final Throwable t) {
                        submission.getResult().setError("Failed to execute query " + query + " against " + searcher + " due to " + t);
                        throw t;
                    }
                }
            }, true);
        } catch (final IOException ioe) {
            // only set the error here if it's not already set because we have the least amount of information here
            if ( submission.getResult().getError() == null ) {
                submission.getResult().setError("Failed to execute query " + query + " due to " + ioe);
            }
        }
        
        return submission;
    }

    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private List<StoredProvenanceEvent> getEvents(final List<JournaledStorageLocation> allLocations, final AtomicInteger retrievalCount) throws IOException {
        final List<StoredProvenanceEvent> matchingRecords = new ArrayList<>();
        final Map<File, List<JournaledStorageLocation>> locationMap = QueryUtils.orderLocations((List) allLocations, config);
        for ( final Map.Entry<File, List<JournaledStorageLocation>> entry : locationMap.entrySet() ) {
            final File journalFile = entry.getKey();
            final List<JournaledStorageLocation> locations = entry.getValue();
            
            if ( retrievalCount.get() <= 0 ) {
                break;
            }
            
            try (final JournalReader reader = new StandardJournalReader(journalFile);
                 final TocReader tocReader = new StandardTocReader(QueryUtils.getTocFile(journalFile))) {
                
                for ( final JournaledStorageLocation location : locations ) {
                    final long blockOffset = tocReader.getBlockOffset(location.getBlockIndex());
                    final ProvenanceEventRecord event = reader.getEvent(blockOffset, location.getEventId());
                    matchingRecords.add(new JournaledProvenanceEvent(event, location));
                    
                    final int recordsLeft = retrievalCount.decrementAndGet();
                    if ( recordsLeft <= 0 ) {
                        break;
                    }
                }
            }
        }
        
        return matchingRecords;
    }
    
    @Override
    public QuerySubmission retrieveQuerySubmission(final String queryIdentifier) {
        return querySubmissionMap.get(queryIdentifier);
    }
    
    @Override
    public ComputeLineageSubmission retrieveLineageSubmission(final String lineageIdentifier) {
        return lineageSubmissionMap.get(lineageIdentifier);
    }
    
    @Override
    public ComputeLineageSubmission submitLineageComputation(final String flowFileUuid) {
        return submitLineageComputation(Collections.singleton(flowFileUuid), LineageComputationType.FLOWFILE_LINEAGE, null, 0L, Long.MAX_VALUE);
    }
    
    private AsyncLineageSubmission submitLineageComputation(final Collection<String> flowFileUuids, final LineageComputationType computationType, final Long eventId, final long startTimestamp, final long endTimestamp) {
        final AsyncLineageSubmission lineageSubmission = new AsyncLineageSubmission(computationType, eventId, flowFileUuids, indexManager.getNumberOfIndices());

        final AtomicInteger retrievalCount = new AtomicInteger(2000);
        final Runnable runnable = new Runnable() {
            @Override
            public void run() {
                try {
                    indexManager.withEachIndex(new VoidIndexAction() {
                        @Override
                        public void perform(EventIndexSearcher searcher) throws IOException {
                            logger.debug("Obtaining lineage events for FlowFile UUIDs {} for {}", flowFileUuids, searcher);
                            final long startNanos = System.nanoTime();
                            
                            final List<JournaledStorageLocation> locations = searcher.getEventsForFlowFiles(flowFileUuids, startTimestamp, endTimestamp);
                            final List<StoredProvenanceEvent> matchingRecords = getEvents(locations, retrievalCount);
                            lineageSubmission.getResult().update(matchingRecords);
                            
                            final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                            logger.debug("Finished querying for lineage events; found {} events in {} millis", matchingRecords.size(), millis);
                        }
                    });
                } catch (final IOException ioe) {
                    lineageSubmission.getResult().setError("Failed to calculate FlowFile Lineage due to " + ioe);
                    logger.error("Failed to calculate FlowFile Lineage due to {}", ioe.toString());
                    if ( logger.isDebugEnabled() ) {
                        logger.error("", ioe);
                    }
                }
            }
        };
        
        executor.submit(runnable);
        lineageSubmissionMap.putIfAbsent(lineageSubmission.getLineageIdentifier(), lineageSubmission);
        return lineageSubmission;
    }

    
    @Override
    public ComputeLineageSubmission submitExpandChildren(final ProvenanceEventRepository eventRepo, final long eventId) {
        final Set<String> flowFileUuids = Collections.synchronizedSet(new HashSet<String>());
        final AsyncLineageSubmission lineageSubmission = new AsyncLineageSubmission(LineageComputationType.EXPAND_CHILDREN, eventId, flowFileUuids, indexManager.getNumberOfIndices());

        final Runnable runnable = new Runnable() {
            @Override
            public void run() {
                try {
                    logger.debug("Obtaining event with id {} in order to expand children", eventId);
                    final StoredProvenanceEvent event = eventRepo.getEvent(eventId);
                    if ( event == null ) {
                        lineageSubmission.getResult().setError("Cannot expand children of event with ID " + eventId + " because that event cannot be found");
                        logger.warn("Cannot expand children of event with ID {} because that event cannot be found", eventId);
                        return;
                    }
                    
                    logger.debug("Found event with id {}; searching for children", eventId);
                    
                    switch (event.getEventType()) {
                        case CLONE:
                        case FORK:
                        case JOIN:
                        case REPLAY:
                            break;
                        default:
                            logger.warn("Cannot expand children of event with ID {} because event type is {}", eventId, event.getEventType());
                            lineageSubmission.getResult().setError("Cannot expand children of event with ID " + eventId + 
                                    " because that event is of type " + event.getEventType() + 
                                    ", and that type does not support expansion of children");
                            return;
                    }
                    
                    final List<String> childUuids = event.getChildUuids();
                    flowFileUuids.addAll(childUuids);
                    
                    final AtomicInteger retrievalCount = new AtomicInteger(100);
                    indexManager.withEachIndex(new VoidIndexAction() {
                        @Override
                        public void perform(EventIndexSearcher searcher) throws IOException {
                            final long startNanos = System.nanoTime();
                            logger.debug("Finding children of event with id {} using {}", eventId, searcher);
                            
                            final List<JournaledStorageLocation> locations = searcher.getEventsForFlowFiles(flowFileUuids, event.getEventTime(), Long.MAX_VALUE);
                            final List<StoredProvenanceEvent> matchingRecords = getEvents(locations, retrievalCount);
                            lineageSubmission.getResult().update(matchingRecords);
                            
                            final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                            logger.debug("Found {} children of event {} in {} millis", matchingRecords.size(), eventId, millis);
                        }
                    });
                } catch (final IOException ioe) {
                    
                }
            }
        };
        
        executor.submit(runnable);
        lineageSubmissionMap.putIfAbsent(lineageSubmission.getLineageIdentifier(), lineageSubmission);
        return lineageSubmission;
    }
    
    @Override
    public ComputeLineageSubmission submitExpandParents(final ProvenanceEventRepository eventRepo, final long eventId) {
        final Set<String> flowFileUuids = Collections.synchronizedSet(new HashSet<String>());
        final AsyncLineageSubmission lineageSubmission = new AsyncLineageSubmission(LineageComputationType.EXPAND_PARENTS, eventId, flowFileUuids, indexManager.getNumberOfIndices());

        final Runnable runnable = new Runnable() {
            @Override
            public void run() {
                try {
                    logger.debug("Obtaining event with id {} in order to expand children", eventId);
                    final StoredProvenanceEvent event = eventRepo.getEvent(eventId);
                    if ( event == null ) {
                        logger.warn("Cannot expand children of event with ID {} because that event cannot be found", eventId);
                        lineageSubmission.getResult().setError("Cannot expand children of event with ID " + eventId + " because that event cannot be found");
                        return;
                    }
                    
                    logger.debug("Found event with id {}; searching for children", eventId);
                    
                    switch (event.getEventType()) {
                        case CLONE:
                        case FORK:
                        case JOIN:
                        case REPLAY:
                            break;
                        default:
                            logger.warn("Cannot expand parents of event with ID {} because event type is {}", eventId, event.getEventType());
                            lineageSubmission.getResult().setError("Cannot expand parents of event with ID " + eventId + 
                                    " because that event is of type " + event.getEventType() + 
                                    ", and that type does not support expansion of children");
                            return;
                    }
                    
                    final List<String> parentUuids = event.getParentUuids();
                    flowFileUuids.addAll(parentUuids);
                    
                    final AtomicInteger retrievalCount = new AtomicInteger(100);
                    indexManager.withEachIndex(new VoidIndexAction() {
                        @Override
                        public void perform(EventIndexSearcher searcher) throws IOException {
                            final long startNanos = System.nanoTime();
                            logger.debug("Finding parents of event with id {} using {}", eventId, searcher);
                            
                            final List<JournaledStorageLocation> locations = searcher.getEventsForFlowFiles(flowFileUuids, event.getLineageStartDate(), event.getEventTime());
                            final List<StoredProvenanceEvent> matchingRecords = getEvents(locations, retrievalCount);
                            lineageSubmission.getResult().update(matchingRecords);
                            
                            final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                            logger.debug("Found {} parents of event {} in {} millis", matchingRecords.size(), eventId, millis);
                        }
                    });
                } catch (final IOException ioe) {
                    
                }
            }
        };
        
        executor.submit(runnable);
        lineageSubmissionMap.putIfAbsent(lineageSubmission.getLineageIdentifier(), lineageSubmission);
        return lineageSubmission;
    }
    
    
    @Override
    public void close() throws IOException {
    }
}
