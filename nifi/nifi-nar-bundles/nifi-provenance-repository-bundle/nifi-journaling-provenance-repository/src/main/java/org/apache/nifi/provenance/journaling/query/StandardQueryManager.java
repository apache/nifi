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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.nifi.provenance.AsyncQuerySubmission;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.StoredProvenanceEvent;
import org.apache.nifi.provenance.journaling.JournaledProvenanceEvent;
import org.apache.nifi.provenance.journaling.JournaledStorageLocation;
import org.apache.nifi.provenance.journaling.config.JournalingRepositoryConfig;
import org.apache.nifi.provenance.journaling.index.EventIndexSearcher;
import org.apache.nifi.provenance.journaling.index.QueryUtils;
import org.apache.nifi.provenance.journaling.index.SearchResult;
import org.apache.nifi.provenance.journaling.journals.JournalReader;
import org.apache.nifi.provenance.journaling.journals.StandardJournalReader;
import org.apache.nifi.provenance.journaling.partition.Partition;
import org.apache.nifi.provenance.journaling.partition.PartitionManager;
import org.apache.nifi.provenance.journaling.partition.VoidPartitionAction;
import org.apache.nifi.provenance.journaling.toc.StandardTocReader;
import org.apache.nifi.provenance.journaling.toc.TocReader;
import org.apache.nifi.provenance.search.Query;
import org.apache.nifi.provenance.search.QuerySubmission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StandardQueryManager implements QueryManager {
    private static final Logger logger = LoggerFactory.getLogger(StandardQueryManager.class);
    
    private final int maxConcurrentQueries;
    private final JournalingRepositoryConfig config;
    private final PartitionManager partitionManager;
    private final ConcurrentMap<String, AsyncQuerySubmission> querySubmissionMap = new ConcurrentHashMap<>();
    
    public StandardQueryManager(final PartitionManager partitionManager, final JournalingRepositoryConfig config, final int maxConcurrentQueries) {
        this.config = config;
        this.maxConcurrentQueries = maxConcurrentQueries;
        this.partitionManager = partitionManager;
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

            querySubmissionMap.put(query.getIdentifier(), result);
            return result;
        }

        final AtomicInteger retrievalCount = new AtomicInteger(query.getMaxResults());
        final AsyncQuerySubmission submission = new AsyncQuerySubmission(query, config.getPartitionCount()) {
            @Override
            public void cancel() {
                super.cancel();
                querySubmissionMap.remove(query.getIdentifier());
            }
        };
        
        querySubmissionMap.put(query.getIdentifier(), submission);

        partitionManager.withEachPartition(new VoidPartitionAction() {
            @SuppressWarnings({ "rawtypes", "unchecked" })
            @Override
            public void perform(final Partition partition) throws IOException {
                logger.debug("Running {} against {}", query, partition);
                
                try (final EventIndexSearcher searcher = partition.newIndexSearcher()) {
                    final SearchResult searchResult = searcher.search(query);
                    logger.debug("{} has {} hits against {} over {} files", query, searchResult.getTotalCount(), partition, searchResult.getLocations().size());
                    
                    final List<StoredProvenanceEvent> matchingRecords = new ArrayList<>();
                    final Map<File, List<JournaledStorageLocation>> locationMap = QueryUtils.orderLocations((List) searchResult.getLocations(), config);
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
                    
                    logger.debug("Finished executing {} against {}", query, partition);
                    submission.getResult().update(matchingRecords, searchResult.getTotalCount());
                } catch (final Exception e) {
                    submission.getResult().setError("Failed to query " + partition + " due to " + e.toString());
                    throw e;
                }
            }
        }, true);
        
        return submission;
    }

    @Override
    public QuerySubmission retrieveQuerySubmission(final String queryIdentifier) {
        return querySubmissionMap.get(queryIdentifier);
    }
}
