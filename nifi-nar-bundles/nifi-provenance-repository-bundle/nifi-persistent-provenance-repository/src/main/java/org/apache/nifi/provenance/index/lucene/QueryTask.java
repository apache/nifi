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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.nifi.provenance.ProgressiveResult;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.SearchableFields;
import org.apache.nifi.provenance.authorization.EventAuthorizer;
import org.apache.nifi.provenance.authorization.EventTransformer;
import org.apache.nifi.provenance.index.EventIndexSearcher;
import org.apache.nifi.provenance.index.SearchFailedException;
import org.apache.nifi.provenance.lucene.IndexManager;
import org.apache.nifi.provenance.store.EventStore;
import org.apache.nifi.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryTask implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(QueryTask.class);
    private static final Set<String> LUCENE_FIELDS_TO_LOAD = Collections.singleton(SearchableFields.Identifier.getSearchableFieldName());

    private final Query query;
    private final ProgressiveResult queryResult;
    private final int maxResults;
    private final IndexManager indexManager;
    private final File indexDir;
    private final EventStore eventStore;
    private final EventAuthorizer authorizer;
    private final EventTransformer transformer;

    public QueryTask(final Query query, final ProgressiveResult result, final int maxResults, final IndexManager indexManager,
        final File indexDir, final EventStore eventStore, final EventAuthorizer authorizer,
        final EventTransformer unauthorizedTransformer) {
        this.query = query;
        this.queryResult = result;
        this.maxResults = maxResults;
        this.indexManager = indexManager;
        this.indexDir = indexDir;
        this.eventStore = eventStore;
        this.authorizer = authorizer;
        this.transformer = unauthorizedTransformer;
    }

    @Override
    public void run() {
        if (queryResult.getTotalHitCount() >= maxResults) {
            logger.debug("Will not query lucene index {} because maximum results have already been obtained", indexDir);
            queryResult.update(Collections.emptyList(), 0L);
            return;
        }

        if (queryResult.isFinished()) {
            logger.debug("Will not query lucene index {} because the query is already finished", indexDir);
            return;
        }


        final long borrowStart = System.nanoTime();
        final EventIndexSearcher searcher;
        try {
            searcher = indexManager.borrowIndexSearcher(indexDir);
        } catch (final FileNotFoundException fnfe) {
            // We do not consider this an error because it may well just be the case that the event index has aged off and
            // been deleted or that we've just created the index and haven't yet committed the writer. So instead, we just
            // update the result ot indicate that this index search is complete with no results.
            queryResult.update(Collections.emptyList(), 0);

            // nothing has been indexed yet, or the data has already aged off
            logger.info("Attempted to search Provenance Index {} but could not find the directory or the directory did not contain a valid Lucene index. "
                + "This usually indicates that either the index was just created and hasn't fully been initialized, or that the index was recently aged off.", indexDir);
            return;
        } catch (final IOException ioe) {
            queryResult.setError("Failed to query index " + indexDir + "; see logs for more details");
            logger.error("Failed to query index " + indexDir, ioe);
            return;
        }

        try {
            final long borrowMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - borrowStart);
            logger.debug("Borrowing index searcher for {} took {} ms", indexDir, borrowMillis);
            final long startNanos = System.nanoTime();

            // If max number of results are retrieved, do not bother querying lucene
            if (queryResult.getTotalHitCount() >= maxResults) {
                logger.debug("Will not query lucene index {} because maximum results have already been obtained", indexDir);
                queryResult.update(Collections.emptyList(), 0L);
                return;
            }

            if (queryResult.isFinished()) {
                logger.debug("Will not query lucene index {} because the query is already finished", indexDir);
                return;
            }

            // Query lucene
            final IndexReader indexReader = searcher.getIndexSearcher().getIndexReader();
            final TopDocs topDocs;
            try {
                topDocs = searcher.getIndexSearcher().search(query, maxResults);
            } catch (final Exception e) {
                logger.error("Failed to query Lucene for index " + indexDir, e);
                queryResult.setError("Failed to query Lucene for index " + indexDir + " due to " + e);
                return;
            } finally {
                final long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
                logger.debug("Querying Lucene for index {} took {} ms", indexDir, ms);
            }

            // If max number of results are retrieved, do not bother reading docs
            if (queryResult.getTotalHitCount() >= maxResults) {
                logger.debug("Will not read events from store for {} because maximum results have already been obtained", indexDir);
                queryResult.update(Collections.emptyList(), 0L);
                return;
            }

            if (queryResult.isFinished()) {
                logger.debug("Will not read events from store for {} because the query has already finished", indexDir);
                return;
            }

            final Tuple<List<ProvenanceEventRecord>, Integer> eventsAndTotalHits = readDocuments(topDocs, indexReader);

            if (eventsAndTotalHits == null) {
                queryResult.update(Collections.emptyList(), 0L);
                logger.info("Will not update query results for queried index {} for query {} because the maximum number of results have been reached already",
                    indexDir, query);
            } else {
                queryResult.update(eventsAndTotalHits.getKey(), eventsAndTotalHits.getValue());

                final long searchNanos = System.nanoTime() - startNanos;
                final long millis = TimeUnit.NANOSECONDS.toMillis(searchNanos);
                logger.info("Successfully queried index {} for query {}; retrieved {} events with a total of {} hits in {} millis",
                    indexDir, query, eventsAndTotalHits.getKey().size(), eventsAndTotalHits.getValue(), millis);
            }
        } catch (final Exception e) {
            logger.error("Failed to query events against index " + indexDir, e);
            queryResult.setError("Failed to complete query due to " + e);
        } finally {
            indexManager.returnIndexSearcher(searcher);
        }
    }

    private Tuple<List<ProvenanceEventRecord>, Integer> readDocuments(final TopDocs topDocs, final IndexReader indexReader) {
        // If no topDocs is supplied, just provide a Tuple that has no records and a hit count of 0.
        if (topDocs == null || topDocs.totalHits == 0) {
            return new Tuple<>(Collections.<ProvenanceEventRecord> emptyList(), 0);
        }

        final long start = System.nanoTime();
        final List<Long> eventIds = Arrays.stream(topDocs.scoreDocs)
            .mapToInt(scoreDoc -> scoreDoc.doc)
            .mapToObj(docId -> {
                try {
                    return indexReader.document(docId, LUCENE_FIELDS_TO_LOAD);
                } catch (final Exception e) {
                    throw new SearchFailedException("Failed to read Provenance Events from Event File", e);
                }
            })
            .map(doc -> doc.getField(SearchableFields.Identifier.getSearchableFieldName()).numericValue().longValue())
            .collect(Collectors.toList());

        final long endConvert = System.nanoTime();
        final long ms = TimeUnit.NANOSECONDS.toMillis(endConvert - start);
        logger.debug("Converting documents took {} ms", ms);

        List<ProvenanceEventRecord> events;
        try {
            events = eventStore.getEvents(eventIds, authorizer, transformer);
        } catch (IOException e) {
            throw new SearchFailedException("Unable to retrieve events from the Provenance Store", e);
        }

        final long fetchEventNanos = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - endConvert);
        logger.debug("Fetching {} events from Event Store took {} ms ({} events actually fetched)", eventIds.size(), fetchEventNanos, events.size());

        final int totalHits = topDocs.totalHits;
        return new Tuple<>(events, totalHits);
    }

}
