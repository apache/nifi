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
package org.apache.nifi.provenance.lucene;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortField.Type;
import org.apache.lucene.search.TopDocs;
import org.apache.nifi.provenance.PersistentProvenanceRepository;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.SearchableFields;
import org.apache.nifi.provenance.StandardQueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexSearch {
    private final Logger logger = LoggerFactory.getLogger(IndexSearch.class);
    private final PersistentProvenanceRepository repository;
    private final File indexDirectory;
    private final IndexManager indexManager;
    private final int maxAttributeChars;

    public IndexSearch(final PersistentProvenanceRepository repo, final File indexDirectory, final IndexManager indexManager, final int maxAttributeChars) {
        this.repository = repo;
        this.indexDirectory = indexDirectory;
        this.indexManager = indexManager;
        this.maxAttributeChars = maxAttributeChars;
    }

    public StandardQueryResult search(final org.apache.nifi.provenance.search.Query provenanceQuery, final long firstEventTimestamp) throws IOException {
        if (!indexDirectory.exists() && !indexDirectory.mkdirs()) {
            throw new IOException("Unable to create Indexing Directory " + indexDirectory);
        }
        if (!indexDirectory.isDirectory()) {
            throw new IOException("Indexing Directory specified is " + indexDirectory + ", but this is not a directory");
        }

        final StandardQueryResult sqr = new StandardQueryResult(provenanceQuery, 1);

        // we need to set the start date because if we do not, the first index may still have events that have aged off from
        // the repository, and we don't want those events to count toward the total number of matches.
        if (provenanceQuery.getStartDate() == null || provenanceQuery.getStartDate().getTime() < firstEventTimestamp) {
            provenanceQuery.setStartDate(new Date(firstEventTimestamp));
        }

        if (provenanceQuery.getEndDate() == null) {
            provenanceQuery.setEndDate(new Date());
        }
        final Query luceneQuery = LuceneUtil.convertQuery(provenanceQuery);

        final long start = System.nanoTime();
        IndexSearcher searcher = null;
        try {
            searcher = indexManager.borrowIndexSearcher(indexDirectory);
            final long searchStartNanos = System.nanoTime();
            final long openSearcherNanos = searchStartNanos - start;

            final Sort sort = new Sort(new SortField(SearchableFields.Identifier.getSearchableFieldName(), Type.LONG, true));
            final TopDocs topDocs = searcher.search(luceneQuery, provenanceQuery.getMaxResults(), sort);
            final long finishSearch = System.nanoTime();
            final long searchNanos = finishSearch - searchStartNanos;

            logger.debug("Searching {} took {} millis; opening searcher took {} millis", this,
                TimeUnit.NANOSECONDS.toMillis(searchNanos), TimeUnit.NANOSECONDS.toMillis(openSearcherNanos));

            if (topDocs.totalHits == 0) {
                sqr.update(Collections.<ProvenanceEventRecord> emptyList(), 0, 0);
                return sqr;
            }

            final DocsReader docsReader = new DocsReader(repository.getConfiguration().getStorageDirectories());
            final Set<ProvenanceEventRecord> matchingRecords = docsReader.read(topDocs, searcher.getIndexReader(), repository.getAllLogFiles(),
                provenanceQuery.getMaxResults(), maxAttributeChars);

            final long readRecordsNanos = System.nanoTime() - finishSearch;
            logger.debug("Reading {} records took {} millis for {}", matchingRecords.size(), TimeUnit.NANOSECONDS.toMillis(readRecordsNanos), this);

            // The records returned are going to be in a sorted set. The sort order will be dependent on
            // the ID of the events, which is also approximately the same as the timestamp of the event (i.e.
            // it's ordered by the time when the event was inserted into the repo, not the time when the event took
            // place). We want to reverse this so that we get the newest events first, so we have to first create a
            // new List object to hold the events, and then reverse the list.
            final List<ProvenanceEventRecord> recordList = new ArrayList<>(matchingRecords);
            Collections.reverse(recordList);

            sqr.update(recordList, topDocs.totalHits, 0);
            return sqr;
        } catch (final FileNotFoundException e) {
            // nothing has been indexed yet, or the data has already aged off
            logger.warn("Attempted to search Provenance Index {} but could not find the file due to {}", indexDirectory, e);
            if (logger.isDebugEnabled()) {
                logger.warn("", e);
            }

            sqr.update(Collections.<ProvenanceEventRecord> emptyList(), 0, 0);
            return sqr;
        } finally {
            if (searcher != null) {
                indexManager.returnIndexSearcher(indexDirectory, searcher);
            }
        }
    }


    @Override
    public String toString() {
        return "IndexSearcher[" + indexDirectory + "]";
    }
}
