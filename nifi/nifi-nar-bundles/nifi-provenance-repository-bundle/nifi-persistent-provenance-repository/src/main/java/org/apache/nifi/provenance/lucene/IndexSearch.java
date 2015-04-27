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
import java.util.Collections;
import java.util.Date;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.nifi.provenance.PersistentProvenanceRepository;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.StandardQueryResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexSearch {
	private final Logger logger = LoggerFactory.getLogger(IndexSearch.class);
    private final PersistentProvenanceRepository repository;
    private final File indexDirectory;
    private final IndexManager indexManager;

    public IndexSearch(final PersistentProvenanceRepository repo, final File indexDirectory, final IndexManager indexManager) {
        this.repository = repo;
        this.indexDirectory = indexDirectory;
        this.indexManager = indexManager;
    }

    public StandardQueryResult search(final org.apache.nifi.provenance.search.Query provenanceQuery, final AtomicInteger retrievedCount) throws IOException {
        if (!indexDirectory.exists() && !indexDirectory.mkdirs()) {
            throw new IOException("Unable to create Indexing Directory " + indexDirectory);
        }
        if (!indexDirectory.isDirectory()) {
            throw new IOException("Indexing Directory specified is " + indexDirectory + ", but this is not a directory");
        }

        final StandardQueryResult sqr = new StandardQueryResult(provenanceQuery, 1);
        final Set<ProvenanceEventRecord> matchingRecords;

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
            
            final TopDocs topDocs = searcher.search(luceneQuery, provenanceQuery.getMaxResults());
            final long finishSearch = System.nanoTime();
            final long searchNanos = finishSearch - searchStartNanos;
            
            logger.debug("Searching {} took {} millis; opening searcher took {} millis", this, 
            		TimeUnit.NANOSECONDS.toMillis(searchNanos), TimeUnit.NANOSECONDS.toMillis(openSearcherNanos));
            
            if (topDocs.totalHits == 0) {
                sqr.update(Collections.<ProvenanceEventRecord>emptyList(), 0);
                return sqr;
            }

            final DocsReader docsReader = new DocsReader(repository.getConfiguration().getStorageDirectories());
            matchingRecords = docsReader.read(topDocs, searcher.getIndexReader(), repository.getAllLogFiles(), retrievedCount, provenanceQuery.getMaxResults());
            
            final long readRecordsNanos = System.nanoTime() - finishSearch;
            logger.debug("Reading {} records took {} millis for {}", matchingRecords.size(), TimeUnit.NANOSECONDS.toMillis(readRecordsNanos), this);
            
            sqr.update(matchingRecords, topDocs.totalHits);
            return sqr;
        } catch (final FileNotFoundException e) {
            // nothing has been indexed yet, or the data has already aged off
        	logger.warn("Attempted to search Provenance Index {} but could not find the file due to {}", indexDirectory, e);
        	if ( logger.isDebugEnabled() ) {
        		logger.warn("", e);
        	}
        	
            sqr.update(Collections.<ProvenanceEventRecord>emptyList(), 0);
            return sqr;
        } finally {
        	if ( searcher != null ) {
        		indexManager.returnIndexSearcher(indexDirectory, searcher);
        	}
        }
    }

    
    @Override
    public String toString() {
    	return "IndexSearcher[" + indexDirectory + "]";
    }
}
