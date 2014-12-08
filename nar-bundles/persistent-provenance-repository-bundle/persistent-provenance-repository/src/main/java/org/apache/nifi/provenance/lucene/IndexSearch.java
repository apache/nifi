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
import java.io.IOException;
import java.util.Collections;
import java.util.Date;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.nifi.provenance.PersistentProvenanceRepository;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.StandardQueryResult;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;

public class IndexSearch {

    private final PersistentProvenanceRepository repository;
    private final File indexDirectory;

    public IndexSearch(final PersistentProvenanceRepository repo, final File indexDirectory) {
        this.repository = repo;
        this.indexDirectory = indexDirectory;
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

        try (final DirectoryReader directoryReader = DirectoryReader.open(FSDirectory.open(indexDirectory))) {
            final IndexSearcher searcher = new IndexSearcher(directoryReader);

            if (provenanceQuery.getEndDate() == null) {
                provenanceQuery.setEndDate(new Date());
            }
            final Query luceneQuery = LuceneUtil.convertQuery(provenanceQuery);

            TopDocs topDocs = searcher.search(luceneQuery, provenanceQuery.getMaxResults());
            if (topDocs.totalHits == 0) {
                sqr.update(Collections.<ProvenanceEventRecord>emptyList(), 0);
                return sqr;
            }

            final DocsReader docsReader = new DocsReader(repository.getConfiguration().getStorageDirectories());
            matchingRecords = docsReader.read(topDocs, directoryReader, repository.getAllLogFiles(), retrievedCount, provenanceQuery.getMaxResults());

            sqr.update(matchingRecords, topDocs.totalHits);
            return sqr;
        } catch (final IndexNotFoundException e) {
            // nothing has been indexed yet.
            sqr.update(Collections.<ProvenanceEventRecord>emptyList(), 0);
            return sqr;
        }
    }

}
