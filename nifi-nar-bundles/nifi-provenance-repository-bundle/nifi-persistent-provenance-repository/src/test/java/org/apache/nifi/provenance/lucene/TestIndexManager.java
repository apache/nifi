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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.UUID;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.nifi.util.file.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestIndexManager {

    private File indexDir;
    private IndexManager manager;

    @Before
    public void setup() {
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.provenance", "DEBUG");
        manager = new IndexManager();

        indexDir = new File("target/testIndexManager/" + UUID.randomUUID().toString());
        indexDir.mkdirs();
    }

    @After
    public void cleanup() throws IOException {
        manager.close();

        FileUtils.deleteFiles(Collections.singleton(indexDir), true);
    }

    @Test
    public void test() throws IOException {
        // Create and IndexWriter and add a document to the index, then close the writer.
        // This gives us something that we can query.
        final IndexWriter writer = manager.borrowIndexWriter(indexDir);
        final Document doc = new Document();
        doc.add(new StringField("unit test", "true", Store.YES));
        writer.addDocument(doc);
        manager.returnIndexWriter(indexDir, writer);

        // Get an Index Searcher that we can use to query the index.
        final IndexSearcher cachedSearcher = manager.borrowIndexSearcher(indexDir);

        // Ensure that we get the expected results.
        assertCount(cachedSearcher, 1);

        // While we already have an Index Searcher, get a writer for the same index.
        // This will cause the Index Searcher to be marked as poisoned.
        final IndexWriter writer2 = manager.borrowIndexWriter(indexDir);

        // Obtain a new Index Searcher with the writer open. This Index Searcher should *NOT*
        // be the same as the previous searcher because the new one will be a Near-Real-Time Index Searcher
        // while the other is not.
        final IndexSearcher nrtSearcher = manager.borrowIndexSearcher(indexDir);
        assertNotSame(cachedSearcher, nrtSearcher);

        // Ensure that we get the expected query results.
        assertCount(nrtSearcher, 1);

        // Return the writer, so that there is no longer an active writer for the index.
        manager.returnIndexWriter(indexDir, writer2);

        // Ensure that we still get the same result.
        assertCount(cachedSearcher, 1);
        manager.returnIndexSearcher(indexDir, cachedSearcher);

        // Ensure that our near-real-time index searcher still gets the same result.
        assertCount(nrtSearcher, 1);
        manager.returnIndexSearcher(indexDir, nrtSearcher);
    }

    private void assertCount(final IndexSearcher searcher, final int count) throws IOException {
        final BooleanQuery query = new BooleanQuery();
        query.add(new BooleanClause(new TermQuery(new Term("unit test", "true")), Occur.MUST));
        final TopDocs topDocs = searcher.search(query, count * 10);
        assertNotNull(topDocs);
        assertEquals(1, topDocs.totalHits);
    }
}
