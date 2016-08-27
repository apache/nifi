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

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.nifi.util.file.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestSimpleIndexManager {
    @BeforeClass
    public static void setLogLevel() {
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.provenance", "DEBUG");
    }


    @Test
    public void testMultipleWritersSimultaneouslySameIndex() throws IOException {
        final SimpleIndexManager mgr = new SimpleIndexManager();
        final File dir = new File("target/" + UUID.randomUUID().toString());
        try {
            final IndexWriter writer1 = mgr.borrowIndexWriter(dir);
            final IndexWriter writer2 = mgr.borrowIndexWriter(dir);

            final Document doc1 = new Document();
            doc1.add(new StringField("id", "1", Store.YES));

            final Document doc2 = new Document();
            doc2.add(new StringField("id", "2", Store.YES));

            writer1.addDocument(doc1);
            writer2.addDocument(doc2);
            mgr.returnIndexWriter(dir, writer2);
            mgr.returnIndexWriter(dir, writer1);

            final IndexSearcher searcher = mgr.borrowIndexSearcher(dir);
            final TopDocs topDocs = searcher.search(new MatchAllDocsQuery(), 2);
            assertEquals(2, topDocs.totalHits);
            mgr.returnIndexSearcher(dir, searcher);
        } finally {
            FileUtils.deleteFile(dir, true);
        }
    }

}
