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

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.LongField;
import org.apache.lucene.index.IndexWriter;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.provenance.RepositoryConfiguration;
import org.apache.nifi.provenance.SearchableFields;
import org.apache.nifi.provenance.index.EventIndexWriter;
import org.apache.nifi.provenance.lucene.IndexManager;
import org.apache.nifi.provenance.lucene.LuceneEventIndexWriter;
import org.apache.nifi.provenance.serialization.StorageSummary;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

public class TestEventIndexTask {

    @BeforeClass
    public static void setupClass() {
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi", "DEBUG");
    }

    @Test(timeout = 5000)
    public void testIndexWriterCommittedWhenAppropriate() throws IOException, InterruptedException {
        final BlockingQueue<StoredDocument> docQueue = new LinkedBlockingQueue<>();
        final RepositoryConfiguration repoConfig = new RepositoryConfiguration();
        final File storageDir = new File("target/storage/TestEventIndexTask/1");
        repoConfig.addStorageDirectory("1", storageDir);

        final AtomicInteger commitCount = new AtomicInteger(0);

        // Mock out an IndexWriter and keep track of the number of events that are indexed.
        final IndexWriter indexWriter = Mockito.mock(IndexWriter.class);
        final EventIndexWriter eventIndexWriter = new LuceneEventIndexWriter(indexWriter, storageDir);

        final IndexManager indexManager = Mockito.mock(IndexManager.class);
        Mockito.when(indexManager.borrowIndexWriter(Mockito.any(File.class))).thenReturn(eventIndexWriter);

        final IndexDirectoryManager directoryManager = new IndexDirectoryManager(repoConfig);

        // Create an EventIndexTask and override the commit(IndexWriter) method so that we can keep track of how
        // many times the index writer gets committed.
        final EventIndexTask task = new EventIndexTask(docQueue, repoConfig, indexManager, directoryManager, 201, EventReporter.NO_OP) {
            @Override
            protected void commit(EventIndexWriter indexWriter) throws IOException {
                commitCount.incrementAndGet();
            }
        };

        // Create 4 threads, each one a daemon thread running the EventIndexTask
        for (int i = 0; i < 4; i++) {
            final Thread t = new Thread(task);
            t.setDaemon(true);
            t.start();
        }

        assertEquals(0, commitCount.get());

        // Index 100 documents with a storage filename of "0.0.prov"
        for (int i = 0; i < 100; i++) {
            final Document document = new Document();
            document.add(new LongField(SearchableFields.EventTime.getSearchableFieldName(), System.currentTimeMillis(), Store.NO));

            final StorageSummary location = new StorageSummary(1L, "0.0.prov", "1", 0, 1000L, 1000L);
            final StoredDocument storedDoc = new StoredDocument(document, location);
            docQueue.add(storedDoc);
        }
        assertEquals(0, commitCount.get());

        // Index 100 documents
        for (int i = 0; i < 100; i++) {
            final Document document = new Document();
            document.add(new LongField(SearchableFields.EventTime.getSearchableFieldName(), System.currentTimeMillis(), Store.NO));

            final StorageSummary location = new StorageSummary(1L, "0.0.prov", "1", 0, 1000L, 1000L);
            final StoredDocument storedDoc = new StoredDocument(document, location);
            docQueue.add(storedDoc);
        }

        // Wait until we've indexed all 200 events
        while (eventIndexWriter.getEventsIndexed() < 200) {
            Thread.sleep(10L);
        }

        // Wait a bit and make sure that we still haven't committed the index writer.
        Thread.sleep(100L);
        assertEquals(0, commitCount.get());

        // Add another document.
        final Document document = new Document();
        document.add(new LongField(SearchableFields.EventTime.getSearchableFieldName(), System.currentTimeMillis(), Store.NO));
        final StorageSummary location = new StorageSummary(1L, "0.0.prov", "1", 0, 1000L, 1000L);

        StoredDocument storedDoc = new StoredDocument(document, location);
        docQueue.add(storedDoc);

        // Wait until index writer is committed.
        while (commitCount.get() == 0) {
            Thread.sleep(10L);
        }
        assertEquals(1, commitCount.get());

        // Add a new IndexableDocument with a count of 1 to ensure that the writer is committed again.
        storedDoc = new StoredDocument(document, location);
        docQueue.add(storedDoc);
        Thread.sleep(100L);
        assertEquals(1, commitCount.get());

        // Add a new IndexableDocument with a count of 3. Index writer should not be committed again.
        storedDoc = new StoredDocument(document, location);
        docQueue.add(storedDoc);
        Thread.sleep(100L);
        assertEquals(1, commitCount.get());
    }
}
