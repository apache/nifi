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
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriter;
import org.apache.nifi.provenance.index.EventIndexWriter;

public class LuceneEventIndexWriter implements EventIndexWriter {
    private final IndexWriter indexWriter;
    private final File directory;
    private final long maxCommitNanos;

    private final AtomicReference<CommitStats> commitStats = new AtomicReference<>();
    private final AtomicLong totalIndexed = new AtomicLong(0L);
    private final AtomicLong lastCommitTotalIndexed = new AtomicLong(0L);

    public LuceneEventIndexWriter(final IndexWriter indexWriter, final File directory) {
        this(indexWriter, directory, TimeUnit.SECONDS.toNanos(30L));
    }

    public LuceneEventIndexWriter(final IndexWriter indexWriter, final File directory, final long maxCommitNanos) {
        this.indexWriter = indexWriter;
        this.directory = directory;
        this.maxCommitNanos = maxCommitNanos;

        commitStats.set(new CommitStats(0, System.nanoTime() + maxCommitNanos));
    }

    @Override
    public void close() throws IOException {
        indexWriter.close();
    }

    @Override
    public boolean index(final Document document, final int commitThreshold) throws IOException {
        return index(Collections.singletonList(document), commitThreshold);
    }

    @Override
    public boolean index(List<Document> documents, final int commitThreshold) throws IOException {
        if (documents.isEmpty()) {
            return false;
        }

        final int numDocs = documents.size();
        indexWriter.addDocuments(documents);
        totalIndexed.addAndGet(numDocs);

        boolean updated = false;
        while (!updated) {
            final CommitStats stats = commitStats.get();
            CommitStats updatedStats = new CommitStats(stats.getIndexedSinceCommit() + numDocs, stats.getNextCommitTimestamp());

            if (updatedStats.getIndexedSinceCommit() >= commitThreshold || System.nanoTime() >= updatedStats.getNextCommitTimestamp()) {
                updatedStats = new CommitStats(0, System.nanoTime() + maxCommitNanos);
                updated = commitStats.compareAndSet(stats, updatedStats);
                if (updated) {
                    return true;
                }
            } else {
                updated = commitStats.compareAndSet(stats, updatedStats);
            }
        }

        return false;
    }

    @Override
    public File getDirectory() {
        return directory;
    }

    @Override
    public long commit() throws IOException {
        final long lastCommitCount = lastCommitTotalIndexed.get();
        final long currentCommitCount = totalIndexed.get();
        indexWriter.commit();
        commitStats.set(new CommitStats(0, System.nanoTime() + maxCommitNanos));
        lastCommitTotalIndexed.set(currentCommitCount);
        return currentCommitCount - lastCommitCount;
    }

    @Override
    public int getEventsIndexedSinceCommit() {
        return commitStats.get().getIndexedSinceCommit();
    }

    @Override
    public long getEventsIndexed() {
        return totalIndexed.get();
    }

    @Override
    public IndexWriter getIndexWriter() {
        return indexWriter;
    }

    @Override
    public String toString() {
        return "LuceneEventIndexWriter[dir=" + directory + "]";
    }

    private static class CommitStats {
        private final long nextCommitTimestamp;
        private final int indexedSinceCommit;

        public CommitStats(final int indexedCount, final long nextCommitTime) {
            this.nextCommitTimestamp = nextCommitTime;
            this.indexedSinceCommit = indexedCount;
        }

        public long getNextCommitTimestamp() {
            return nextCommitTimestamp;
        }

        public int getIndexedSinceCommit() {
            return indexedSinceCommit;
        }
    }
}
