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
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.lucene.document.Document;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.provenance.RepositoryConfiguration;
import org.apache.nifi.provenance.SearchableFields;
import org.apache.nifi.provenance.index.EventIndexWriter;
import org.apache.nifi.provenance.lucene.IndexManager;
import org.apache.nifi.reporting.Severity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventIndexTask implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(EventIndexTask.class);
    private static final String EVENT_CATEGORY = "Provenance Repository";
    public static final int MAX_DOCUMENTS_PER_THREAD = 100;
    public static final int DEFAULT_MAX_EVENTS_PER_COMMIT = 1_000_000;

    private final BlockingQueue<StoredDocument> documentQueue;
    private final IndexManager indexManager;
    private volatile boolean shutdown = false;

    private final IndexDirectoryManager directoryManager;
    private final EventReporter eventReporter;
    private final int commitThreshold;

    public EventIndexTask(final BlockingQueue<StoredDocument> documentQueue, final RepositoryConfiguration repoConfig, final IndexManager indexManager,
        final IndexDirectoryManager directoryManager, final int maxEventsPerCommit, final EventReporter eventReporter) {
        this.documentQueue = documentQueue;
        this.indexManager = indexManager;
        this.directoryManager = directoryManager;
        this.commitThreshold = maxEventsPerCommit;
        this.eventReporter = eventReporter;
    }

    public void shutdown() {
        this.shutdown = true;
    }

    private void fetchDocuments(final List<StoredDocument> destination) throws InterruptedException {
        // We want to fetch up to INDEX_BUFFER_SIZE documents at a time. However, we don't want to continually
        // call #drainTo on the queue. So we call poll, blocking for up to 1 second. If we get any event, then
        // we will call drainTo to gather the rest. If we get no events, then we just return, having gathered
        // no events.
        StoredDocument firstDoc = documentQueue.poll(1, TimeUnit.SECONDS);
        if (firstDoc == null) {
            return;
        }

        destination.add(firstDoc);
        documentQueue.drainTo(destination, MAX_DOCUMENTS_PER_THREAD - 1);
    }

    @Override
    public void run() {
        final List<StoredDocument> toIndex = new ArrayList<>(MAX_DOCUMENTS_PER_THREAD);

        while (!shutdown) {
            try {
                // Get the Documents that we want to index.
                toIndex.clear();
                fetchDocuments(toIndex);

                if (toIndex.isEmpty()) {
                    continue;
                }

                // Write documents to the currently active index.
                final Map<String, List<StoredDocument>> docsByPartition = toIndex.stream()
                    .collect(Collectors.groupingBy(doc -> doc.getStorageSummary().getPartitionName().get()));

                for (final Map.Entry<String, List<StoredDocument>> entry : docsByPartition.entrySet()) {
                    final String partitionName = entry.getKey();
                    final List<StoredDocument> docs = entry.getValue();

                    index(docs, partitionName);
                }
            } catch (final Exception e) {
                logger.error("Failed to index Provenance Events", e);
                eventReporter.reportEvent(Severity.ERROR, EVENT_CATEGORY, "Failed to index Provenance Events. See logs for more information.");
            }
        }
    }


    /**
     * Re-indexes the documents given. The IndexableDocument's provided are required to have the IndexDirectory provided.
     */
    void reIndex(final List<IndexableDocument> toIndex, final CommitPreference commitPreference) throws IOException {
        if (toIndex.isEmpty()) {
            return;
        }

        final Map<File, List<IndexableDocument>> docsByIndexDir = toIndex.stream().collect(Collectors.groupingBy(doc -> doc.getIndexDirectory()));
        for (final Map.Entry<File, List<IndexableDocument>> entry : docsByIndexDir.entrySet()) {
            final File indexDirectory = entry.getKey();
            final List<IndexableDocument> documentsForIndex = entry.getValue();

            final EventIndexWriter indexWriter = indexManager.borrowIndexWriter(indexDirectory);
            try {
                // Remove any documents that already exist in this index that are overlapping.
                long minId = Long.MAX_VALUE;
                long maxId = Long.MIN_VALUE;

                for (final IndexableDocument doc : toIndex) {
                    final long eventId = doc.getDocument().getField(SearchableFields.Identifier.getSearchableFieldName()).numericValue().longValue();
                    if (eventId < minId) {
                        minId = eventId;
                    }
                    if (eventId > maxId) {
                        maxId = eventId;
                    }
                }

                final NumericRangeQuery<Long> query = NumericRangeQuery.newLongRange(
                    SearchableFields.Identifier.getSearchableFieldName(), minId, maxId, true, true);
                indexWriter.getIndexWriter().deleteDocuments(query);

                final List<Document> documents = documentsForIndex.stream()
                    .map(doc -> doc.getDocument())
                    .collect(Collectors.toList());

                indexWriter.index(documents, commitThreshold);
            } finally {
                indexManager.returnIndexWriter(indexWriter, CommitPreference.FORCE_COMMIT.equals(commitPreference), false);
            }
        }
    }


    private void index(final List<StoredDocument> toIndex, final String partitionName) throws IOException {
        if (toIndex.isEmpty()) {
            return;
        }

        // Convert the IndexableDocument list into a List of Documents so that we can pass them to the Index Writer.
        final List<Document> documents = toIndex.stream()
            .map(doc -> doc.getDocument())
            .collect(Collectors.toList());

        boolean requestClose = false;
        boolean requestCommit = false;

        final long minEventTime = toIndex.stream()
            .mapToLong(doc -> doc.getDocument().getField(SearchableFields.EventTime.getSearchableFieldName()).numericValue().longValue())
            .min()
            .getAsLong();

        // Synchronize on the directory manager because we don't want the active directory to change
        // while we are obtaining an index writer for it. I.e., determining the active directory
        // and obtaining an Index Writer for it need to be done atomically.
        final EventIndexWriter indexWriter;
        final File indexDirectory;
        synchronized (directoryManager) {
            indexDirectory = directoryManager.getWritableIndexingDirectory(minEventTime, partitionName);
            indexWriter = indexManager.borrowIndexWriter(indexDirectory);
        }

        try {
            // Perform the actual indexing.
            boolean writerIndicatesCommit = indexWriter.index(documents, commitThreshold);

            // If we don't need to commit index based on what index writer tells us, we will still want
            // to commit the index if it's assigned to a partition and this is no longer the active index
            // for that partition. This prevents the following case:
            //
            // Thread T1: pulls events from queue
            //            Maps events to Index Directory D1
            // Thread T2: pulls events from queue
            //            Maps events to Index Directory D1, the active index for Partition P1.
            //            Writes events to D1.
            //            Commits Index Writer for D1.
            //            Closes Index Writer for D1.
            // Thread T1: Writes events to D1.
            //            Determines that Index Writer for D1 does not need to be committed or closed.
            //
            // In the case outlined above, we would potentially lose those events from the index! To avoid this,
            // we simply decide to commit the index if this writer is no longer the active writer for the index.
            // However, if we have 10 threads, we don't want all 10 threads trying to commit the index after each
            // update. We want to commit when they've all finished. This is what the IndexManager will do if we request
            // that it commit the index. It will also close the index if requested, once all writers have finished.
            // So when this is the case, we will request that the Index Manager both commit and close the writer.

            final Optional<File> activeIndexDirOption = directoryManager.getActiveIndexDirectory(partitionName);
            if (!activeIndexDirOption.isPresent() || !activeIndexDirOption.get().equals(indexDirectory)) {
                requestCommit = true;
                requestClose = true;
            }

            if (writerIndicatesCommit) {
                commit(indexWriter);
                requestCommit = false; // we've already committed the index writer so no need to request that the index manager do so also.
                final boolean directoryManagerIndicatesClose = directoryManager.onIndexCommitted(indexDirectory);
                requestClose = requestClose || directoryManagerIndicatesClose;

                if (logger.isDebugEnabled()) {
                    final long maxId = documents.stream()
                        .mapToLong(doc -> doc.getField(SearchableFields.Identifier.getSearchableFieldName()).numericValue().longValue())
                        .max()
                        .orElse(-1L);
                    logger.debug("Committed index {} after writing a max Event ID of {}", indexDirectory, maxId);
                }
            }
        } finally {
            indexManager.returnIndexWriter(indexWriter, requestCommit, requestClose);
        }
    }


    protected void commit(final EventIndexWriter indexWriter) throws IOException {
        final long start = System.nanoTime();
        final long approximateCommitCount = indexWriter.commit();
        final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
        logger.debug("Successfully committed approximately {} Events to {} in {} millis", approximateCommitCount, indexWriter, millis);
    }
}
