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

import org.apache.lucene.document.Document;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.index.EventIndexWriter;
import org.apache.nifi.provenance.lucene.IndexManager;
import org.apache.nifi.provenance.store.EventStore;
import org.apache.nifi.provenance.store.iterator.EventIterator;
import org.apache.nifi.reporting.Severity;
import org.apache.nifi.util.StopWatch;
import org.apache.nifi.util.file.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class MigrateDefunctIndex implements Runnable {
    private static final String TEMP_FILENAME_PREFIX = "temp-lucene-8-";
    private static final String MIGRATED_FILENAME_PREFIX = "lucene-8-";
    private static final Logger logger = LoggerFactory.getLogger(MigrateDefunctIndex.class);

    private final File indexDirectory;
    private final IndexManager indexManager;
    private final IndexDirectoryManager directoryManager;
    private final EventStore eventStore;
    private final EventReporter eventReporter;
    private final ConvertEventToLuceneDocument eventConverter;
    private final long minTimestamp;
    private final long maxTimestamp;
    private final AtomicInteger rebuildCount;
    private final int totalCount;

    private long successCount = 0L;


    public MigrateDefunctIndex(final File indexDirectory, final IndexManager indexManager, final IndexDirectoryManager directoryManager, final long minTimestamp, final long maxTimestamp,
                               final EventStore eventStore, final EventReporter eventReporter, final ConvertEventToLuceneDocument eventConverter, final AtomicInteger rebuildCount,
                               final int totalCount) {
        this.indexDirectory = indexDirectory;
        this.indexManager = indexManager;
        this.directoryManager = directoryManager;
        this.eventStore = eventStore;
        this.eventReporter = eventReporter;
        this.minTimestamp = minTimestamp;
        this.maxTimestamp = maxTimestamp;
        this.eventConverter = eventConverter;
        this.rebuildCount = rebuildCount;
        this.totalCount = totalCount;
    }

    @Override
    public void run() {
        final File tempIndexDir = new File(indexDirectory.getParentFile(), TEMP_FILENAME_PREFIX + indexDirectory.getName());
        final File migratedIndexDir = new File(indexDirectory.getParentFile(), MIGRATED_FILENAME_PREFIX + indexDirectory.getName());

        final boolean preconditionsMet = verifyPreconditions(tempIndexDir, migratedIndexDir);
        if (!preconditionsMet) {
            rebuildCount.incrementAndGet(); // increment count so that reporting is accurate
            return;
        }

        // Rebuild the directory or report the error
        try {
            rebuildIndex(tempIndexDir, migratedIndexDir);
            directoryManager.replaceDirectory(indexDirectory, migratedIndexDir, true);

            logger.info("Successfully rebuilt Lucene Index {} as {}; {} of {} indices remain to be rebuilt", indexDirectory, migratedIndexDir,
                totalCount - rebuildCount.incrementAndGet(), totalCount);
        } catch (final Exception e) {
            logger.error("Failed to migrate event index {} to {} after successfully re-indexing {} events", indexDirectory, tempIndexDir, successCount, e);
            eventReporter.reportEvent(Severity.ERROR, "Provenance Event Index Migration", "Failed to  migrate event index " + indexDirectory + " - see logs for more details.");
            rebuildCount.incrementAndGet(); // increment count so that reporting is accurate
        }
    }


    private boolean verifyPreconditions(final File tempIndexDir, final File migratedIndexDir) {
        // If the temp directory exists, delete it or fail.
        if (tempIndexDir.exists()) {
            try {
                FileUtils.deleteFile(tempIndexDir, true);
            } catch (final Exception e) {
                logger.error("Attempted to rebuild index for {} but there already exists a temporary Lucene 8 index at {}. " +
                    "Attempted to delete existing temp directory but failed. This index will not be rebuilt.", tempIndexDir, e);
                return false;
            }
        }

        // If the migrated directory exists, delete it or fail.
        if (migratedIndexDir.exists()) {
            try {
                FileUtils.deleteFile(migratedIndexDir, true);
            } catch (final Exception e) {
                logger.error("Attempted to rebuild index for {} but there already exists a Lucene 8 index at {}. " +
                    "Attempted to delete existing Lucene 8 directory but failed. This index will not be rebuilt.", migratedIndexDir, e);
                return false;
            }
        }

        return true;
    }


    private void rebuildIndex(final File tempIndexDir, final File migratedIndexDir) throws IOException {
        final EventIndexWriter writer = indexManager.borrowIndexWriter(tempIndexDir);

        try {
            final EventIterator eventIterator = eventStore.getEventsByTimestamp(minTimestamp, maxTimestamp);

            final StopWatch stopWatch = new StopWatch(true);

            Optional<ProvenanceEventRecord> optionalEvent;
            while ((optionalEvent = eventIterator.nextEvent()).isPresent()) {
                final ProvenanceEventRecord event = optionalEvent.get();

                final Document document = eventConverter.convert(event, event.getEventId());
                writer.index(document, Integer.MAX_VALUE);
                successCount++;
            }

            writer.commit();
            stopWatch.stop();
            logger.info("Successfully indexed {} events to {} in {}", successCount, tempIndexDir, stopWatch.getDuration());
        } finally {
            indexManager.returnIndexWriter(writer, true, true);
        }

        Files.move(tempIndexDir.toPath(), migratedIndexDir.toPath());
    }
}
