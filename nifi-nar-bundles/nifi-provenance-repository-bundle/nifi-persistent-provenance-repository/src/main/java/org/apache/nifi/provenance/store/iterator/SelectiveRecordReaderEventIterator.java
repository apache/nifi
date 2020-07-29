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

package org.apache.nifi.provenance.store.iterator;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.serialization.RecordReader;
import org.apache.nifi.provenance.store.RecordReaderFactory;
import org.apache.nifi.provenance.util.DirectoryUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SelectiveRecordReaderEventIterator implements EventIterator {
    private static final Logger logger = LoggerFactory.getLogger(SelectiveRecordReaderEventIterator.class);
    private final List<File> files;
    private final RecordReaderFactory readerFactory;
    private final List<Long> eventIds;
    private final Iterator<Long> idIterator;
    private final int maxAttributeChars;

    private boolean closed = false;
    private RecordReader reader;
    private File currentFile;

    public SelectiveRecordReaderEventIterator(final List<File> filesToRead, final RecordReaderFactory readerFactory, final List<Long> eventIds, final int maxAttributeChars) {
        this.readerFactory = readerFactory;

        this.eventIds = new ArrayList<>(eventIds);
        Collections.sort(this.eventIds);
        idIterator = this.eventIds.iterator();

        // Make a copy of the list of files and prune out any Files that are not relevant to the Event ID's that we were given.
        if (eventIds.isEmpty() || filesToRead.isEmpty()) {
            this.files = Collections.emptyList();
        } else {
            this.files = filterUnneededFiles(filesToRead, this.eventIds);
        }

        this.maxAttributeChars = maxAttributeChars;
    }

    protected static List<File> filterUnneededFiles(final List<File> filesToRead, final List<Long> eventIds) {
        final List<File> files = new ArrayList<>();
        final Long firstEventId = eventIds.get(0);
        final Long lastEventId = eventIds.get(eventIds.size() - 1);

        final List<File> sortedFileList = new ArrayList<>(filesToRead);
        Collections.sort(sortedFileList, DirectoryUtils.SMALLEST_ID_FIRST);

        File lastFile = null;
        for (final File file : filesToRead) {
            final long firstIdInFile = DirectoryUtils.getMinId(file);
            if (firstIdInFile > lastEventId) {
                continue;
            }

            if (firstIdInFile > firstEventId) {
                if (files.isEmpty() && lastFile != null) {
                    files.add(lastFile);
                }

                files.add(file);
            }

            lastFile = file;
        }

        if (files.isEmpty() && lastFile != null) {
            files.add(lastFile);
        }

        return files;
    }

    @Override
    public void close() throws IOException {
        closed = true;

        if (reader != null) {
            reader.close();
        }
    }


    @Override
    public Optional<ProvenanceEventRecord> nextEvent() throws IOException {
        if (closed) {
            throw new IOException("EventIterator is already closed");
        }

        final long start = System.nanoTime();
        try {
            while (idIterator.hasNext()) {
                // Determine the next event ID to fetch
                final long eventId = idIterator.next();

                // Determine which file the event should be in.
                final File fileForEvent = getFileForEventId(eventId);
                if (fileForEvent == null) {
                    continue;
                }

                try {
                    // If we determined which file the event should be in, and that's not the file that
                    // we are currently reading from, rotate the reader to the appropriate one.
                    if (!fileForEvent.equals(currentFile)) {
                        if (reader != null) {
                            try {
                                reader.close();
                            } catch (final Exception e) {
                                logger.warn("Failed to close {}; some resources may not be cleaned up appropriately", reader);
                            }
                        }

                        reader = readerFactory.newRecordReader(fileForEvent, Collections.emptyList(), maxAttributeChars);
                        this.currentFile = fileForEvent;
                    }

                    final Optional<ProvenanceEventRecord> eventOption = reader.skipToEvent(eventId);
                    if (eventOption.isPresent() && eventOption.get().getEventId() == eventId) {
                        reader.nextRecord();    // consume the event from the stream.
                        return eventOption;
                    }
                } catch (final FileNotFoundException | EOFException e) {
                    logger.warn("Failed to retrieve Event with ID {}", eventId, e);
                }
            }

            return Optional.empty();
        } finally {
            final long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            logger.trace("Took {} ms to read next event", ms);
        }
    }

    private File getFileForEventId(final long eventId) {
        File lastFile = null;
        for (final File file : files) {
            final long firstEventId = DirectoryUtils.getMinId(file);
            if (firstEventId == eventId) {
                return file;
            }

            if (firstEventId > eventId) {
                return lastFile;
            }

            lastFile = file;
        }

        return lastFile;
    }
}
