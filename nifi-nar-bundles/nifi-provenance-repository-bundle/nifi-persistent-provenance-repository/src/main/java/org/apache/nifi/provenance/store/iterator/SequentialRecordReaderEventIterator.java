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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.serialization.RecordReader;
import org.apache.nifi.provenance.store.RecordReaderFactory;

public class SequentialRecordReaderEventIterator implements EventIterator {
    private final Iterator<File> fileIterator;
    private final RecordReaderFactory readerFactory;
    private final long minimumEventId;
    private final int maxAttributeChars;

    private boolean closed = false;
    private RecordReader reader;

    public SequentialRecordReaderEventIterator(final List<File> filesToRead, final RecordReaderFactory readerFactory, final long minimumEventId, final int maxAttributeChars) {
        this.fileIterator = filesToRead.iterator();
        this.readerFactory = readerFactory;
        this.minimumEventId = minimumEventId;
        this.maxAttributeChars = maxAttributeChars;
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

        if (reader == null) {
            if (!rotateReader()) {
                return Optional.empty();
            }
        }

        while (true) {
            ProvenanceEventRecord event;
            try {
                event = reader.nextRecord();
            } catch (final EOFException eof) {
                // We have run out of data. Treat the same as getting back null.
                // This happens particularly if reading the same file that is being
                // written to (because we use a BufferedOutputStream to write to it, so we
                // may read half-way through a record and then hit the end of what was
                // buffered and flushed).
                event = null;
            }

            if (event == null) {
                if (rotateReader()) {
                    continue;
                } else {
                    return Optional.empty();
                }
            } else {
                return Optional.of(event);
            }
        }
    }

    private boolean rotateReader() throws IOException {
        final boolean readerExists = (reader != null);
        if (readerExists) {
            reader.close();
        }

        boolean multipleReadersOpened = false;
        while (true) {
            if (!fileIterator.hasNext()) {
                return false;
            }

            final File eventFile = fileIterator.next();
            try {
                reader = readerFactory.newRecordReader(eventFile, Collections.emptyList(), maxAttributeChars);
                break;
            } catch (final FileNotFoundException | EOFException e) {
                multipleReadersOpened = true;
                // File may have aged off or was not fully written. Move to next file
                continue;
            }
        }

        // If this is the first file in our list, the event of interest may not be the first event,
        // so skip to the event that we want.
        if (!readerExists && !multipleReadersOpened) {
            reader.skipToEvent(minimumEventId);
        }

        return true;
    }
}
