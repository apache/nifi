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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.nifi.provenance.EventIdFirstSchemaRecordWriter;
import org.apache.nifi.provenance.IdentifierLookup;
import org.apache.nifi.provenance.ProvenanceEventRecord;
import org.apache.nifi.provenance.TestUtil;
import org.apache.nifi.provenance.serialization.RecordReaders;
import org.apache.nifi.provenance.serialization.RecordWriter;
import org.apache.nifi.provenance.store.RecordReaderFactory;
import org.apache.nifi.provenance.toc.StandardTocWriter;
import org.apache.nifi.provenance.toc.TocUtil;
import org.apache.nifi.provenance.toc.TocWriter;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;


public class TestSelectiveRecordReaderEventIterator {


    private RecordWriter createWriter(final File file, final TocWriter tocWriter, final boolean compressed, final int uncompressedBlockSize) throws IOException {
        return new EventIdFirstSchemaRecordWriter(file, new AtomicLong(0L), tocWriter, compressed, uncompressedBlockSize, IdentifierLookup.EMPTY);
    }

    @Test
    public void testFilterUnneededFiles() {
        final File file1 = new File("1.prov");
        final File file1000 = new File("1000.prov");
        final File file2000 = new File("2000.prov");
        final File file3000 = new File("3000.prov");

        // Filter out the first file.
        final List<File> files = new ArrayList<>();
        files.add(file1);
        files.add(file1000);
        files.add(file2000);
        files.add(file3000);

        List<Long> eventIds = new ArrayList<>();
        eventIds.add(1048L);
        eventIds.add(2048L);
        eventIds.add(3048L);

        List<File> filteredFiles = SelectiveRecordReaderEventIterator.filterUnneededFiles(files, eventIds);
        assertEquals(Arrays.asList(new File[] {file1000, file2000, file3000}), filteredFiles);

        // Filter out file at end
        eventIds.clear();
        eventIds.add(1L);
        eventIds.add(1048L);

        filteredFiles = SelectiveRecordReaderEventIterator.filterUnneededFiles(files, eventIds);
        assertEquals(Arrays.asList(new File[] {file1, file1000}), filteredFiles);
    }

    @Test
    public void testFileNotFound() throws IOException {
        final File file1 = new File("1.prov");

        // Filter out the first file.
        final List<File> files = new ArrayList<>();
        files.add(file1);

        List<Long> eventIds = new ArrayList<>();
        eventIds.add(1L);
        eventIds.add(5L);

        final RecordReaderFactory readerFactory = (file, logs, maxChars) -> {
            return RecordReaders.newRecordReader(file, logs, maxChars);
        };

        final SelectiveRecordReaderEventIterator itr = new SelectiveRecordReaderEventIterator(files, readerFactory, eventIds, 65536);
        final Optional<ProvenanceEventRecord> firstRecordOption = itr.nextEvent();
        assertFalse(firstRecordOption.isPresent());
    }

    @Test
    @Ignore("For local testing only. Runs indefinitely")
    public void testPerformanceOfRandomAccessReads() throws Exception {
        final File dir = new File("target/storage/" + UUID.randomUUID().toString());
        final File journalFile = new File(dir, "/4.prov.gz");
        final File tocFile = TocUtil.getTocFile(journalFile);

        final int blockSize = 1024 * 32;
        try (final RecordWriter writer = createWriter(journalFile, new StandardTocWriter(tocFile, true, false), true, blockSize)) {
            writer.writeHeader(0L);

            for (int i = 0; i < 100_000; i++) {
                writer.writeRecord(TestUtil.createEvent());
            }
        }

        final Long[] eventIds = new Long[] {
            4L, 80L, 1024L, 1025L, 1026L, 1027L, 1028L, 1029L, 1030L, 40_000L, 80_000L, 99_000L
        };

        final RecordReaderFactory readerFactory = (file, logs, maxChars) -> RecordReaders.newRecordReader(file, logs, maxChars);

        final List<File> files = new ArrayList<>();
        files.add(new File(dir, "0.prov"));
        files.add(new File(dir, "0.prov"));
        files.add(new File(dir, "1.prov"));
        files.add(new File(dir, "2.prov"));
        files.add(new File(dir, "3.prov"));
        files.add(journalFile);
        files.add(new File(dir, "100000000.prov"));

        boolean loopForever = true;
        while (loopForever) {
            final long start = System.nanoTime();
            for (int i = 0; i < 1000; i++) {
                final SelectiveRecordReaderEventIterator iterator = new SelectiveRecordReaderEventIterator(
                    Collections.singletonList(journalFile), readerFactory, Arrays.asList(eventIds), 32 * 1024);

                for (final long id : eventIds) {
                    time(() -> {
                        return iterator.nextEvent().orElse(null);
                    }, id);
                }
            }

            final long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            System.out.println(ms + " ms total");
        }
    }

    private void time(final Callable<ProvenanceEventRecord> task, final long id) throws Exception {
        final long start = System.nanoTime();
        final ProvenanceEventRecord event = task.call();
        Assert.assertNotNull(event);
        Assert.assertEquals(id, event.getEventId());
        //        System.out.println(event);
        final long nanos = System.nanoTime() - start;
        final long millis = TimeUnit.NANOSECONDS.toMillis(nanos);
        //        System.out.println("Took " + millis + " ms to " + taskDescription);
    }
}
