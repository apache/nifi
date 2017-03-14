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

package org.apache.nifi.provenance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.nifi.provenance.serialization.RecordReader;
import org.apache.nifi.provenance.serialization.RecordWriter;
import org.apache.nifi.provenance.toc.StandardTocReader;
import org.apache.nifi.provenance.toc.StandardTocWriter;
import org.apache.nifi.provenance.toc.TocReader;
import org.apache.nifi.provenance.toc.TocUtil;
import org.apache.nifi.provenance.toc.TocWriter;
import org.apache.nifi.util.file.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;


public abstract class AbstractTestRecordReaderWriter {
    @BeforeClass
    public static void setLogLevel() {
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.provenance", "INFO");
    }

    protected ProvenanceEventRecord createEvent() {
        return TestUtil.createEvent();
    }

    @Test
    public void testSimpleWriteWithToc() throws IOException {
        final File journalFile = new File("target/storage/" + UUID.randomUUID().toString() + "/testSimpleWrite");
        final File tocFile = TocUtil.getTocFile(journalFile);
        final TocWriter tocWriter = new StandardTocWriter(tocFile, false, false);
        final RecordWriter writer = createWriter(journalFile, tocWriter, false, 1024 * 1024);

        writer.writeHeader(1L);
        writer.writeRecord(createEvent());
        writer.close();

        final TocReader tocReader = new StandardTocReader(tocFile);
        final String expectedTransitUri = "nifi://unit-test";
            final int expectedBlockIndex = 0;

        assertRecoveredRecord(journalFile, tocReader, expectedTransitUri, expectedBlockIndex);

        FileUtils.deleteFile(journalFile.getParentFile(), true);
    }

    private void assertRecoveredRecord(File journalFile, TocReader tocReader, String expectedTransitUri, int expectedBlockIndex) throws IOException {
        try (final FileInputStream fis = new FileInputStream(journalFile);
             final RecordReader reader = createReader(fis, journalFile.getName(), tocReader, 2048)) {
            assertEquals(expectedBlockIndex, reader.getBlockIndex());
            reader.skipToBlock(expectedBlockIndex);
            final StandardProvenanceEventRecord recovered = reader.nextRecord();
            assertNotNull(recovered);

            assertEquals(expectedTransitUri, recovered.getTransitUri());
            assertNull(reader.nextRecord());
        }
    }


    @Test
    public void testSingleRecordCompressed() throws IOException {
        final File journalFile = new File("target/storage/" + UUID.randomUUID().toString() + "/testSimpleWrite.gz");
        final File tocFile = TocUtil.getTocFile(journalFile);
        final TocWriter tocWriter = new StandardTocWriter(tocFile, false, false);
        final RecordWriter writer = createWriter(journalFile, tocWriter, true, 8192);

        writer.writeHeader(1L);
        writer.writeRecord(createEvent());
        writer.close();

        final TocReader tocReader = new StandardTocReader(tocFile);

        assertRecoveredRecord(journalFile, tocReader, "nifi://unit-test", 0);

        FileUtils.deleteFile(journalFile.getParentFile(), true);
    }


    @Test
    public void testMultipleRecordsSameBlockCompressed() throws IOException {
        final File journalFile = new File("target/storage/" + UUID.randomUUID().toString() + "/testSimpleWrite.gz");
        final File tocFile = TocUtil.getTocFile(journalFile);
        final TocWriter tocWriter = new StandardTocWriter(tocFile, false, false);
        // new record each 1 MB of uncompressed data
        final RecordWriter writer = createWriter(journalFile, tocWriter, true, 1024 * 1024);

        writer.writeHeader(1L);
        for (int i = 0; i < 10; i++) {
            writer.writeRecord(createEvent());
        }
        writer.close();

        final TocReader tocReader = new StandardTocReader(tocFile);

        try (final FileInputStream fis = new FileInputStream(journalFile);
            final RecordReader reader = createReader(fis, journalFile.getName(), tocReader, 2048)) {
            for (int i = 0; i < 10; i++) {
                assertEquals(0, reader.getBlockIndex());

                // call skipToBlock half the time to ensure that we can; avoid calling it
                // the other half of the time to ensure that it's okay.
                if (i <= 5) {
                    reader.skipToBlock(0);
                }

                final StandardProvenanceEventRecord recovered = reader.nextRecord();
                assertNotNull(recovered);
                assertEquals("nifi://unit-test", recovered.getTransitUri());
            }

            assertNull(reader.nextRecord());
        }

        FileUtils.deleteFile(journalFile.getParentFile(), true);
    }


    @Test
    public void testMultipleRecordsMultipleBlocksCompressed() throws IOException {
        final File journalFile = new File("target/storage/" + UUID.randomUUID().toString() + "/testSimpleWrite.gz");
        final File tocFile = TocUtil.getTocFile(journalFile);
        final TocWriter tocWriter = new StandardTocWriter(tocFile, false, false);
        // new block each 10 bytes
        final RecordWriter writer = createWriter(journalFile, tocWriter, true, 100);

        writer.writeHeader(1L);
        for (int i = 0; i < 10; i++) {
            writer.writeRecord(createEvent());
        }
        writer.close();

        final TocReader tocReader = new StandardTocReader(tocFile);

        try (final FileInputStream fis = new FileInputStream(journalFile);
            final RecordReader reader = createReader(fis, journalFile.getName(), tocReader, 2048)) {
            for (int i = 0; i < 10; i++) {
                final StandardProvenanceEventRecord recovered = reader.nextRecord();
                System.out.println(recovered);
                assertNotNull(recovered);
                assertEquals(i, recovered.getEventId());
                assertEquals("nifi://unit-test", recovered.getTransitUri());

                final Map<String, String> updatedAttrs = recovered.getUpdatedAttributes();
                assertNotNull(updatedAttrs);
                assertEquals(2, updatedAttrs.size());
                assertEquals("1.txt", updatedAttrs.get("filename"));
                assertTrue(updatedAttrs.containsKey("uuid"));
            }

            assertNull(reader.nextRecord());
        }

        FileUtils.deleteFile(journalFile.getParentFile(), true);
    }

    @Test
    public void testSkipToEvent() throws IOException {
        final File journalFile = new File("target/storage/" + UUID.randomUUID().toString() + "/testSimpleWrite.gz");
        final File tocFile = TocUtil.getTocFile(journalFile);
        final TocWriter tocWriter = new StandardTocWriter(tocFile, false, false);
        // new block each 10 bytes
        final RecordWriter writer = createWriter(journalFile, tocWriter, true, 100);

        writer.writeHeader(0L);
        final int numEvents = 10;
        final List<ProvenanceEventRecord> events = new ArrayList<>();
        for (int i = 0; i < numEvents; i++) {
            final ProvenanceEventRecord event = createEvent();
            events.add(event);
            writer.writeRecord(event);
        }
        writer.close();

        final TocReader tocReader = new StandardTocReader(tocFile);

        try (final FileInputStream fis = new FileInputStream(journalFile);
            final RecordReader reader = createReader(fis, journalFile.getName(), tocReader, 2048)) {

            for (int i = 0; i < numEvents; i++) {
                final Optional<ProvenanceEventRecord> eventOption = reader.skipToEvent(i);
                assertTrue(eventOption.isPresent());
                assertEquals(i, eventOption.get().getEventId());
                assertEquals(events.get(i), eventOption.get());

                final StandardProvenanceEventRecord consumedEvent = reader.nextRecord();
                assertEquals(eventOption.get(), consumedEvent);
            }

            assertFalse(reader.skipToEvent(numEvents + 1).isPresent());
        }

        try (final FileInputStream fis = new FileInputStream(journalFile);
            final RecordReader reader = createReader(fis, journalFile.getName(), tocReader, 2048)) {

            for (int i = 0; i < 3; i++) {
                final Optional<ProvenanceEventRecord> eventOption = reader.skipToEvent(8);
                assertTrue(eventOption.isPresent());
                assertEquals(events.get(8), eventOption.get());
            }

            final StandardProvenanceEventRecord consumedEvent = reader.nextRecord();
            assertEquals(events.get(8), consumedEvent);
        }
    }

    protected abstract RecordWriter createWriter(File file, TocWriter tocWriter, boolean compressed, int uncompressedBlockSize) throws IOException;

    protected abstract RecordReader createReader(InputStream in, String journalFilename, TocReader tocReader, int maxAttributeSize) throws IOException;
}
