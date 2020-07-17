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

import org.apache.nifi.provenance.serialization.RecordReader;
import org.apache.nifi.provenance.serialization.RecordWriter;
import org.apache.nifi.provenance.toc.StandardTocReader;
import org.apache.nifi.provenance.toc.StandardTocWriter;
import org.apache.nifi.provenance.toc.TocReader;
import org.apache.nifi.provenance.toc.TocUtil;
import org.apache.nifi.provenance.toc.TocWriter;
import org.apache.nifi.util.file.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class TestEventIdFirstSchemaRecordReaderWriter extends AbstractTestRecordReaderWriter {
    private final AtomicLong idGenerator = new AtomicLong(0L);
    private File journalFile;
    private File tocFile;

    @BeforeClass
    public static void setupLogger() {
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi", "DEBUG");
    }

    @Before
    public void setup() {
        journalFile = new File("target/storage/" + UUID.randomUUID().toString() + "/testEventIdFirstSchemaRecordReaderWriter");
        tocFile = TocUtil.getTocFile(journalFile);
        idGenerator.set(0L);
    }

    @Test
    public void testContentClaimUnchanged() throws IOException {
        final File journalFile = new File("target/storage/" + UUID.randomUUID().toString() + "/testSimpleWrite.gz");
        final File tocFile = TocUtil.getTocFile(journalFile);
        final TocWriter tocWriter = new StandardTocWriter(tocFile, false, false);
        final RecordWriter writer = createWriter(journalFile, tocWriter, true, 8192);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("filename", "1.txt");
        attributes.put("uuid", UUID.randomUUID().toString());

        final ProvenanceEventBuilder builder = new StandardProvenanceEventRecord.Builder();
        builder.setEventTime(System.currentTimeMillis());
        builder.setEventType(ProvenanceEventType.RECEIVE);
        builder.setTransitUri("nifi://unit-test");
        builder.fromFlowFile(TestUtil.createFlowFile(3L, 3000L, attributes));
        builder.setComponentId("1234");
        builder.setComponentType("dummy processor");
        builder.setPreviousContentClaim("container-1", "section-1", "identifier-1", 1L, 1L);
        builder.setCurrentContentClaim("container-1", "section-1", "identifier-1", 1L, 1L);
        final ProvenanceEventRecord record = builder.build();

        writer.writeHeader(1L);
        writer.writeRecords(Collections.singletonList(record));
        writer.close();

        final TocReader tocReader = new StandardTocReader(tocFile);

        try (final FileInputStream fis = new FileInputStream(journalFile);
            final RecordReader reader = createReader(fis, journalFile.getName(), tocReader, 2048)) {
            assertEquals(0, reader.getBlockIndex());
            reader.skipToBlock(0);
            final StandardProvenanceEventRecord recovered = reader.nextRecord();
            assertNotNull(recovered);

            assertEquals("nifi://unit-test", recovered.getTransitUri());

            assertEquals("container-1", recovered.getPreviousContentClaimContainer());
            assertEquals("container-1", recovered.getContentClaimContainer());

            assertEquals("section-1", recovered.getPreviousContentClaimSection());
            assertEquals("section-1", recovered.getContentClaimSection());

            assertEquals("identifier-1", recovered.getPreviousContentClaimIdentifier());
            assertEquals("identifier-1", recovered.getContentClaimIdentifier());

            assertEquals(1L, recovered.getPreviousContentClaimOffset().longValue());
            assertEquals(1L, recovered.getContentClaimOffset().longValue());

            assertEquals(1L, recovered.getPreviousFileSize().longValue());
            assertEquals(1L, recovered.getContentClaimOffset().longValue());

            assertNull(reader.nextRecord());
        }

        FileUtils.deleteFile(journalFile.getParentFile(), true);
    }

    @Test
    public void testContentClaimRemoved() throws IOException {
        final File journalFile = new File("target/storage/" + UUID.randomUUID().toString() + "/testSimpleWrite.gz");
        final File tocFile = TocUtil.getTocFile(journalFile);
        final TocWriter tocWriter = new StandardTocWriter(tocFile, false, false);
        final RecordWriter writer = createWriter(journalFile, tocWriter, true, 8192);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("filename", "1.txt");
        attributes.put("uuid", UUID.randomUUID().toString());

        final ProvenanceEventBuilder builder = new StandardProvenanceEventRecord.Builder();
        builder.setEventTime(System.currentTimeMillis());
        builder.setEventType(ProvenanceEventType.RECEIVE);
        builder.setTransitUri("nifi://unit-test");
        builder.fromFlowFile(TestUtil.createFlowFile(3L, 3000L, attributes));
        builder.setComponentId("1234");
        builder.setComponentType("dummy processor");
        builder.setPreviousContentClaim("container-1", "section-1", "identifier-1", 1L, 1L);
        builder.setCurrentContentClaim(null, null, null, 0L, 0L);
        final ProvenanceEventRecord record = builder.build();

        writer.writeHeader(1L);
        writer.writeRecords(Collections.singletonList(record));
        writer.close();

        final TocReader tocReader = new StandardTocReader(tocFile);

        try (final FileInputStream fis = new FileInputStream(journalFile);
            final RecordReader reader = createReader(fis, journalFile.getName(), tocReader, 2048)) {
            assertEquals(0, reader.getBlockIndex());
            reader.skipToBlock(0);
            final StandardProvenanceEventRecord recovered = reader.nextRecord();
            assertNotNull(recovered);

            assertEquals("nifi://unit-test", recovered.getTransitUri());

            assertEquals("container-1", recovered.getPreviousContentClaimContainer());
            assertNull(recovered.getContentClaimContainer());

            assertEquals("section-1", recovered.getPreviousContentClaimSection());
            assertNull(recovered.getContentClaimSection());

            assertEquals("identifier-1", recovered.getPreviousContentClaimIdentifier());
            assertNull(recovered.getContentClaimIdentifier());

            assertEquals(1L, recovered.getPreviousContentClaimOffset().longValue());
            assertNull(recovered.getContentClaimOffset());

            assertEquals(1L, recovered.getPreviousFileSize().longValue());
            assertEquals(0L, recovered.getFileSize());

            assertNull(reader.nextRecord());
        }

        FileUtils.deleteFile(journalFile.getParentFile(), true);
    }

    @Test
    public void testContentClaimAdded() throws IOException {
        final File journalFile = new File("target/storage/" + UUID.randomUUID().toString() + "/testSimpleWrite.gz");
        final File tocFile = TocUtil.getTocFile(journalFile);
        final TocWriter tocWriter = new StandardTocWriter(tocFile, false, false);
        final RecordWriter writer = createWriter(journalFile, tocWriter, true, 8192);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("filename", "1.txt");
        attributes.put("uuid", UUID.randomUUID().toString());

        final ProvenanceEventBuilder builder = new StandardProvenanceEventRecord.Builder();
        builder.setEventTime(System.currentTimeMillis());
        builder.setEventType(ProvenanceEventType.RECEIVE);
        builder.setTransitUri("nifi://unit-test");
        builder.fromFlowFile(TestUtil.createFlowFile(3L, 3000L, attributes));
        builder.setComponentId("1234");
        builder.setComponentType("dummy processor");
        builder.setCurrentContentClaim("container-1", "section-1", "identifier-1", 1L, 1L);
        final ProvenanceEventRecord record = builder.build();

        writer.writeHeader(1L);
        writer.writeRecords(Collections.singletonList(record));
        writer.close();

        final TocReader tocReader = new StandardTocReader(tocFile);

        try (final FileInputStream fis = new FileInputStream(journalFile);
            final RecordReader reader = createReader(fis, journalFile.getName(), tocReader, 2048)) {
            assertEquals(0, reader.getBlockIndex());
            reader.skipToBlock(0);
            final StandardProvenanceEventRecord recovered = reader.nextRecord();
            assertNotNull(recovered);

            assertEquals("nifi://unit-test", recovered.getTransitUri());

            assertEquals("container-1", recovered.getContentClaimContainer());
            assertNull(recovered.getPreviousContentClaimContainer());

            assertEquals("section-1", recovered.getContentClaimSection());
            assertNull(recovered.getPreviousContentClaimSection());

            assertEquals("identifier-1", recovered.getContentClaimIdentifier());
            assertNull(recovered.getPreviousContentClaimIdentifier());

            assertEquals(1L, recovered.getContentClaimOffset().longValue());
            assertNull(recovered.getPreviousContentClaimOffset());

            assertEquals(1L, recovered.getFileSize());
            assertNull(recovered.getPreviousContentClaimOffset());

            assertNull(reader.nextRecord());
        }

        FileUtils.deleteFile(journalFile.getParentFile(), true);
    }

    @Test
    public void testContentClaimChanged() throws IOException {
        final File journalFile = new File("target/storage/" + UUID.randomUUID().toString() + "/testSimpleWrite.gz");
        final File tocFile = TocUtil.getTocFile(journalFile);
        final TocWriter tocWriter = new StandardTocWriter(tocFile, false, false);
        final RecordWriter writer = createWriter(journalFile, tocWriter, true, 8192);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("filename", "1.txt");
        attributes.put("uuid", UUID.randomUUID().toString());

        final ProvenanceEventBuilder builder = new StandardProvenanceEventRecord.Builder();
        builder.setEventTime(System.currentTimeMillis());
        builder.setEventType(ProvenanceEventType.RECEIVE);
        builder.setTransitUri("nifi://unit-test");
        builder.fromFlowFile(TestUtil.createFlowFile(3L, 3000L, attributes));
        builder.setComponentId("1234");
        builder.setComponentType("dummy processor");
        builder.setPreviousContentClaim("container-1", "section-1", "identifier-1", 1L, 1L);
        builder.setCurrentContentClaim("container-2", "section-2", "identifier-2", 2L, 2L);
        final ProvenanceEventRecord record = builder.build();

        writer.writeHeader(1L);
        writer.writeRecords(Collections.singletonList(record));
        writer.close();

        final TocReader tocReader = new StandardTocReader(tocFile);

        try (final FileInputStream fis = new FileInputStream(journalFile);
            final RecordReader reader = createReader(fis, journalFile.getName(), tocReader, 2048)) {
            assertEquals(0, reader.getBlockIndex());
            reader.skipToBlock(0);
            final StandardProvenanceEventRecord recovered = reader.nextRecord();
            assertNotNull(recovered);

            assertEquals("nifi://unit-test", recovered.getTransitUri());

            assertEquals("container-1", recovered.getPreviousContentClaimContainer());
            assertEquals("container-2", recovered.getContentClaimContainer());

            assertEquals("section-1", recovered.getPreviousContentClaimSection());
            assertEquals("section-2", recovered.getContentClaimSection());

            assertEquals("identifier-1", recovered.getPreviousContentClaimIdentifier());
            assertEquals("identifier-2", recovered.getContentClaimIdentifier());

            assertEquals(1L, recovered.getPreviousContentClaimOffset().longValue());
            assertEquals(2L, recovered.getContentClaimOffset().longValue());

            assertEquals(1L, recovered.getPreviousFileSize().longValue());
            assertEquals(2L, recovered.getContentClaimOffset().longValue());

            assertNull(reader.nextRecord());
        }

        FileUtils.deleteFile(journalFile.getParentFile(), true);
    }

    @Test
    public void testEventIdAndTimestampCorrect() throws IOException {
        final File journalFile = new File("target/storage/" + UUID.randomUUID().toString() + "/testSimpleWrite.gz");
        final File tocFile = TocUtil.getTocFile(journalFile);
        final TocWriter tocWriter = new StandardTocWriter(tocFile, false, false);
        final RecordWriter writer = createWriter(journalFile, tocWriter, true, 8192);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("filename", "1.txt");
        attributes.put("uuid", UUID.randomUUID().toString());

        final long timestamp = System.currentTimeMillis() - 10000L;

        final StandardProvenanceEventRecord.Builder builder = new StandardProvenanceEventRecord.Builder();
        builder.setEventId(1_000_000);
        builder.setEventTime(timestamp);
        builder.setEventType(ProvenanceEventType.RECEIVE);
        builder.setTransitUri("nifi://unit-test");
        builder.fromFlowFile(TestUtil.createFlowFile(3L, 3000L, attributes));
        builder.setComponentId("1234");
        builder.setComponentType("dummy processor");
        builder.setPreviousContentClaim("container-1", "section-1", "identifier-1", 1L, 1L);
        builder.setCurrentContentClaim("container-2", "section-2", "identifier-2", 2L, 2L);
        final ProvenanceEventRecord record = builder.build();

        writer.writeHeader(500_000L);
        writer.writeRecords(Collections.singletonList(record));
        writer.close();

        final TocReader tocReader = new StandardTocReader(tocFile);

        try (final FileInputStream fis = new FileInputStream(journalFile);
            final RecordReader reader = createReader(fis, journalFile.getName(), tocReader, 2048)) {

            final ProvenanceEventRecord event = reader.nextRecord();
            assertNotNull(event);
            assertEquals(1_000_000L, event.getEventId());
            assertEquals(timestamp, event.getEventTime());
            assertNull(reader.nextRecord());
        }

        FileUtils.deleteFile(journalFile.getParentFile(), true);
    }


    @Test
    public void testComponentIdInlineAndLookup() throws IOException {
        final File journalFile = new File("target/storage/" + UUID.randomUUID().toString() + "/testSimpleWrite.prov");
        final File tocFile = TocUtil.getTocFile(journalFile);
        final TocWriter tocWriter = new StandardTocWriter(tocFile, false, false);

        final IdentifierLookup lookup = new IdentifierLookup() {
            @Override
            public List<String> getQueueIdentifiers() {
                return Collections.emptyList();
            }

            @Override
            public List<String> getComponentTypes() {
                return Collections.singletonList("unit-test-component-1");
            }

            @Override
            public List<String> getComponentIdentifiers() {
                return Collections.singletonList("1234");
            }
        };

        final RecordWriter writer = new EventIdFirstSchemaRecordWriter(journalFile, idGenerator, tocWriter, false, 1024 * 32, lookup);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("filename", "1.txt");
        attributes.put("uuid", UUID.randomUUID().toString());

        final StandardProvenanceEventRecord.Builder builder = new StandardProvenanceEventRecord.Builder();
        builder.setEventId(1_000_000);
        builder.setEventTime(System.currentTimeMillis());
        builder.setEventType(ProvenanceEventType.RECEIVE);
        builder.setTransitUri("nifi://unit-test");
        builder.fromFlowFile(TestUtil.createFlowFile(3L, 3000L, attributes));
        builder.setComponentId("1234");
        builder.setComponentType("unit-test-component-2");
        builder.setPreviousContentClaim("container-1", "section-1", "identifier-1", 1L, 1L);
        builder.setCurrentContentClaim("container-2", "section-2", "identifier-2", 2L, 2L);

        writer.writeHeader(500_000L);
        writer.writeRecords(Collections.singletonList(builder.build()));

        builder.setEventId(1_000_001L);
        builder.setComponentId("4444");
        builder.setComponentType("unit-test-component-1");
        writer.writeRecords(Collections.singletonList(builder.build()));

        writer.close();

        final TocReader tocReader = new StandardTocReader(tocFile);

        try (final FileInputStream fis = new FileInputStream(journalFile);
            final RecordReader reader = createReader(fis, journalFile.getName(), tocReader, 2048)) {

            ProvenanceEventRecord event = reader.nextRecord();
            assertNotNull(event);
            assertEquals(1_000_000L, event.getEventId());
            assertEquals("1234", event.getComponentId());
            assertEquals("unit-test-component-2", event.getComponentType());

            event = reader.nextRecord();
            assertNotNull(event);
            assertEquals(1_000_001L, event.getEventId());
            assertEquals("4444", event.getComponentId());
            assertEquals("unit-test-component-1", event.getComponentType());

            assertNull(reader.nextRecord());
        }

        FileUtils.deleteFile(journalFile.getParentFile(), true);
    }

    @Override
    protected RecordWriter createWriter(final File file, final TocWriter tocWriter, final boolean compressed, final int uncompressedBlockSize) throws IOException {
        return new EventIdFirstSchemaRecordWriter(file, idGenerator, tocWriter, compressed, uncompressedBlockSize, IdentifierLookup.EMPTY);
    }

    @Override
    protected RecordReader createReader(final InputStream in, final String journalFilename, final TocReader tocReader, final int maxAttributeSize) throws IOException {
        return new EventIdFirstSchemaRecordReader(in, journalFilename, tocReader, maxAttributeSize);
    }

    @Test
    @Ignore
    public void testPerformanceOfRandomAccessReads() throws Exception {
        journalFile = new File("target/storage/" + UUID.randomUUID().toString() + "/testPerformanceOfRandomAccessReads.gz");
        tocFile = TocUtil.getTocFile(journalFile);

        final int blockSize = 1024 * 32;
        try (final RecordWriter writer = createWriter(journalFile, new StandardTocWriter(tocFile, true, false), true, blockSize)) {
            writer.writeHeader(0L);

            for (int i = 0; i < 100_000; i++) {
                writer.writeRecords(Collections.singletonList(createEvent()));
            }
        }

        final long[] eventIds = new long[] {
            4, 80, 1024, 1025, 1026, 1027, 1028, 1029, 1030, 40_000, 80_000, 99_000
        };

        boolean loopForever = true;
        while (loopForever) {
            final long start = System.nanoTime();
            for (int i = 0; i < 1000; i++) {
                try (final InputStream in = new FileInputStream(journalFile);
                    final RecordReader reader = createReader(in, journalFile.getName(), new StandardTocReader(tocFile), 32 * 1024)) {

                    for (final long id : eventIds) {
                        time(() -> {
                            reader.skipToEvent(id);
                            return reader.nextRecord();
                        }, id);
                    }
                }
            }

            final long ms = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
            System.out.println(ms + " ms total");
        }
    }

    private void time(final Callable<StandardProvenanceEventRecord> task, final long id) throws Exception {
        final long start = System.nanoTime();
        final StandardProvenanceEventRecord event = task.call();
        Assert.assertNotNull(event);
        Assert.assertEquals(id, event.getEventId());
        //        System.out.println(event);
        final long nanos = System.nanoTime() - start;
        final long millis = TimeUnit.NANOSECONDS.toMillis(nanos);
        //        System.out.println("Took " + millis + " ms to " + taskDescription);
    }
}
