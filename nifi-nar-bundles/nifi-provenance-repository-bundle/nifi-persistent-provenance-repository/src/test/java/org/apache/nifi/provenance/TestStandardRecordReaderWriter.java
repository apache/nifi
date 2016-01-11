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

import static org.apache.nifi.provenance.TestUtil.createFlowFile;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.apache.nifi.provenance.toc.StandardTocReader;
import org.apache.nifi.provenance.toc.StandardTocWriter;
import org.apache.nifi.provenance.toc.TocReader;
import org.apache.nifi.provenance.toc.TocUtil;
import org.apache.nifi.provenance.toc.TocWriter;
import org.apache.nifi.util.file.FileUtils;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestStandardRecordReaderWriter {
    @BeforeClass
    public static void setLogLevel() {
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.nifi.provenance", "DEBUG");
    }

    private ProvenanceEventRecord createEvent() {
        final Map<String, String> attributes = new HashMap<>();
        attributes.put("filename", "1.txt");
        attributes.put("uuid", UUID.randomUUID().toString());

        final ProvenanceEventBuilder builder = new StandardProvenanceEventRecord.Builder();
        builder.setEventTime(System.currentTimeMillis());
        builder.setEventType(ProvenanceEventType.RECEIVE);
        builder.setTransitUri("nifi://unit-test");
        builder.fromFlowFile(createFlowFile(3L, 3000L, attributes));
        builder.setComponentId("1234");
        builder.setComponentType("dummy processor");
        final ProvenanceEventRecord record = builder.build();

        return record;
    }

    @Test
    public void testSimpleWriteWithToc() throws IOException {
        final File journalFile = new File("target/storage/" + UUID.randomUUID().toString() + "/testSimpleWrite");
        final File tocFile = TocUtil.getTocFile(journalFile);
        final TocWriter tocWriter = new StandardTocWriter(tocFile, false, false);
        final StandardRecordWriter writer = new StandardRecordWriter(journalFile, tocWriter, false, 1024 * 1024);

        writer.writeHeader(1L);
        writer.writeRecord(createEvent(), 1L);
        writer.close();

        final TocReader tocReader = new StandardTocReader(tocFile);

        try (final FileInputStream fis = new FileInputStream(journalFile);
            final StandardRecordReader reader = new StandardRecordReader(fis, journalFile.getName(), tocReader, 2048)) {
            assertEquals(0, reader.getBlockIndex());
            reader.skipToBlock(0);
            final StandardProvenanceEventRecord recovered = reader.nextRecord();
            assertNotNull(recovered);

            assertEquals("nifi://unit-test", recovered.getTransitUri());
            assertNull(reader.nextRecord());
        }

        FileUtils.deleteFile(journalFile.getParentFile(), true);
    }


    @Test
    public void testSingleRecordCompressed() throws IOException {
        final File journalFile = new File("target/storage/" + UUID.randomUUID().toString() + "/testSimpleWrite.gz");
        final File tocFile = TocUtil.getTocFile(journalFile);
        final TocWriter tocWriter = new StandardTocWriter(tocFile, false, false);
        final StandardRecordWriter writer = new StandardRecordWriter(journalFile, tocWriter, true, 100);

        writer.writeHeader(1L);
        writer.writeRecord(createEvent(), 1L);
        writer.close();

        final TocReader tocReader = new StandardTocReader(tocFile);

        try (final FileInputStream fis = new FileInputStream(journalFile);
            final StandardRecordReader reader = new StandardRecordReader(fis, journalFile.getName(), tocReader, 2048)) {
            assertEquals(0, reader.getBlockIndex());
            reader.skipToBlock(0);
            final StandardProvenanceEventRecord recovered = reader.nextRecord();
            assertNotNull(recovered);

            assertEquals("nifi://unit-test", recovered.getTransitUri());
            assertNull(reader.nextRecord());
        }

        FileUtils.deleteFile(journalFile.getParentFile(), true);
    }


    @Test
    public void testMultipleRecordsSameBlockCompressed() throws IOException {
        final File journalFile = new File("target/storage/" + UUID.randomUUID().toString() + "/testSimpleWrite.gz");
        final File tocFile = TocUtil.getTocFile(journalFile);
        final TocWriter tocWriter = new StandardTocWriter(tocFile, false, false);
        // new record each 1 MB of uncompressed data
        final StandardRecordWriter writer = new StandardRecordWriter(journalFile, tocWriter, true, 1024 * 1024);

        writer.writeHeader(1L);
        for (int i=0; i < 10; i++) {
            writer.writeRecord(createEvent(), i);
        }
        writer.close();

        final TocReader tocReader = new StandardTocReader(tocFile);

        try (final FileInputStream fis = new FileInputStream(journalFile);
            final StandardRecordReader reader = new StandardRecordReader(fis, journalFile.getName(), tocReader, 2048)) {
            for (int i=0; i < 10; i++) {
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
        final StandardRecordWriter writer = new StandardRecordWriter(journalFile, tocWriter, true, 100);

        writer.writeHeader(1L);
        for (int i=0; i < 10; i++) {
            writer.writeRecord(createEvent(), i);
        }
        writer.close();

        final TocReader tocReader = new StandardTocReader(tocFile);

        try (final FileInputStream fis = new FileInputStream(journalFile);
            final StandardRecordReader reader = new StandardRecordReader(fis, journalFile.getName(), tocReader, 2048)) {
            for (int i=0; i < 10; i++) {
                final StandardProvenanceEventRecord recovered = reader.nextRecord();
                System.out.println(recovered);
                assertNotNull(recovered);
                assertEquals(i, recovered.getEventId());
                assertEquals("nifi://unit-test", recovered.getTransitUri());
            }

            assertNull(reader.nextRecord());
        }

        FileUtils.deleteFile(journalFile.getParentFile(), true);
    }
}
