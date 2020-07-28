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
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.provenance.serialization.RecordReader;
import org.apache.nifi.provenance.serialization.RecordWriter;
import org.apache.nifi.provenance.toc.NopTocWriter;
import org.apache.nifi.provenance.toc.TocReader;
import org.apache.nifi.provenance.toc.TocWriter;
import org.apache.nifi.stream.io.NullOutputStream;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

@SuppressWarnings("deprecation")
public class TestStandardRecordReaderWriter extends AbstractTestRecordReaderWriter {
    private AtomicLong idGenerator = new AtomicLong(0L);

    @Before
    public void resetIds() {
        idGenerator.set(0L);
    }

    @Test
    @Ignore("For local testing only")
    public void testWritePerformance() throws IOException {
        // This is a simple micro-benchmarking test so that we can determine how fast the serialization/deserialization is before
        // making significant changes. This allows us to ensure that changes that we make do not have significant adverse effects
        // on performance of the repository.
        final ProvenanceEventRecord event = createEvent();

        final TocWriter tocWriter = new NopTocWriter();

        final int numEvents = 10_000_000;
        final long startNanos = System.nanoTime();
        try (final OutputStream nullOut = new NullOutputStream();
            final RecordWriter writer = new StandardRecordWriter(nullOut, "devnull", idGenerator, tocWriter, false, 100000)) {

            writer.writeHeader(0L);

            for (int i = 0; i < numEvents; i++) {
                writer.writeRecord(event);
            }
        }

        final long nanos = System.nanoTime() - startNanos;
        final long millis = TimeUnit.NANOSECONDS.toMillis(nanos);
        System.out.println("Took " + millis + " millis to write " + numEvents + " events");
    }

    @Test
    @Ignore("For local testing only")
    public void testReadPerformance() throws IOException {
        // This is a simple micro-benchmarking test so that we can determine how fast the serialization/deserialization is before
        // making significant changes. This allows us to ensure that changes that we make do not have significant adverse effects
        // on performance of the repository.
        final ProvenanceEventRecord event = createEvent();

        final TocReader tocReader = null;

        final byte[] header;
        try (final ByteArrayOutputStream headerOut = new ByteArrayOutputStream();
            final DataOutputStream out = new DataOutputStream(headerOut)) {
            out.writeUTF(PersistentProvenanceRepository.class.getName());
            out.writeInt(9);
            header = headerOut.toByteArray();
        }

        final byte[] serializedRecord;
        try (final ByteArrayOutputStream headerOut = new ByteArrayOutputStream();
            final StandardRecordWriter writer = new StandardRecordWriter(headerOut, "devnull", idGenerator, null, false, 0)) {

            writer.writeHeader(1L);
            headerOut.reset();

            writer.writeRecord(event);
            writer.flush();
            serializedRecord = headerOut.toByteArray();
        }

        final int numEvents = 10_000_000;
        final long startNanos = System.nanoTime();
        try (final InputStream in = new LoopingInputStream(header, serializedRecord);
            final RecordReader reader = new StandardRecordReader(in, "filename", tocReader, 100000)) {

            for (int i = 0; i < numEvents; i++) {
                reader.nextRecord();
            }
        }

        final long nanos = System.nanoTime() - startNanos;
        final long millis = TimeUnit.NANOSECONDS.toMillis(nanos);
        System.out.println("Took " + millis + " millis to read " + numEvents + " events");
    }

    @Test
    public void testWriteUtfLargerThan64k() throws IOException, InterruptedException {

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
        final String seventyK = StringUtils.repeat("X", 70000);
        assertTrue(seventyK.length() > 65535);
        assertTrue(seventyK.getBytes("UTF-8").length > 65535);
        builder.setDetails(seventyK);
        final ProvenanceEventRecord record = builder.build();

        try (final ByteArrayOutputStream headerOut = new ByteArrayOutputStream();
             final DataOutputStream out = new DataOutputStream(headerOut)) {
            out.writeUTF(PersistentProvenanceRepository.class.getName());
            out.writeInt(9);
        }

        try (final ByteArrayOutputStream recordOut = new ByteArrayOutputStream();
            final StandardRecordWriter writer = new StandardRecordWriter(recordOut, "devnull", idGenerator, null, false, 0)) {

            writer.writeHeader(1L);
            recordOut.reset();

            writer.writeRecord(record);
        }
    }

    @Override
    protected RecordWriter createWriter(File file, TocWriter tocWriter, boolean compressed, int uncompressedBlockSize) throws IOException {
        return new StandardRecordWriter(file, idGenerator, tocWriter, compressed, uncompressedBlockSize);
    }

    @Override
    protected RecordReader createReader(InputStream in, String journalFilename, TocReader tocReader, int maxAttributeSize) throws IOException {
        return new StandardRecordReader(in, journalFilename, tocReader, maxAttributeSize);
    }

}
