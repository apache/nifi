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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.nifi.provenance.schema.EventRecord;
import org.apache.nifi.provenance.schema.EventRecordFields;
import org.apache.nifi.provenance.schema.ProvenanceEventSchema;
import org.apache.nifi.provenance.serialization.RecordReader;
import org.apache.nifi.provenance.serialization.RecordWriter;
import org.apache.nifi.provenance.toc.NopTocWriter;
import org.apache.nifi.provenance.toc.StandardTocReader;
import org.apache.nifi.provenance.toc.StandardTocWriter;
import org.apache.nifi.provenance.toc.TocReader;
import org.apache.nifi.provenance.toc.TocUtil;
import org.apache.nifi.provenance.toc.TocWriter;
import org.apache.nifi.repository.schema.FieldMapRecord;
import org.apache.nifi.repository.schema.FieldType;
import org.apache.nifi.repository.schema.Record;
import org.apache.nifi.repository.schema.RecordField;
import org.apache.nifi.repository.schema.RecordSchema;
import org.apache.nifi.repository.schema.Repetition;
import org.apache.nifi.repository.schema.SimpleRecordField;
import org.apache.nifi.stream.io.DataOutputStream;
import org.apache.nifi.stream.io.NullOutputStream;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class TestSchemaRecordReaderWriter extends AbstractTestRecordReaderWriter {

    private File journalFile;
    private File tocFile;

    @Before
    public void setup() {
        journalFile = new File("target/storage/" + UUID.randomUUID().toString() + "/testFieldAddedToSchema");
        tocFile = TocUtil.getTocFile(journalFile);
    }


    @Test
    public void testFieldAddedToSchema() throws IOException {
        final RecordField unitTestField = new SimpleRecordField("Unit Test Field", FieldType.STRING, Repetition.EXACTLY_ONE);
        final Consumer<List<RecordField>> schemaModifier = fields -> fields.add(unitTestField);

        final Map<RecordField, Object> toAdd = new HashMap<>();
        toAdd.put(unitTestField, "hello");

        try (final ByteArraySchemaRecordWriter writer = createSchemaWriter(schemaModifier, toAdd)) {
            writer.writeHeader(1L);
            writer.writeRecord(createEvent(), 3L);
            writer.writeRecord(createEvent(), 3L);
        }

        try (final InputStream in = new FileInputStream(journalFile);
            final TocReader tocReader = new StandardTocReader(tocFile);
            final RecordReader reader = createReader(in, journalFile.getName(), tocReader, 10000)) {

            for (int i = 0; i < 2; i++) {
                final StandardProvenanceEventRecord event = reader.nextRecord();
                assertNotNull(event);
                assertEquals(3L, event.getEventId());
                assertEquals("1234", event.getComponentId());
                assertEquals(ProvenanceEventType.RECEIVE, event.getEventType());

                assertNotNull(event.getUpdatedAttributes());
                assertFalse(event.getUpdatedAttributes().isEmpty());
            }
        }
    }

    @Test
    public void testFieldRemovedFromSchema() throws IOException {
        final TocWriter tocWriter = new StandardTocWriter(tocFile, false, false);
        try {
            // Create a schema that has the fields modified
            final RecordSchema schemaV1 = ProvenanceEventSchema.PROVENANCE_EVENT_SCHEMA_V1;
            final List<RecordField> fields = new ArrayList<>(schemaV1.getFields());
            fields.remove(new SimpleRecordField(EventRecordFields.Names.UPDATED_ATTRIBUTES, FieldType.STRING, Repetition.EXACTLY_ONE));
            fields.remove(new SimpleRecordField(EventRecordFields.Names.PREVIOUS_ATTRIBUTES, FieldType.STRING, Repetition.EXACTLY_ONE));
            final RecordSchema recordSchema = new RecordSchema(fields);

            // Create a record writer whose schema does not contain updated attributes or previous attributes.
            // This means that we must also override the method that writes out attributes so that we are able
            // to avoid actually writing them out.
            final ByteArraySchemaRecordWriter writer = new ByteArraySchemaRecordWriter(journalFile, tocWriter, false, 0) {
                @Override
                public void writeHeader(long firstEventId, DataOutputStream out) throws IOException {
                    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    recordSchema.writeTo(baos);

                    out.writeInt(baos.size());
                    baos.writeTo(out);
                }

                @Override
                protected Record createRecord(final ProvenanceEventRecord event, final long eventId) {
                    final RecordSchema contentClaimSchema = new RecordSchema(recordSchema.getField(EventRecordFields.Names.CONTENT_CLAIM).getSubFields());
                    return new EventRecord(event, eventId, recordSchema, contentClaimSchema);
                }
            };

            try {
                writer.writeHeader(1L);
                writer.writeRecord(createEvent(), 3L);
                writer.writeRecord(createEvent(), 3L);
            } finally {
                writer.close();
            }
        } finally {
            tocWriter.close();
        }

        // Read the records in and make sure that they have the info that we expect.
        try (final InputStream in = new FileInputStream(journalFile);
            final TocReader tocReader = new StandardTocReader(tocFile);
            final RecordReader reader = createReader(in, journalFile.getName(), tocReader, 10000)) {

            for (int i = 0; i < 2; i++) {
                final StandardProvenanceEventRecord event = reader.nextRecord();
                assertNotNull(event);
                assertEquals(3L, event.getEventId());
                assertEquals(ProvenanceEventType.RECEIVE, event.getEventType());

                // We will still have a Map<String, String> for updated attributes because the
                // Provenance Event Builder will create an empty map.
                assertNotNull(event.getUpdatedAttributes());
                assertTrue(event.getUpdatedAttributes().isEmpty());
            }
        }
    }

    @Test
    public void testAddOneRecordReadTwice() throws IOException {
        final RecordField unitTestField = new SimpleRecordField("Unit Test Field", FieldType.STRING, Repetition.EXACTLY_ONE);
        final Consumer<List<RecordField>> schemaModifier = fields -> fields.add(unitTestField);

        final Map<RecordField, Object> toAdd = new HashMap<>();
        toAdd.put(unitTestField, "hello");

        try (final ByteArraySchemaRecordWriter writer = createSchemaWriter(schemaModifier, toAdd)) {
            writer.writeHeader(1L);
            writer.writeRecord(createEvent(), 3L);
        }

        try (final InputStream in = new FileInputStream(journalFile);
            final TocReader tocReader = new StandardTocReader(tocFile);
            final RecordReader reader = createReader(in, journalFile.getName(), tocReader, 10000)) {

            final ProvenanceEventRecord firstEvent = reader.nextRecord();
            assertNotNull(firstEvent);

            final ProvenanceEventRecord secondEvent = reader.nextRecord();
            assertNull(secondEvent);
        }
    }


    /**
     * Creates a SchemaRecordWriter that uses a modified schema
     *
     * @param fieldModifier the callback for modifying the schema
     * @return a SchemaRecordWriter that uses the modified schema
     * @throws IOException if unable to create the writer
     */
    private ByteArraySchemaRecordWriter createSchemaWriter(final Consumer<List<RecordField>> fieldModifier, final Map<RecordField, Object> fieldsToAdd) throws IOException {
        final TocWriter tocWriter = new StandardTocWriter(tocFile, false, false);

        // Create a schema that has the fields modified
        final RecordSchema schemaV1 = ProvenanceEventSchema.PROVENANCE_EVENT_SCHEMA_V1;
        final List<RecordField> fields = new ArrayList<>(schemaV1.getFields());
        fieldModifier.accept(fields);

        final RecordSchema recordSchema = new RecordSchema(fields);
        final RecordSchema contentClaimSchema = new RecordSchema(recordSchema.getField(EventRecordFields.Names.CONTENT_CLAIM).getSubFields());

        final ByteArraySchemaRecordWriter writer = new ByteArraySchemaRecordWriter(journalFile, tocWriter, false, 0) {
            @Override
            public void writeHeader(long firstEventId, DataOutputStream out) throws IOException {
                final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                recordSchema.writeTo(baos);

                out.writeInt(baos.size());
                baos.writeTo(out);
            }

            @Override
            protected Record createRecord(final ProvenanceEventRecord event, final long eventId) {
                final Map<RecordField, Object> values = new HashMap<>();

                final EventRecord eventRecord = new EventRecord(event, eventId, recordSchema, contentClaimSchema);
                for (final RecordField field : recordSchema.getFields()) {
                    final Object value = eventRecord.getFieldValue(field);
                    values.put(field, value);
                }

                values.putAll(fieldsToAdd);
                return new FieldMapRecord(values, recordSchema);
            }
        };

        return writer;
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
            final RecordWriter writer = new ByteArraySchemaRecordWriter(nullOut, tocWriter, false, 0)) {

            writer.writeHeader(0L);

            for (int i = 0; i < numEvents; i++) {
                writer.writeRecord(event, i);
            }

        }

        final long nanos = System.nanoTime() - startNanos;
        final long millis = TimeUnit.NANOSECONDS.toMillis(nanos);
        System.out.println("Took " + millis + " millis to write " + numEvents + " events");
    }


    @Test
    @Ignore("For local performance testing only")
    public void testReadPerformance() throws IOException, InterruptedException {
        // This is a simple micro-benchmarking test so that we can determine how fast the serialization/deserialization is before
        // making significant changes. This allows us to ensure that changes that we make do not have significant adverse effects
        // on performance of the repository.
        final ProvenanceEventRecord event = createEvent();

        final TocReader tocReader = null;

        final byte[] header;
        try (final ByteArrayOutputStream headerOut = new ByteArrayOutputStream();
            final DataOutputStream out = new DataOutputStream(headerOut)) {

            final RecordWriter schemaWriter = new ByteArraySchemaRecordWriter(out, null, false, 0);
            schemaWriter.writeHeader(1L);

            header = headerOut.toByteArray();
        }

        final byte[] serializedRecord;
        try (final ByteArrayOutputStream headerOut = new ByteArrayOutputStream();
            final RecordWriter writer = new ByteArraySchemaRecordWriter(headerOut, null, false, 0)) {

            writer.writeHeader(1L);
            headerOut.reset();

            writer.writeRecord(event, 1L);
            writer.flush();
            serializedRecord = headerOut.toByteArray();
        }

        final int numEvents = 10_000_000;
        final int recordBytes = serializedRecord.length;
        final long totalRecordBytes = (long) recordBytes * (long) numEvents;

        final long startNanos = System.nanoTime();
        try (final InputStream in = new LoopingInputStream(header, serializedRecord);
            final RecordReader reader = new ByteArraySchemaRecordReader(in, "filename", tocReader, 100000)) {

            for (int i = 0; i < numEvents; i++) {
                reader.nextRecord();
            }
        }

        final long nanos = System.nanoTime() - startNanos;
        final long millis = TimeUnit.NANOSECONDS.toMillis(nanos);
        final double seconds = millis / 1000D;
        final long bytesPerSecond = (long) (totalRecordBytes / seconds);
        final long megaBytesPerSecond = bytesPerSecond / 1024 / 1024;
        System.out.println("Took " + millis + " millis to read " + numEvents + " events or " + megaBytesPerSecond + " MB/sec");
    }


    @Override
    protected RecordWriter createWriter(File file, TocWriter tocWriter, boolean compressed, int uncompressedBlockSize) throws IOException {
        return new ByteArraySchemaRecordWriter(file, tocWriter, compressed, uncompressedBlockSize);
    }


    @Override
    protected RecordReader createReader(InputStream in, String journalFilename, TocReader tocReader, int maxAttributeSize) throws IOException {
        final ByteArraySchemaRecordReader reader = new ByteArraySchemaRecordReader(in, journalFilename, tocReader, maxAttributeSize);
        return reader;
    }

    private static interface WriteRecordInterceptor {
        void writeRawRecord(ProvenanceEventRecord event, long recordIdentifier, DataOutputStream out) throws IOException;
    }

    private static WriteRecordInterceptor NOP_INTERCEPTOR = (event, id, out) -> {};
    private static WriteRecordInterceptor WRITE_DUMMY_STRING_INTERCEPTOR = (event, id, out) -> out.writeUTF("hello");
}
