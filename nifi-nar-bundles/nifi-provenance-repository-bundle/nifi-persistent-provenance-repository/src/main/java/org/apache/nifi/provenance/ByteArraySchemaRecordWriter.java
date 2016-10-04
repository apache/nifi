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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.nifi.provenance.schema.EventRecord;
import org.apache.nifi.provenance.schema.EventRecordFields;
import org.apache.nifi.provenance.schema.ProvenanceEventSchema;
import org.apache.nifi.provenance.serialization.CompressableRecordWriter;
import org.apache.nifi.provenance.toc.TocWriter;
import org.apache.nifi.repository.schema.Record;
import org.apache.nifi.repository.schema.RecordSchema;
import org.apache.nifi.repository.schema.SchemaRecordWriter;
import org.apache.nifi.stream.io.DataOutputStream;

public class ByteArraySchemaRecordWriter extends CompressableRecordWriter {
    private static final RecordSchema eventSchema = ProvenanceEventSchema.PROVENANCE_EVENT_SCHEMA_V1;
    private static final RecordSchema contentClaimSchema = new RecordSchema(eventSchema.getField(EventRecordFields.Names.CONTENT_CLAIM).getSubFields());
    public static final int SERIALIZATION_VERSION = 1;
    public static final String SERIALIZATION_NAME = "ByteArraySchemaRecordWriter";

    private final SchemaRecordWriter recordWriter = new SchemaRecordWriter();

    public ByteArraySchemaRecordWriter(final File file, final TocWriter tocWriter, final boolean compressed, final int uncompressedBlockSize) throws IOException {
        super(file, tocWriter, compressed, uncompressedBlockSize);
    }

    public ByteArraySchemaRecordWriter(final OutputStream out, final TocWriter tocWriter, final boolean compressed, final int uncompressedBlockSize) throws IOException {
        super(out, tocWriter, compressed, uncompressedBlockSize);
    }

    @Override
    protected String getSerializationName() {
        return SERIALIZATION_NAME;
    }

    @Override
    protected int getSerializationVersion() {
        return SERIALIZATION_VERSION;
    }

    @Override
    public void writeHeader(final long firstEventId, final DataOutputStream out) throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        eventSchema.writeTo(baos);

        out.writeInt(baos.size());
        baos.writeTo(out);
    }

    protected Record createRecord(final ProvenanceEventRecord event, final long eventId) {
        return new EventRecord(event, eventId, eventSchema, contentClaimSchema);
    }

    @Override
    protected void writeRecord(final ProvenanceEventRecord event, final long eventId, final DataOutputStream out) throws IOException {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream(256)) {

            final Record eventRecord = createRecord(event, eventId);
            recordWriter.writeRecord(eventRecord, baos);

            out.writeInt(baos.size());
            baos.writeTo(out);
        }
    }
}
