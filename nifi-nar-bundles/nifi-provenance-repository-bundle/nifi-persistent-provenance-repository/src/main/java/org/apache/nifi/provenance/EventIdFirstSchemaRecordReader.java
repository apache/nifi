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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;
import org.apache.nifi.provenance.schema.EventIdFirstHeaderSchema;
import org.apache.nifi.provenance.schema.LookupTableEventRecord;
import org.apache.nifi.provenance.serialization.CompressableRecordReader;
import org.apache.nifi.provenance.toc.TocReader;
import org.apache.nifi.repository.schema.Record;
import org.apache.nifi.repository.schema.RecordSchema;
import org.apache.nifi.repository.schema.SchemaRecordReader;
import org.apache.nifi.stream.io.LimitingInputStream;
import org.apache.nifi.stream.io.StreamUtils;

public class EventIdFirstSchemaRecordReader extends CompressableRecordReader {
    RecordSchema getSchema() {
        return schema;
    }

    SchemaRecordReader getRecordReader() {
        return recordReader;
    }

    private RecordSchema schema; // effectively final
    private SchemaRecordReader recordReader;  // effectively final

    private List<String> componentIds;
    private List<String> componentTypes;
    private List<String> queueIds;
    private List<String> eventTypes;
    private long firstEventId;

    List<String> getComponentIds() {
        return componentIds;
    }

    List<String> getComponentTypes() {
        return componentTypes;
    }

    List<String> getQueueIds() {
        return queueIds;
    }

    List<String> getEventTypes() {
        return eventTypes;
    }

    long getFirstEventId() {
        return firstEventId;
    }

    long getSystemTimeOffset() {
        return systemTimeOffset;
    }

    private long systemTimeOffset;

    public EventIdFirstSchemaRecordReader(final InputStream in, final String filename, final TocReader tocReader, final int maxAttributeChars) throws IOException {
        super(in, filename, tocReader, maxAttributeChars);
    }

    protected void verifySerializationVersion(final int serializationVersion) {
        if (serializationVersion > EventIdFirstSchemaRecordWriter.SERIALIZATION_VERSION) {
            throw new IllegalArgumentException("Unable to deserialize record because the version is " + serializationVersion
                    + " and supported versions are 1-" + EventIdFirstSchemaRecordWriter.SERIALIZATION_VERSION);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected synchronized void readHeader(final DataInputStream in, final int serializationVersion) throws IOException {
        verifySerializationVersion(serializationVersion);
        final int eventSchemaLength = in.readInt();
        final byte[] buffer = new byte[eventSchemaLength];
        StreamUtils.fillBuffer(in, buffer);

        try (final ByteArrayInputStream bais = new ByteArrayInputStream(buffer)) {
            schema = RecordSchema.readFrom(bais);
        }

        recordReader = SchemaRecordReader.fromSchema(schema);

        final int headerSchemaLength = in.readInt();
        final byte[] headerSchemaBuffer = new byte[headerSchemaLength];
        StreamUtils.fillBuffer(in, headerSchemaBuffer);

        final RecordSchema headerSchema;
        try (final ByteArrayInputStream bais = new ByteArrayInputStream(headerSchemaBuffer)) {
            headerSchema = RecordSchema.readFrom(bais);
        }

        final SchemaRecordReader headerReader = SchemaRecordReader.fromSchema(headerSchema);
        final Record headerRecord = headerReader.readRecord(in);
        componentIds = (List<String>) headerRecord.getFieldValue(EventIdFirstHeaderSchema.FieldNames.COMPONENT_IDS);
        componentTypes = (List<String>) headerRecord.getFieldValue(EventIdFirstHeaderSchema.FieldNames.COMPONENT_TYPES);
        queueIds = (List<String>) headerRecord.getFieldValue(EventIdFirstHeaderSchema.FieldNames.QUEUE_IDS);
        eventTypes = (List<String>) headerRecord.getFieldValue(EventIdFirstHeaderSchema.FieldNames.EVENT_TYPES);
        firstEventId = (Long) headerRecord.getFieldValue(EventIdFirstHeaderSchema.FieldNames.FIRST_EVENT_ID);
        systemTimeOffset = (Long) headerRecord.getFieldValue(EventIdFirstHeaderSchema.FieldNames.TIMESTAMP_OFFSET);
    }

    @Override
    protected StandardProvenanceEventRecord nextRecord(final DataInputStream in, final int serializationVersion) throws IOException {
        verifySerializationVersion(serializationVersion);

        final long byteOffset = getBytesConsumed();
        final long eventId = in.readInt() + firstEventId;
        final int recordLength = in.readInt();

        return readRecord(in, eventId, byteOffset, recordLength);
    }

    private StandardProvenanceEventRecord readRecord(final DataInputStream in, final long eventId, final long startOffset, final int recordLength) throws IOException {
        final InputStream limitedIn = new LimitingInputStream(in, recordLength);

        final Record eventRecord = recordReader.readRecord(limitedIn);
        if (eventRecord == null) {
            return null;
        }

        final StandardProvenanceEventRecord deserializedEvent = LookupTableEventRecord.getEvent(eventRecord, getFilename(), startOffset, getMaxAttributeLength(),
                firstEventId, systemTimeOffset, componentIds, componentTypes, queueIds, eventTypes);
        deserializedEvent.setEventId(eventId);
        return deserializedEvent;
    }

    protected boolean isData(final InputStream in) throws IOException {
        in.mark(1);
        final int nextByte = in.read();
        in.reset();

        return nextByte > -1;
    }

    @Override
    protected Optional<StandardProvenanceEventRecord> readToEvent(final long eventId, final DataInputStream dis, final int serializationVersion) throws IOException {
        verifySerializationVersion(serializationVersion);

        while (isData(dis)) {
            final long startOffset = getBytesConsumed();
            final long id = dis.readInt() + firstEventId;
            final int recordLength = dis.readInt();

            if (id >= eventId) {
                final StandardProvenanceEventRecord event = readRecord(dis, id, startOffset, recordLength);
                return Optional.ofNullable(event);
            } else {
                // This is not the record we want. Skip over it instead of deserializing it.
                StreamUtils.skip(dis, recordLength);
            }
        }

        return Optional.empty();
    }

    @Override
    public String toString() {
        return getDescription();
    }

    private String getDescription() {
        try {
            return "EventIdFirstSchemaRecordReader, toc: " + getTocReader().getFile().getAbsolutePath() + ", journal: " + getFilename();
        } catch (Exception e) {
            return "EventIdFirstSchemaRecordReader@" + Integer.toHexString(this.hashCode());
        }
    }
}
