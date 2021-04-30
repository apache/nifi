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

import org.apache.nifi.provenance.schema.EventFieldNames;
import org.apache.nifi.provenance.schema.EventIdFirstHeaderSchema;
import org.apache.nifi.provenance.schema.LookupTableEventRecord;
import org.apache.nifi.provenance.schema.LookupTableEventSchema;
import org.apache.nifi.provenance.serialization.CompressableRecordWriter;
import org.apache.nifi.provenance.serialization.StorageSummary;
import org.apache.nifi.provenance.toc.TocWriter;
import org.apache.nifi.repository.schema.FieldMapRecord;
import org.apache.nifi.repository.schema.Record;
import org.apache.nifi.repository.schema.RecordSchema;
import org.apache.nifi.repository.schema.SchemaRecordWriter;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class EventIdFirstSchemaRecordWriter extends CompressableRecordWriter {
    private static final RecordSchema eventSchema = LookupTableEventSchema.EVENT_SCHEMA;
    private static final RecordSchema contentClaimSchema = new RecordSchema(eventSchema.getField(EventFieldNames.CONTENT_CLAIM).getSubFields());
    private static final RecordSchema previousContentClaimSchema = new RecordSchema(eventSchema.getField(EventFieldNames.PREVIOUS_CONTENT_CLAIM).getSubFields());
    private static final RecordSchema headerSchema = EventIdFirstHeaderSchema.SCHEMA;

    public static final int SERIALIZATION_VERSION = 1;
    public static final String SERIALIZATION_NAME = "EventIdFirstSchemaRecordWriter";
    private final IdentifierLookup idLookup;

    private final SchemaRecordWriter schemaRecordWriter = new SchemaRecordWriter();
    private final AtomicInteger recordCount = new AtomicInteger(0);

    private final Map<String, Integer> componentIdMap;
    private final Map<String, Integer> componentTypeMap;
    private final Map<String, Integer> queueIdMap;
    private static final Map<String, Integer> eventTypeMap;
    private static final List<String> eventTypeNames;

    private long firstEventId;
    private long systemTimeOffset;

    static {
        eventTypeMap = new HashMap<>();
        eventTypeNames = new ArrayList<>();

        int count = 0;
        for (final ProvenanceEventType eventType : ProvenanceEventType.values()) {
            eventTypeMap.put(eventType.name(), count++);
            eventTypeNames.add(eventType.name());
        }
    }

    public EventIdFirstSchemaRecordWriter(final File file, final AtomicLong idGenerator, final TocWriter writer, final boolean compressed,
        final int uncompressedBlockSize, final IdentifierLookup idLookup) throws IOException {
        super(file, idGenerator, writer, compressed, uncompressedBlockSize);

        this.idLookup = idLookup;
        componentIdMap = idLookup.invertComponentIdentifiers();
        componentTypeMap = idLookup.invertComponentTypes();
        queueIdMap = idLookup.invertQueueIdentifiers();
    }


    @Override
    public Map<ProvenanceEventRecord, StorageSummary> writeRecords(final Iterable<ProvenanceEventRecord> events) throws IOException {
        if (isDirty()) {
            throw new IOException("Cannot update Provenance Repository because this Record Writer has already failed to write to the Repository");
        }

        final int heapThreshold = 1_000_000;
        final Map<ProvenanceEventRecord, StorageSummary> storageSummaries = new HashMap<>();

        final Map<ProvenanceEventRecord, byte[]> serializedEvents = new LinkedHashMap<>();
        int totalBytes = 0;

        for (final ProvenanceEventRecord event : events) {
            final byte[] serialized = serializeEvent(event);
            serializedEvents.put(event, serialized);
            totalBytes += serialized.length;

            if (totalBytes >= heapThreshold) {
                storeEvents(serializedEvents, storageSummaries);
                recordCount.addAndGet(serializedEvents.size());
                serializedEvents.clear();
                totalBytes = 0;
            }
        }

        storeEvents(serializedEvents, storageSummaries);
        recordCount.addAndGet(serializedEvents.size());

        return storageSummaries;
    }

    protected byte[] serializeEvent(final ProvenanceEventRecord event) throws IOException {
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream();
             final DataOutputStream dataOutputStream = new DataOutputStream(baos)) {
            writeRecord(event, 0L, dataOutputStream);
            dataOutputStream.flush();
            return baos.toByteArray();
        }
    }

    private synchronized void storeEvents(final Map<ProvenanceEventRecord, byte[]> serializedEvents, final Map<ProvenanceEventRecord, StorageSummary> summaryMap) throws IOException {
        for (final Map.Entry<ProvenanceEventRecord, byte[]> entry : serializedEvents.entrySet()) {
            final ProvenanceEventRecord event = entry.getKey();
            final byte[] serialized = entry.getValue();

            final long startBytes;
            final long endBytes;
            final long recordIdentifier;

            try {
                recordIdentifier = event.getEventId() == -1 ? getIdGenerator().getAndIncrement() : event.getEventId();
                startBytes = getBytesWritten();

                ensureStreamState(recordIdentifier, startBytes);

                final DataOutputStream out = getBufferedOutputStream();
                final int recordIdOffset = (int) (recordIdentifier - firstEventId);
                out.writeInt(recordIdOffset);

                out.writeInt(serialized.length);
                out.write(serialized);

                endBytes = getBytesWritten();
            } catch (final IOException ioe) {
                markDirty();
                throw ioe;
            }

            final long serializedLength = endBytes - startBytes;
            final TocWriter tocWriter = getTocWriter();
            final Integer blockIndex = tocWriter == null ? null : tocWriter.getCurrentBlockIndex();
            final File file = getFile();
            final String storageLocation = file.getParentFile().getName() + "/" + file.getName();
            final StorageSummary storageSummary = new StorageSummary(recordIdentifier, storageLocation, blockIndex, serializedLength, endBytes);
            summaryMap.put(event, storageSummary);
        }
    }

    @Override
    public StorageSummary writeRecord(final ProvenanceEventRecord record) {
        // This method should never be called because it's only called by super.writeRecords. That method is overridden in this class and never delegates to this method.
        throw new UnsupportedOperationException();
    }

    @Override
    public int getRecordsWritten() {
        return recordCount.get();
    }

    protected Record createRecord(final ProvenanceEventRecord event, final long eventId) {
        return new LookupTableEventRecord(event, eventId, eventSchema, contentClaimSchema, previousContentClaimSchema, firstEventId, systemTimeOffset,
            componentIdMap, componentTypeMap, queueIdMap, eventTypeMap);
    }

    @Override
    protected void writeRecord(final ProvenanceEventRecord event, final long eventId, final DataOutputStream out) throws IOException {
        final Record eventRecord = createRecord(event, eventId);
        schemaRecordWriter.writeRecord(eventRecord, out);
    }

    @Override
    protected synchronized void writeHeader(final long firstEventId, final DataOutputStream out) throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        eventSchema.writeTo(baos);

        out.writeInt(baos.size());
        baos.writeTo(out);

        baos.reset();
        headerSchema.writeTo(baos);
        out.writeInt(baos.size());
        baos.writeTo(out);

        this.firstEventId = firstEventId;
        this.systemTimeOffset = System.currentTimeMillis();

        final Map<String, Object> headerValues = new HashMap<>();
        headerValues.put(EventIdFirstHeaderSchema.FieldNames.FIRST_EVENT_ID, firstEventId);
        headerValues.put(EventIdFirstHeaderSchema.FieldNames.TIMESTAMP_OFFSET, systemTimeOffset);
        headerValues.put(EventIdFirstHeaderSchema.FieldNames.COMPONENT_IDS, idLookup.getComponentIdentifiers());
        headerValues.put(EventIdFirstHeaderSchema.FieldNames.COMPONENT_TYPES, idLookup.getComponentTypes());
        headerValues.put(EventIdFirstHeaderSchema.FieldNames.QUEUE_IDS, idLookup.getQueueIdentifiers());
        headerValues.put(EventIdFirstHeaderSchema.FieldNames.EVENT_TYPES, eventTypeNames);
        final FieldMapRecord headerInfo = new FieldMapRecord(headerSchema, headerValues);

        schemaRecordWriter.writeRecord(headerInfo, out);
    }

    @Override
    protected int getSerializationVersion() {
        return SERIALIZATION_VERSION;
    }

    @Override
    protected String getSerializationName() {
        return SERIALIZATION_NAME;
    }
}
