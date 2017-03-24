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
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
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
import org.apache.nifi.util.timebuffer.LongEntityAccess;
import org.apache.nifi.util.timebuffer.TimedBuffer;
import org.apache.nifi.util.timebuffer.TimestampedLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventIdFirstSchemaRecordWriter extends CompressableRecordWriter {
    private static final Logger logger = LoggerFactory.getLogger(EventIdFirstSchemaRecordWriter.class);

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

    private static final TimedBuffer<TimestampedLong> serializeTimes = new TimedBuffer<>(TimeUnit.SECONDS, 60, new LongEntityAccess());
    private static final TimedBuffer<TimestampedLong> lockTimes = new TimedBuffer<>(TimeUnit.SECONDS, 60, new LongEntityAccess());
    private static final TimedBuffer<TimestampedLong> writeTimes = new TimedBuffer<>(TimeUnit.SECONDS, 60, new LongEntityAccess());
    private static final TimedBuffer<TimestampedLong> bytesWritten = new TimedBuffer<>(TimeUnit.SECONDS, 60, new LongEntityAccess());
    private static final AtomicLong totalRecordCount = new AtomicLong(0L);

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

    public EventIdFirstSchemaRecordWriter(final OutputStream out, final String storageLocation, final AtomicLong idGenerator, final TocWriter tocWriter, final boolean compressed,
        final int uncompressedBlockSize, final IdentifierLookup idLookup) throws IOException {
        super(out, storageLocation, idGenerator, tocWriter, compressed, uncompressedBlockSize);

        this.idLookup = idLookup;
        componentIdMap = idLookup.invertComponentIdentifiers();
        componentTypeMap = idLookup.invertComponentTypes();
        queueIdMap = idLookup.invertQueueIdentifiers();
    }

    @Override
    public StorageSummary writeRecord(final ProvenanceEventRecord record) throws IOException {
        if (isDirty()) {
            throw new IOException("Cannot update Provenance Repository because this Record Writer has already failed to write to the Repository");
        }

        final long serializeStart = System.nanoTime();
        final byte[] serialized;
        try (final ByteArrayOutputStream baos = new ByteArrayOutputStream(256);
            final DataOutputStream dos = new DataOutputStream(baos)) {
            writeRecord(record, 0L, dos);
            serialized = baos.toByteArray();
        }

        final long lockStart = System.nanoTime();
        final long writeStart;
        final long startBytes;
        final long endBytes;
        final long recordIdentifier;
        synchronized (this) {
            writeStart = System.nanoTime();
            try {
                recordIdentifier = record.getEventId() == -1L ? getIdGenerator().getAndIncrement() : record.getEventId();
                startBytes = getBytesWritten();

                ensureStreamState(recordIdentifier, startBytes);

                final DataOutputStream out = getBufferedOutputStream();
                final int recordIdOffset = (int) (recordIdentifier - firstEventId);
                out.writeInt(recordIdOffset);
                out.writeInt(serialized.length);
                out.write(serialized);

                recordCount.incrementAndGet();
                endBytes = getBytesWritten();
            } catch (final IOException ioe) {
                markDirty();
                throw ioe;
            }
        }

        if (logger.isDebugEnabled()) {
            // Collect stats and periodically dump them if log level is set to at least info.
            final long writeNanos = System.nanoTime() - writeStart;
            writeTimes.add(new TimestampedLong(writeNanos));

            final long serializeNanos = lockStart - serializeStart;
            serializeTimes.add(new TimestampedLong(serializeNanos));

            final long lockNanos = writeStart - lockStart;
            lockTimes.add(new TimestampedLong(lockNanos));
            bytesWritten.add(new TimestampedLong(endBytes - startBytes));

            final long recordCount = totalRecordCount.incrementAndGet();
            if (recordCount % 1_000_000 == 0) {
                final long sixtySecondsAgo = System.currentTimeMillis() - 60000L;
                final Long writeNanosLast60 = writeTimes.getAggregateValue(sixtySecondsAgo).getValue();
                final Long lockNanosLast60 = lockTimes.getAggregateValue(sixtySecondsAgo).getValue();
                final Long serializeNanosLast60 = serializeTimes.getAggregateValue(sixtySecondsAgo).getValue();
                final Long bytesWrittenLast60 = bytesWritten.getAggregateValue(sixtySecondsAgo).getValue();
                logger.debug("In the last 60 seconds, have spent {} millis writing to file ({} MB), {} millis waiting on synchronize block, {} millis serializing events",
                    TimeUnit.NANOSECONDS.toMillis(writeNanosLast60),
                    bytesWrittenLast60 / 1024 / 1024,
                    TimeUnit.NANOSECONDS.toMillis(lockNanosLast60),
                    TimeUnit.NANOSECONDS.toMillis(serializeNanosLast60));
            }
        }

        final long serializedLength = endBytes - startBytes;
        final TocWriter tocWriter = getTocWriter();
        final Integer blockIndex = tocWriter == null ? null : tocWriter.getCurrentBlockIndex();
        final File file = getFile();
        final String storageLocation = file.getParentFile().getName() + "/" + file.getName();
        return new StorageSummary(recordIdentifier, storageLocation, blockIndex, serializedLength, endBytes);
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

    /* Getters for internal state written to by subclass EncryptedSchemaRecordWriter */

    IdentifierLookup getIdLookup() {
        return idLookup;
    }

    SchemaRecordWriter getSchemaRecordWriter() {
        return schemaRecordWriter;
    }

    AtomicInteger getRecordCount() {
        return recordCount;
    }

    static TimedBuffer<TimestampedLong> getSerializeTimes() {
        return serializeTimes;
    }

    static TimedBuffer<TimestampedLong> getLockTimes() {
        return lockTimes;
    }

    static TimedBuffer<TimestampedLong> getWriteTimes() {
        return writeTimes;
    }

    static TimedBuffer<TimestampedLong> getBytesWrittenBuffer() {
        return bytesWritten;
    }

    static AtomicLong getTotalRecordCount() {
        return totalRecordCount;
    }

    long getFirstEventId() {
        return firstEventId;
    }

    long getSystemTimeOffset() {
        return systemTimeOffset;
    }

}
