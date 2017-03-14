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
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.nifi.provenance.schema.LookupTableEventRecord;
import org.apache.nifi.provenance.toc.TocReader;
import org.apache.nifi.repository.schema.Record;
import org.apache.nifi.stream.io.LimitingInputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.timebuffer.LongEntityAccess;
import org.apache.nifi.util.timebuffer.TimedBuffer;
import org.apache.nifi.util.timebuffer.TimestampedLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EncryptedSchemaRecordReader extends EventIdFirstSchemaRecordReader {
    private static final Logger logger = LoggerFactory.getLogger(EncryptedSchemaRecordReader.class);

    private static final int DEFAULT_DEBUG_FREQUENCY = 1_000_000;

    private ProvenanceEventEncryptor provenanceEventEncryptor;

    private static final TimedBuffer<TimestampedLong> decryptTimes = new TimedBuffer<>(TimeUnit.SECONDS, 60, new LongEntityAccess());

    private int debugFrequency = DEFAULT_DEBUG_FREQUENCY;
    public static final int SERIALIZATION_VERSION = 1;

    public static final String SERIALIZATION_NAME = "EncryptedSchemaRecordWriter";

    public EncryptedSchemaRecordReader(final InputStream inputStream, final String filename, final TocReader tocReader, final int maxAttributeChars,
                                       ProvenanceEventEncryptor provenanceEventEncryptor) throws IOException {
        this(inputStream, filename, tocReader, maxAttributeChars, provenanceEventEncryptor, DEFAULT_DEBUG_FREQUENCY);
    }

    public EncryptedSchemaRecordReader(final InputStream inputStream, final String filename, final TocReader tocReader, final int maxAttributeChars,
                                       ProvenanceEventEncryptor provenanceEventEncryptor, int debugFrequency) throws IOException {
        super(inputStream, filename, tocReader, maxAttributeChars);
        this.provenanceEventEncryptor = provenanceEventEncryptor;
        this.debugFrequency = debugFrequency;
    }

    @Override
    protected StandardProvenanceEventRecord nextRecord(final DataInputStream in, final int serializationVersion) throws IOException {
        verifySerializationVersion(serializationVersion);

        final long byteOffset = getBytesConsumed();
        final long eventId = in.readInt() + getFirstEventId();
        final int recordLength = in.readInt();

        return readRecord(in, eventId, byteOffset, recordLength);
    }

    private StandardProvenanceEventRecord readRecord(final DataInputStream inputStream, final long eventId, final long startOffset, final int recordLength) throws IOException {
        try {
            final InputStream limitedIn = new LimitingInputStream(inputStream, recordLength);

            byte[] encryptedSerializedBytes = new byte[recordLength];
            DataInputStream encryptedInputStream = new DataInputStream(limitedIn);
            encryptedInputStream.readFully(encryptedSerializedBytes);

            byte[] plainSerializedBytes = decrypt(encryptedSerializedBytes, Long.toString(eventId));
            InputStream plainStream = new ByteArrayInputStream(plainSerializedBytes);

            final Record eventRecord = getRecordReader().readRecord(plainStream);
            if (eventRecord == null) {
                return null;
            }

            final StandardProvenanceEventRecord deserializedEvent = LookupTableEventRecord.getEvent(eventRecord, getFilename(), startOffset, getMaxAttributeLength(),
                    getFirstEventId(), getSystemTimeOffset(), getComponentIds(), getComponentTypes(), getQueueIds(), getEventTypes());
            deserializedEvent.setEventId(eventId);
            return deserializedEvent;
        } catch (EncryptionException e) {
            logger.error("Encountered an error reading the record: ", e);
            throw new IOException(e);
        }
    }

    // TODO: Copied from EventIdFirstSchemaRecordReader to force local/overridden readRecord()
    @Override
    protected Optional<StandardProvenanceEventRecord> readToEvent(final long eventId, final DataInputStream dis, final int serializationVersion) throws IOException {
        verifySerializationVersion(serializationVersion);

        while (isData(dis)) {
            final long startOffset = getBytesConsumed();
            final long id = dis.readInt() + getFirstEventId();
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

    private byte[] decrypt(byte[] encryptedBytes, String eventId) throws IOException, EncryptionException {
        try {
            return provenanceEventEncryptor.decrypt(encryptedBytes, eventId);
        } catch (Exception e) {
            logger.error("Encountered an error: ", e);
            throw new EncryptionException(e);
        }
    }

    @Override
    public String toString() {
        return getDescription();
    }

    private String getDescription() {
        try {
            return "EncryptedSchemaRecordReader, toc: " + getTocReader().getFile().getAbsolutePath() + ", journal: " + getFilename();
        } catch (Exception e) {
            return "EncryptedSchemaRecordReader@" + Integer.toHexString(this.hashCode());
        }
    }

    /**
     * Sets the encryptor to use (necessary because the
     * {@link org.apache.nifi.provenance.serialization.RecordReaders#newRecordReader(File, Collection, int)} method doesn't accept the encryptor.
     *
     * @param provenanceEventEncryptor the encryptor
     */
    void setProvenanceEventEncryptor(ProvenanceEventEncryptor provenanceEventEncryptor) {
        this.provenanceEventEncryptor = provenanceEventEncryptor;
    }
}
