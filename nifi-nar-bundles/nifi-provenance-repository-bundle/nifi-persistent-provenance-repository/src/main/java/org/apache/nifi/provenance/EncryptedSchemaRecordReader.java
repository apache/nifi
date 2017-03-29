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
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import org.apache.nifi.provenance.schema.EventFieldNames;
import org.apache.nifi.provenance.schema.EventIdFirstHeaderSchema;
import org.apache.nifi.provenance.schema.LookupTableEventRecord;
import org.apache.nifi.provenance.schema.LookupTableEventSchema;
import org.apache.nifi.provenance.toc.TocReader;
import org.apache.nifi.repository.schema.Record;
import org.apache.nifi.repository.schema.RecordSchema;
import org.apache.nifi.security.util.EncryptionMethod;
import org.apache.nifi.security.util.crypto.AESKeyedCipherProvider;
import org.apache.nifi.stream.io.LimitingInputStream;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.timebuffer.LongEntityAccess;
import org.apache.nifi.util.timebuffer.TimedBuffer;
import org.apache.nifi.util.timebuffer.TimestampedLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EncryptedSchemaRecordReader extends EventIdFirstSchemaRecordReader {
    private static final Logger logger = LoggerFactory.getLogger(EncryptedSchemaRecordReader.class);

    private static final RecordSchema eventSchema = LookupTableEventSchema.ENCRYPTED_EVENT_SCHEMA;
    private static final RecordSchema contentClaimSchema = new RecordSchema(eventSchema.getField(EventFieldNames.CONTENT_CLAIM).getSubFields());
    private static final RecordSchema previousContentClaimSchema = new RecordSchema(eventSchema.getField(EventFieldNames.PREVIOUS_CONTENT_CLAIM).getSubFields());
    private static final RecordSchema headerSchema = EventIdFirstHeaderSchema.SCHEMA;
    private static final int DEFAULT_DEBUG_FREQUENCY = 1_000_000;

    private KeyProvider keyProvider;
    private ProvenanceEventEncryptor provenanceEventEncryptor;

    private static final TimedBuffer<TimestampedLong> decryptTimes = new TimedBuffer<>(TimeUnit.SECONDS, 60, new LongEntityAccess());

    private String keyId;

    private int debugFrequency;
    public static final int SERIALIZATION_VERSION = 1;

    public static final String SERIALIZATION_NAME = "EncryptedSchemaRecordWriter";

    public EncryptedSchemaRecordReader(final InputStream inputStream, final String filename, final TocReader tocReader, final int maxAttributeChars, KeyProvider keyProvider,
                                       ProvenanceEventEncryptor provenanceEventEncryptor) throws IOException {
        this(inputStream, filename, tocReader, maxAttributeChars, keyProvider, provenanceEventEncryptor, DEFAULT_DEBUG_FREQUENCY);
    }

    public EncryptedSchemaRecordReader(final InputStream inputStream, final String filename, final TocReader tocReader, final int maxAttributeChars, KeyProvider keyProvider,
                                       ProvenanceEventEncryptor provenanceEventEncryptor, int debugFrequency) throws IOException {
        super(inputStream, filename, tocReader, maxAttributeChars);
        this.keyProvider = keyProvider;
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

            byte[] plainSerializedBytes = decrypt(encryptedSerializedBytes);
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

    private byte[] decrypt(byte[] ivAndCipherBytes) throws IOException, EncryptionException {
        String keyId = getKeyId();
        try {
            // TODO: Clean up
            final byte[] SENTINEL = new byte[]{ 0x01};
            // Detect if the first byte is the sentinel and remove it before attempting to decrypt
            if (Arrays.equals(Arrays.copyOfRange(ivAndCipherBytes, 0, 1), SENTINEL)) {
                ivAndCipherBytes = Arrays.copyOfRange(ivAndCipherBytes, 1, ivAndCipherBytes.length);
            }
            byte[] ivBytes = Arrays.copyOfRange(ivAndCipherBytes, 0, 16);

            // TODO: Need to deserialize and parse encryption details fields (algo, IV, keyId, version) outside of decryption
            byte[] cipherBytes = Arrays.copyOfRange(ivAndCipherBytes, 16, ivAndCipherBytes.length);

            SecretKey key = keyProvider.getKey(keyId);
            Cipher cipher = new AESKeyedCipherProvider().getCipher(EncryptionMethod.AES_GCM, key, ivBytes, false);

            byte[] plainBytes = cipher.doFinal(cipherBytes);
            return plainBytes;
        } catch (Exception e) {
            logger.error("Encountered an error: ", e);
            throw new EncryptionException(e);
        }
    }

    public String getKeyId() {
        return keyId;
    }

    @Override
    public String toString() {
        return getDescription();
    }

    private String getDescription() {
        try {
            return "EncryptedSchemaRecordReader, toc: " + getTocReader().getFile().getAbsolutePath() + ", journal: " + getFilename() + ", keyId: " + getKeyId();
        } catch (Exception e) {
            return "EncryptedSchemaRecordReader@" + Integer.toHexString(this.hashCode());
        }
    }
}
