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
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import org.apache.nifi.provenance.schema.EventFieldNames;
import org.apache.nifi.provenance.schema.EventIdFirstHeaderSchema;
import org.apache.nifi.provenance.schema.LookupTableEventRecord;
import org.apache.nifi.provenance.schema.LookupTableEventSchema;
import org.apache.nifi.provenance.toc.TocReader;
import org.apache.nifi.repository.schema.Record;
import org.apache.nifi.repository.schema.RecordSchema;
import org.apache.nifi.stream.io.LimitingInputStream;
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

    public EncryptedSchemaRecordReader(final InputStream inputStream, final String filename, final TocReader tocReader, final int maxAttributeChars, KeyProvider keyProvider, ProvenanceEventEncryptor provenanceEventEncryptor) throws IOException {
        this(inputStream, filename, tocReader, maxAttributeChars, keyProvider, provenanceEventEncryptor, DEFAULT_DEBUG_FREQUENCY);
    }

    public EncryptedSchemaRecordReader(final InputStream inputStream, final String filename, final TocReader tocReader, final int maxAttributeChars, KeyProvider keyProvider, ProvenanceEventEncryptor provenanceEventEncryptor, int debugFrequency) throws IOException {
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

    private byte[] decrypt(byte[] ivAndCipherBytes) throws IOException, EncryptionException {
        String keyId = getKeyId();

        // TODO: Delegate to encryptor with proper error-checking and customization
        try {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            // TODO: Clean up
            final byte[] SENTINEL = new byte[]{ 0x01};
            // Detect if the first byte is the sentinel and remove it before attempting to decrypt
            if (Arrays.equals(Arrays.copyOfRange(ivAndCipherBytes, 0, 1), SENTINEL)) {
                ivAndCipherBytes = Arrays.copyOfRange(ivAndCipherBytes, 1, ivAndCipherBytes.length);
            }

            // TODO: IV is constant for initial testing
            byte[] ivBytes = Arrays.copyOfRange(ivAndCipherBytes, 0, 16);
            IvParameterSpec iv = new IvParameterSpec(ivBytes);

            // TODO: Need to deserialize and parse encryption details fields (algo, IV, keyId, version) outside of decryption
            byte[] cipherBytes = Arrays.copyOfRange(ivAndCipherBytes, 16, ivAndCipherBytes.length);

            SecretKey key = keyProvider.getKey(keyId);
            cipher.init(Cipher.DECRYPT_MODE, key, iv);

            byte[] plainBytes = cipher.doFinal(cipherBytes);
            return plainBytes;
        } catch (NoSuchAlgorithmException | NoSuchPaddingException | BadPaddingException | IllegalBlockSizeException | InvalidAlgorithmParameterException | InvalidKeyException | KeyManagementException e) {
            logger.error("Encountered an error: ", e);
            throw new EncryptionException(e);
        }
    }

    public String getKeyId() {
        return keyId;
    }
}
