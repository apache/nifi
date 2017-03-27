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
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import org.apache.nifi.provenance.schema.EventFieldNames;
import org.apache.nifi.provenance.schema.EventIdFirstHeaderSchema;
import org.apache.nifi.provenance.schema.LookupTableEventSchema;
import org.apache.nifi.provenance.serialization.StorageSummary;
import org.apache.nifi.provenance.toc.TocWriter;
import org.apache.nifi.repository.schema.RecordSchema;
import org.apache.nifi.util.timebuffer.LongEntityAccess;
import org.apache.nifi.util.timebuffer.TimedBuffer;
import org.apache.nifi.util.timebuffer.TimestampedLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EncryptedSchemaRecordWriter extends EventIdFirstSchemaRecordWriter {
    private static final Logger logger = LoggerFactory.getLogger(EncryptedSchemaRecordWriter.class);

    private static final RecordSchema eventSchema = LookupTableEventSchema.ENCRYPTED_EVENT_SCHEMA;
    private static final RecordSchema contentClaimSchema = new RecordSchema(eventSchema.getField(EventFieldNames.CONTENT_CLAIM).getSubFields());
    private static final RecordSchema previousContentClaimSchema = new RecordSchema(eventSchema.getField(EventFieldNames.PREVIOUS_CONTENT_CLAIM).getSubFields());
    private static final RecordSchema headerSchema = EventIdFirstHeaderSchema.SCHEMA;
    private static final int DEFAULT_DEBUG_FREQUENCY = 1_000_000;

    private KeyProvider keyProvider;
    private ProvenanceEventEncryptor provenanceEventEncryptor;

    private static final TimedBuffer<TimestampedLong> encryptTimes = new TimedBuffer<>(TimeUnit.SECONDS, 60, new LongEntityAccess());

    private String keyId;

    private int debugFrequency;
    public static final int SERIALIZATION_VERSION = 1;

    public static final String SERIALIZATION_NAME = "EncryptedSchemaRecordWriter";

    public EncryptedSchemaRecordWriter(final File file, final AtomicLong idGenerator, final TocWriter writer, final boolean compressed,
                                       final int uncompressedBlockSize, final IdentifierLookup idLookup, KeyProvider keyProvider, ProvenanceEventEncryptor provenanceEventEncryptor) throws IOException {
      this(file, idGenerator, writer, compressed, uncompressedBlockSize, idLookup, keyProvider, provenanceEventEncryptor, DEFAULT_DEBUG_FREQUENCY);
    }

    public EncryptedSchemaRecordWriter(final File file, final AtomicLong idGenerator, final TocWriter writer, final boolean compressed,
                                       final int uncompressedBlockSize, final IdentifierLookup idLookup, KeyProvider keyProvider, ProvenanceEventEncryptor provenanceEventEncryptor, int debugFrequency) throws IOException {
        super(file, idGenerator, writer, compressed, uncompressedBlockSize, idLookup);
        this.keyProvider = keyProvider;
        this.provenanceEventEncryptor = provenanceEventEncryptor;
        this.debugFrequency = debugFrequency;
    }

    @Override
    public StorageSummary writeRecord(final ProvenanceEventRecord record) throws IOException {
        final long encryptStart = System.nanoTime();
        // TODO: May be temporary encryption & serialization; if I can get the normal SchemaRecordWriter to work, I'll use that
        byte[] cipherBytes;
        try {
            byte[] serialized;
            try (final ByteArrayOutputStream baos = new ByteArrayOutputStream(256);
                 final DataOutputStream dos = new DataOutputStream(baos)) {
                writeRecord(record, 0L, dos);
                serialized = baos.toByteArray();
            }
            cipherBytes = encrypt(serialized);
        } catch (EncryptionException e) {
            logger.error("Encountered an error: ", e);
            throw new IOException("Error encrypting the provenance record", e);
        }
        final long encryptStop = System.nanoTime();

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
                final int recordIdOffset = (int) (recordIdentifier - getFirstEventId());
                out.writeInt(recordIdOffset);
                out.writeInt(cipherBytes.length);
                out.write(cipherBytes);

                getRecordCount().incrementAndGet();
                endBytes = getBytesWritten();
            } catch (final IOException ioe) {
                markDirty();
                throw ioe;
            }
        }

        if (logger.isDebugEnabled()) {
            // Collect stats and periodically dump them if log level is set to at least info.
            final long writeNanos = System.nanoTime() - writeStart;
            getWriteTimes().add(new TimestampedLong(writeNanos));

            final long serializeNanos = lockStart - encryptStart;
            getSerializeTimes().add(new TimestampedLong(serializeNanos));

            final long encryptNanos = encryptStop - encryptStart;
            getEncryptTimes().add(new TimestampedLong(encryptNanos));

            final long lockNanos = writeStart - lockStart;
            getLockTimes().add(new TimestampedLong(lockNanos));
            getBytesWrittenBuffer().add(new TimestampedLong(endBytes - startBytes));

            final long recordCount = getTotalRecordCount().incrementAndGet();
            if (recordCount % debugFrequency == 0) {
                printStats();
            }
        }

        final long serializedLength = endBytes - startBytes;
        final TocWriter tocWriter = getTocWriter();
        final Integer blockIndex = tocWriter == null ? null : tocWriter.getCurrentBlockIndex();
        final File file = getFile();
        final String storageLocation = file.getParentFile().getName() + "/" + file.getName();
        return new StorageSummary(recordIdentifier, storageLocation, blockIndex, serializedLength, endBytes);
    }

    private void printStats() {
        final long sixtySecondsAgo = System.currentTimeMillis() - 60000L;
        final Long writeNanosLast60 = getWriteTimes().getAggregateValue(sixtySecondsAgo).getValue();
        final Long lockNanosLast60 = getLockTimes().getAggregateValue(sixtySecondsAgo).getValue();
        final Long serializeNanosLast60 = getSerializeTimes().getAggregateValue(sixtySecondsAgo).getValue();
        final Long encryptNanosLast60 = getEncryptTimes().getAggregateValue(sixtySecondsAgo).getValue();
        final Long bytesWrittenLast60 = getBytesWrittenBuffer().getAggregateValue(sixtySecondsAgo).getValue();
        logger.debug("In the last 60 seconds, have spent {} millis writing to file ({} MB), {} millis waiting on synchronize block, {} millis serializing events, {} millis encrypting events",
                TimeUnit.NANOSECONDS.toMillis(writeNanosLast60),
                bytesWrittenLast60 / 1024 / 1024,
                TimeUnit.NANOSECONDS.toMillis(lockNanosLast60),
                TimeUnit.NANOSECONDS.toMillis(serializeNanosLast60),
                TimeUnit.NANOSECONDS.toMillis(encryptNanosLast60));
    }

    static TimedBuffer<TimestampedLong> getEncryptTimes() {
        return encryptTimes;
    }

    private byte[] encrypt(byte[] serialized) throws IOException, EncryptionException {
       String keyId = getKeyId();
        // TODO: Delegate to encryptor with proper error-checking and customization
        try {
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            // TODO: IV is constant for initial testing
            byte[] ivBytes = new byte[16];
            IvParameterSpec iv = new IvParameterSpec(ivBytes);

            SecretKey key = keyProvider.getKey(keyId);
            cipher.init(Cipher.ENCRYPT_MODE, key, iv);

            byte[] cipherBytes = cipher.doFinal(serialized);

            // TODO: Need to serialize and concat encryption details fields (algo, IV, keyId, version) outside of encryption

            byte[] ivAndCipherBytes = new byte[16 + cipherBytes.length];
            System.arraycopy(ivBytes, 0, ivAndCipherBytes, 0, ivBytes.length);
            System.arraycopy(cipherBytes, 0, ivAndCipherBytes, 16, cipherBytes.length);

            // TODO: Refactor to use concatByteArrays() for performance
            // Add the sentinel byte of 0x01
            byte[] sentinelAndAllBytes = new byte[1 + ivAndCipherBytes.length];
            final byte[] SENTINEL = new byte[]{ 0x01};
            System.arraycopy(SENTINEL, 0, sentinelAndAllBytes, 0, 1);
            System.arraycopy(ivAndCipherBytes, 0, sentinelAndAllBytes, 1, ivAndCipherBytes.length);

            return sentinelAndAllBytes;
        } catch (NoSuchAlgorithmException | NoSuchPaddingException | BadPaddingException | IllegalBlockSizeException | InvalidAlgorithmParameterException | InvalidKeyException | KeyManagementException e) {
            logger.error("Encountered an error: ", e);
            throw new EncryptionException(e);
        }
    }

    @Override
    protected int getSerializationVersion() {
        return SERIALIZATION_VERSION;
    }

    @Override
    protected String getSerializationName() {
        return SERIALIZATION_NAME;
    }

    public String getKeyId() {
        return keyId;
    }
}
