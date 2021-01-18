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

package org.apache.nifi.controller.repository;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.security.KeyManagementException;
import java.util.Map;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.security.kms.EncryptionException;
import org.apache.nifi.security.kms.KeyProvider;
import org.apache.nifi.security.repository.RepositoryEncryptorUtils;
import org.apache.nifi.security.repository.block.RepositoryObjectBlockEncryptor;
import org.apache.nifi.security.repository.block.aes.RepositoryObjectAESGCMEncryptor;
import org.apache.nifi.security.repository.config.FlowFileRepositoryEncryptionConfiguration;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wali.SerDe;
import org.wali.UpdateType;

/**
 * This class is an implementation of the {@link SerDe} interface which provides transparent
 * encryption/decryption of flowfile record data during file system interaction. As of Apache NiFi 1.11.0
 * (January 2020), this implementation is considered <a href="https://nifi.apache.org/docs/nifi-docs/html/user-guide.html#experimental-warning">*experimental*</a>. For further details, review the
 * <a href="https://nifi.apache.org/docs/nifi-docs/html/user-guide.html#encrypted-flowfile">Apache NiFi User Guide -
 * Encrypted FlowFile Repository</a> and
 * <a href="https://nifi.apache.org/docs/nifi-docs/html/administration-guide.html#encrypted-flowfile-repository-properties">Apache NiFi Admin Guide - Encrypted FlowFile
 * Repository Properties</a>.
 */
public class EncryptedSchemaRepositoryRecordSerde implements SerDe<SerializedRepositoryRecord> {
    private static final Logger logger = LoggerFactory.getLogger(EncryptedSchemaRepositoryRecordSerde.class);
    private final SerDe<SerializedRepositoryRecord> wrappedSerDe;
    private final KeyProvider keyProvider;
    private String activeKeyId;

    /**
     * Creates an instance of the serializer/deserializer which wraps another SerDe instance but transparently encrypts/decrypts the data before/after writing/reading from the streams.
     *
     * @param wrappedSerDe                              the wrapped SerDe instance which performs the object <-> bytes (de)serialization
     * @param flowFileRepositoryEncryptionConfiguration the configuration values necessary to encrypt/decrypt the data
     * @throws IOException if there is a problem retrieving the configuration values
     */
    public EncryptedSchemaRepositoryRecordSerde(final SerDe<SerializedRepositoryRecord> wrappedSerDe, final FlowFileRepositoryEncryptionConfiguration
            flowFileRepositoryEncryptionConfiguration) throws IOException {
        if (wrappedSerDe == null) {
            throw new IllegalArgumentException("This implementation must be provided another serde instance to function");
        }
        this.wrappedSerDe = wrappedSerDe;

        // Initialize the encryption-specific fields
        this.keyProvider = RepositoryEncryptorUtils.validateAndBuildRepositoryKeyProvider(flowFileRepositoryEncryptionConfiguration);

        // Set active key ID
        setActiveKeyId(flowFileRepositoryEncryptionConfiguration.getEncryptionKeyId());
    }

    /**
     * Creates an instance of the serializer/deserializer which wraps another SerDe instance but transparently encrypts/decrypts the data before/after writing/reading from the streams.
     *
     * @param wrappedSerDe   the wrapped SerDe instance which performs the object <-> bytes (de)serialization
     * @param niFiProperties the configuration values necessary to encrypt/decrypt the data
     * @throws IOException if there is a problem retrieving the configuration values
     */
    public EncryptedSchemaRepositoryRecordSerde(final SerDe<SerializedRepositoryRecord> wrappedSerDe, final NiFiProperties niFiProperties) throws IOException {
        this(wrappedSerDe, new FlowFileRepositoryEncryptionConfiguration(niFiProperties));
    }

    /**
     * Returns the active key ID used for encryption.
     *
     * @return the active key ID
     */
    String getActiveKeyId() {
        return activeKeyId;
    }

    /**
     * Sets the active key ID used for encryption.
     *
     * @param activeKeyId the key ID to use
     */
    public void setActiveKeyId(String activeKeyId) {
        // Key must not be blank and key provider must make key available
        if (StringUtils.isNotBlank(activeKeyId) && keyProvider.keyExists(activeKeyId)) {
            this.activeKeyId = activeKeyId;
            logger.debug("Set active key ID to '" + activeKeyId + "'");
        } else {
            logger.warn("Attempted to set active key ID to '" + activeKeyId + "' but that is not a valid or available key ID. Keeping active key ID as '" + this.activeKeyId + "'");
        }
    }

    @Override
    public void writeHeader(final DataOutputStream out) throws IOException {
        wrappedSerDe.writeHeader(out);
        if (logger.isDebugEnabled()) {
            logger.debug("Wrote schema header ({} bytes) to output stream", out.size());
        }
    }

    @Override
    public void readHeader(final DataInputStream in) throws IOException {
        wrappedSerDe.readHeader(in);
    }

    /**
     * <p>
     * Serializes an Edit Record to the log via the given
     * {@link DataOutputStream}.
     * </p>
     *
     * @param previousRecordState previous state
     * @param newRecordState      new state
     * @param out                 stream to write to
     * @throws IOException if fail during write
     * @deprecated it is not beneficial to serialize the deltas, so this method just passes through to
     * {@link #serializeRecord(SerializedRepositoryRecord, DataOutputStream)}. It is preferable to use that method directly.
     */
    @Deprecated
    @Override
    public void serializeEdit(SerializedRepositoryRecord previousRecordState, SerializedRepositoryRecord newRecordState, DataOutputStream out) throws IOException {
        serializeRecord(newRecordState, out);
    }

    /**
     * Serializes the provided {@link RepositoryRecord} to the provided stream in an encrypted format.
     *
     * @param record the record to encrypt and serialize
     * @param out    the output stream to write to
     * @throws IOException if there is a problem writing to the stream
     */
    @Override
    public void serializeRecord(final SerializedRepositoryRecord record, final DataOutputStream out) throws IOException {
        // Create BAOS wrapped in DOS to intercept the output
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DataOutputStream tempDataStream = new DataOutputStream(byteArrayOutputStream);

        // Use the delegate to serialize the actual record and extract the output
        String recordId = getRecordIdentifier(record).toString();
        wrappedSerDe.serializeRecord(record, tempDataStream);
        tempDataStream.flush();
        byte[] plainSerializedBytes = byteArrayOutputStream.toByteArray();
        logger.debug("Serialized flowfile record {} to temp stream with length {}", recordId, plainSerializedBytes.length);

        // Encrypt the serialized byte[] to the real stream
        encryptToStream(plainSerializedBytes, recordId, out);
        logger.debug("Encrypted serialized flowfile record {} to actual output stream", recordId);
    }

    /**
     * Encrypts the plain serialized bytes and writes them to the output stream. Precedes
     * the cipher bytes with the plaintext
     * {@link org.apache.nifi.security.repository.RepositoryObjectEncryptionMetadata} data
     * to allow for on-demand deserialization and decryption.
     *
     * @param plainSerializedBytes the plain serialized bytes
     * @param recordId             the unique identifier for this record to be stored in the encryption metadata
     * @param out                  the output stream
     * @throws IOException if there is a problem writing to the stream
     */
    private void encryptToStream(byte[] plainSerializedBytes, String recordId, DataOutputStream out) throws IOException {
        try {
            RepositoryObjectBlockEncryptor encryptor = new RepositoryObjectAESGCMEncryptor();
            encryptor.initialize(keyProvider);
            logger.debug("Initialized {} for flowfile record {}", encryptor.toString(), recordId);

            byte[] cipherBytes = encryptor.encrypt(plainSerializedBytes, recordId, getActiveKeyId());
            logger.debug("Encrypted {} bytes for flowfile record {}", cipherBytes.length, recordId);

            // Maybe remove cipher bytes length because it's included in encryption metadata; deserialization might need to change?
            out.writeInt(cipherBytes.length);
            out.write(cipherBytes);
            logger.debug("Wrote {} bytes (encrypted, including length) for flowfile record {} to output stream", cipherBytes.length + 4, recordId);
        } catch (KeyManagementException | EncryptionException e) {
            logger.error("Encountered an error encrypting & serializing flowfile record {} due to {}", recordId, e.getLocalizedMessage());
            if (logger.isDebugEnabled()) {
                logger.debug(e.getLocalizedMessage(), e);
            }

            // The calling method contract only throws IOException
            throw new IOException("Encountered an error encrypting & serializing flowfile record " + recordId, e);
        }
    }

    /**
     * <p>
     * Reads an Edit Record from the given {@link DataInputStream} and merges
     * that edit with the current version of the record, returning the new,
     * merged version. If the Edit Record indicates that the entity was deleted,
     * must return a Record with an UpdateType of {@link UpdateType#DELETE}.
     * This method must never return <code>null</code>.
     * </p>
     *
     * @param in                  to deserialize from
     * @param currentRecordStates an unmodifiable map of Record ID's to the
     *                            current state of that record
     * @param version             the version of the SerDe that was used to serialize the
     *                            edit record
     * @return deserialized record
     * @throws IOException if failure reading
     * @deprecated it is not beneficial to serialize the deltas, so this method throws a {@link EOFException}. It is preferable to use {@link #deserializeRecord(DataInputStream, int)}.
     */
    @Deprecated
    @Override
    public SerializedRepositoryRecord deserializeEdit(DataInputStream in, Map<Object, SerializedRepositoryRecord> currentRecordStates, int version) throws IOException {
        return deserializeRecord(in, version);

        // deserializeRecord may return a null if there is no more data. However, when we are deserializing
        // an edit, we do so only when we know that we should have data. This is why the JavaDocs for this method
        // on the interface indicate that this method should never return null. As a result, if there is no data
        // available, we handle this by throwing an EOFException.
        // throw new EOFException();
    }

    /**
     * Returns the deserialized and decrypted {@link RepositoryRecord} from the input stream.
     *
     * @param in      stream to read from
     * @param version the version of the SerDe that was used to serialize the
     *                record
     * @return the deserialized record
     * @throws IOException if there is a problem reading from the stream
     */
    @Override
    public SerializedRepositoryRecord deserializeRecord(final DataInputStream in, final int version) throws IOException {
        // Read the expected length of the encrypted record (including the encryption metadata)
        int encryptedRecordLength = in.readInt();
        if (encryptedRecordLength == -1) {
            return null;
        }

        // Read the encrypted record bytes
        byte[] cipherBytes = new byte[encryptedRecordLength];
        StreamUtils.fillBuffer(in, cipherBytes);
        logger.debug("Read {} bytes (encrypted, including length) from actual input stream", encryptedRecordLength + 4);

        // Decrypt the byte[]
        DataInputStream wrappedInputStream = decryptToStream(cipherBytes);

        // Deserialize the plain bytes using the delegate serde
        final SerializedRepositoryRecord deserializedRecord = wrappedSerDe.deserializeRecord(wrappedInputStream, version);
        logger.debug("Deserialized flowfile record {} from temp stream", getRecordIdentifier(deserializedRecord));
        return deserializedRecord;
    }

    /**
     * Returns a {@link DataInputStream} containing the plaintext bytes so the record can be properly deserialized by the wrapped serde.
     *
     * @param cipherBytes the serialized, encrypted bytes
     * @return a stream wrapping the plain bytes
     */
    private DataInputStream decryptToStream(byte[] cipherBytes) throws IOException {
        try {
            RepositoryObjectBlockEncryptor encryptor = new RepositoryObjectAESGCMEncryptor();
            encryptor.initialize(keyProvider);
            logger.debug("Initialized {} for decrypting flowfile record", encryptor.toString());

            byte[] plainSerializedBytes = encryptor.decrypt(cipherBytes, "[pending record ID]");
            logger.debug("Decrypted {} bytes for flowfile record", cipherBytes.length);

            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(plainSerializedBytes);
            return new DataInputStream(byteArrayInputStream);
        } catch (KeyManagementException | EncryptionException e) {
            logger.error("Encountered an error decrypting & deserializing flowfile record due to {}", e.getLocalizedMessage());
            if (logger.isDebugEnabled()) {
                logger.debug(e.getLocalizedMessage(), e);
            }

            // The calling method contract only throws IOException
            throw new IOException("Encountered an error decrypting & deserializing flowfile record", e);
        }
    }

    /**
     * Returns the unique ID for the given record.
     *
     * @param record to obtain identifier for
     * @return identifier of record
     */
    @Override
    public Object getRecordIdentifier(SerializedRepositoryRecord record) {
        return wrappedSerDe.getRecordIdentifier(record);
    }

    /**
     * Returns the UpdateType for the given record.
     *
     * @param record to retrieve update type for
     * @return update type
     */
    @Override
    public UpdateType getUpdateType(SerializedRepositoryRecord record) {
        return wrappedSerDe.getUpdateType(record);
    }

    /**
     * Returns the external location of the given record; this is used when a
     * record is moved away from WALI or is being re-introduced to WALI. For
     * example, WALI can be updated with a record of type
     * {@link UpdateType#SWAP_OUT} that indicates a Location of
     * file://tmp/external1 and can then be re-introduced to WALI by updating
     * WALI with a record of type {@link UpdateType#CREATE} that indicates a
     * Location of file://tmp/external1
     *
     * @param record to get location of
     * @return location
     */
    @Override
    public String getLocation(SerializedRepositoryRecord record) {
        return wrappedSerDe.getLocation(record);
    }

    /**
     * Returns the version that this SerDe will use when writing. This used used
     * when serializing/deserializing the edit logs so that if the version
     * changes, we are still able to deserialize old versions
     *
     * @return version
     */
    @Override
    public int getVersion() {
        return wrappedSerDe.getVersion();
    }
}
