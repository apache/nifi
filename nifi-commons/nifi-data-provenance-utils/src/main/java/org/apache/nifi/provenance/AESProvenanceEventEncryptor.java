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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.security.KeyManagementException;
import java.security.Security;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.SecretKey;
import org.apache.nifi.security.util.EncryptionMethod;
import org.apache.nifi.security.util.crypto.AESKeyedCipherProvider;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AESProvenanceEventEncryptor implements ProvenanceEventEncryptor {
    private static final Logger logger = LoggerFactory.getLogger(AESProvenanceEventEncryptor.class);
    private static final String ALGORITHM = "AES/GCM/NoPadding";
    private static final int IV_LENGTH = 16;
    private static final byte[] EMPTY_IV = new byte[IV_LENGTH];
    private static final String VERSION = "v1";
    private static final int MIN_METADATA_LENGTH = IV_LENGTH + 3 + 3; // 3 delimiters and 3 non-zero elements
    private static final int METADATA_DEFAULT_LENGTH = (20 + ALGORITHM.length() + IV_LENGTH + VERSION.length()) * 2; // Default to twice the expected length
    private static final byte[] SENTINEL = new byte[]{0x01};

    private KeyProvider keyProvider;

    private Map<String, Cipher> encryptors = new HashMap<>();
    private Map<String, Cipher> decryptors = new HashMap<>();
    private AESKeyedCipherProvider aesKeyedCipherProvider = new AESKeyedCipherProvider();

    /**
     * Initializes the encryptor with a {@link KeyProvider}.
     *
     * @param keyProvider the key provider which will be responsible for accessing keys
     * @throws KeyManagementException if there is an issue configuring the key provider
     */
    @Override
    public void initialize(KeyProvider keyProvider) throws KeyManagementException {
        this.keyProvider = keyProvider;

        if (Security.getProvider("BC") == null) {
            Security.addProvider(new BouncyCastleProvider());
        }
    }

    /**
     * Available for dependency injection to override the default {@link AESKeyedCipherProvider} if necessary.
     *
     * @param cipherProvider the AES cipher provider to use
     */
    void setCipherProvider(AESKeyedCipherProvider cipherProvider) {
        this.aesKeyedCipherProvider = cipherProvider;
    }

    /**
     * Encrypts the provided {@link ProvenanceEventRecord}, serialized to a byte[] by the RecordWriter.
     *
     * @param plainRecord the plain record, serialized to a byte[]
     * @param recordId    an identifier for this record (eventId, generated, etc.)
     * @param keyId       the ID of the key to use
     * @return the encrypted record
     * @throws EncryptionException if there is an issue encrypting this record
     */
    @Override
    public byte[] encrypt(byte[] plainRecord, String recordId, String keyId) throws EncryptionException {
        if (plainRecord == null || CryptoUtils.isEmpty(keyId)) {
            throw new EncryptionException("The provenance record and key ID cannot be missing");
        }

        if (keyProvider == null || !keyProvider.keyExists(keyId)) {
            throw new EncryptionException("The requested key ID is not available");
        } else {
            byte[] ivBytes = new byte[IV_LENGTH];
            // Check if the cipher is in the cache
            try {
                logger.debug("Encrypting provenance record " + recordId + " with key ID " + keyId);
                // Check if the cipher is in the cache
                if (!encryptors.containsKey(keyId)) {
                    buildAndStoreCiphers(keyId, ivBytes);
                }
                Cipher cipher = encryptors.get(keyId);
                ivBytes = cipher.getIV();

                // Perform the actual encryption
                byte[] cipherBytes = cipher.doFinal(plainRecord);

                // Serialize and concat encryption details fields (keyId, algo, IV, version, CB length) outside of encryption
                EncryptionMetadata metadata = new EncryptionMetadata(keyId, ALGORITHM, ivBytes, VERSION, cipherBytes.length);
                byte[] serializedEncryptionMetadata = serializeEncryptionMetadata(metadata);

                // Add the sentinel byte of 0x01
                logger.info("Encrypted provenance event record " + recordId + " with key ID " + keyId);
                return CryptoUtils.concatByteArrays(SENTINEL, serializedEncryptionMetadata, cipherBytes);
            } catch (EncryptionException | BadPaddingException | IllegalBlockSizeException | IOException e) {
                final String msg = "Encountered an exception encrypting provenance record " + recordId;
                logger.error(msg, e);
                throw new EncryptionException(msg, e);
            }
        }
    }

    private byte[] serializeEncryptionMetadata(EncryptionMetadata metadata) throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream outputStream = new ObjectOutputStream(baos);
        outputStream.writeObject(metadata);
        outputStream.close();
        return baos.toByteArray();
    }

    private Cipher initCipher(int mode, SecretKey key, byte[] ivBytes) throws EncryptionException {
        Cipher cipher;
        try {
            cipher = aesKeyedCipherProvider.getCipher(EncryptionMethod.AES_GCM, key, ivBytes, mode == Cipher.ENCRYPT_MODE);
            return cipher;
        } catch (Exception e) {
            logger.error("Encountered an exception initializing the cipher", e);
            throw new EncryptionException(e);
        }
    }

    private static boolean isValidIV(byte[] ivBytes) {
        return ivBytes != null && ivBytes.length == IV_LENGTH && !Arrays.equals(ivBytes, EMPTY_IV);
    }

    /**
     * Decrypts the provided byte[] (an encrypted record with accompanying metadata).
     *
     * @param encryptedRecord the encrypted record in byte[] form
     * @param recordId        an identifier for this record (eventId, generated, etc.)
     * @return the decrypted record
     * @throws EncryptionException if there is an issue decrypting this record
     */
    @Override
    public byte[] decrypt(byte[] encryptedRecord, String recordId) throws EncryptionException {
        if (encryptedRecord == null) {
            throw new EncryptionException("The encrypted provenance record cannot be missing");
        }

        EncryptionMetadata metadata;
        try {
            metadata = extractEncryptionMetadata(encryptedRecord);
        } catch (IOException | ClassNotFoundException e) {
            final String msg = "Encountered an error reading the encryption metadata: ";
            logger.error(msg, e);
            throw new EncryptionException(msg, e);
        }

        if (keyProvider == null || !keyProvider.keyExists(metadata.keyId) || CryptoUtils.isEmpty(metadata.keyId)) {
            throw new EncryptionException("The requested key ID " + metadata.keyId + " is not available");
        } else {
            try {
                logger.debug("Decrypting provenance record " + recordId + " with key ID " + metadata.keyId);
                // Check if the cipher is in the cache
                if (!decryptors.containsKey(metadata.keyId)) {
                    buildAndStoreCiphers(metadata.keyId, metadata.ivBytes);
                }
                Cipher cipher = decryptors.get(metadata.keyId);

                // Strip the metadata away to get just the cipher bytes
                byte[] cipherBytes = extractCipherBytes(encryptedRecord, metadata);

                // Perform the actual decryption
                byte[] plainBytes = cipher.doFinal(cipherBytes);

                logger.info("Decrypted provenance event record " + recordId + " with key ID " + metadata.keyId);
                return plainBytes;
            } catch (EncryptionException | BadPaddingException | IllegalBlockSizeException e) {
                final String msg = "Encountered an exception decrypting provenance record " + recordId;
                logger.error(msg, e);
                throw new EncryptionException(msg, e);
            }
        }
    }

    private EncryptionMetadata extractEncryptionMetadata(byte[] encryptedRecord) throws EncryptionException, IOException, ClassNotFoundException {
        if (encryptedRecord == null || encryptedRecord.length < MIN_METADATA_LENGTH) {
            throw new EncryptionException("The encrypted record is too short to contain the metadata");
        }

        // TODO: Trim the encryptedRecord to a reasonable length before parsing, as this is wasteful and the cipherBytes are irrelevant
        // Skip the first byte (SENTINEL) and don't need to copy all the serialized record
        // byte[] metadataBytes = Arrays.copyOfRange(encryptedRecord, 1, METADATA_DEFAULT_LENGTH);
        byte[] metadataBytes = Arrays.copyOfRange(encryptedRecord, 1, encryptedRecord.length);
        ByteArrayInputStream bais = new ByteArrayInputStream(metadataBytes);
        try (ObjectInputStream ois = new ObjectInputStream(bais)) {
            return (EncryptionMetadata) ois.readObject();
        }
    }

    private boolean isValidTag(byte tag) {
        // TODO: Validate tag
        return 0 < tag;
    }

    /**
     * Initializes two cipher objects for the provided keyId -- one for encryption and one for decryption
     * and stores them in the respective caches. Also validates the provided IV and will replace an invalid
     * parameter with a valid IV.
     *
     * @param keyId   the key identifier
     * @param ivBytes the IV (16 bytes)
     * @throws EncryptionException if there is an error building the cipher objects
     */
    private void buildAndStoreCiphers(String keyId, byte[] ivBytes) throws EncryptionException {
        // Initialize a cipher with the key and a new IV, and store it in the cache(s)
        try {
            final SecretKey key = keyProvider.getKey(keyId);
            // The method will detect the current IV is empty and generate a new one
            Cipher encryptCipher = initCipher(Cipher.ENCRYPT_MODE, key, ivBytes);
            ivBytes = encryptCipher.getIV();
            encryptors.put(keyId, encryptCipher);

            // Create the decryptor
            Cipher decryptCipher = initCipher(Cipher.DECRYPT_MODE, key, ivBytes);
            decryptors.put(keyId, decryptCipher);
        } catch (KeyManagementException | EncryptionException e) {
            final String message = "Error building and caching the ciphers";
            logger.error(message + ": " + e.getLocalizedMessage());
            throw new EncryptionException(message, e);
        }
    }

    // TODO: Remove individual extractors once object extractor is finished
    private String extractKeyId(byte[] encryptedRecord) {
        return null;
    }

    private byte[] extractCipherBytes(byte[] encryptedRecord, EncryptionMetadata metadata) {
        return Arrays.copyOfRange(encryptedRecord, encryptedRecord.length - metadata.cipherByteLength, encryptedRecord.length);
    }

    private byte[] extractIv(byte[] encryptedRecord) {
        return new byte[0];
    }

    @Override
    public String toString() {
        return "AES Provenance Event Encryptor with Key Provider: " + keyProvider.toString();
    }

    // TODO: #clear()/#reset()?
}
