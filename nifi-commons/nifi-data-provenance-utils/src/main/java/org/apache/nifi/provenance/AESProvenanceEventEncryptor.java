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

import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.security.Security;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AESProvenanceEventEncryptor implements ProvenanceEventEncryptor {
    private static final Logger logger = LoggerFactory.getLogger(AESProvenanceEventEncryptor.class);
    private static final String ALGORITHM = "AES/GCM/NoPadding";
    private static final int IV_LENGTH = 16;
    private static final byte[] EMPTY_IV = new byte[IV_LENGTH];
    private static final String VERSION = "v1";

    private KeyProvider keyProvider;

    private Map<String, Cipher> encryptors = new HashMap<>();
    private Map<String, Cipher> decryptors = new HashMap<>();

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
     * Encrypts the provided {@see ProvenanceEventRecord}.
     *
     * @param plainRecord the plain record
     * @param keyId       the ID of the key to use
     * @return the encrypted record
     * @throws EncryptionException if there is an issue encrypting this record
     */
    @Override
    public EncryptedProvenanceEventRecord encrypt(ProvenanceEventRecord plainRecord, String keyId) throws EncryptionException {
        if (plainRecord == null || CryptoUtils.isEmpty(keyId)) {
            throw new EncryptionException("The provenance record and key ID cannot be missing");
        }

        if (keyProvider == null || !keyProvider.keyExists(keyId)) {
            throw new EncryptionException("The requested key ID is not available");
        } else {
            Cipher cipher;
            byte[] ivBytes = new byte[IV_LENGTH];
            // Check if the cipher is in the cache
            try {
                String id = ((StandardProvenanceEventRecord) plainRecord).getBestEventIdentifier();
                logger.debug("Encrypting provenance record " + id + " with key ID " + keyId);
                if (encryptors.containsKey(keyId)) {
                    cipher = encryptors.get(keyId);
                } else {
                    // Initialize a cipher with the key and a new IV, and store it in the cache(s)
                    // The method will detect the current IV is empty and generate a new one
                    final SecretKey key = keyProvider.getKey(keyId);
                    cipher = initCipher(Cipher.ENCRYPT_MODE, key, ivBytes);
                    ivBytes = cipher.getIV();
                    encryptors.put(keyId, cipher);

                    // Create the decryptor
                    Cipher decryptCipher = initCipher(Cipher.DECRYPT_MODE, key, ivBytes);
                    decryptors.put(keyId, decryptCipher);
                }

                // Perform the actual encryption on each available property
                EncryptedProvenanceEventRecord encryptedRecord = EncryptedProvenanceEventRecord.copy(plainRecord);
                encryptedRecord.encrypt(cipher, keyId, VERSION);
                logger.info("Encrypted provenance event record " + id + " with key ID " + keyId);
                return encryptedRecord;
            } catch (EncryptionException | KeyManagementException e) {
                final String msg = "Encountered an exception encrypting provenance record " + ((StandardProvenanceEventRecord) plainRecord).getBestEventIdentifier();
                logger.error(msg, e);
                throw new EncryptionException(msg, e);
            }
        }
    }

    private Cipher initCipher(int mode, SecretKey key, byte[] ivBytes) throws EncryptionException {
        Cipher cipher;
        try {
            cipher = Cipher.getInstance(ALGORITHM, "BC");
            if (ivBytes == null || ivBytes.length != IV_LENGTH || Arrays.equals(ivBytes, EMPTY_IV)) {
                if (mode == Cipher.DECRYPT_MODE) {
                    throw new InvalidAlgorithmParameterException("Must provide a valid IV for decryption");
                }
                logger.warn("No IV provided for cipher init so a new value is being generated");
                ivBytes = new byte[IV_LENGTH];
                new SecureRandom().nextBytes(ivBytes);
            }
            IvParameterSpec iv = new IvParameterSpec(ivBytes);
            cipher.init(mode, key, iv);
            return cipher;
        } catch (NoSuchAlgorithmException | NoSuchProviderException | NoSuchPaddingException | InvalidAlgorithmParameterException | InvalidKeyException e) {
            logger.error("Encountered an exception initializing the cipher", e);
            throw new EncryptionException(e);
        }
    }

    /**
     * Decrypts the provided {@see ProvenanceEventRecord}.
     *
     * @param encryptedRecord the encrypted record
     * @return the decrypted record
     * @throws EncryptionException if there is an issue decrypting this record
     */
    @Override
    public ProvenanceEventRecord decrypt(EncryptedProvenanceEventRecord encryptedRecord) throws EncryptionException {
        return null;
    }

    // TODO: #clear()/#reset()?
}
