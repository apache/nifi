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
package org.apache.nifi.processors.standard.util;

import org.apache.nifi.processor.exception.ProcessException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PBEParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayOutputStream;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;

public class NiFiLegacyKeyDeriver implements KeyDeriver {
    private static final Logger logger = LoggerFactory.getLogger(NiFiLegacyKeyDeriver.class);

    private static final int DEFAULT_SALT_SIZE = 16;
    private static final SecureRandom secureRandom = new SecureRandom();

    // Legacy magic number value
    private final int ITERATION_COUNT = 1000;

    // This is required because the legacy KDF (unnecessarily) depends on the algorithm to determine the salt size
    private final String algorithm;
    // Hard coded because all algorithms use BC and it is redundant to require a second constructor param for a feature that should be removed soon
    private final String providerName = "BC";

    public NiFiLegacyKeyDeriver(String algorithm) {
        this.algorithm = algorithm;
    }

    /**
     * Returns the key derived from this input.
     *
     * @param password the secret input
     * @param salt     the salt in bytes
     * @return the key
     */
    @Override
    public SecretKey deriveKey(String password, byte[] salt) throws ProcessException {
        try {
            Cipher cipher = getInitializedCipher(password, salt);
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            byte[] digest = concat(password.getBytes("UTF-8"), salt);
            for (int i = 0; i < ITERATION_COUNT; i++) {
digest = md5.digest(digest);
            }

            // TODO: Truncate key to algorithm specification

            return new SecretKeySpec(digest, cipher.getAlgorithm());
//            return ((SecretKey) cipher.getParameters());
        } catch (Exception e) {
            throw new ProcessException("Error deriving the encryption key", e);
        }
    }

    /**
     * Generates a random salt compatible with this implementation of the Key Derivation Function.
     *
     * @return the salt in bytes
     */
    @Override
    public byte[] generateSalt() {
        byte[] salt = new byte[getSaltSize()];
        secureRandom.nextBytes(salt);
        return salt;
    }

    /**
     * This implementation relies on the OpenSSL key derivation process to get the IV.
     *
     * @return
     */
    public byte[] getIV(String password, byte[] salt) {
        try {
            Cipher cipher = getInitializedCipher(password, salt);
            return cipher.getIV();
        } catch (Exception e) {
            throw new ProcessException("Error deriving the IV", e);
        }
    }

    private Cipher getInitializedCipher(String password, byte[] salt) throws NoSuchAlgorithmException, NoSuchProviderException, InvalidKeySpecException, NoSuchPaddingException, InvalidKeyException, InvalidAlgorithmParameterException {
        // Initialize secret key from password
        final PBEKeySpec pbeKeySpec = new PBEKeySpec(password.toCharArray());
        final SecretKeyFactory factory = SecretKeyFactory.getInstance(algorithm, providerName);
        SecretKey tempKey = factory.generateSecret(pbeKeySpec);

        final PBEParameterSpec parameterSpec = new PBEParameterSpec(salt, ITERATION_COUNT);
        Cipher cipher = Cipher.getInstance(algorithm, providerName);
        cipher.init(Cipher.DECRYPT_MODE, tempKey, parameterSpec);
        return cipher;
    }

    private int getSaltSize() {
        int algorithmBlockSize = DEFAULT_SALT_SIZE;
        try {
            Cipher cipher = Cipher.getInstance(algorithm, "BC");

            algorithmBlockSize = cipher.getBlockSize();
        } catch (Exception e) {
            logger.warn("Error determining salt size for legacy NiFi KDF with {}", algorithm);
        }
        return algorithmBlockSize;
    }

    // TODO: Should move to utility class
    private byte[] concat(byte[]... arrays) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
            for (byte[] bytes : arrays) {
                outputStream.write(bytes);
            }
        } catch (Exception e) {
            throw new ProcessException(e);
        }

        return outputStream.toByteArray();
    }

}
