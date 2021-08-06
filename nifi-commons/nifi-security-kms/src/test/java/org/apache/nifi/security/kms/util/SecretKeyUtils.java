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
package org.apache.nifi.security.kms.util;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Map;
import java.util.Properties;

public class SecretKeyUtils {
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    private static final Base64.Encoder ENCODER = Base64.getEncoder();

    private static final String CIPHER_ALGORITHM = "AES/GCM/NoPadding";

    private static final String KEY_ALGORITHM = "AES";

    private static final int KEY_LENGTH = 32;

    private static final int IV_LENGTH = 16;

    private static final int TAG_LENGTH = 128;

    /**
     * Get Encrypted Secret Keys as Properties
     *
     * @param rootKey Root Key used to encrypt Secret Keys
     * @param secretKeys Map of Key Identifier to Secret Key
     * @return Properties containing encrypted Secret Keys
     * @throws GeneralSecurityException Thrown on getEncryptedSecretKey()
     */
    public static Properties getEncryptedSecretKeys(final SecretKey rootKey, final Map<String, SecretKey> secretKeys) throws GeneralSecurityException {
        final Properties properties = new Properties();
        for (final Map.Entry<String, SecretKey> secretKeyEntry : secretKeys.entrySet()) {
            final SecretKey secretKey = secretKeyEntry.getValue();
            final String encryptedSecretKey = getEncryptedSecretKey(rootKey, secretKey);
            properties.setProperty(secretKeyEntry.getKey(), encryptedSecretKey);
        }
        return properties;
    }

    /**
     * Get Random AES Secret Key
     *
     * @return Secret Key
     */
    public static SecretKey getSecretKey() {
        final byte[] encodedKey = new byte[KEY_LENGTH];
        SECURE_RANDOM.nextBytes(encodedKey);
        return new SecretKeySpec(encodedKey, KEY_ALGORITHM);
    }

    /**
     * Get Encrypted Secret Key using AES-GCM with Base64 encoded string prefixed with initialization vector
     *
     * @param rootKey Root Key used to encrypt Secret Key
     * @param secretKey Secret Key to be encrypted
     * @return Base64 encoded and encrypted Secret Key
     * @throws GeneralSecurityException Thrown when unable to encrypt Secret Key
     */
    private static String getEncryptedSecretKey(final SecretKey rootKey, final SecretKey secretKey) throws GeneralSecurityException {
        final Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);

        final byte[] initializationVector = new byte[IV_LENGTH];
        SECURE_RANDOM.nextBytes(initializationVector);
        cipher.init(Cipher.ENCRYPT_MODE, rootKey, new GCMParameterSpec(TAG_LENGTH, initializationVector));
        final byte[] encryptedSecretKey = cipher.doFinal(secretKey.getEncoded());

        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
            outputStream.write(initializationVector);
            outputStream.write(encryptedSecretKey);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
        final byte[] encryptedProperty = outputStream.toByteArray();
        return ENCODER.encodeToString(encryptedProperty);
    }
}
