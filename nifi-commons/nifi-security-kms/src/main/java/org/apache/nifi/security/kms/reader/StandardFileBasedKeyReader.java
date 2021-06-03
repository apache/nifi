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
package org.apache.nifi.security.kms.reader;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * Standard File Based Key Reader reads Secret Keys from Properties files encrypted using AES-GCM with Tag Size of 128
 */
public class StandardFileBasedKeyReader implements FileBasedKeyReader {
    protected static final String CIPHER_ALGORITHM = "AES/GCM/NoPadding";

    protected static final int IV_LENGTH_BYTES = 16;

    protected static final int TAG_SIZE_BITS = 128;

    private static final Base64.Decoder DECODER = Base64.getDecoder();

    private static final String SECRET_KEY_ALGORITHM = "AES";

    /**
     * Read Secret Keys using provided Root Secret Key
     *
     * @param path File Path contains a properties file with Key Identifier and Base64-encoded encrypted values
     * @param rootKey Root Secret Key
     * @return Map of Key Identifier to decrypted Secret Key
     */
    @Override
    public Map<String, SecretKey> readSecretKeys(final Path path, final SecretKey rootKey) {
        Objects.requireNonNull(path, "Path required");
        Objects.requireNonNull(rootKey, "Root Key required");
        final Map<String, SecretKey> secretKeys = new HashMap<>();

        final Properties properties = getProperties(path);
        for (final String keyId : properties.stringPropertyNames()) {
            final String encodedProperty = properties.getProperty(keyId);
            final SecretKey secretKey = readSecretKey(keyId, encodedProperty, rootKey);
            secretKeys.put(keyId, secretKey);
        }
        return secretKeys;
    }

    private Properties getProperties(final Path path) {
        final Properties properties = new Properties();
        try (final FileInputStream inputStream = new FileInputStream(path.toFile())) {
            properties.load(inputStream);
        } catch (final IOException e) {
            throw new KeyReaderException(String.format("Reading Secret Keys Failed [%s]", path), e);
        }
        return properties;
    }

    private SecretKey readSecretKey(final String keyId, final String encodedProperty, final SecretKey rootKey) {
        final byte[] encryptedProperty = DECODER.decode(encodedProperty);
        final Cipher cipher = getCipher(keyId, encryptedProperty, rootKey);
        final byte[] encryptedSecretKey = Arrays.copyOfRange(encryptedProperty, IV_LENGTH_BYTES, encryptedProperty.length);
        try {
            final byte[] secretKey = cipher.doFinal(encryptedSecretKey);
            return new SecretKeySpec(secretKey, SECRET_KEY_ALGORITHM);
        } catch (final IllegalBlockSizeException|BadPaddingException e) {
            throw new KeyReaderException(String.format("Key Identifier [%s] decryption failed", keyId), e);
        }
    }

    private Cipher getCipher(final String keyId, final byte[] encryptedProperty, final SecretKey rootKey) {
        final byte[] initializationVector = Arrays.copyOfRange(encryptedProperty, 0, IV_LENGTH_BYTES);
        final Cipher cipher = getCipher();
        try {
            cipher.init(Cipher.DECRYPT_MODE, rootKey, new GCMParameterSpec(TAG_SIZE_BITS, initializationVector));
        } catch (final InvalidAlgorithmParameterException|InvalidKeyException e) {
            throw new KeyReaderException(String.format("Cipher initialization failed for Key Identifier [%s]", keyId), e);
        }
        return cipher;
    }

    private Cipher getCipher() {
        try {
            return Cipher.getInstance(CIPHER_ALGORITHM);
        } catch (final NoSuchAlgorithmException|NoSuchPaddingException e) {
            throw new KeyReaderException(String.format("Cipher Algorithm [%s] initialization failed", CIPHER_ALGORITHM), e);
        }
    }
}
