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
package org.apache.nifi.security.util.crypto;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.NoSuchAlgorithmException;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processors.standard.EncryptContent.Encryptor;
import org.apache.nifi.security.util.EncryptionMethod;
import org.apache.nifi.security.util.KeyDerivationFunction;

public class KeyedEncryptor implements Encryptor {

    private EncryptionMethod encryptionMethod;
    private SecretKey key;
    private byte[] iv;

    private static final int DEFAULT_MAX_ALLOWED_KEY_LENGTH = 128;

    private static boolean isUnlimitedStrengthCryptographyEnabled;

    // Evaluate an unlimited strength algorithm to determine if we support the capability we have on the system
    static {
        try {
            isUnlimitedStrengthCryptographyEnabled = (Cipher.getMaxAllowedKeyLength("AES") > DEFAULT_MAX_ALLOWED_KEY_LENGTH);
        } catch (NoSuchAlgorithmException e) {
            // if there are issues with this, we default back to the value established
            isUnlimitedStrengthCryptographyEnabled = false;
        }
    }

    public KeyedEncryptor(final EncryptionMethod encryptionMethod, final SecretKey key) {
        this(encryptionMethod, key == null ? new byte[0] : key.getEncoded(), new byte[0]);
    }

    public KeyedEncryptor(final EncryptionMethod encryptionMethod, final SecretKey key, final byte[] iv) {
        this(encryptionMethod, key == null ? new byte[0] : key.getEncoded(), iv);
    }

    public KeyedEncryptor(final EncryptionMethod encryptionMethod, final byte[] keyBytes) {
        this(encryptionMethod, keyBytes, new byte[0]);
    }

    public KeyedEncryptor(final EncryptionMethod encryptionMethod, final byte[] keyBytes, final byte[] iv) {
        super();
        try {
            if (encryptionMethod == null) {
                throw new IllegalArgumentException("Cannot instantiate a keyed encryptor with null encryption method");
            }
            if (!encryptionMethod.isKeyedCipher()) {
                throw new IllegalArgumentException("Cannot instantiate a keyed encryptor with encryption method " + encryptionMethod.name());
            }
            this.encryptionMethod = encryptionMethod;
            if (keyBytes == null || keyBytes.length == 0) {
                throw new IllegalArgumentException("Cannot instantiate a keyed encryptor with empty key");
            }
            if (!CipherUtility.isValidKeyLengthForAlgorithm(keyBytes.length * 8, encryptionMethod.getAlgorithm())) {
                throw new IllegalArgumentException("Cannot instantiate a keyed encryptor with key of length " + keyBytes.length);
            }
            String cipherName = CipherUtility.parseCipherFromAlgorithm(encryptionMethod.getAlgorithm());
            this.key = new SecretKeySpec(keyBytes, cipherName);

            this.iv = iv;
        } catch (Exception e) {
            throw new ProcessException(e);
        }
    }

    public static int getMaxAllowedKeyLength(final String algorithm) {
        if (StringUtils.isEmpty(algorithm)) {
            return DEFAULT_MAX_ALLOWED_KEY_LENGTH;
        }
        String parsedCipher = CipherUtility.parseCipherFromAlgorithm(algorithm);
        try {
            return Cipher.getMaxAllowedKeyLength(parsedCipher);
        } catch (NoSuchAlgorithmException e) {
            // Default algorithm max key length on unmodified JRE
            return DEFAULT_MAX_ALLOWED_KEY_LENGTH;
        }
    }

    public static boolean supportsUnlimitedStrength() {
        return isUnlimitedStrengthCryptographyEnabled;
    }

    @Override
    public StreamCallback getEncryptionCallback() throws ProcessException {
        return new EncryptCallback();
    }

    @Override
    public StreamCallback getDecryptionCallback() throws ProcessException {
        return new DecryptCallback();
    }

    private class DecryptCallback implements StreamCallback {

        public DecryptCallback() {
        }

        @Override
        public void process(final InputStream in, final OutputStream out) throws IOException {
            // Initialize cipher provider
            KeyedCipherProvider cipherProvider = (KeyedCipherProvider) CipherProviderFactory.getCipherProvider(KeyDerivationFunction.NONE);

            // Generate cipher
            try {
                Cipher cipher;
                // The IV could have been set by the constructor, but if not, read from the cipher stream
                if (iv.length == 0) {
                    iv = cipherProvider.readIV(in);
                }
                cipher = cipherProvider.getCipher(encryptionMethod, key, iv, false);
                CipherUtility.processStreams(cipher, in, out);
            } catch (Exception e) {
                throw new ProcessException(e);
            }
        }
    }

    private class EncryptCallback implements StreamCallback {

        public EncryptCallback() {
        }

        @Override
        public void process(final InputStream in, final OutputStream out) throws IOException {
            // Initialize cipher provider
            KeyedCipherProvider cipherProvider = (KeyedCipherProvider) CipherProviderFactory.getCipherProvider(KeyDerivationFunction.NONE);

            // Generate cipher
            try {
                Cipher cipher = cipherProvider.getCipher(encryptionMethod, key, iv, true);
                cipherProvider.writeIV(cipher.getIV(), out);
                CipherUtility.processStreams(cipher, in, out);
            } catch (Exception e) {
                throw new ProcessException(e);
            }
        }
    }
}