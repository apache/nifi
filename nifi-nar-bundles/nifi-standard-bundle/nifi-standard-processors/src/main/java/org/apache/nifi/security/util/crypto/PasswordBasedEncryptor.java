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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.NoSuchAlgorithmException;
import javax.crypto.Cipher;
import javax.crypto.spec.PBEKeySpec;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processors.standard.EncryptContent.Encryptor;
import org.apache.nifi.security.util.EncryptionMethod;
import org.apache.nifi.security.util.KeyDerivationFunction;

public class PasswordBasedEncryptor implements Encryptor {

    private EncryptionMethod encryptionMethod;
    private PBEKeySpec password;
    private KeyDerivationFunction kdf;

    private static final int DEFAULT_MAX_ALLOWED_KEY_LENGTH = 128;
    private static final int MINIMUM_SAFE_PASSWORD_LENGTH = 10;

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

    public PasswordBasedEncryptor(final EncryptionMethod encryptionMethod, final char[] password, KeyDerivationFunction kdf) {
        super();
        try {
            if (encryptionMethod == null) {
                throw new IllegalArgumentException("Cannot initialize password-based encryptor with null encryption method");
            }
            this.encryptionMethod = encryptionMethod;
            if (kdf == null || kdf.equals(KeyDerivationFunction.NONE)) {
                throw new IllegalArgumentException("Cannot initialize password-based encryptor with null KDF");
            }
            this.kdf = kdf;
            if (password == null || password.length == 0) {
                throw new IllegalArgumentException("Cannot initialize password-based encryptor with empty password");
            }
            this.password = new PBEKeySpec(password);
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

    /**
     * Returns a recommended minimum length for passwords. This can be modified over time and does not take full entropy calculations (patterns, character space, etc.) into account.
     *
     * @return the minimum safe password length
     */
    public static int getMinimumSafePasswordLength() {
        return MINIMUM_SAFE_PASSWORD_LENGTH;
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

    @SuppressWarnings("deprecation")
    private class DecryptCallback implements StreamCallback {

        public DecryptCallback() {
        }

        @Override
        public void process(final InputStream in, final OutputStream out) throws IOException {
            // Initialize cipher provider
            PBECipherProvider cipherProvider = (PBECipherProvider) CipherProviderFactory.getCipherProvider(kdf);

            // Read salt
            byte[] salt;
            try {
                // NiFi legacy code determined the salt length based on the cipher block size
                if (cipherProvider instanceof org.apache.nifi.security.util.crypto.NiFiLegacyCipherProvider) {
                    salt = ((org.apache.nifi.security.util.crypto.NiFiLegacyCipherProvider) cipherProvider).readSalt(encryptionMethod, in);
                } else {
                    salt = cipherProvider.readSalt(in);
                }
            } catch (final EOFException e) {
                throw new ProcessException("Cannot decrypt because file size is smaller than salt size", e);
            }

            // Determine necessary key length
            int keyLength = CipherUtility.parseKeyLengthFromAlgorithm(encryptionMethod.getAlgorithm());

            // Generate cipher
            try {
                Cipher cipher;
                // Read IV if necessary
                if (cipherProvider instanceof RandomIVPBECipherProvider) {
                    RandomIVPBECipherProvider rivpcp = (RandomIVPBECipherProvider) cipherProvider;
                    byte[] iv = rivpcp.readIV(in);
                    cipher = rivpcp.getCipher(encryptionMethod, new String(password.getPassword()), salt, iv, keyLength, false);
                } else {
                    cipher = cipherProvider.getCipher(encryptionMethod, new String(password.getPassword()), salt, keyLength, false);
                }
                CipherUtility.processStreams(cipher, in, out);
            } catch (Exception e) {
                throw new ProcessException(e);
            }
        }
    }

    @SuppressWarnings("deprecation")
    private class EncryptCallback implements StreamCallback {

        public EncryptCallback() {
        }

        @Override
        public void process(final InputStream in, final OutputStream out) throws IOException {
            // Initialize cipher provider
            PBECipherProvider cipherProvider = (PBECipherProvider) CipherProviderFactory.getCipherProvider(kdf);

            // Generate salt
            byte[] salt;
            // NiFi legacy code determined the salt length based on the cipher block size
            if (cipherProvider instanceof org.apache.nifi.security.util.crypto.NiFiLegacyCipherProvider) {
                salt = ((org.apache.nifi.security.util.crypto.NiFiLegacyCipherProvider) cipherProvider).generateSalt(encryptionMethod);
            } else {
                salt = cipherProvider.generateSalt();
            }

            // Write to output stream
            cipherProvider.writeSalt(salt, out);

            // Determine necessary key length
            int keyLength = CipherUtility.parseKeyLengthFromAlgorithm(encryptionMethod.getAlgorithm());

            // Generate cipher
            try {
                Cipher cipher = cipherProvider.getCipher(encryptionMethod, new String(password.getPassword()), salt, keyLength, true);
                // Write IV if necessary
                if (cipherProvider instanceof RandomIVPBECipherProvider) {
                    ((RandomIVPBECipherProvider) cipherProvider).writeIV(cipher.getIV(), out);
                }
                CipherUtility.processStreams(cipher, in, out);
            } catch (Exception e) {
                throw new ProcessException(e);
            }
        }
    }
}