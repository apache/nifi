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

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.List;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PBEParameterSpec;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processors.standard.EncryptContent.Encryptor;
import org.apache.nifi.security.util.KeyDerivationFunction;
import org.apache.nifi.stream.io.StreamUtils;

public class PasswordBasedEncryptor implements Encryptor {

    private Cipher cipher;
    private int saltSize;
    private SecretKey secretKey;
    private KeyDerivationFunction kdf;
    private int iterationsCount = LEGACY_KDF_ITERATIONS;

    @Deprecated
    private static final String SECURE_RANDOM_ALGORITHM = "SHA1PRNG";
    private static final int DEFAULT_SALT_SIZE = 8;
    // TODO: Eventually KDF-specific values should be refactored into injectable interface impls
    private static final int LEGACY_KDF_ITERATIONS = 1000;
    private static final int OPENSSL_EVP_HEADER_SIZE = 8;
    private static final int OPENSSL_EVP_SALT_SIZE = 8;
    private static final String OPENSSL_EVP_HEADER_MARKER = "Salted__";
    private static final int OPENSSL_EVP_KDF_ITERATIONS = 0;
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

    public PasswordBasedEncryptor(final String algorithm, final String providerName, final char[] password, KeyDerivationFunction kdf) {
        super();
        try {
            // initialize cipher
            this.cipher = Cipher.getInstance(algorithm, providerName);
            this.kdf = kdf;

            if (isOpenSSLKDF()) {
                this.saltSize = OPENSSL_EVP_SALT_SIZE;
                this.iterationsCount = OPENSSL_EVP_KDF_ITERATIONS;
            } else {
                int algorithmBlockSize = cipher.getBlockSize();
                this.saltSize = (algorithmBlockSize > 0) ? algorithmBlockSize : DEFAULT_SALT_SIZE;
            }

            // initialize SecretKey from password
            final PBEKeySpec pbeKeySpec = new PBEKeySpec(password);
            final SecretKeyFactory factory = SecretKeyFactory.getInstance(algorithm, providerName);
            this.secretKey = factory.generateSecret(pbeKeySpec);
        } catch (Exception e) {
            throw new ProcessException(e);
        }
    }

    public static int getMaxAllowedKeyLength(final String algorithm) {
        if (StringUtils.isEmpty(algorithm)) {
            return DEFAULT_MAX_ALLOWED_KEY_LENGTH;
        }
        String parsedCipher = parseCipherFromAlgorithm(algorithm);
        try {
            return Cipher.getMaxAllowedKeyLength(parsedCipher);
        } catch (NoSuchAlgorithmException e) {
            // Default algorithm max key length on unmodified JRE
            return DEFAULT_MAX_ALLOWED_KEY_LENGTH;
        }
    }

    private static String parseCipherFromAlgorithm(final String algorithm) {
        // This is not optimal but the algorithms do not have a standard format
        final String AES = "AES";
        final String TDES = "TRIPLEDES";
        final String DES = "DES";
        final String RC4 = "RC4";
        final String RC2 = "RC2";
        final String TWOFISH = "TWOFISH";
        final List<String> SYMMETRIC_CIPHERS = Arrays.asList(AES, TDES, DES, RC4, RC2, TWOFISH);

        // The algorithms contain "TRIPLEDES" but the cipher name is "DESede"
        final String ACTUAL_TDES_CIPHER = "DESede";

        for (String cipher : SYMMETRIC_CIPHERS) {
            if (algorithm.contains(cipher)) {
                if (cipher.equals(TDES)) {
                    return ACTUAL_TDES_CIPHER;
                } else {
                    return cipher;
                }
            }
        }

        return algorithm;
    }

    public static boolean supportsUnlimitedStrength() {
        return isUnlimitedStrengthCryptographyEnabled;
    }

    @Override
    public StreamCallback getEncryptionCallback() throws ProcessException {
        try {
            byte[] salt = new byte[saltSize];
            SecureRandom secureRandom = new SecureRandom();
            secureRandom.nextBytes(salt);
            return new EncryptCallback(salt);
        } catch (Exception e) {
            throw new ProcessException(e);
        }
    }

    @Override
    public StreamCallback getDecryptionCallback() throws ProcessException {
        return new DecryptCallback();
    }

    private int getIterationsCount() {
        return iterationsCount;
    }

    private boolean isOpenSSLKDF() {
        return KeyDerivationFunction.OPENSSL_EVP_BYTES_TO_KEY.equals(kdf);
    }

    private class DecryptCallback implements StreamCallback {

        public DecryptCallback() {
        }
        @Override
        public void process(final InputStream in, final OutputStream out) throws IOException {
            byte[] salt = new byte[saltSize];

            try {
                // If the KDF is OpenSSL, try to read the salt from the input stream
                if (isOpenSSLKDF()) {
                    // The header and salt format is "Salted__salt x8b" in ASCII

                    // Try to read the header and salt from the input
                    byte[] header = new byte[PasswordBasedEncryptor.OPENSSL_EVP_HEADER_SIZE];

                    // Mark the stream in case there is no salt
                    in.mark(OPENSSL_EVP_HEADER_SIZE + 1);
                    StreamUtils.fillBuffer(in, header);

                    final byte[] headerMarkerBytes = OPENSSL_EVP_HEADER_MARKER.getBytes(StandardCharsets.US_ASCII);

                    if (!Arrays.equals(headerMarkerBytes, header)) {
                        // No salt present
                        salt = new byte[0];
                        // Reset the stream because we skipped 8 bytes of cipher text
                        in.reset();
                    }
                }

                StreamUtils.fillBuffer(in, salt);
            } catch (final EOFException e) {
                throw new ProcessException("Cannot decrypt because file size is smaller than salt size", e);
            }

            final PBEParameterSpec parameterSpec = new PBEParameterSpec(salt, getIterationsCount());
            try {
                cipher.init(Cipher.DECRYPT_MODE, secretKey, parameterSpec);
            } catch (final Exception e) {
                throw new ProcessException(e);
            }

            final byte[] buffer = new byte[65536];
            int len;
            while ((len = in.read(buffer)) > 0) {
                final byte[] decryptedBytes = cipher.update(buffer, 0, len);
                if (decryptedBytes != null) {
                    out.write(decryptedBytes);
                }
            }

            try {
                out.write(cipher.doFinal());
            } catch (final Exception e) {
                throw new ProcessException(e);
            }
        }
    }

    private class EncryptCallback implements StreamCallback {

        private final byte[] salt;

        public EncryptCallback(final byte[] salt) {
            this.salt = salt;
        }

        @Override
        public void process(final InputStream in, final OutputStream out) throws IOException {
            final PBEParameterSpec parameterSpec = new PBEParameterSpec(salt, getIterationsCount());
            try {
                cipher.init(Cipher.ENCRYPT_MODE, secretKey, parameterSpec);
            } catch (final Exception e) {
                throw new ProcessException(e);
            }

            // If this is OpenSSL EVP, the salt must be preceded by the header
            if (isOpenSSLKDF()) {
                out.write(OPENSSL_EVP_HEADER_MARKER.getBytes(StandardCharsets.US_ASCII));
            }

            out.write(salt);

            final byte[] buffer = new byte[65536];
            int len;
            while ((len = in.read(buffer)) > 0) {
                final byte[] encryptedBytes = cipher.update(buffer, 0, len);
                if (encryptedBytes != null) {
                    out.write(encryptedBytes);
                }
            }

            try {
                out.write(cipher.doFinal());
            } catch (final IllegalBlockSizeException | BadPaddingException e) {
                throw new ProcessException(e);
            }
        }
    }
}
