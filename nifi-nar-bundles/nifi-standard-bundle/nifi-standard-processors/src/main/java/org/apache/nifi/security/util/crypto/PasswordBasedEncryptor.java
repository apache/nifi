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

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.spec.PBEKeySpec;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processors.standard.EncryptContent;
import org.apache.nifi.security.util.EncryptionMethod;
import org.apache.nifi.security.util.KeyDerivationFunction;
import org.apache.nifi.stream.io.ByteCountingInputStream;
import org.apache.nifi.stream.io.ByteCountingOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PasswordBasedEncryptor extends AbstractEncryptor {
    private static final Logger logger = LoggerFactory.getLogger(PasswordBasedEncryptor.class);

    private EncryptionMethod encryptionMethod;
    private PBEKeySpec password;
    private KeyDerivationFunction kdf;

    private static final int DEFAULT_MAX_ALLOWED_KEY_LENGTH = 128;
    private static final int MINIMUM_SAFE_PASSWORD_LENGTH = 10;

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

    static Map<String, String> writeAttributes(EncryptionMethod encryptionMethod,
                                               KeyDerivationFunction kdf, byte[] iv, byte[] kdfSalt, ByteCountingInputStream bcis, ByteCountingOutputStream bcos, boolean encryptMode) {
        Map<String, String> attributes = AbstractEncryptor.writeAttributes(encryptionMethod, kdf, iv, bcis, bcos, encryptMode);

        if (kdf.hasFormattedSalt()) {
            final String saltString = new String(kdfSalt, StandardCharsets.UTF_8);
            attributes.put(EncryptContent.KDF_SALT_ATTR, saltString);
            attributes.put(EncryptContent.KDF_SALT_LEN_ATTR, String.valueOf(saltString.length()));
        }

        byte[] rawSalt = CipherUtility.extractRawSalt(kdfSalt, kdf);
        attributes.put(EncryptContent.SALT_ATTR, Hex.encodeHexString(rawSalt));
        attributes.put(EncryptContent.SALT_LEN_ATTR, String.valueOf(rawSalt.length));

        return attributes;
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

        private static final boolean DECRYPT = false;

        // Limit retry mechanism to 10 MiB; more than that is wasteful
        private static final int RETRY_LIMIT_LENGTH = 10 * 1024 * 1024;

        public DecryptCallback() {
        }

        @Override
        public void process(final InputStream in, final OutputStream out) throws IOException {
            // Initialize cipher provider
            PBECipherProvider cipherProvider = (PBECipherProvider) CipherProviderFactory.getCipherProvider(kdf);

            // Wrap the streams for byte counting if necessary
            ByteCountingInputStream bcis = CipherUtility.wrapStreamForCounting(in);
            ByteCountingOutputStream bcos = CipherUtility.wrapStreamForCounting(out);

            // Read salt
            byte[] salt;
            try {
                // NiFi legacy code determined the salt length based on the cipher block size
                if (cipherProvider instanceof org.apache.nifi.security.util.crypto.NiFiLegacyCipherProvider) {
                    salt = ((org.apache.nifi.security.util.crypto.NiFiLegacyCipherProvider) cipherProvider).readSalt(encryptionMethod, bcis);
                } else {
                    salt = cipherProvider.readSalt(bcis);
                }
            } catch (final EOFException e) {
                throw new ProcessException("Cannot decrypt because file size is smaller than salt size", e);
            }

            // Determine necessary key length
            int keyLength = CipherUtility.parseKeyLengthFromAlgorithm(encryptionMethod.getAlgorithm());

            // Generate cipher
            try {
                Cipher cipher;
                final String password = new String(PasswordBasedEncryptor.this.password.getPassword());
                // Read IV if necessary
                byte[] iv = new byte[0];
                if (cipherProvider instanceof RandomIVPBECipherProvider) {
                    RandomIVPBECipherProvider rivpcp = (RandomIVPBECipherProvider) cipherProvider;
                    iv = rivpcp.readIV(bcis);
                    cipher = rivpcp.getCipher(encryptionMethod, password, salt, iv, keyLength, DECRYPT);
                } else {
                    cipher = cipherProvider.getCipher(encryptionMethod, password, salt, keyLength, DECRYPT);
                }

                // Bcrypt has multiple versions of KDF which can cause BC problems
                if (kdf == KeyDerivationFunction.BCRYPT) {
                    final boolean cipherProviderIsSafeType = cipherProvider instanceof BcryptCipherProvider;
                    logger.debug("Cipher provider instance of BcryptCipherProvider: {}", cipherProviderIsSafeType);
                    if (!cipherProviderIsSafeType) {
                        throw new ProcessException("The KDF is Bcrypt but the cipher provider is not a Bcrypt cipher provider; " + cipherProvider.getClass().getSimpleName());
                    }
                    handleBcryptDecryption((BcryptCipherProvider) cipherProvider, bcis, bcos, salt, keyLength, cipher, iv, password);
                } else {
                    // For any other KDF, only attempt decryption once
                    CipherUtility.processStreams(cipher, bcis, bcos);
                }

                // Update the attributes in the temporary holder
                flowfileAttributes.putAll(writeAttributes(encryptionMethod, kdf, cipher.getIV(), salt, bcis, bcos, DECRYPT));
            } catch (Exception e) {
                throw new ProcessException(e);
            }
        }

        /**
         * Handles the {@code Bcrypt} decryption separately, as the Bcrypt key derivation process changed during NiFi 1.12.0
         * and some cipher texts encrypted with a legacy key may need to be decrypted. This method attempts to decrypt normally,
         * and if certain conditions are met (the cipher text is under 10 MiB and the failure was specifically due to the wrong
         * key), the legacy key derivation is used to make a second attempt.
         *
         * @param bcryptCipherProvider the cipher provider
         * @param bcis                 the input stream (cipher text)
         * @param bcos                 the output stream to write the plaintext to
         * @param salt                 the salt
         * @param keyLength            the key length to derive in bits
         * @param cipher               the initial cipher object
         * @param iv                   the IV
         * @param password             the password
         * @throws IOException      if there is a problem reading/writing to the streams
         * @throws ProcessException if there is any other exception
         */
        protected void handleBcryptDecryption(BcryptCipherProvider bcryptCipherProvider, ByteCountingInputStream bcis,
                                              ByteCountingOutputStream bcos, byte[] salt, int keyLength, Cipher cipher,
                                              byte[] iv, String password)
                throws IOException, ProcessException {
            // Attempt the decryption and if it fails due to pad block exception (wrong key), derive key using legacy process and attempt to decrypt again
            bcis.mark(RETRY_LIMIT_LENGTH);

            try {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                CipherUtility.processStreams(cipher, bcis, baos);
                bcos.write(baos.toByteArray());
            } catch (ProcessException e) {
                // All exceptions from processStreams are wrapped in ProcessException, so check the underlying cause
                if (shouldAttemptLegacyDecrypt(e, bcis.getBytesConsumed())) {
                    logger.warn("Error decrypting content using Bcrypt-derived key; attempting legacy key derivation for backward compatibility");
                    bcis.reset();
                    cipher = bcryptCipherProvider.getLegacyDecryptCipher(encryptionMethod, password, salt, iv, keyLength);
                    CipherUtility.processStreams(cipher, bcis, bcos);
                } else {
                    // Some other exception or too much content to retry
                    throw e;
                }
            }
        }

        /**
         * Returns {@code true} if the Bcrypt decryption failed for reasons that might be resolved by attempting a second
         * decryption using the legacy key derivation process.
         *
         * @param e             the exception thrown during the initial decryption attempt
         * @param bytesConsumed the number of bytes consumed from the cipher text stream
         * @return true if the legacy key derivation process should be attempted
         */
        protected boolean shouldAttemptLegacyDecrypt(ProcessException e, long bytesConsumed) {
            final boolean causeIsWrongKey = e.getCause() instanceof BadPaddingException;
            final boolean bytesConsumedWithinLimit = bytesConsumed < RETRY_LIMIT_LENGTH;
            if (logger.isDebugEnabled()) {
                logger.debug("Exception cause instance of BadPaddingException: {}", causeIsWrongKey);
                logger.debug("Bytes consumed ({} B) < retry limit ({} B): {}", bytesConsumed, RETRY_LIMIT_LENGTH, bytesConsumedWithinLimit);
            }
            return causeIsWrongKey && bytesConsumedWithinLimit;
        }
    }

    @SuppressWarnings("deprecation")
    private class EncryptCallback implements StreamCallback {

        private static final boolean ENCRYPT = true;

        public EncryptCallback() {
        }

        @Override
        public void process(final InputStream in, final OutputStream out) throws IOException {
            // Initialize cipher provider
            PBECipherProvider cipherProvider = (PBECipherProvider) CipherProviderFactory.getCipherProvider(kdf);
            if (kdf == KeyDerivationFunction.PBKDF2) {
                flowfileAttributes.put("encryptcontent.pbkdf2_iterations", String.valueOf(((PBKDF2CipherProvider) cipherProvider).getIterationCount()));
            }

            // Generate salt
            byte[] salt;
            // NiFi legacy code determined the salt length based on the cipher block size
            if (cipherProvider instanceof org.apache.nifi.security.util.crypto.NiFiLegacyCipherProvider) {
                salt = ((org.apache.nifi.security.util.crypto.NiFiLegacyCipherProvider) cipherProvider).generateSalt(encryptionMethod);
            } else {
                salt = cipherProvider.generateSalt();
            }

            // Wrap the streams for byte counting if necessary
            ByteCountingInputStream bcis = CipherUtility.wrapStreamForCounting(in);
            ByteCountingOutputStream bcos = CipherUtility.wrapStreamForCounting(out);

            // Write to output stream
            cipherProvider.writeSalt(salt, bcos);

            // Determine necessary key length
            int keyLength = CipherUtility.parseKeyLengthFromAlgorithm(encryptionMethod.getAlgorithm());

            // Generate cipher
            try {
                Cipher cipher = cipherProvider.getCipher(encryptionMethod, new String(password.getPassword()), salt, keyLength, ENCRYPT);

                // Write IV if necessary
                if (cipherProvider instanceof RandomIVPBECipherProvider) {
                    ((RandomIVPBECipherProvider) cipherProvider).writeIV(cipher.getIV(), bcos);
                }
                CipherUtility.processStreams(cipher, bcis, bcos);

                // Update the attributes in the temporary holder
                flowfileAttributes.putAll(writeAttributes(encryptionMethod, kdf, cipher.getIV(), salt, bcis, bcos, ENCRYPT));
            } catch (Exception e) {
                throw new ProcessException(e);
            }
        }
    }
}