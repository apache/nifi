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

import java.io.UnsupportedEncodingException;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;
import java.util.List;
import javax.crypto.Cipher;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.security.util.EncryptionMethod;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a standard implementation of {@link KeyedCipherProvider} which supports {@code AES} cipher families with arbitrary modes of operation (currently only {@code CBC}, {@code CTR}, and {@code
 * GCM} are supported as {@link EncryptionMethod}s.
 */
public class AESKeyedCipherProvider extends KeyedCipherProvider {
    private static final Logger logger = LoggerFactory.getLogger(AESKeyedCipherProvider.class);
    private static final int IV_LENGTH = 16;
    private static final List<Integer> VALID_KEY_LENGTHS = Arrays.asList(128, 192, 256);

    /**
     * Returns an initialized cipher for the specified algorithm. The IV is provided externally to allow for non-deterministic IVs, as IVs
     * deterministically derived from the password are a potential vulnerability and compromise semantic security. See
     * <a href="http://crypto.stackexchange.com/a/3970/12569">Ilmari Karonen's answer on Crypto Stack Exchange</a>
     *
     * @param encryptionMethod the {@link EncryptionMethod}
     * @param key              the key
     * @param iv               the IV or nonce (cannot be all 0x00)
     * @param encryptMode      true for encrypt, false for decrypt
     * @return the initialized cipher
     * @throws Exception if there is a problem initializing the cipher
     */
    @Override
    public Cipher getCipher(EncryptionMethod encryptionMethod, SecretKey key, byte[] iv, boolean encryptMode) throws Exception {
        try {
            return getInitializedCipher(encryptionMethod, key, iv, encryptMode);
        } catch (IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new ProcessException("Error initializing the cipher", e);
        }
    }

    /**
     * Returns an initialized cipher for the specified algorithm. The IV will be generated internally (for encryption). If decryption is requested, it will throw an exception.
     *
     * @param encryptionMethod the {@link EncryptionMethod}
     * @param key              the key
     * @param encryptMode      true for encrypt, false for decrypt
     * @return the initialized cipher
     * @throws Exception if there is a problem initializing the cipher or if decryption is requested
     */
    @Override
    public Cipher getCipher(EncryptionMethod encryptionMethod, SecretKey key, boolean encryptMode) throws Exception {
        return getCipher(encryptionMethod, key, new byte[0], encryptMode);
    }

    protected Cipher getInitializedCipher(EncryptionMethod encryptionMethod, SecretKey key, byte[] iv,
                                          boolean encryptMode) throws NoSuchAlgorithmException, NoSuchProviderException,
            InvalidKeySpecException, NoSuchPaddingException, InvalidKeyException, InvalidAlgorithmParameterException, UnsupportedEncodingException {
        if (encryptionMethod == null) {
            throw new IllegalArgumentException("The encryption method must be specified");
        }

        if (!encryptionMethod.isKeyedCipher()) {
            throw new IllegalArgumentException(encryptionMethod.name() + " requires a PBECipherProvider");
        }

        String algorithm = encryptionMethod.getAlgorithm();
        String provider = encryptionMethod.getProvider();

        if (key == null) {
            throw new IllegalArgumentException("The key must be specified");
        }

        if (!isValidKeyLength(key)) {
            throw new IllegalArgumentException("The key must be of length [" + StringUtils.join(VALID_KEY_LENGTHS, ", ") + "]");
        }

        Cipher cipher = Cipher.getInstance(algorithm, provider);
        final String operation = encryptMode ? "encrypt" : "decrypt";

        boolean ivIsInvalid = false;

        // If an IV was not provided already, generate a random IV and inject it in the cipher
        int ivLength = cipher.getBlockSize();
        if (iv.length != ivLength) {
            logger.warn("An IV was provided of length {} bytes for {}ion but should be {} bytes", iv.length, operation, ivLength);
            ivIsInvalid = true;
        }

        final byte[] emptyIv = new byte[ivLength];
        if (Arrays.equals(iv, emptyIv)) {
            logger.warn("An empty IV was provided of length {} for {}ion", iv.length, operation);
            ivIsInvalid = true;
        }

        if (ivIsInvalid) {
            if (encryptMode) {
                logger.warn("Generating new IV. The value can be obtained in the calling code by invoking 'cipher.getIV()';");
                iv = generateIV();
            } else {
                // Can't decrypt without an IV
                throw new IllegalArgumentException("Cannot decrypt without a valid IV");
            }
        }
        cipher.init(encryptMode ? Cipher.ENCRYPT_MODE : Cipher.DECRYPT_MODE, key, new IvParameterSpec(iv));

        return cipher;
    }

    private boolean isValidKeyLength(SecretKey key) {
        return VALID_KEY_LENGTHS.contains(key.getEncoded().length * 8);
    }

    /**
     * Generates a new random IV of 16 bytes using {@link java.security.SecureRandom}.
     *
     * @return the IV
     */
    public byte[] generateIV() {
        byte[] iv = new byte[IV_LENGTH];
        new SecureRandom().nextBytes(iv);
        return iv;
    }
}
