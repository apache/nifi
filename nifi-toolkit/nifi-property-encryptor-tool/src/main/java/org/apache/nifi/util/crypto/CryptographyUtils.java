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
package org.apache.nifi.util.crypto;

import org.apache.commons.codec.binary.Hex;
import org.bouncycastle.crypto.generators.SCrypt;

import javax.crypto.Cipher;
import java.nio.charset.StandardCharsets;
import java.security.KeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.List;

public class CryptographyUtils {

    private static final String NIFI_SCRYPT_SALT = "NIFI_SCRYPT_SALT";
    private static final int DEFAULT_MIN_PASSPHRASE_LENGTH = 12;

    // Strong parameters as of 12 Aug 2016
    private static final int SCRYPT_N = (int) Math.pow(2, 16);
    private static final int SCRYPT_R = 8;
    private static final int SCRYPT_P = 1;

    public static String deriveKeyFromPassphrase(final String passphrase) throws KeyException, NoSuchAlgorithmException {
        return deriveKeyFromPassphrase(passphrase, DEFAULT_MIN_PASSPHRASE_LENGTH);
    }

    public static boolean isUnlimitedStrengthCryptoAvailable() {
        try {
            return Cipher.getMaxAllowedKeyLength("AES") > 128;
        } catch (NoSuchAlgorithmException e) {
            return false;
        }
    }

    private static String deriveKeyFromPassphrase(final String passphrase, final int minPassphraseLength) throws KeyException, NoSuchAlgorithmException {
        final String trimmedPassphrase = passphrase.trim();
        if (trimmedPassphrase.length() < minPassphraseLength) {
            throw new KeyException(String.format("Cannot derive key from empty/short passphrase -- passphrase must be at least %d characters", DEFAULT_MIN_PASSPHRASE_LENGTH));
        }

        // Generate a 128 bit salt
        byte[] salt = generateScryptSalt();
        int keyLengthInBytes = getValidKeyLengths().stream().max(Integer::compare).get() / 8;
        byte[] derivedKeyBytes = SCrypt.generate(passphrase.getBytes(StandardCharsets.UTF_8), salt, SCRYPT_N, SCRYPT_R, SCRYPT_P, keyLengthInBytes);
        return Hex.encodeHexString(derivedKeyBytes).toUpperCase();
    }

    private static byte[] generateScryptSalt() {
        /* It is not ideal to use a static salt, but the KDF operation must be deterministic
        for a given passphrase, and storing and retrieving the salt in bootstrap.conf causes
        compatibility concerns
        */
        return NIFI_SCRYPT_SALT.getBytes(StandardCharsets.UTF_8);
    }

    private static List<Integer> getValidKeyLengths() throws NoSuchAlgorithmException {
        return Cipher.getMaxAllowedKeyLength("AES") > 128 ? Arrays.asList(128, 192, 256) : Arrays.asList(128);
    }
}
