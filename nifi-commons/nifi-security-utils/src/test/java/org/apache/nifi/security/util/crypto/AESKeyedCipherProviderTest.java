/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License") you may not use this file except in compliance with
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

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.apache.nifi.security.util.EncryptionMethod;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.security.SecureRandom;
import java.security.Security;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AESKeyedCipherProviderTest {
    private static final String KEY_HEX = "0123456789ABCDEFFEDCBA9876543210";

    private static final String PLAINTEXT = "ExactBlockSizeRequiredForProcess";

    private static final List<EncryptionMethod> keyedEncryptionMethods = Arrays.stream(EncryptionMethod.values())
            .filter(EncryptionMethod::isKeyedCipher)
            .collect(Collectors.toList());

    private static SecretKey key;


    @BeforeAll
    static void setUpOnce() throws Exception {
        Security.addProvider(new BouncyCastleProvider());

        try {
            key = new SecretKeySpec(Hex.decodeHex(KEY_HEX.toCharArray()), "AES");
        } catch (final DecoderException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    void testGetCipherShouldBeInternallyConsistent() throws Exception {
        // Arrange
        KeyedCipherProvider cipherProvider = new AESKeyedCipherProvider();

        // Act
        for (EncryptionMethod em : keyedEncryptionMethods) {
            // Initialize a cipher for encryption
            Cipher cipher = cipherProvider.getCipher(em, key, true);
            byte[] iv = cipher.getIV();

            byte[] cipherBytes = cipher.doFinal(PLAINTEXT.getBytes("UTF-8"));

            cipher = cipherProvider.getCipher(em, key, iv, false);
            byte[] recoveredBytes = cipher.doFinal(cipherBytes);
            String recovered = new String(recoveredBytes, "UTF-8");

            // Assert
            assertEquals(PLAINTEXT, recovered);
        }
    }

    @Test
    void testGetCipherWithExternalIVShouldBeInternallyConsistent() throws Exception {
        // Arrange
        KeyedCipherProvider cipherProvider = new AESKeyedCipherProvider();

        // Act
        for (final EncryptionMethod em : keyedEncryptionMethods) {
            byte[] iv = cipherProvider.generateIV();

            // Initialize a cipher for encryption
            Cipher cipher = cipherProvider.getCipher(em, key, iv, true);

            byte[] cipherBytes = cipher.doFinal(PLAINTEXT.getBytes("UTF-8"));

            cipher = cipherProvider.getCipher(em, key, iv, false);
            byte[] recoveredBytes = cipher.doFinal(cipherBytes);
            String recovered = new String(recoveredBytes, "UTF-8");

            // Assert
            assertEquals(PLAINTEXT, recovered);
        }
    }

    @Test
    void testGetCipherWithUnlimitedStrengthShouldBeInternallyConsistent() throws Exception {
        // Arrange
        KeyedCipherProvider cipherProvider = new AESKeyedCipherProvider();
        final List<Integer> longKeyLengths = Arrays.asList(192, 256);

        SecureRandom secureRandom = new SecureRandom();

        // Act
        for (final EncryptionMethod em : keyedEncryptionMethods) {
            // Re-use the same IV for the different length keys to ensure the encryption is different
            byte[] iv = cipherProvider.generateIV();

            for (final int keyLength: longKeyLengths) {
                // Generate a key
                byte[] keyBytes = new byte[keyLength / 8];
                secureRandom.nextBytes(keyBytes);
                SecretKey localKey = new SecretKeySpec(keyBytes, "AES");

                // Initialize a cipher for encryption
                Cipher cipher = cipherProvider.getCipher(em, localKey, iv, true);

                byte[] cipherBytes = cipher.doFinal(PLAINTEXT.getBytes("UTF-8"));

                cipher = cipherProvider.getCipher(em, localKey, iv, false);
                byte[] recoveredBytes = cipher.doFinal(cipherBytes);
                String recovered = new String(recoveredBytes, "UTF-8");

                // Assert
                assertEquals(PLAINTEXT, recovered);
            }
        }
    }

    @Test
    void testShouldRejectEmptyKey() {
        // Arrange
        KeyedCipherProvider cipherProvider = new AESKeyedCipherProvider();

        final EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC;

        // Act
        IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                () -> cipherProvider.getCipher(encryptionMethod, null, true));

        // Assert
        assertTrue(iae.getMessage().contains("The key must be specified"));
    }

    @Test
    void testShouldRejectIncorrectLengthKey() throws Exception {
        // Arrange
        KeyedCipherProvider cipherProvider = new AESKeyedCipherProvider();

        SecretKey localKey = new SecretKeySpec(Hex.decodeHex("0123456789ABCDEF".toCharArray()), "AES");
        assertFalse(Arrays.asList(128, 192, 256).contains(localKey.getEncoded().length));

        final EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC;

        // Act
        IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                () -> cipherProvider.getCipher(encryptionMethod, localKey, true));

        // Assert
        assertTrue(iae.getMessage().contains("The key must be of length [128, 192, 256]"));
    }

    @Test
    void testShouldRejectEmptyEncryptionMethod() throws Exception {
        // Arrange
        KeyedCipherProvider cipherProvider = new AESKeyedCipherProvider();

        // Act
        IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                () -> cipherProvider.getCipher(null, key, true));

        // Assert
        assertTrue(iae.getMessage().contains("The encryption method must be specified"));
    }

    @Test
    void testShouldRejectUnsupportedEncryptionMethod() throws Exception {
        // Arrange
        KeyedCipherProvider cipherProvider = new AESKeyedCipherProvider();

        final EncryptionMethod encryptionMethod = EncryptionMethod.MD5_128AES;

        // Act
        IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                () -> cipherProvider.getCipher(encryptionMethod, key, true));

        // Assert
        assertTrue(iae.getMessage().contains("requires a PBECipherProvider"));
    }

    @Test
    void testGetCipherShouldSupportExternalCompatibility() throws Exception {
        // Arrange
        KeyedCipherProvider cipherProvider = new AESKeyedCipherProvider();

        final String plaintext = "This is a plaintext message.";

        // These values can be generated by running `$ ./openssl_aes.rb` in the terminal
        final byte[] IV = Hex.decodeHex("e0bc8cc7fbc0bdfdc184dc22ce2fcb5b".toCharArray());
        final byte[] LOCAL_KEY = Hex.decodeHex("c72943d27c3e5a276169c5998a779117".toCharArray());
        final String CIPHER_TEXT = "a2725ea55c7dd717664d044cab0f0b5f763653e322c27df21954f5be394efb1b";
        byte[] cipherBytes = Hex.decodeHex(CIPHER_TEXT.toCharArray());

        SecretKey localKey = new SecretKeySpec(LOCAL_KEY, "AES");

        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC;

        // Act
        Cipher cipher = cipherProvider.getCipher(encryptionMethod, localKey, IV, false);
        byte[] recoveredBytes = cipher.doFinal(cipherBytes);
        String recovered = new String(recoveredBytes, "UTF-8");

        // Assert
        assertEquals(plaintext, recovered);
    }

    @Test
    void testGetCipherForDecryptShouldRequireIV() throws Exception {
        // Arrange
        KeyedCipherProvider cipherProvider = new AESKeyedCipherProvider();

        // Act
        for (final EncryptionMethod em : keyedEncryptionMethods) {
            byte[] iv = cipherProvider.generateIV();

            // Initialize a cipher for encryption
            Cipher cipher = cipherProvider.getCipher(em, key, iv, true);

            byte[] cipherBytes = cipher.doFinal(PLAINTEXT.getBytes("UTF-8"));

            IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                    () -> cipherProvider.getCipher(em, key, false));

            // Assert
            assertTrue(iae.getMessage().contains("Cannot decrypt without a valid IV"));
        }
    }

    @Test
    void testGetCipherShouldRejectInvalidIVLengths() throws Exception {
        // Arrange
        KeyedCipherProvider cipherProvider = new AESKeyedCipherProvider();

        final int MAX_LENGTH = 15;
        final List<byte[]> INVALID_IVS = new ArrayList<>();
        for (int length = 0; length <= MAX_LENGTH; length++) {
            INVALID_IVS.add(new byte[length]);
        }

        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC;

        // Act
        for (final byte[] badIV : INVALID_IVS) {
            // Encrypt should print a warning about the bad IV but overwrite it
            Cipher cipher = cipherProvider.getCipher(encryptionMethod, key, badIV, true);

            // Decrypt should fail
            IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                    () -> cipherProvider.getCipher(encryptionMethod, key, badIV, false));

            // Assert
            assertTrue(iae.getMessage().contains("Cannot decrypt without a valid IV"));
        }
    }

    @Test
    void testGetCipherShouldRejectEmptyIV() throws Exception {
        // Arrange
        KeyedCipherProvider cipherProvider = new AESKeyedCipherProvider();

        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC;

        byte[] badIV = new byte[16];
        Arrays.fill(badIV, (byte) '\0');

        // Encrypt should print a warning about the bad IV but overwrite it
        Cipher cipher = cipherProvider.getCipher(encryptionMethod, key, badIV, true);

        // Decrypt should fail
        IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                () -> cipherProvider.getCipher(encryptionMethod, key, badIV, false));

        // Assert
        assertTrue(iae.getMessage().contains("Cannot decrypt without a valid IV"));
    }
}
