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

import at.favre.lib.crypto.bcrypt.BCrypt;
import at.favre.lib.crypto.bcrypt.Radix64Encoder;
import org.apache.commons.codec.binary.Hex;
import org.apache.nifi.security.util.EncryptionMethod;
import org.apache.nifi.util.StringUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.Security;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BcryptCipherProviderTest {
    private static final String PLAINTEXT = "ExactBlockSizeRequiredForProcess";
    private static final String BAD_PASSWORD = "thisIsABadPassword";
    private static final String SHORT_PASSWORD = "shortPassword";

    private static List<EncryptionMethod> strongKDFEncryptionMethods;

    private static final int DEFAULT_KEY_LENGTH = 128;
    private static List<Integer> AES_KEY_LENGTHS;


    @BeforeAll
    static void setUpOnce() throws Exception {
        Security.addProvider(new BouncyCastleProvider());
        strongKDFEncryptionMethods = Arrays.stream(EncryptionMethod.values())
                .filter(EncryptionMethod::isCompatibleWithStrongKDFs)
                .collect(Collectors.toList());

        AES_KEY_LENGTHS = Arrays.asList(128, 192, 256);
    }

    @Test
    void testGetCipherShouldBeInternallyConsistent() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider = new BcryptCipherProvider(4);

        final byte[] SALT = cipherProvider.generateSalt();

        // Act
        for (EncryptionMethod em : strongKDFEncryptionMethods) {
            // Initialize a cipher for encryption
            Cipher cipher = cipherProvider.getCipher(em, SHORT_PASSWORD, SALT, DEFAULT_KEY_LENGTH, true);
            byte[] iv = cipher.getIV();

            byte[] cipherBytes = cipher.doFinal(PLAINTEXT.getBytes("UTF-8"));

            cipher = cipherProvider.getCipher(em, SHORT_PASSWORD, SALT, iv, DEFAULT_KEY_LENGTH, false);
            byte[] recoveredBytes = cipher.doFinal(cipherBytes);
            String recovered = new String(recoveredBytes, "UTF-8");

            // Assert
            assertEquals(PLAINTEXT, recovered);
        }
    }

    @Test
    void testGetCipherWithExternalIVShouldBeInternallyConsistent() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider = new BcryptCipherProvider(4);

        final byte[] SALT = cipherProvider.generateSalt();
        final byte[] IV = Hex.decodeHex(StringUtils.repeat("01", 16).toCharArray());

        // Act
        for (EncryptionMethod em : strongKDFEncryptionMethods) {
            // Initialize a cipher for encryption
            Cipher cipher = cipherProvider.getCipher(em, SHORT_PASSWORD, SALT, IV, DEFAULT_KEY_LENGTH, true);

            byte[] cipherBytes = cipher.doFinal(PLAINTEXT.getBytes("UTF-8"));

            cipher = cipherProvider.getCipher(em, SHORT_PASSWORD, SALT, IV, DEFAULT_KEY_LENGTH, false);
            byte[] recoveredBytes = cipher.doFinal(cipherBytes);
            String recovered = new String(recoveredBytes, "UTF-8");

            // Assert
            assertEquals(PLAINTEXT, recovered);
        }
    }

    @Test
    void testGetCipherWithUnlimitedStrengthShouldBeInternallyConsistent() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider = new BcryptCipherProvider(4);

        final byte[] SALT = cipherProvider.generateSalt();

        final int LONG_KEY_LENGTH = 256;

        // Act
        for (EncryptionMethod em : strongKDFEncryptionMethods) {
            // Initialize a cipher for encryption
            Cipher cipher = cipherProvider.getCipher(em, SHORT_PASSWORD, SALT, LONG_KEY_LENGTH, true);
            byte[] iv = cipher.getIV();

            byte[] cipherBytes = cipher.doFinal(PLAINTEXT.getBytes("UTF-8"));

            cipher = cipherProvider.getCipher(em, SHORT_PASSWORD, SALT, iv, LONG_KEY_LENGTH, false);
            byte[] recoveredBytes = cipher.doFinal(cipherBytes);
            String recovered = new String(recoveredBytes, "UTF-8");

            // Assert
            assertEquals(PLAINTEXT, recovered);
        }
    }

    @Test
    void testHashPWShouldMatchTestVectors() {
        // Arrange
        final byte[] PASSWORD = "abcdefghijklmnopqrstuvwxyz".getBytes(StandardCharsets.UTF_8);
        final byte[] SALT = new Radix64Encoder.Default().decode("fVH8e28OQRj9tqiDXs1e1u".getBytes(StandardCharsets.UTF_8));
        final String EXPECTED_HASH = "$2a$10$fVH8e28OQRj9tqiDXs1e1uxpsjN0c7II7YPKXua2NAKYvM6iQk7dq";
        final int WORK_FACTOR = 10;

        // Act
        String libraryCalculatedHash = new String(BCrypt.withDefaults().hash(WORK_FACTOR, SALT, PASSWORD), StandardCharsets.UTF_8);

        BcryptSecureHasher bcryptSecureHasher = new BcryptSecureHasher(WORK_FACTOR);
        String secureHasherCalculatedHash = new String(bcryptSecureHasher.hashRaw(PASSWORD, SALT), StandardCharsets.UTF_8);

        // Assert
        assertEquals(EXPECTED_HASH, secureHasherCalculatedHash);
        assertEquals(EXPECTED_HASH, secureHasherCalculatedHash);
    }

    @Test
    void testGetCipherShouldSupportExternalCompatibility() throws Exception {
        // Arrange
        final int WORK_FACTOR = 10;
        RandomIVPBECipherProvider cipherProvider = new BcryptCipherProvider(WORK_FACTOR);

        final String PLAINTEXT = "This is a plaintext message.";

        // These values can be generated by running `$ ./openssl_bcrypt` in the terminal
        // The Ruby bcrypt gem does not expose the custom Radix64 decoder, so maintain the R64 encoding from the output and decode here
        final byte[] SALT = new Radix64Encoder.Default().decode("LBVzJoPgh.85YCvnos4BKO".getBytes());
        final byte[] IV = Hex.decodeHex("bae8a9d935748a75ff0e0bbd95a4f024".toCharArray());

        // $v2$w2$base64_salt_22__base64_hash_31
        final String FULL_HASH = "$2a$10$LBVzJoPgh.85YCvnos4BKOyYM.LRni6UbU4v/CEPBkmFIiigADJZi";

        final String CIPHER_TEXT = "d232b68e7aa38242d195c54b8f360d8b8d6b7580b190ffdeef99f5fe460bd6b0";
        byte[] cipherBytes = Hex.decodeHex(CIPHER_TEXT.toCharArray());

        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC;

        // Sanity check
        Cipher rubyCipher = Cipher.getInstance(encryptionMethod.getAlgorithm(), "BC");
        SecretKeySpec rubyKey = new SecretKeySpec(Hex.decodeHex("01ea96ccc48a1d045bd7f461721b94a8".toCharArray()), "AES");
        IvParameterSpec ivSpec = new IvParameterSpec(IV);
        rubyCipher.init(Cipher.ENCRYPT_MODE, rubyKey, ivSpec);
        byte[] rubyCipherBytes = rubyCipher.doFinal(PLAINTEXT.getBytes());
        rubyCipher.init(Cipher.DECRYPT_MODE, rubyKey, ivSpec);
        assertArrayEquals(PLAINTEXT.getBytes(), rubyCipher.doFinal(rubyCipherBytes));
        assertArrayEquals(PLAINTEXT.getBytes(), rubyCipher.doFinal(cipherBytes));

        // Sanity for hash generation
        final String FULL_SALT = FULL_HASH.substring(0, 29);
        String generatedHash = new String(BCrypt.withDefaults().hash(WORK_FACTOR, BcryptCipherProvider.extractRawSalt(FULL_SALT), BAD_PASSWORD.getBytes()));
        assertEquals(FULL_HASH, generatedHash);

        // Act
        Cipher cipher = cipherProvider.getCipher(encryptionMethod, BAD_PASSWORD, FULL_SALT.getBytes(), IV, DEFAULT_KEY_LENGTH, false);
        byte[] recoveredBytes = cipher.doFinal(cipherBytes);
        String recovered = new String(recoveredBytes, "UTF-8");

        // Assert
        assertEquals(PLAINTEXT, recovered);
    }

    private static byte[] customB64Decode(String input) {
        return customB64Decode(input.getBytes());
    }

    private static byte[] customB64Decode(byte[] input) {
        return new Radix64Encoder.Default().decode(input);
    }

    private static String customB64Encode(String input) {
        return customB64Encode(input.getBytes());
    }

    private static String customB64Encode(byte[] input) {
        return new String(new Radix64Encoder.Default().encode(input), StandardCharsets.UTF_8);
    }


    @Test
    void testGetCipherShouldHandleFullSalt() throws Exception {
        // Arrange
        final int WORK_FACTOR = 10;
        RandomIVPBECipherProvider cipherProvider = new BcryptCipherProvider(WORK_FACTOR);

        final String PLAINTEXT = "This is a plaintext message.";

        // These values can be generated by running `$ ./openssl_bcrypt.rb` in the terminal
        final byte[] IV = Hex.decodeHex("bae8a9d935748a75ff0e0bbd95a4f024".toCharArray());

        // $v2$w2$base64_salt_22__base64_hash_31
        final String FULL_HASH = "$2a$10$LBVzJoPgh.85YCvnos4BKOyYM.LRni6UbU4v/CEPBkmFIiigADJZi";
        final String FULL_SALT = FULL_HASH.substring(0, 29);

        final String CIPHER_TEXT = "d232b68e7aa38242d195c54b8f360d8b8d6b7580b190ffdeef99f5fe460bd6b0";
        byte[] cipherBytes = Hex.decodeHex(CIPHER_TEXT.toCharArray());

        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC;

        // Act
        Cipher cipher = cipherProvider.getCipher(encryptionMethod, BAD_PASSWORD, FULL_SALT.getBytes(), IV, DEFAULT_KEY_LENGTH, false);
        byte[] recoveredBytes = cipher.doFinal(cipherBytes);
        String recovered = new String(recoveredBytes, "UTF-8");

        // Assert
        assertEquals(PLAINTEXT, recovered);
    }

    @Test
    void testGetCipherShouldHandleUnformedSalt() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider = new BcryptCipherProvider(4);

        final List<String> INVALID_SALTS = Arrays.asList("$ab$00$acbdefghijklmnopqrstuv", "bad_salt", "$3a$11$", "x", "$2a$10$");

        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC;

        // Act
        for (final String salt : INVALID_SALTS) {
            IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                    () -> cipherProvider.getCipher(encryptionMethod, BAD_PASSWORD, salt.getBytes(), DEFAULT_KEY_LENGTH, true));

            // Assert
            assertTrue(iae.getMessage().contains("The salt must be of the format $2a$10$gUVbkVzp79H8YaCOsCVZNu. To generate a salt, use BcryptCipherProvider#generateSalt"));
        }
    }

    @Test
    void testGetCipherShouldRejectEmptySalt() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider = new BcryptCipherProvider(4);

        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC;

        // Two different errors -- one explaining the no-salt method is not supported, and the other for an empty byte[] passed

        // Act
        IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                () -> cipherProvider.getCipher(encryptionMethod, BAD_PASSWORD, new byte[0], DEFAULT_KEY_LENGTH, true));

        // Assert
        assertTrue(iae.getMessage().contains("format"));
    }

    @Test
    void testGetCipherForDecryptShouldRequireIV() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider = new BcryptCipherProvider(4);

        final byte[] SALT = cipherProvider.generateSalt();
        final byte[] IV = Hex.decodeHex(StringUtils.repeat("00", 16).toCharArray());

        // Act
        for (EncryptionMethod em : strongKDFEncryptionMethods) {
            // Initialize a cipher for encryption
            Cipher cipher = cipherProvider.getCipher(em, SHORT_PASSWORD, SALT, IV, DEFAULT_KEY_LENGTH, true);

            byte[] cipherBytes = cipher.doFinal(PLAINTEXT.getBytes("UTF-8"));

            IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                    () -> cipherProvider.getCipher(em, SHORT_PASSWORD, SALT, DEFAULT_KEY_LENGTH, false));

            // Assert
            assertTrue(iae.getMessage().contains("Cannot decrypt without a valid IV"));
        }
    }

    @Test
    void testGetCipherShouldAcceptValidKeyLengths() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider = new BcryptCipherProvider(4);

        final byte[] SALT = cipherProvider.generateSalt();
        final byte[] IV = Hex.decodeHex(StringUtils.repeat("01", 16).toCharArray());

        // Currently only AES ciphers are compatible with Bcrypt, so redundant to test all algorithms
        final List<Integer> VALID_KEY_LENGTHS = AES_KEY_LENGTHS;
        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC;

        // Act
        for (final int keyLength : VALID_KEY_LENGTHS) {
            // Initialize a cipher for encryption
            Cipher cipher = cipherProvider.getCipher(encryptionMethod, SHORT_PASSWORD, SALT, IV, keyLength, true);

            byte[] cipherBytes = cipher.doFinal(PLAINTEXT.getBytes("UTF-8"));

            cipher = cipherProvider.getCipher(encryptionMethod, SHORT_PASSWORD, SALT, IV, keyLength, false);
            byte[] recoveredBytes = cipher.doFinal(cipherBytes);
            String recovered = new String(recoveredBytes, "UTF-8");

            // Assert
            assertEquals(PLAINTEXT, recovered);
        }
    }

    @Test
    void testGetCipherShouldNotAcceptInvalidKeyLengths() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider = new BcryptCipherProvider(4);

        final byte[] SALT = cipherProvider.generateSalt();
        final byte[] IV = Hex.decodeHex(StringUtils.repeat("00", 16).toCharArray());

        // Currently only AES ciphers are compatible with Bcrypt, so redundant to test all algorithms
        final List<Integer> INVALID_KEY_LENGTHS = Arrays.asList(-1, 40, 64, 112, 512);
        EncryptionMethod encryptionMethod = EncryptionMethod.AES_CBC;

        // Act
        for (final int keyLength : INVALID_KEY_LENGTHS) {
            // Initialize a cipher for encryption
            IllegalArgumentException iae = assertThrows(IllegalArgumentException.class,
                    () -> cipherProvider.getCipher(encryptionMethod, SHORT_PASSWORD, SALT, IV, keyLength, true));

            // Assert
            assertTrue(iae.getMessage().contains(keyLength + " is not a valid key length for AES"));
        }
    }

    @Test
    void testGenerateSaltShouldUseProvidedWorkFactor() throws Exception {
        // Arrange
        BcryptCipherProvider cipherProvider = new BcryptCipherProvider(11);
        int workFactor = cipherProvider.getWorkFactor();

        // Act
        final byte[] saltBytes = cipherProvider.generateSalt();
        String salt = new String(saltBytes);

        // Assert
        final Matcher matcher = Pattern.compile("^\\$2[axy]\\$\\d{2}\\$").matcher(salt);
        assertTrue(matcher.find());
        assertTrue(salt.contains("$" + workFactor + "$"));
    }

    /**
     * For {@code 1.12.0} the key derivation process was changed. Previously, the entire hash output
     * ({@code $2a$10$9XUQnxGEUsRdLqEhxY3xNujOQQkW3spKqxssi.Ox39VhhxB.z4496}) was fed to {@code SHA-512}
     * to stretch the hash output to a custom key length (128, 192, or 256 bits) because the Bcrypt hash
     * output length is fixed at 184 bits. The new key derivation process only feeds the <em>non-salt
     * hash output</em> (({@code jOQQkW3spKqxssi.Ox39VhhxB.z4496})) into the digest.
     */
    @Test
    void testGetCipherShouldUseHashOutputOnlyToDeriveKey() throws Exception {
        // Arrange
        BcryptCipherProvider cipherProvider = new BcryptCipherProvider(4);

        final byte[] SALT = cipherProvider.generateSalt();
        String saltString = new String(SALT, StandardCharsets.UTF_8);

        // Determine the expected key bytes using the new key derivation process
        BcryptSecureHasher bcryptSecureHasher = new BcryptSecureHasher(cipherProvider.getWorkFactor(), cipherProvider.getDefaultSaltLength());
        byte[] rawSaltBytes = BcryptCipherProvider.extractRawSalt(saltString);
        byte[] hashOutputBytes = bcryptSecureHasher.hashRaw(SHORT_PASSWORD.getBytes(StandardCharsets.UTF_8), rawSaltBytes);

        MessageDigest sha512 = MessageDigest.getInstance("SHA-512", "BC");
        byte[] keyDigestBytes = sha512.digest(Arrays.copyOfRange(hashOutputBytes, hashOutputBytes.length - 31, hashOutputBytes.length));

        // Act
        for (EncryptionMethod em : strongKDFEncryptionMethods) {
            // Initialize a cipher for encryption
            Cipher cipher = cipherProvider.getCipher(em, SHORT_PASSWORD, SALT, DEFAULT_KEY_LENGTH, true);
            byte[] iv = cipher.getIV();

            byte[] cipherBytes = cipher.doFinal(PLAINTEXT.getBytes("UTF-8"));

            cipher = cipherProvider.getCipher(em, SHORT_PASSWORD, SALT, iv, DEFAULT_KEY_LENGTH, false);
            byte[] recoveredBytes = cipher.doFinal(cipherBytes);
            String recovered = new String(recoveredBytes, "UTF-8");

            // Expected key verification
            int keyLength = CipherUtility.parseKeyLengthFromAlgorithm(em.getAlgorithm());
            byte[] derivedKeyBytes = Arrays.copyOf(keyDigestBytes, keyLength / 8);

            Cipher verificationCipher = Cipher.getInstance(em.getAlgorithm());
            verificationCipher.init(Cipher.DECRYPT_MODE, new SecretKeySpec(derivedKeyBytes, em.getAlgorithm()), new IvParameterSpec(iv));
            byte[] verificationBytes = verificationCipher.doFinal(cipherBytes);
            String verificationRecovered = new String(verificationBytes, StandardCharsets.UTF_8);

            // Assert
            assertEquals(PLAINTEXT, recovered);
            assertEquals(PLAINTEXT, verificationRecovered);
        }
    }

    @Test
    void testGetCipherShouldBeBackwardCompatibleWithFullHashKeyDerivation() throws Exception {
        // Arrange
        BcryptCipherProvider cipherProvider = new BcryptCipherProvider(4);

        final byte[] SALT = cipherProvider.generateSalt();

        // Act
        for (EncryptionMethod em : strongKDFEncryptionMethods) {
            // Initialize a cipher for encryption using the legacy key derivation process
            Cipher cipher = cipherProvider.getInitializedCipher(em, SHORT_PASSWORD, SALT, new byte[0], DEFAULT_KEY_LENGTH, true, true);
            byte[] iv = cipher.getIV();

            byte[] cipherBytes = cipher.doFinal(PLAINTEXT.getBytes("UTF-8"));

            cipher = cipherProvider.getLegacyDecryptCipher(em, SHORT_PASSWORD, SALT, iv, DEFAULT_KEY_LENGTH);
            byte[] recoveredBytes = cipher.doFinal(cipherBytes);
            String recovered = new String(recoveredBytes, "UTF-8");

            // Assert
            assertEquals(PLAINTEXT, recovered);
        }
    }

    @Test
    void testGetCipherShouldHandleNullSalt() throws Exception {
        // Arrange
        RandomIVPBECipherProvider cipherProvider = new BcryptCipherProvider(4);

        final String PASSWORD = "shortPassword";
        final byte[] SALT = null;
        final EncryptionMethod em = EncryptionMethod.AES_CBC;

        // Act
        // Initialize a cipher for encryption
        IllegalArgumentException encryptIae = assertThrows(IllegalArgumentException.class,
                () -> cipherProvider.getCipher(em, PASSWORD, SALT, DEFAULT_KEY_LENGTH, true));

        IllegalArgumentException decryptIae = assertThrows(IllegalArgumentException.class, () -> {
            final byte[] iv = new byte[16];
            Arrays.fill(iv, (byte) '\0');
            cipherProvider.getCipher(em, PASSWORD, SALT, iv, DEFAULT_KEY_LENGTH, false);
        });

        // Assert
        assertTrue(encryptIae.getMessage().contains("The salt must be of the format"));
        assertTrue(decryptIae.getMessage().contains("The salt must be of the format"));
    }
}