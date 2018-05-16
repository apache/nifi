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
package org.apache.nifi.properties

import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.bouncycastle.util.encoders.DecoderException
import org.bouncycastle.util.encoders.Hex
import org.junit.After
import org.junit.Assume
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.crypto.Cipher
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.SecretKeySpec
import java.nio.charset.StandardCharsets
import java.security.SecureRandom
import java.security.Security

@RunWith(JUnit4.class)
class AESSensitivePropertyProviderTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(AESSensitivePropertyProviderTest.class)

    private static final String KEY_128_HEX = "0123456789ABCDEFFEDCBA9876543210"
    private static final String KEY_256_HEX = KEY_128_HEX * 2
    private static final int IV_LENGTH = AESSensitivePropertyProvider.getIvLength()

    private static final List<Integer> KEY_SIZES = getAvailableKeySizes()

    private static final SecureRandom secureRandom = new SecureRandom()

    private static final Base64.Encoder encoder = Base64.encoder
    private static final Base64.Decoder decoder = Base64.decoder

    @BeforeClass
    static void setUpOnce() throws Exception {
        Security.addProvider(new BouncyCastleProvider())

        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @Before
    void setUp() throws Exception {

    }

    @After
    void tearDown() throws Exception {

    }

    private static Cipher getCipher(boolean encrypt = true, int keySize = 256, byte[] iv = [0x00] * IV_LENGTH) {
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding")
        String key = getKeyOfSize(keySize)
        cipher.init((encrypt ? Cipher.ENCRYPT_MODE : Cipher.DECRYPT_MODE) as int, new SecretKeySpec(Hex.decode(key), "AES"), new IvParameterSpec(iv))
        logger.setup("Initialized a cipher in ${encrypt ? "encrypt" : "decrypt"} mode with a key of length ${keySize} bits")
        cipher
    }

    private static String getKeyOfSize(int keySize = 256) {
        switch (keySize) {
            case 128:
                return KEY_128_HEX
            case 192:
            case 256:
                if (Cipher.getMaxAllowedKeyLength("AES") < keySize) {
                    throw new IllegalArgumentException("The JCE unlimited strength cryptographic jurisdiction policies are not installed, so the max key size is 128 bits")
                }
                return KEY_256_HEX[0..<(keySize / 4)]
            default:
                throw new IllegalArgumentException("Key size ${keySize} bits is not valid")
        }
    }

    private static List<Integer> getAvailableKeySizes() {
        if (Cipher.getMaxAllowedKeyLength("AES") > 128) {
            [128, 192, 256]
        } else {
            [128]
        }
    }

    private static String manipulateString(String input, int start = 0, int end = input?.length()) {
        if ((input[start..end] as List).unique().size() == 1) {
            throw new IllegalArgumentException("Can't manipulate a String where the entire range is identical [${input[start..end]}]")
        }
        List shuffled = input[start..end] as List
        Collections.shuffle(shuffled)
        String reconstituted = input[0..<start] + shuffled.join() + input[end + 1..-1]
        return reconstituted != input ? reconstituted : manipulateString(input, start, end)
    }

    @Test
    void testShouldThrowExceptionOnInitializationWithoutBouncyCastle() throws Exception {
        // Arrange
        try {
            Security.removeProvider(new BouncyCastleProvider().getName())

            // Act
            def msg = shouldFail(SensitivePropertyProtectionException) {
                SensitivePropertyProvider spp = new AESSensitivePropertyProvider(Hex.decode(KEY_128_HEX))
                logger.error("This should not be reached")
            }

            // Assert
            assert msg =~ "Error initializing the protection cipher"
        } finally {
            Security.addProvider(new BouncyCastleProvider())
        }
    }

    // TODO: testShouldGetName()

    @Test
    void testShouldProtectValue() throws Exception {
        final String PLAINTEXT = "This is a plaintext value"

        // Act
        Map<Integer, String> CIPHER_TEXTS = KEY_SIZES.collectEntries { int keySize ->
            SensitivePropertyProvider spp = new AESSensitivePropertyProvider(Hex.decode(getKeyOfSize(keySize)))
            logger.info("Initialized ${spp.name} with key size ${keySize}")
            [(keySize): spp.protect(PLAINTEXT)]
        }
        CIPHER_TEXTS.each { ks, ct -> logger.info("Encrypted for ${ks} length key: ${ct}") }

        // Assert

        // The IV generation is part of #protect, so the expected cipher text values must be generated after #protect has run
        Map<Integer, Cipher> decryptionCiphers = CIPHER_TEXTS.collectEntries { int keySize, String cipherText ->
            // The 12 byte IV is the first 16 Base64-encoded characters of the "complete" cipher text
            byte[] iv = decoder.decode(cipherText[0..<16])
            [(keySize): getCipher(false, keySize, iv)]
        }
        Map<Integer, String> plaintexts = decryptionCiphers.collectEntries { Map.Entry<Integer, Cipher> e ->
            String cipherTextWithoutIVAndDelimiter = CIPHER_TEXTS[e.key][18..-1]
            String plaintext = new String(e.value.doFinal(decoder.decode(cipherTextWithoutIVAndDelimiter)), StandardCharsets.UTF_8)
            [(e.key): plaintext]
        }
        CIPHER_TEXTS.each { key, ct -> logger.expected("Cipher text for ${key} length key: ${ct}") }

        assert plaintexts.every { int ks, String pt -> pt == PLAINTEXT }
    }

    @Test
    void testShouldHandleProtectEmptyValue() throws Exception {
        final List<String> EMPTY_PLAINTEXTS = ["", "    ", null]

        // Act
        KEY_SIZES.collectEntries { int keySize ->
            SensitivePropertyProvider spp = new AESSensitivePropertyProvider(Hex.decode(getKeyOfSize(keySize)))
            logger.info("Initialized ${spp.name} with key size ${keySize}")
            EMPTY_PLAINTEXTS.each { String emptyPlaintext ->
                def msg = shouldFail(IllegalArgumentException) {
                    spp.protect(emptyPlaintext)
                }
                logger.expected("${msg} for keySize ${keySize} and plaintext [${emptyPlaintext}]")

                // Assert
                assert msg == "Cannot encrypt an empty value"
            }
        }
    }

    @Test
    void testShouldUnprotectValue() throws Exception {
        // Arrange
        final String PLAINTEXT = "This is a plaintext value"

        Map<Integer, Cipher> encryptionCiphers = KEY_SIZES.collectEntries { int keySize ->
            byte[] iv = new byte[IV_LENGTH]
            secureRandom.nextBytes(iv)
            [(keySize): getCipher(true, keySize, iv)]
        }

        Map<Integer, String> CIPHER_TEXTS = encryptionCiphers.collectEntries { Map.Entry<Integer, Cipher> e ->
            String iv = encoder.encodeToString(e.value.getIV())
            String cipherText = encoder.encodeToString(e.value.doFinal(PLAINTEXT.getBytes(StandardCharsets.UTF_8)))
            [(e.key): "${iv}||${cipherText}"]
        }
        CIPHER_TEXTS.each { key, ct -> logger.expected("Cipher text for ${key} length key: ${ct}") }

        // Act
        Map<Integer, String> plaintexts = CIPHER_TEXTS.collectEntries { int keySize, String cipherText ->
            SensitivePropertyProvider spp = new AESSensitivePropertyProvider(Hex.decode(getKeyOfSize(keySize)))
            logger.info("Initialized ${spp.name} with key size ${keySize}")
            [(keySize): spp.unprotect(cipherText)]
        }
        plaintexts.each { ks, pt -> logger.info("Decrypted for ${ks} length key: ${pt}") }

        // Assert
        assert plaintexts.every { int ks, String pt -> pt == PLAINTEXT }
    }

    /**
     * Tests inputs where the entire String is empty/blank space/{@code null}.
     *
     * @throws Exception
     */
    @Test
    void testShouldHandleUnprotectEmptyValue() throws Exception {
        // Arrange
        final List<String> EMPTY_CIPHER_TEXTS = ["", "    ", null]

        // Act
        KEY_SIZES.each { int keySize ->
            SensitivePropertyProvider spp = new AESSensitivePropertyProvider(Hex.decode(getKeyOfSize(keySize)))
            logger.info("Initialized ${spp.name} with key size ${keySize}")
            EMPTY_CIPHER_TEXTS.each { String emptyCipherText ->
                def msg = shouldFail(IllegalArgumentException) {
                    spp.unprotect(emptyCipherText)
                }
                logger.expected("${msg} for keySize ${keySize} and cipher text [${emptyCipherText}]")

                // Assert
                assert msg == "Cannot decrypt a cipher text shorter than ${AESSensitivePropertyProvider.minCipherTextLength} chars".toString()
            }
        }
    }

    @Test
    void testShouldUnprotectValueWithWhitespace() throws Exception {
        // Arrange
        final String PLAINTEXT = "This is a plaintext value"

        Map<Integer, Cipher> encryptionCiphers = KEY_SIZES.collectEntries { int keySize ->
            byte[] iv = new byte[IV_LENGTH]
            secureRandom.nextBytes(iv)
            [(keySize): getCipher(true, keySize, iv)]
        }

        Map<Integer, String> CIPHER_TEXTS = encryptionCiphers.collectEntries { Map.Entry<Integer, Cipher> e ->
            String iv = encoder.encodeToString(e.value.getIV())
            String cipherText = encoder.encodeToString(e.value.doFinal(PLAINTEXT.getBytes(StandardCharsets.UTF_8)))
            [(e.key): "${iv}||${cipherText}"]
        }
        CIPHER_TEXTS.each { key, ct -> logger.expected("Cipher text for ${key} length key: ${ct}") }

        // Act
        Map<Integer, String> plaintexts = CIPHER_TEXTS.collectEntries { int keySize, String cipherText ->
            SensitivePropertyProvider spp = new AESSensitivePropertyProvider(Hex.decode(getKeyOfSize(keySize)))
            logger.info("Initialized ${spp.name} with key size ${keySize}")
            [(keySize): spp.unprotect("\t" + cipherText + "\n")]
        }
        plaintexts.each { ks, pt -> logger.info("Decrypted for ${ks} length key: ${pt}") }

        // Assert
        assert plaintexts.every { int ks, String pt -> pt == PLAINTEXT }
    }

    @Test
    void testShouldHandleUnprotectMalformedValue() throws Exception {
        // Arrange
        final String PLAINTEXT = "This is a plaintext value"

        // Act
        KEY_SIZES.each { int keySize ->
            SensitivePropertyProvider spp = new AESSensitivePropertyProvider(Hex.decode(getKeyOfSize(keySize)))
            logger.info("Initialized ${spp.name} with key size ${keySize}")
            String cipherText = spp.protect(PLAINTEXT)
            // Swap two characters in the cipher text
            final String MALFORMED_CIPHER_TEXT = manipulateString(cipherText, 25, 28)
            logger.info("Manipulated ${cipherText} to\n${MALFORMED_CIPHER_TEXT.padLeft(163)}")

            def msg = shouldFail(SensitivePropertyProtectionException) {
                spp.unprotect(MALFORMED_CIPHER_TEXT)
            }
            logger.expected("${msg} for keySize ${keySize} and cipher text [${MALFORMED_CIPHER_TEXT}]")

            // Assert
            assert msg == "Error decrypting a protected value"
        }
    }

    @Test
    void testShouldHandleUnprotectMissingIV() throws Exception {
        // Arrange
        final String PLAINTEXT = "This is a plaintext value"

        // Act
        KEY_SIZES.each { int keySize ->
            SensitivePropertyProvider spp = new AESSensitivePropertyProvider(Hex.decode(getKeyOfSize(keySize)))
            logger.info("Initialized ${spp.name} with key size ${keySize}")
            String cipherText = spp.protect(PLAINTEXT)
            // Remove the IV from the "complete" cipher text
            final String MISSING_IV_CIPHER_TEXT = cipherText[18..-1]
            logger.info("Manipulated ${cipherText} to\n${MISSING_IV_CIPHER_TEXT.padLeft(163)}")

            def msg = shouldFail(IllegalArgumentException) {
                spp.unprotect(MISSING_IV_CIPHER_TEXT)
            }
            logger.expected("${msg} for keySize ${keySize} and cipher text [${MISSING_IV_CIPHER_TEXT}]")

            // Remove the IV from the "complete" cipher text but keep the delimiter
            final String MISSING_IV_CIPHER_TEXT_WITH_DELIMITER = cipherText[16..-1]
            logger.info("Manipulated ${cipherText} to\n${MISSING_IV_CIPHER_TEXT_WITH_DELIMITER.padLeft(163)}")

            def msgWithDelimiter = shouldFail(DecoderException) {
                spp.unprotect(MISSING_IV_CIPHER_TEXT_WITH_DELIMITER)
            }
            logger.expected("${msgWithDelimiter} for keySize ${keySize} and cipher text [${MISSING_IV_CIPHER_TEXT_WITH_DELIMITER}]")

            // Assert
            assert msg == "The cipher text does not contain the delimiter || -- it should be of the form Base64(IV) || Base64(cipherText)"

            // Assert
            assert msgWithDelimiter =~ "unable to decode base64 string"
        }
    }

    /**
     * Tests inputs which have a valid IV and delimiter but no "cipher text".
     *
     * @throws Exception
     */
    @Test
    void testShouldHandleUnprotectEmptyCipherText() throws Exception {
        // Arrange
        final String IV_AND_DELIMITER = "${encoder.encodeToString("Bad IV value".getBytes(StandardCharsets.UTF_8))}||"
        logger.info("IV and delimiter: ${IV_AND_DELIMITER}")

        final List<String> EMPTY_CIPHER_TEXTS = ["", "      ", "\n"].collect { "${IV_AND_DELIMITER}${it}" }

        // Act
        KEY_SIZES.each { int keySize ->
            SensitivePropertyProvider spp = new AESSensitivePropertyProvider(Hex.decode(getKeyOfSize(keySize)))
            logger.info("Initialized ${spp.name} with key size ${keySize}")
            EMPTY_CIPHER_TEXTS.each { String emptyCipherText ->
                def msg = shouldFail(IllegalArgumentException) {
                    spp.unprotect(emptyCipherText)
                }
                logger.expected("${msg} for keySize ${keySize} and cipher text [${emptyCipherText}]")

                // Assert
                assert msg == "Cannot decrypt a cipher text shorter than ${AESSensitivePropertyProvider.minCipherTextLength} chars".toString()
            }
        }
    }

    @Test
    void testShouldHandleUnprotectMalformedIV() throws Exception {
        // Arrange
        final String PLAINTEXT = "This is a plaintext value"

        // Act
        KEY_SIZES.each { int keySize ->
            SensitivePropertyProvider spp = new AESSensitivePropertyProvider(Hex.decode(getKeyOfSize(keySize)))
            logger.info("Initialized ${spp.name} with key size ${keySize}")
            String cipherText = spp.protect(PLAINTEXT)
            // Swap two characters in the IV
            final String MALFORMED_IV_CIPHER_TEXT = manipulateString(cipherText, 8, 11)
            logger.info("Manipulated ${cipherText} to\n${MALFORMED_IV_CIPHER_TEXT.padLeft(163)}")

            def msg = shouldFail(SensitivePropertyProtectionException) {
                spp.unprotect(MALFORMED_IV_CIPHER_TEXT)
            }
            logger.expected("${msg} for keySize ${keySize} and cipher text [${MALFORMED_IV_CIPHER_TEXT}]")

            // Assert
            assert msg == "Error decrypting a protected value"
        }
    }

    @Test
    void testShouldGetIdentifierKeyWithDifferentMaxKeyLengths() throws Exception {
        // Arrange
        def keys = getAvailableKeySizes().collectEntries { int keySize ->
            [(keySize): getKeyOfSize(keySize)]
        }
        logger.info("Keys: ${keys}")

        // Act
        keys.each { int size, String key ->
            String identifierKey = new AESSensitivePropertyProvider(key).getIdentifierKey()
            logger.info("Identifier key: ${identifierKey} for size ${size}")

            // Assert
            assert identifierKey =~ /aes\/gcm\/${size}/
        }
    }

    @Test
    void testShouldNotAllowEmptyKey() throws Exception {
        // Arrange
        final String INVALID_KEY = ""

        // Act
        def msg = shouldFail(SensitivePropertyProtectionException) {
            AESSensitivePropertyProvider spp = new AESSensitivePropertyProvider(INVALID_KEY)
        }

        // Assert
        assert msg == "The key cannot be empty"
    }

    @Test
    void testShouldNotAllowIncorrectlySizedKey() throws Exception {
        // Arrange
        final String INVALID_KEY = "Z" * 31

        // Act
        def msg = shouldFail(SensitivePropertyProtectionException) {
            AESSensitivePropertyProvider spp = new AESSensitivePropertyProvider(INVALID_KEY)
        }

        // Assert
        assert msg == "The key must be a valid hexadecimal key"
    }

    @Test
    void testShouldNotAllowInvalidKey() throws Exception {
        // Arrange
        final String INVALID_KEY = "Z" * 32

        // Act
        def msg = shouldFail(SensitivePropertyProtectionException) {
            AESSensitivePropertyProvider spp = new AESSensitivePropertyProvider(INVALID_KEY)
        }

        // Assert
        assert msg == "The key must be a valid hexadecimal key"
    }

    /**
     * This test is to ensure internal consistency and allow for encrypting value for various property files
     */
    @Test
    void testShouldEncryptArbitraryValues() {
        // Arrange
        def values = ["thisIsABadPassword", "thisIsABadSensitiveKeyPassword", "thisIsABadKeystorePassword", "thisIsABadKeyPassword", "thisIsABadTruststorePassword", "This is an encrypted banner message", "nififtw!"]

        String key = "2C576A9585DB862F5ECBEE5B4FFFCCA1" //getKeyOfSize(128)
        // key = "0" * 64

        SensitivePropertyProvider spp = new AESSensitivePropertyProvider(key)

        // Act
        def encryptedValues = values.collect { String v ->
            def encryptedValue = spp.protect(v)
            logger.info("${v} -> ${encryptedValue}")
            def (String iv, String cipherText) = encryptedValue.tokenize("||")
            logger.info("Normal Base64 encoding would be ${encoder.encodeToString(decoder.decode(iv))}||${encoder.encodeToString(decoder.decode(cipherText))}")
            encryptedValue
        }

        // Assert
        assert values == encryptedValues.collect { spp.unprotect(it) }
    }

    /**
     * This test is to ensure external compatibility in case someone encodes the encrypted value with Base64 and does not remove the padding
     */
    @Test
    void testShouldDecryptPaddedValue() {
        // Arrange
        Assume.assumeTrue("JCE unlimited strength crypto policy must be installed for this test", Cipher.getMaxAllowedKeyLength("AES") > 128)

        final String EXPECTED_VALUE = getKeyOfSize(256) // "thisIsABadKeyPassword"
        String cipherText = "aYDkDKys1ENr3gp+||sTBPpMlIvHcOLTGZlfWct8r9RY8BuDlDkoaYmGJ/9m9af9tZIVzcnDwvYQAaIKxRGF7vI2yrY7Xd6x9GTDnWGiGiRXlaP458BBMMgfzH2O8"
        String unpaddedCipherText = cipherText.replaceAll("=", "")

        String key = "AAAABBBBCCCCDDDDEEEEFFFF00001111" * 2 // getKeyOfSize(256)

        SensitivePropertyProvider spp = new AESSensitivePropertyProvider(key)

        // Act
        String rawValue = spp.unprotect(cipherText)
        logger.info("Decrypted ${cipherText} to ${rawValue}")
        String rawUnpaddedValue = spp.unprotect(unpaddedCipherText)
        logger.info("Decrypted ${unpaddedCipherText} to ${rawUnpaddedValue}")

        // Assert
        assert rawValue == EXPECTED_VALUE
        assert rawUnpaddedValue == EXPECTED_VALUE
    }
}
