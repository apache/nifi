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
package org.apache.nifi.security.kms

import org.apache.nifi.util.NiFiProperties
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.bouncycastle.util.encoders.Hex
import org.junit.After
import org.junit.AfterClass
import org.junit.Before
import org.junit.BeforeClass
import org.junit.ClassRule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.crypto.Cipher
import javax.crypto.SecretKey
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.SecretKeySpec
import java.security.KeyManagementException
import java.security.SecureRandom
import java.security.Security

import static groovy.test.GroovyAssert.shouldFail

@RunWith(JUnit4.class)
class KeyProviderFactoryTest {
    private static final Logger logger = LoggerFactory.getLogger(KeyProviderFactoryTest.class)

    private static final String KEY_ID = "K1"
    private static final String KEY_HEX_128 = "0123456789ABCDEFFEDCBA9876543210"
    private static final String KEY_HEX_256 = KEY_HEX_128 * 2
    private static final String KEY_HEX = isUnlimitedStrengthCryptoAvailable() ? KEY_HEX_256 : KEY_HEX_128

    private static final String LEGACY_SKP_FQCN = "org.apache.nifi.provenance.StaticKeyProvider"
    private static final String LEGACY_FBKP_FQCN = "org.apache.nifi.provenance.FileBasedKeyProvider"

    private static final String ORIGINAL_PROPERTIES_PATH = System.getProperty(NiFiProperties.PROPERTIES_FILE_PATH)

    private static final SecretKey MASTER_KEY = new SecretKeySpec(Hex.decode(KEY_HEX), "AES")

    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder()

    @BeforeClass
    static void setUpOnce() throws Exception {
        Security.addProvider(new BouncyCastleProvider())

        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }

        logger.info("Original \$PROPERTIES_FILE_PATH is ${ORIGINAL_PROPERTIES_PATH}")
        String testPath = new File("src/test/resources/${isUnlimitedStrengthCryptoAvailable() ? "256" : "128"}/conf/.").getAbsolutePath()
        logger.info("Temporarily setting to ${testPath}")
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, testPath)
    }

    @Before
    void setUp() throws Exception {
        tempFolder.create()
    }

    @After
    void tearDown() throws Exception {
        tempFolder?.delete()
    }

    @AfterClass
    static void tearDownOnce() throws Exception {
        if (ORIGINAL_PROPERTIES_PATH) {
            logger.info("Restored \$PROPERTIES_FILE_PATH to ${ORIGINAL_PROPERTIES_PATH}")
            System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, ORIGINAL_PROPERTIES_PATH)
        }
    }

    private static boolean isUnlimitedStrengthCryptoAvailable() {
        Cipher.getMaxAllowedKeyLength("AES") > 128
    }

    private static void populateKeyDefinitionsFile(String path = "src/test/resources/conf/filebased.kp") {
        String masterKeyHex = KEY_HEX
        SecretKey masterKey = new SecretKeySpec(Hex.decode(masterKeyHex), "AES")

        // Generate the file
        File keyFile = new File(path)
        final int KEY_COUNT = 1
        List<String> lines = []
        KEY_COUNT.times { int i ->
            lines.add("K${i + 1}=${generateEncryptedKey(masterKey)}")
        }

        keyFile.text = lines.join("\n")
    }

    private static String generateEncryptedKey(SecretKey masterKey) {
        byte[] ivBytes = new byte[16]
        byte[] keyBytes = new byte[isUnlimitedStrengthCryptoAvailable() ? 32 : 16]

        SecureRandom sr = new SecureRandom()
        sr.nextBytes(ivBytes)
        sr.nextBytes(keyBytes)

        Cipher masterCipher = Cipher.getInstance("AES/GCM/NoPadding", "BC")
        masterCipher.init(Cipher.ENCRYPT_MODE, masterKey, new IvParameterSpec(ivBytes))
        byte[] cipherBytes = masterCipher.doFinal(keyBytes)

        Base64.encoder.encodeToString(CryptoUtils.concatByteArrays(ivBytes, cipherBytes))
    }

    @Test
    void testShouldBuildStaticKeyProvider() {
        // Arrange
        String staticProvider = StaticKeyProvider.class.name
        String providerLocation = null

        // Act
        KeyProvider keyProvider = KeyProviderFactory.buildKeyProvider(staticProvider, providerLocation, KEY_ID, [(KEY_ID): KEY_HEX], null)
        logger.info("Key Provider ${staticProvider} with location ${providerLocation} and keyId ${KEY_ID} / ${KEY_HEX} formed: ${keyProvider}")

        // Assert
        assert keyProvider instanceof StaticKeyProvider
        assert keyProvider.getAvailableKeyIds() == [KEY_ID]
    }

    @Test
    void testShouldBuildStaticKeyProviderWithLegacyPackage() {
        // Arrange
        String staticProvider = LEGACY_SKP_FQCN
        String providerLocation = null

        // Act
        KeyProvider keyProvider = KeyProviderFactory.buildKeyProvider(staticProvider, providerLocation, KEY_ID, [(KEY_ID): KEY_HEX], null)
        logger.info("Key Provider ${staticProvider} with location ${providerLocation} and keyId ${KEY_ID} / ${KEY_HEX} formed: ${keyProvider}")

        // Assert
        assert keyProvider instanceof StaticKeyProvider
        assert keyProvider.getAvailableKeyIds() == [KEY_ID]
    }

    @Test
    void testShouldBuildFileBasedKeyProvider() {
        // Arrange
        String fileBasedProvider = FileBasedKeyProvider.class.name
        File fileBasedProviderFile = tempFolder.newFile("filebased.kp")
        String providerLocation = fileBasedProviderFile.path
        populateKeyDefinitionsFile(providerLocation)
        logger.info("Created temporary file based key provider: ${providerLocation}")

        // Act
        KeyProvider keyProvider = KeyProviderFactory.buildKeyProvider(fileBasedProvider, providerLocation, KEY_ID, [(KEY_ID): KEY_HEX], MASTER_KEY)
        logger.info("Key Provider ${fileBasedProvider} with location ${providerLocation} and keyId ${KEY_ID} / ${KEY_HEX} formed: ${keyProvider}")

        // Assert
        assert keyProvider instanceof FileBasedKeyProvider
        assert keyProvider.getAvailableKeyIds() == [KEY_ID]
    }

    @Test
    void testShouldBuildFileBasedKeyProviderWithLegacyPackage() {
        // Arrange
        String fileBasedProvider = LEGACY_FBKP_FQCN
        File fileBasedProviderFile = tempFolder.newFile("filebased.kp")
        String providerLocation = fileBasedProviderFile.path
        populateKeyDefinitionsFile(providerLocation)
        logger.info("Created temporary file based key provider: ${providerLocation}")

        // Act
        KeyProvider keyProvider = KeyProviderFactory.buildKeyProvider(fileBasedProvider, providerLocation, KEY_ID, [(KEY_ID): KEY_HEX], MASTER_KEY)
        logger.info("Key Provider ${fileBasedProvider} with location ${providerLocation} and keyId ${KEY_ID} / ${KEY_HEX} formed: ${keyProvider}")

        // Assert
        assert keyProvider instanceof FileBasedKeyProvider
        assert keyProvider.getAvailableKeyIds() == [KEY_ID]
    }

    @Test
    void testShouldNotBuildFileBasedKeyProviderWithoutMasterKey() {
        // Arrange
        String fileBasedProvider = FileBasedKeyProvider.class.name
        File fileBasedProviderFile = tempFolder.newFile("filebased.kp")
        String providerLocation = fileBasedProviderFile.path
        populateKeyDefinitionsFile(providerLocation)
        logger.info("Created temporary file based key provider: ${providerLocation}")

        // Act
        def msg = shouldFail(KeyManagementException) {
            KeyProvider keyProvider = KeyProviderFactory.buildKeyProvider(fileBasedProvider, providerLocation, KEY_ID, [(KEY_ID): KEY_HEX], null)
            logger.info("Key Provider ${fileBasedProvider} with location ${providerLocation} and keyId ${KEY_ID} / ${KEY_HEX} formed: ${keyProvider}")
        }

        // Assert
        assert msg =~ "The master key must be provided to decrypt the individual keys"
    }

    @Test
    void testShouldNotBuildUnknownKeyProvider() {
        // Arrange
        String providerImplementation = "org.apache.nifi.provenance.ImaginaryKeyProvider"
        String providerLocation = null

        // Act
        def msg = shouldFail(KeyManagementException) {
            KeyProvider keyProvider = KeyProviderFactory.buildKeyProvider(providerImplementation, providerLocation, KEY_ID, [(KEY_ID): KEY_HEX], null)
            logger.info("Key Provider ${providerImplementation} with location ${providerLocation} and keyId ${KEY_ID} / ${KEY_HEX} formed: ${keyProvider}")
        }

        // Assert
        assert msg =~ "Invalid key provider implementation provided"
    }
}
