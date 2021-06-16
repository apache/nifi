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
package org.apache.nifi.security.repository


import org.apache.nifi.security.kms.StaticKeyProvider
import org.apache.nifi.security.util.EncryptionMethod
import org.apache.nifi.security.util.crypto.AESKeyedCipherProvider
import org.apache.nifi.util.NiFiProperties
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.bouncycastle.util.encoders.Hex
import org.junit.After
import org.junit.AfterClass
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.crypto.Cipher
import javax.crypto.SecretKey
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.SecretKeySpec
import java.security.Security

@RunWith(JUnit4.class)
class RepositoryEncryptorUtilsTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(RepositoryEncryptorUtilsTest.class)

    private static final String KEY_HEX_128 = "0123456789ABCDEFFEDCBA9876543210"
    private static final String KEY_HEX_256 = KEY_HEX_128 * 2
    private static final String KEY_HEX_1 = isUnlimitedStrengthCryptoAvailable() ? KEY_HEX_256 : KEY_HEX_128

    private static final String KEY_HEX_2 = "00" * (isUnlimitedStrengthCryptoAvailable() ? 32 : 16)
    private static final String KEY_HEX_3 = "AA" * (isUnlimitedStrengthCryptoAvailable() ? 32 : 16)

    private static final String KEY_ID_1 = "K1"
    private static final String KEY_ID_2 = "K2"
    private static final String KEY_ID_3 = "K3"

    private static AESKeyedCipherProvider mockCipherProvider

    private static String ORIGINAL_LOG_LEVEL

    private NiFiProperties nifiProperties
    private static final String LOG_PACKAGE = "org.slf4j.simpleLogger.log.org.apache.nifi.controller.repository.crypto"

    private static final boolean isLossTolerant = false

    // Mapping of key IDs to keys
    final def KEYS = [
            (KEY_ID_1): new SecretKeySpec(Hex.decode(KEY_HEX_1), "AES"),
            (KEY_ID_2): new SecretKeySpec(Hex.decode(KEY_HEX_2), "AES"),
            (KEY_ID_3): new SecretKeySpec(Hex.decode(KEY_HEX_3), "AES"),
    ]
    private static final String DEFAULT_NIFI_PROPS_PATH = "/conf/nifi.properties"

    private static final Map<String, String> DEFAULT_ENCRYPTION_PROPS = [
            (NiFiProperties.CONTENT_REPOSITORY_IMPLEMENTATION)                              : "org.apache.nifi.controller.repository.crypto.EncryptedFileSystemRepository",
            (NiFiProperties.CONTENT_REPOSITORY_ENCRYPTION_KEY_ID)                           : KEY_ID_1,
            (NiFiProperties.CONTENT_REPOSITORY_ENCRYPTION_KEY)                              : KEY_HEX_1,
            (NiFiProperties.CONTENT_REPOSITORY_ENCRYPTION_KEY_PROVIDER_IMPLEMENTATION_CLASS): StaticKeyProvider.class.name,
            (NiFiProperties.CONTENT_REPOSITORY_ENCRYPTION_KEY_PROVIDER_LOCATION)            : ""
    ]

    @BeforeClass
    static void setUpOnce() throws Exception {
        ORIGINAL_LOG_LEVEL = System.getProperty(LOG_PACKAGE)
        System.setProperty(LOG_PACKAGE, "DEBUG")

        Security.addProvider(new BouncyCastleProvider())

        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }

        mockCipherProvider = [
                getCipher: { EncryptionMethod em, SecretKey key, byte[] ivBytes, boolean encryptMode ->
                    logger.mock("Getting cipher for ${em} with IV ${Hex.toHexString(ivBytes)} encrypt ${encryptMode}")
                    Cipher cipher = Cipher.getInstance(em.algorithm)
                    cipher.init((encryptMode ? Cipher.ENCRYPT_MODE : Cipher.DECRYPT_MODE) as int, key, new IvParameterSpec(ivBytes))
                    cipher
                }
        ] as AESKeyedCipherProvider
    }

    @Before
    void setUp() throws Exception {
    }

    @After
    void tearDown() throws Exception {
    }

    @AfterClass
    static void tearDownOnce() throws Exception {
        if (ORIGINAL_LOG_LEVEL) {
            System.setProperty(LOG_PACKAGE, ORIGINAL_LOG_LEVEL)
        }
    }

    private static boolean isUnlimitedStrengthCryptoAvailable() {
        Cipher.getMaxAllowedKeyLength("AES") > 128
    }

    @Test
    void testShouldDetermineKeyProviderImplementationClassName() {
        // Arrange
        final Map EXPECTED_CLASS_NAMES = [
                (RepositoryType.CONTENT)   : NiFiProperties.CONTENT_REPOSITORY_ENCRYPTION_KEY_PROVIDER_IMPLEMENTATION_CLASS,
                (RepositoryType.FLOWFILE)  : NiFiProperties.FLOWFILE_REPOSITORY_ENCRYPTION_KEY_PROVIDER_IMPLEMENTATION_CLASS,
                (RepositoryType.PROVENANCE): NiFiProperties.PROVENANCE_REPO_ENCRYPTION_KEY_PROVIDER_IMPLEMENTATION_CLASS,
        ]

        // Act
        Map<RepositoryType, String> actualClassNames = RepositoryType.values().collectEntries { RepositoryType rt ->
            [rt, RepositoryEncryptorUtils.determineKeyProviderImplementationClassName(rt)]
        }

        // Assert
        actualClassNames.each { RepositoryType rt, String actualClassName ->
            assert actualClassName == EXPECTED_CLASS_NAMES[rt]
        }
    }

    @Test
    void testDetermineKeyProviderImplementationClassNameShouldHandleUnsupportedRepositoryTypes() {
        // Arrange

        // Act
        def actualClassName = RepositoryEncryptorUtils.determineKeyProviderImplementationClassName(null)

        // Assert
        assert actualClassName == "no_such_key_provider_defined"
    }
}
