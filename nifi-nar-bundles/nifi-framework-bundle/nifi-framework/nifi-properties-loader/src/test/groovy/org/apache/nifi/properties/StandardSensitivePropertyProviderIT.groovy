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

import org.apache.nifi.properties.sensitive.StandardSensitivePropertyProvider
import org.apache.nifi.properties.sensitive.property.provider.aes.AESSensitivePropertyProvider
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.junit.runners.JUnit4

import java.security.Security
import java.security.SecureRandom
import javax.crypto.Cipher


@RunWith(JUnit4.class)
class StandardSensitivePropertyProviderIT {
    private static final Logger logger = LoggerFactory.getLogger(StandardSensitivePropertyProviderIT.class)

    private String AES_128_KEY
    private String AES_256_KEY

    @BeforeClass
    static void setUpOnce() throws Exception {
        Security.addProvider(new BouncyCastleProvider())

        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    /**
     * This method builds random test values
     */
    @Before
    void setUp() throws Exception {
        def random = new SecureRandom()
        byte[] bytes = new byte[80]
        random.nextBytes(bytes)
        String material = bytes.encodeHex()

        AES_128_KEY = material[0..< 32]
        AES_256_KEY = material[0..< 64]
    }

    /**
     * This test shows that the SSPP creates an AES provider with 128 bits hex.
     */
    @Test
    void testKnownAES128KeyProducesAESProvider() throws Exception {
        def sensitivePropertyProvider = StandardSensitivePropertyProvider.fromKey(AES_128_KEY)
        assert sensitivePropertyProvider.getName() == new AESSensitivePropertyProvider(AES_128_KEY).getName()
    }

    /**
     * This test shows that the SSPP creates an AES provider with 256 bits hex.
     */
    @Test
    void testKnownAES256KeyProducesAESProvider() throws Exception {
        def sensitivePropertyProvider = StandardSensitivePropertyProvider.fromKey(AES_256_KEY)
        assert sensitivePropertyProvider.getName() == new AESSensitivePropertyProvider(AES_256_KEY).getName()
    }

    /**
     * This test shows that the SSPP default protection scheme is the AES default protection scheme.
     */
    @Test
    void testDefaultProtectionSchemeMatches() throws Exception {
        def defaultProtectionScheme = StandardSensitivePropertyProvider.getDefaultProtectionScheme()
        assert defaultProtectionScheme == AESSensitivePropertyProvider.getDefaultProtectionScheme()
    }

    /**
     * This test shows that the SSPP default protection scheme is AES/GCM/ + the max available key length.
     */
    @Test
    void testShouldGetDefaultProviderKey() throws Exception {
        // Arrange
        final String EXPECTED_PROVIDER_KEY = "aes/gcm/${Cipher.getMaxAllowedKeyLength("AES") > 128 ? 256 : 128}"
        logger.info("Expected provider key: ${EXPECTED_PROVIDER_KEY}")

        // Act
        String defaultKey = StandardSensitivePropertyProvider.getDefaultProtectionScheme()
        logger.info("Default key: ${defaultKey}")
        // Assert
        assert defaultKey == EXPECTED_PROVIDER_KEY
    }
}
