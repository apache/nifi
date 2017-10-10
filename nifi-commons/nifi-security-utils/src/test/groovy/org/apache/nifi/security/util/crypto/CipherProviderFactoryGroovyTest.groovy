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
package org.apache.nifi.security.util.crypto

import org.apache.nifi.security.util.KeyDerivationFunction
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Ignore
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.security.Security

@RunWith(JUnit4.class)
class CipherProviderFactoryGroovyTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(CipherProviderFactoryGroovyTest.class)

    private static final Map<KeyDerivationFunction, Class> EXPECTED_CIPHER_PROVIDERS = [
            (KeyDerivationFunction.BCRYPT)                  : BcryptCipherProvider.class,
            (KeyDerivationFunction.NIFI_LEGACY)             : NiFiLegacyCipherProvider.class,
            (KeyDerivationFunction.NONE)                    : AESKeyedCipherProvider.class,
            (KeyDerivationFunction.OPENSSL_EVP_BYTES_TO_KEY): OpenSSLPKCS5CipherProvider.class,
            (KeyDerivationFunction.PBKDF2)                  : PBKDF2CipherProvider.class,
            (KeyDerivationFunction.SCRYPT)                  : ScryptCipherProvider.class
    ]

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

    @Test
    void testGetCipherProviderShouldResolveRegisteredKDFs() {
        // Arrange

        // Act
        KeyDerivationFunction.values().each { KeyDerivationFunction kdf ->
            logger.info("Expected: ${kdf.name} -> ${EXPECTED_CIPHER_PROVIDERS.get(kdf).simpleName}")
            CipherProvider cp = CipherProviderFactory.getCipherProvider(kdf)
            logger.info("Resolved: ${kdf.name} -> ${cp.class.simpleName}")

            // Assert
            assert cp.class == (EXPECTED_CIPHER_PROVIDERS.get(kdf))
        }
    }

    @Ignore("Cannot mock enum using Groovy map coercion")
    @Test
    void testGetCipherProviderShouldHandleUnregisteredKDFs() {
        // Arrange

        // Can't mock this; see http://stackoverflow.com/questions/5323505/mocking-java-enum-to-add-a-value-to-test-fail-case
        KeyDerivationFunction invalidKDF = [name: "Unregistered", description: "Not a registered KDF"] as KeyDerivationFunction
        logger.info("Expected: ${invalidKDF.name} -> error")

        // Act
        def msg = shouldFail(IllegalArgumentException) {
            CipherProvider cp = CipherProviderFactory.getCipherProvider(invalidKDF)
            logger.info("Resolved: ${invalidKDF.name} -> ${cp.class.simpleName}")
        }
        logger.expected(msg)

        // Assert
        assert msg =~ "No cipher provider registered for ${invalidKDF.name}"
    }
}
