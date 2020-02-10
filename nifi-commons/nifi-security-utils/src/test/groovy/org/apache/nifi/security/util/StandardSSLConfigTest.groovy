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
package org.apache.nifi.security.util


import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@RunWith(JUnit4.class)
class StandardSSLConfigTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(StandardSSLConfigTest.class)

    @BeforeClass
    static void setUpOnce() throws Exception {
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
    void testShouldHandleNullValues() {
        // Arrange
        SSLConfig sslConfig = new StandardSSLConfig()

        final String CLIENT_AUTH_MSG = "No enum constant org.apache.nifi.security.util.SslContextFactory.ClientAuth"

        // Act
        sslConfig.setAlias(null)
        sslConfig.setKeystorePath(null)
        sslConfig.setKeystorePassword(null as String)
        sslConfig.setKeyPassword(null as String)
        sslConfig.setTruststorePath(null)
        sslConfig.setTruststorePassword(null as String)
        sslConfig.setKeystoreType(null as String)
        sslConfig.setTruststoreType(null as String)

        def caMessage = shouldFail(IllegalArgumentException) {
            sslConfig.setClientAuth(null as String)
        }
        logger.expected(caMessage)

        // Assert
        assert sslConfig
        assert caMessage =~ CLIENT_AUTH_MSG
    }

    @Test
    void testShouldHandleTrimmingStrings() {
        // Arrange
        SSLConfig sslConfig = new StandardSSLConfig()

        final String LEADING_SPACES = "    value"
        final String TRAILING_SPACES = "value    "
        final String BOTH_SPACES = "    value    "
        final String EMPTY_SPACES = "    "

        def spaceStrings = [LEADING_SPACES, TRAILING_SPACES, BOTH_SPACES]
        def propsToSet = ["Alias", "KeystorePath", "KeystorePassword", "KeyPassword", "TruststorePath", "TruststorePassword"]

        // Act
        propsToSet.each { String p ->
            spaceStrings.each { String input ->
                logger.debug("Calling set${p}(${input})")
                sslConfig."set${p}"(input)

                // Assert
                assert sslConfig."get${p}"() == "value"
            }

            sslConfig."set${p}"(EMPTY_SPACES)

            // Assert
            assert sslConfig."get${p}"() == null
        }
    }

    @Test
    void testShouldSetKeyPassword() {
        // Arrange
        SSLConfig sslConfig = new StandardSSLConfig()

        final String KEYSTORE_PASSWORD = "keystorePassword"
        final String KEY_PASSWORD = "keyPassword"

        // Act & Assert

        // Set the keystore password and leave the key password empty
        sslConfig.setKeystorePassword(KEYSTORE_PASSWORD)
        assert sslConfig.getKeystorePassword() == KEYSTORE_PASSWORD
        assert sslConfig.getKeyOrKeystorePassword() == KEYSTORE_PASSWORD
        assert !sslConfig.isUniqueKeyPasswordSet()
        assert sslConfig.getKeyPassword() == null

        // Set the key password identical to the keystore password
        sslConfig.setKeyPassword(KEYSTORE_PASSWORD)
        assert sslConfig.getKeystorePassword() == KEYSTORE_PASSWORD
        assert sslConfig.getKeyOrKeystorePassword() == KEYSTORE_PASSWORD
        assert !sslConfig.isUniqueKeyPasswordSet()
        assert sslConfig.getKeyPassword() == KEYSTORE_PASSWORD

        // Set the key password to a different value
        sslConfig.setKeyPassword(KEY_PASSWORD)
        assert sslConfig.getKeystorePassword() == KEYSTORE_PASSWORD
        assert sslConfig.getKeyOrKeystorePassword() == KEY_PASSWORD
        assert sslConfig.isUniqueKeyPasswordSet()
        assert sslConfig.getKeyPassword() == KEY_PASSWORD
    }

    @Test
    void testShouldHandleInvalidKeystoreTypes() {
        // Arrange
        SSLConfig sslConfig = new StandardSSLConfig()

        final String KEYSTORE_TYPE_MSG = "No enum constant org.apache.nifi.security.util.KeystoreType"

        // Act
        def ktMessage = shouldFail(IllegalArgumentException) {
            sslConfig.setKeystoreType("invalid keystore type")
        }
        logger.expected(ktMessage)

        def ttMessage = shouldFail(IllegalArgumentException) {
            sslConfig.setTruststoreType("invalid truststore type")
        }
        logger.expected(ttMessage)

        // Assert
        assert sslConfig
        assert ktMessage =~ KEYSTORE_TYPE_MSG
        assert ttMessage =~ KEYSTORE_TYPE_MSG
    }

    // testShouldHandleKeyPasswords
}
