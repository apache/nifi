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
package org.apache.nifi.authorization

import org.apache.nifi.authorization.generated.Property
import org.apache.nifi.properties.AESSensitivePropertyProvider
import org.bouncycastle.jce.provider.BouncyCastleProvider
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
import java.security.Security

@RunWith(JUnit4.class)
class AuthorizerFactoryBeanTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(AuthorizerFactoryBeanTest.class)

    // These blocks configure the constant values depending on JCE policies of the machine running the tests
    private static final String KEY_HEX_128 = "0123456789ABCDEFFEDCBA9876543210"
    private static final String KEY_HEX_256 = KEY_HEX_128 * 2
    public static final String KEY_HEX = isUnlimitedStrengthCryptoAvailable() ? KEY_HEX_256 : KEY_HEX_128

    private static final String CIPHER_TEXT_128 = "6pqdM1urBEPHtj+L||ds0Z7RpqOA2321c/+7iPMfxDrqmH5Qx6UwQG0eIYB//3Ng"
    private static final String CIPHER_TEXT_256 = "TepMCD7v3LAMF0KX||ydSRWPRl1/JXgTsZtfzCnDXu7a0lTLysjPL2I06EPUCHzw"
    public static final String CIPHER_TEXT = isUnlimitedStrengthCryptoAvailable() ? CIPHER_TEXT_256 : CIPHER_TEXT_128

    private static final String ENCRYPTION_SCHEME_128 = "aes/gcm/128"
    private static final String ENCRYPTION_SCHEME_256 = "aes/gcm/256"
    public static
    final String ENCRYPTION_SCHEME = isUnlimitedStrengthCryptoAvailable() ? ENCRYPTION_SCHEME_256 : ENCRYPTION_SCHEME_128

    private static final String PASSWORD = "thisIsABadPassword"

    @BeforeClass
    public static void setUpOnce() throws Exception {
        Security.addProvider(new BouncyCastleProvider())

        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @AfterClass
    public static void tearDownOnce() throws Exception {
    }

    @Before
    public void setUp() throws Exception {
        AuthorizerFactoryBean.SENSITIVE_PROPERTY_PROVIDER = new AESSensitivePropertyProvider(KEY_HEX)
    }

    @After
    public void tearDown() throws Exception {
        AuthorizerFactoryBean.SENSITIVE_PROPERTY_PROVIDER = null
        AuthorizerFactoryBean.SENSITIVE_PROPERTY_PROVIDER_FACTORY = null
    }

    private static boolean isUnlimitedStrengthCryptoAvailable() {
        Cipher.getMaxAllowedKeyLength("AES") > 128
    }

    private static int getKeyLength(String keyHex = KEY_HEX) {
        keyHex?.size() * 4
    }

    @Test
    void testShouldDecryptValue() {
        // Arrange
        logger.info("Encryption scheme: ${ENCRYPTION_SCHEME}")
        logger.info("Cipher text: ${CIPHER_TEXT}")

        // Act
        String decrypted = new AuthorizerFactoryBean().decryptValue(CIPHER_TEXT, ENCRYPTION_SCHEME)
        logger.info("Decrypted ${CIPHER_TEXT} -> ${decrypted}")

        // Assert
        assert decrypted == PASSWORD
    }

    @Test
    void testShouldLoadEncryptedAuthorizersConfiguration() {
        // Arrange
        def identifier = "ldap-user-group-provider"
        def managerPasswordName = "Manager Password"
        Property managerPasswordProperty = new Property(name: managerPasswordName, value: CIPHER_TEXT, encryption: ENCRYPTION_SCHEME)
        List<Property> properties = [managerPasswordProperty]

        logger.info("Manager Password property: ${managerPasswordProperty.dump()}")
        def bean = new AuthorizerFactoryBean()

        // Act
        def context = bean.loadAuthorizerConfiguration(identifier, properties)
        logger.info("Loaded context: ${context.dump()}")

        // Assert
        String decryptedPropertyValue = context.getProperty(managerPasswordName)
        assert decryptedPropertyValue == PASSWORD
    }
}
