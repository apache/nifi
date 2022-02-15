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
package org.apache.nifi.registry.properties

import org.bouncycastle.jce.provider.BouncyCastleProvider
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
class NiFiRegistryPropertiesLoaderGroovyTest extends GroovyTestCase {

    private static final Logger logger = LoggerFactory.getLogger(NiFiRegistryPropertiesLoaderGroovyTest.class)

    private static final String KEYSTORE_PASSWORD_KEY = NiFiRegistryProperties.SECURITY_KEYSTORE_PASSWD
    private static final String KEY_PASSWORD_KEY = NiFiRegistryProperties.SECURITY_KEY_PASSWD
    private static final String TRUSTSTORE_PASSWORD_KEY = NiFiRegistryProperties.SECURITY_TRUSTSTORE_PASSWD

    private static final String KEY_HEX_128 = "0123456789ABCDEFFEDCBA9876543210"
    private static final String KEY_HEX_256 = KEY_HEX_128 * 2
    private static final String KEY_HEX = Cipher.getMaxAllowedKeyLength("AES") < 256 ? KEY_HEX_128 : KEY_HEX_256

    private static final String PASSWORD_KEY_HEX_128 = "2C576A9585DB862F5ECBEE5B4FFFCCA1"

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

    @AfterClass
    static void tearDownOnce() {
    }

    @Test
    void testConstructorShouldCreateNewInstance() throws Exception {
        // Arrange

        // Act
        NiFiRegistryPropertiesLoader propertiesLoader = new NiFiRegistryPropertiesLoader()

        // Assert
        assert !propertiesLoader.@keyHex
    }

    @Test
    public void testShouldCreateInstanceWithKey() throws Exception {
        // Arrange

        // Act
        NiFiRegistryPropertiesLoader propertiesLoader = NiFiRegistryPropertiesLoader.withKey(KEY_HEX)

        // Assert
        assert propertiesLoader.@keyHex == KEY_HEX
    }

    @Test
    public void testConstructorShouldCreateMultipleInstances() throws Exception {
        // Arrange
        NiFiRegistryPropertiesLoader propertiesLoader1 = NiFiRegistryPropertiesLoader.withKey(KEY_HEX)

        // Act
        NiFiRegistryPropertiesLoader propertiesLoader2 = new NiFiRegistryPropertiesLoader()

        // Assert
        assert propertiesLoader1.@keyHex == KEY_HEX
        assert !propertiesLoader2.@keyHex
    }

    @Test
    public void testShouldLoadUnprotectedPropertiesFromFile() throws Exception {
        // Arrange
        File unprotectedFile = new File("src/test/resources/conf/nifi-registry.properties")
        NiFiRegistryPropertiesLoader propertiesLoader = new NiFiRegistryPropertiesLoader()

        // Act
        NiFiRegistryProperties properties = propertiesLoader.load(unprotectedFile)

        // Assert
        assert properties.size() > 0

        // Ensure it is not a ProtectedNiFiProperties
        assert properties instanceof NiFiRegistryProperties
    }

    @Test
    public void testShouldNotLoadUnprotectedPropertiesFromNullFile() throws Exception {
        // Arrange
        NiFiRegistryPropertiesLoader propertiesLoader = new NiFiRegistryPropertiesLoader()

        // Act
        def msg = shouldFail(IllegalArgumentException) {
            NiFiRegistryProperties properties = propertiesLoader.load(null as File)
        }
        logger.info(msg)

        // Assert
        assert msg == "NiFi Registry properties file missing or unreadable"
    }

    @Test
    public void testShouldNotLoadUnprotectedPropertiesFromMissingFile() throws Exception {
        // Arrange
        File missingFile = new File("src/test/resources/conf/nifi-registry.missing.properties")
        assert !missingFile.exists()

        NiFiRegistryPropertiesLoader propertiesLoader = new NiFiRegistryPropertiesLoader()

        // Act
        def msg = shouldFail(IllegalArgumentException) {
            NiFiRegistryProperties properties = propertiesLoader.load(missingFile)
        }
        logger.info(msg)

        // Assert
        assert msg == "NiFi Registry properties file missing or unreadable"
    }

    @Test
    public void testShouldLoadUnprotectedPropertiesFromPath() throws Exception {
        // Arrange
        File unprotectedFile = new File("src/test/resources/conf/nifi-registry.properties")
        NiFiRegistryPropertiesLoader propertiesLoader = new NiFiRegistryPropertiesLoader()

        // Act
        NiFiRegistryProperties properties = propertiesLoader.load(unprotectedFile.path)

        // Assert
        assert properties.size() > 0

        // Ensure it is not a ProtectedNiFiProperties
        assert properties instanceof NiFiRegistryProperties
    }

    @Test
    public void testShouldLoadUnprotectedPropertiesFromProtectedFile() throws Exception {
        // Arrange
        File protectedFile = new File("src/test/resources/conf/nifi-registry.with_sensitive_props_protected_aes_128.properties")
        NiFiRegistryPropertiesLoader propertiesLoader = NiFiRegistryPropertiesLoader.withKey(KEY_HEX_128)

        final def EXPECTED_PLAIN_VALUES = [
                (KEYSTORE_PASSWORD_KEY): "thisIsABadPassword",
                (KEY_PASSWORD_KEY): "thisIsABadPassword",
        ]

        // This method is covered in tests above, so safe to use here to retrieve protected properties
        ProtectedNiFiRegistryProperties protectedNiFiProperties = propertiesLoader.readProtectedPropertiesFromDisk(protectedFile)
        int totalKeysCount = protectedNiFiProperties.getPropertyKeysIncludingProtectionSchemes().size()
        int protectedKeysCount = protectedNiFiProperties.getProtectedPropertyKeys().size()
        logger.info("Read ${totalKeysCount} total properties (${protectedKeysCount} protected) from ${protectedFile.canonicalPath}")

        // Act
        NiFiRegistryProperties properties = propertiesLoader.load(protectedFile)

        // Assert
        assert properties.size() == totalKeysCount - protectedKeysCount

        // Ensure that any key marked as protected above is different in this instance
        protectedNiFiProperties.getProtectedPropertyKeys().keySet().each { String key ->
            String plainValue = properties.getProperty(key)
            String protectedValue = protectedNiFiProperties.getProperty(key)

            logger.info("Checking that [${protectedValue}] -> [${plainValue}] == [${EXPECTED_PLAIN_VALUES[key]}]")

            assert plainValue == EXPECTED_PLAIN_VALUES[key]
            assert plainValue != protectedValue
            assert plainValue.length() <= protectedValue.length()
        }

        // Ensure it is not a ProtectedNiFiProperties
        assert properties instanceof NiFiRegistryProperties
    }

    @Test
    public void testShouldUpdateKeyInFactory() throws Exception {
        // Arrange
        File originalKeyFile = new File("src/test/resources/conf/nifi-registry.with_sensitive_props_protected_aes_128.properties")
        File passwordKeyFile = new File("src/test/resources/conf/nifi-registry.with_sensitive_props_protected_aes_128_password.properties")
        NiFiRegistryPropertiesLoader propertiesLoader = NiFiRegistryPropertiesLoader.withKey(KEY_HEX_128)

        NiFiRegistryProperties properties = propertiesLoader.load(originalKeyFile)
        logger.info("Read ${properties.size()} total properties from ${originalKeyFile.canonicalPath}")

        // Act
        NiFiRegistryPropertiesLoader passwordNiFiRegistryPropertiesLoader = NiFiRegistryPropertiesLoader.withKey(PASSWORD_KEY_HEX_128)

        NiFiRegistryProperties passwordProperties = passwordNiFiRegistryPropertiesLoader.load(passwordKeyFile)
        logger.info("Read ${passwordProperties.size()} total properties from ${passwordKeyFile.canonicalPath}")

        // Assert
        assert properties.size() == passwordProperties.size()


        def readPropertiesAndValues = properties.getPropertyKeys().collectEntries {
            [(it): properties.getProperty(it)]
        }
        def readPasswordPropertiesAndValues = passwordProperties.getPropertyKeys().collectEntries {
            [(it): passwordProperties.getProperty(it)]
        }

        assert readPropertiesAndValues == readPasswordPropertiesAndValues
    }
}
