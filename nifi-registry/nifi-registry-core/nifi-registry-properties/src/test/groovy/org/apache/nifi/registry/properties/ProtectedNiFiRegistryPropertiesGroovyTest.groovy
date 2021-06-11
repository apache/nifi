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

import org.apache.nifi.properties.ApplicationPropertiesProtector
import org.apache.nifi.properties.MultipleSensitivePropertyProtectionException
import org.apache.nifi.properties.PropertyProtectionScheme
import org.apache.nifi.properties.SensitivePropertyProtectionException
import org.apache.nifi.properties.SensitivePropertyProvider
import org.apache.nifi.properties.StandardSensitivePropertyProviderFactory
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.junit.After
import org.junit.AfterClass
import org.junit.Assume
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
class ProtectedNiFiRegistryPropertiesGroovyTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(ProtectedNiFiRegistryPropertiesGroovyTest.class)

    private static final String KEYSTORE_PASSWORD_KEY = NiFiRegistryProperties.SECURITY_KEYSTORE_PASSWD
    private static final String KEY_PASSWORD_KEY = NiFiRegistryProperties.SECURITY_KEY_PASSWD
    private static final String TRUSTSTORE_PASSWORD_KEY = NiFiRegistryProperties.SECURITY_TRUSTSTORE_PASSWD

    private static final def DEFAULT_SENSITIVE_PROPERTIES = [
            KEYSTORE_PASSWORD_KEY,
            KEY_PASSWORD_KEY,
            TRUSTSTORE_PASSWORD_KEY
    ]

    private static final String KEY_HEX_128 = "0123456789ABCDEFFEDCBA9876543210"
    private static final String KEY_HEX_256 = KEY_HEX_128 * 2
    private static final String KEY_HEX = Cipher.getMaxAllowedKeyLength("AES") < 256 ? KEY_HEX_128 : KEY_HEX_256

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

    @AfterClass
    static void tearDownOnce() {
    }

    private static ProtectedNiFiRegistryProperties loadFromResourceFile(String propertiesFilePath) {
        return loadFromResourceFile(propertiesFilePath, KEY_HEX)
    }

    private static ProtectedNiFiRegistryProperties loadFromResourceFile(String propertiesFilePath, String keyHex) {
        File file = fileForResource(propertiesFilePath)

        if (file == null || !file.exists() || !file.canRead()) {
            String path = (file == null ? "missing file" : file.getAbsolutePath())
            logger.error("Cannot read from '{}' -- file is missing or not readable", path)
            throw new IllegalArgumentException("NiFi Registry properties file missing or unreadable")
        }

        FileReader reader = new FileReader(file)

        try {
            final Properties props = new Properties()
            props.load(reader)
            NiFiRegistryProperties properties = new NiFiRegistryProperties(props)
            logger.info("Loaded {} properties from {}", properties.size(), file.getAbsolutePath())

            ProtectedNiFiRegistryProperties protectedNiFiProperties = new ProtectedNiFiRegistryProperties(properties)

            // If it has protected keys, inject the SPP
            if (protectedNiFiProperties.hasProtectedKeys()) {
                protectedNiFiProperties.addSensitivePropertyProvider(StandardSensitivePropertyProviderFactory.withKey(keyHex)
                        .getProvider(PropertyProtectionScheme.AES_GCM))
            }

            return protectedNiFiProperties
        } catch (final Exception ex) {
            logger.error("Cannot load properties file due to " + ex.getLocalizedMessage())
            throw new RuntimeException("Cannot load properties file due to " +
                    ex.getLocalizedMessage(), ex)
        }
    }

    private static File fileForResource(String resourcePath) {
        String filePath
        try {
            URL resourceURL = ProtectedNiFiRegistryPropertiesGroovyTest.class.getResource(resourcePath)
            if (!resourceURL) {
                throw new RuntimeException("File ${resourcePath} not found in class resources, cannot load.")
            }
            filePath = resourceURL.toURI().getPath()
        } catch (URISyntaxException ex) {
            throw new RuntimeException("Cannot load resource file due to " + ex.getLocalizedMessage(), ex)
        }
        File file = new File(filePath)
        return file
    }

    @Test
    void testShouldDetectIfPropertyIsSensitive() throws Exception {
        // Arrange
        final String INSENSITIVE_PROPERTY_KEY = "nifi.registry.web.http.port"
        final String SENSITIVE_PROPERTY_KEY = "nifi.registry.security.keystorePasswd"

        ProtectedNiFiRegistryProperties properties = loadFromResourceFile("/conf/nifi-registry.properties")

        // Act
        boolean bannerIsSensitive = properties.isPropertySensitive(INSENSITIVE_PROPERTY_KEY)
        logger.info("${INSENSITIVE_PROPERTY_KEY} is ${bannerIsSensitive ? "SENSITIVE" : "NOT SENSITIVE"}")
        boolean passwordIsSensitive = properties.isPropertySensitive(SENSITIVE_PROPERTY_KEY)
        logger.info("${SENSITIVE_PROPERTY_KEY} is ${passwordIsSensitive ? "SENSITIVE" : "NOT SENSITIVE"}")

        // Assert
        assert !bannerIsSensitive
        assert passwordIsSensitive
    }

    @Test
    void testShouldGetDefaultSensitiveProperties() throws Exception {
        // Arrange
        logger.info("${DEFAULT_SENSITIVE_PROPERTIES.size()} default sensitive properties: ${DEFAULT_SENSITIVE_PROPERTIES.join(", ")}")

        ProtectedNiFiRegistryProperties properties = loadFromResourceFile("/conf/nifi-registry.properties")

        // Act
        List defaultSensitiveProperties = properties.getSensitivePropertyKeys()
        logger.info("${defaultSensitiveProperties.size()} default sensitive properties: ${defaultSensitiveProperties.join(", ")}")

        // Assert
        assert defaultSensitiveProperties.size() == DEFAULT_SENSITIVE_PROPERTIES.size()
        assert defaultSensitiveProperties.containsAll(DEFAULT_SENSITIVE_PROPERTIES)
    }

    @Test
    void testShouldGetAdditionalSensitiveProperties() throws Exception {
        // Arrange
        def completeSensitiveProperties = DEFAULT_SENSITIVE_PROPERTIES + ["nifi.registry.web.http.port", "nifi.registry.web.http.host"]
        logger.info("${completeSensitiveProperties.size()} total sensitive properties: ${completeSensitiveProperties.join(", ")}")

        ProtectedNiFiRegistryProperties properties = loadFromResourceFile("/conf/nifi-registry.with_additional_sensitive_keys.properties")

        // Act
        List retrievedSensitiveProperties = properties.getSensitivePropertyKeys()
        logger.info("${retrievedSensitiveProperties.size()} retrieved sensitive properties: ${retrievedSensitiveProperties.join(", ")}")

        // Assert
        assert retrievedSensitiveProperties.size() == completeSensitiveProperties.size()
        assert retrievedSensitiveProperties.containsAll(completeSensitiveProperties)
    }

    @Test
    void testGetAdditionalSensitivePropertiesShouldNotIncludeSelf() throws Exception {
        // Arrange
        def completeSensitiveProperties = DEFAULT_SENSITIVE_PROPERTIES + ["nifi.registry.web.http.port", "nifi.registry.web.http.host"]
        logger.info("${completeSensitiveProperties.size()} total sensitive properties: ${completeSensitiveProperties.join(", ")}")

        ProtectedNiFiRegistryProperties properties = loadFromResourceFile("/conf/nifi-registry.with_additional_sensitive_keys.properties")

        // Act
        List retrievedSensitiveProperties = properties.getSensitivePropertyKeys()
        logger.info("${retrievedSensitiveProperties.size()} retrieved sensitive properties: ${retrievedSensitiveProperties.join(", ")}")

        // Assert
        assert retrievedSensitiveProperties.size() == completeSensitiveProperties.size()
        assert retrievedSensitiveProperties.containsAll(completeSensitiveProperties)
    }

    /**
     * In the default (no protection enabled) scenario, a call to retrieve a sensitive property should return the raw value transparently.
     * @throws Exception
     */
    @Test
    void testShouldGetUnprotectedValueOfSensitiveProperty() throws Exception {
        // Arrange
        final String expectedKeystorePassword = "thisIsABadKeystorePassword"

        ProtectedNiFiRegistryProperties properties = loadFromResourceFile("/conf/nifi-registry.with_sensitive_props_unprotected.properties")

        boolean isSensitive = properties.isPropertySensitive(KEYSTORE_PASSWORD_KEY)
        boolean isProtected = properties.isPropertyProtected(KEYSTORE_PASSWORD_KEY)
        logger.info("The property is ${isSensitive ? "sensitive" : "not sensitive"} and ${isProtected ? "protected" : "not protected"}")

        // Act
        String retrievedKeystorePassword = properties.getProperty(KEYSTORE_PASSWORD_KEY)
        logger.info("${KEYSTORE_PASSWORD_KEY}: ${retrievedKeystorePassword}")

        // Assert
        assert retrievedKeystorePassword == expectedKeystorePassword
        assert isSensitive
        assert !isProtected
    }

    /**
     * In the default (no protection enabled) scenario, a call to retrieve a sensitive property (which is empty) should return the raw value transparently.
     * @throws Exception
     */
    @Test
    void testShouldGetEmptyUnprotectedValueOfSensitiveProperty() throws Exception {
        // Arrange
        final String expectedTruststorePassword = ""

        ProtectedNiFiRegistryProperties properties = loadFromResourceFile("/conf/nifi-registry.with_sensitive_props_unprotected.properties")

        boolean isSensitive = properties.isPropertySensitive(TRUSTSTORE_PASSWORD_KEY)
        boolean isProtected = properties.isPropertyProtected(TRUSTSTORE_PASSWORD_KEY)
        logger.info("The property is ${isSensitive ? "sensitive" : "not sensitive"} and ${isProtected ? "protected" : "not protected"}")

        // Act
        NiFiRegistryProperties unprotectedProperties = properties.getUnprotectedProperties()
        String retrievedTruststorePassword = unprotectedProperties.getProperty(TRUSTSTORE_PASSWORD_KEY)
        logger.info("${TRUSTSTORE_PASSWORD_KEY}: ${retrievedTruststorePassword}")

        // Assert
        assert retrievedTruststorePassword == expectedTruststorePassword
        assert isSensitive
        assert !isProtected
    }

    /**
     * The new model no longer needs to maintain the protected state -- it is used as a wrapper/decorator during load to unprotect the sensitive properties and then return an instance of raw properties.
     *
     * @throws Exception
     */
    @Test
    void testShouldGetUnprotectedValueOfSensitivePropertyWhenProtected() throws Exception {
        // Arrange
        final String expectedKeystorePassword = "thisIsABadPassword"

        ProtectedNiFiRegistryProperties properties = loadFromResourceFile("/conf/nifi-registry.with_sensitive_props_protected_aes_128.properties", KEY_HEX_128)

        boolean isSensitive = properties.isPropertySensitive(KEYSTORE_PASSWORD_KEY)
        boolean isProtected = properties.isPropertyProtected(KEYSTORE_PASSWORD_KEY)
        logger.info("The property is ${isSensitive ? "sensitive" : "not sensitive"} and ${isProtected ? "protected" : "not protected"}")

        // Act
        NiFiRegistryProperties unprotectedProperties = properties.getUnprotectedProperties()
        String retrievedKeystorePassword = unprotectedProperties.getProperty(KEYSTORE_PASSWORD_KEY)
        logger.info("${KEYSTORE_PASSWORD_KEY}: ${retrievedKeystorePassword}")

        // Assert
        assert retrievedKeystorePassword == expectedKeystorePassword
        assert isSensitive
        assert isProtected
    }

    /**
     * In the protection enabled scenario, a call to retrieve a sensitive property should handle if the property is protected with an unknown protection scheme.
     * @throws Exception
     */
    @Test
    void testGetValueOfSensitivePropertyShouldHandleUnknownProtectionScheme() throws Exception {
        // Arrange

        // Raw properties
        Properties rawProperties = new Properties()
        rawProperties.load(new FileReader(fileForResource("/conf/nifi-registry.with_sensitive_props_protected_unknown.properties")))
        final String expectedKeystorePassword = rawProperties.getProperty(KEYSTORE_PASSWORD_KEY)
        logger.info("Raw value for ${KEYSTORE_PASSWORD_KEY}: ${expectedKeystorePassword}")

        ProtectedNiFiRegistryProperties properties = loadFromResourceFile("/conf/nifi-registry.with_sensitive_props_protected_unknown.properties")

        boolean isSensitive = properties.isPropertySensitive(KEYSTORE_PASSWORD_KEY)
        boolean isProtected = properties.isPropertyProtected(KEYSTORE_PASSWORD_KEY)

        // While the value is "protected", the scheme is not registered
        logger.info("The property is ${isSensitive ? "sensitive" : "not sensitive"} and ${isProtected ? "protected" : "not protected"}")

        // Act
        def msg = shouldFail(IllegalStateException) {
            NiFiRegistryProperties unprotectedProperties = properties.getUnprotectedProperties()
            String retrievedKeystorePassword = unprotectedProperties.getProperty(KEYSTORE_PASSWORD_KEY)
            logger.info("${KEYSTORE_PASSWORD_KEY}: ${retrievedKeystorePassword}")
        }

        // Assert
        assert msg == "No provider available for nifi.registry.security.keyPasswd"
        assert isSensitive
        assert isProtected
    }

    /**
     * In the protection enabled scenario, a call to retrieve a sensitive property should handle if the property is unable to be unprotected due to a malformed value.
     * @throws Exception
     */
    @Test
    void testGetValueOfSensitivePropertyShouldHandleSingleMalformedValue() throws Exception {
        // Arrange
        ProtectedNiFiRegistryProperties properties = loadFromResourceFile("/conf/nifi-registry.with_sensitive_props_protected_aes_single_malformed.properties", KEY_HEX_128)
        boolean isSensitive = properties.isPropertySensitive(KEYSTORE_PASSWORD_KEY)
        boolean isProtected = properties.isPropertyProtected(KEYSTORE_PASSWORD_KEY)
        logger.info("The property is ${isSensitive ? "sensitive" : "not sensitive"} and ${isProtected ? "protected" : "not protected"}")

        // Act
        def msg = shouldFail(SensitivePropertyProtectionException) {
            NiFiRegistryProperties unprotectedProperties = properties.getUnprotectedProperties()
            String retrievedKeystorePassword = unprotectedProperties.getProperty(KEYSTORE_PASSWORD_KEY)
            logger.info("${KEYSTORE_PASSWORD_KEY}: ${retrievedKeystorePassword}")
        }
        logger.info(msg)

        // Assert
        assert msg =~ "Failed to unprotect key ${KEYSTORE_PASSWORD_KEY}"
        assert isSensitive
        assert isProtected
    }

    /**
     * In the protection enabled scenario, a call to retrieve a sensitive property should handle if the property is unable to be unprotected due to a malformed value.
     * @throws Exception
     */
    @Test
    void testGetValueOfSensitivePropertyShouldHandleMultipleMalformedValues() throws Exception {
        // Arrange

        // Raw properties
        ProtectedNiFiRegistryProperties properties =
                loadFromResourceFile("/conf/nifi-registry.with_sensitive_props_protected_aes_multiple_malformed.properties", KEY_HEX_128)

        // Iterate over the protected keys and track the ones that fail to decrypt
        SensitivePropertyProvider spp = StandardSensitivePropertyProviderFactory.withKey(KEY_HEX_128)
                .getProvider(PropertyProtectionScheme.AES_GCM)
        Set<String> malformedKeys = properties.getProtectedPropertyKeys()
                .findAll { String key, String scheme -> scheme == spp.identifierKey }
                .keySet()
                .findAll { String key ->
            try {
                spp.unprotect(properties.getProperty(key))
                return false
            } catch (SensitivePropertyProtectionException e) {
                logger.expected("Caught a malformed value for ${key}")
                return true
            }
        }

        logger.expected("Malformed keys: ${malformedKeys.join(", ")}")

        // Act
        def e = groovy.test.GroovyAssert.shouldFail(SensitivePropertyProtectionException) {
            NiFiRegistryProperties unprotectedProperties = properties.getUnprotectedProperties()
            String retrievedKeystorePassword = unprotectedProperties.getProperty(KEYSTORE_PASSWORD_KEY)
            logger.info("${KEYSTORE_PASSWORD_KEY}: ${retrievedKeystorePassword}")
        }
        logger.expected(e.getMessage())

        // Assert
        assert e instanceof MultipleSensitivePropertyProtectionException
        assert e.getMessage() =~ "Failed to unprotect keys"
        assert e.getFailedKeys() == malformedKeys

    }

    /**
     * In the protection enabled scenario, a call to retrieve a sensitive property should handle if the internal cache of providers is empty.
     * @throws Exception
     */
    @Test
    void testGetValueOfSensitivePropertyShouldHandleInvalidatedInternalCache() throws Exception {
        // Arrange
        ProtectedNiFiRegistryProperties properties =
                loadFromResourceFile("/conf/nifi-registry.with_sensitive_props_protected_aes_128.properties", KEY_HEX_128)
        final String expectedKeystorePassword = properties.getProperty(KEYSTORE_PASSWORD_KEY)
        logger.info("Read raw value from properties: ${expectedKeystorePassword}")

        // Overwrite the internal cache
        properties.getSensitivePropertyProviders().clear()

        boolean isSensitive = properties.isPropertySensitive(KEYSTORE_PASSWORD_KEY)
        boolean isProtected = properties.isPropertyProtected(KEYSTORE_PASSWORD_KEY)
        logger.info("The property is ${isSensitive ? "sensitive" : "not sensitive"} and ${isProtected ? "protected" : "not protected"}")

        // Act
        def msg = shouldFail(IllegalStateException) {
            NiFiRegistryProperties unprotectedProperties = properties.getUnprotectedProperties()
            String retrievedKeystorePassword = unprotectedProperties.getProperty(KEYSTORE_PASSWORD_KEY)
            logger.info("${KEYSTORE_PASSWORD_KEY}: ${retrievedKeystorePassword}")
        }

        // Assert
        assert msg == "No provider available for nifi.registry.security.keyPasswd"
        assert isSensitive
        assert isProtected
    }

    @Test
    void testShouldDetectIfPropertyIsProtected() throws Exception {
        // Arrange
        final String unprotectedPropertyKey = TRUSTSTORE_PASSWORD_KEY
        final String protectedPropertyKey = KEYSTORE_PASSWORD_KEY

        ProtectedNiFiRegistryProperties properties =
                loadFromResourceFile("/conf/nifi-registry.with_sensitive_props_protected_aes_128.properties", KEY_HEX_128)

        // Act
        boolean unprotectedPasswordIsSensitive = properties.isPropertySensitive(unprotectedPropertyKey)
        boolean unprotectedPasswordIsProtected = properties.isPropertyProtected(unprotectedPropertyKey)
        logger.info("${unprotectedPropertyKey} is ${unprotectedPasswordIsSensitive ? "SENSITIVE" : "NOT SENSITIVE"}")
        logger.info("${unprotectedPropertyKey} is ${unprotectedPasswordIsProtected ? "PROTECTED" : "NOT PROTECTED"}")
        boolean protectedPasswordIsSensitive = properties.isPropertySensitive(protectedPropertyKey)
        boolean protectedPasswordIsProtected = properties.isPropertyProtected(protectedPropertyKey)
        logger.info("${protectedPropertyKey} is ${protectedPasswordIsSensitive ? "SENSITIVE" : "NOT SENSITIVE"}")
        logger.info("${protectedPropertyKey} is ${protectedPasswordIsProtected ? "PROTECTED" : "NOT PROTECTED"}")

        // Assert
        assert unprotectedPasswordIsSensitive
        assert !unprotectedPasswordIsProtected

        assert protectedPasswordIsSensitive
        assert protectedPasswordIsProtected
    }

    @Test
    void testShouldDetectIfPropertyWithEmptyProtectionSchemeIsProtected() throws Exception {
        // Arrange
        final String unprotectedPropertyKey = KEY_PASSWORD_KEY
        ProtectedNiFiRegistryProperties properties =
                loadFromResourceFile("/conf/nifi-registry.with_sensitive_props_unprotected_extra_line.properties")

        // Act
        boolean unprotectedPasswordIsSensitive = properties.isPropertySensitive(unprotectedPropertyKey)
        boolean unprotectedPasswordIsProtected = properties.isPropertyProtected(unprotectedPropertyKey)
        logger.info("${unprotectedPropertyKey} is ${unprotectedPasswordIsSensitive ? "SENSITIVE" : "NOT SENSITIVE"}")
        logger.info("${unprotectedPropertyKey} is ${unprotectedPasswordIsProtected ? "PROTECTED" : "NOT PROTECTED"}")

        // Assert
        assert unprotectedPasswordIsSensitive
        assert !unprotectedPasswordIsProtected
    }

    @Test
    void testShouldGetPercentageOfSensitivePropertiesProtected_0() throws Exception {
        // Arrange
        ProtectedNiFiRegistryProperties properties = loadFromResourceFile("/conf/nifi-registry.properties")

        logger.info("Sensitive property keys: ${properties.getSensitivePropertyKeys()}")
        logger.info("Protected property keys: ${properties.getProtectedPropertyKeys().keySet()}")

        // Act
        double percentProtected = getPercentOfSensitivePropertiesProtected(properties)
        logger.info("${percentProtected}% (${properties.getProtectedPropertyKeys().size()} of ${properties.getPopulatedSensitivePropertyKeys().size()}) protected")

        // Assert
        assert percentProtected == 0.0
    }

    private static double getPercentOfSensitivePropertiesProtected(final ProtectedNiFiRegistryProperties properties) {
        return (int) Math.round(properties.getProtectedPropertyKeys().size() / ((double) properties.getPopulatedSensitivePropertyKeys().size()) * 100);
    }

    @Test
    void testShouldGetPercentageOfSensitivePropertiesProtected_75() throws Exception {
        // Arrange
        ProtectedNiFiRegistryProperties properties =
                loadFromResourceFile("/conf/nifi-registry.with_sensitive_props_protected_aes_128.properties", KEY_HEX_128)

        logger.info("Sensitive property keys: ${properties.getSensitivePropertyKeys()}")
        logger.info("Protected property keys: ${properties.getProtectedPropertyKeys().keySet()}")

        // Act
        double percentProtected = getPercentOfSensitivePropertiesProtected(properties)
        logger.info("${percentProtected}% (${properties.getProtectedPropertyKeys().size()} of ${properties.getPopulatedSensitivePropertyKeys().size()}) protected")

        // Assert
        assert percentProtected == 67.0
    }

    @Test
    void testShouldGetPercentageOfSensitivePropertiesProtected_100() throws Exception {
        // Arrange
        ProtectedNiFiRegistryProperties properties =
                loadFromResourceFile("/conf/nifi-registry.with_sensitive_props_fully_protected_aes_128.properties", KEY_HEX_128)

        logger.info("Sensitive property keys: ${properties.getSensitivePropertyKeys()}")
        logger.info("Protected property keys: ${properties.getProtectedPropertyKeys().keySet()}")

        // Act
        double percentProtected = getPercentOfSensitivePropertiesProtected(properties)
        logger.info("${percentProtected}% (${properties.getProtectedPropertyKeys().size()} of ${properties.getPopulatedSensitivePropertyKeys().size()}) protected")

        // Assert
        assert percentProtected == 100.0
    }

    @Test
    void testInstanceWithNoProtectedPropertiesShouldNotLoadSPP() throws Exception {
        // Arrange
        ProtectedNiFiRegistryProperties properties = loadFromResourceFile("/conf/nifi-registry.properties")
        assert properties.getSensitivePropertyProviders().isEmpty()

        logger.info("Has protected properties: ${properties.hasProtectedKeys()}")
        assert !properties.hasProtectedKeys()

        // Act
        Map localCache = properties.getSensitivePropertyProviders()
        logger.info("Internal cache ${localCache} has ${localCache.size()} providers loaded")

        // Assert
        assert localCache.isEmpty()
    }

    @Test
    void testShouldAddSensitivePropertyProvider() throws Exception {
        // Arrange
        ProtectedNiFiRegistryProperties properties = new ProtectedNiFiRegistryProperties()
        assert properties.getSensitivePropertyProviders().isEmpty()

        SensitivePropertyProvider mockProvider =
                [unprotect       : { String input ->
                    logger.mock("Mock call to #unprotect(${input})")
                    input.reverse()
                },
                 getIdentifierKey: { -> "mockProvider" }] as SensitivePropertyProvider

        // Act
        properties.addSensitivePropertyProvider(mockProvider)

        // Assert
        assert properties.getSensitivePropertyProviders().size() == 1
    }

    @Test
    void testShouldNotAddNullSensitivePropertyProvider() throws Exception {
        // Arrange
        ProtectedNiFiRegistryProperties properties = new ProtectedNiFiRegistryProperties()
        assert properties.getSensitivePropertyProviders().isEmpty()

        // Act
        def msg = shouldFail(NullPointerException) {
            properties.addSensitivePropertyProvider(null)
        }
        logger.info(msg)

        // Assert
        assert properties.getSensitivePropertyProviders().size() == 0
        assert msg == "Cannot add null SensitivePropertyProvider"
    }

    @Test
    void testShouldNotAllowOverwriteOfProvider() throws Exception {
        // Arrange
        ProtectedNiFiRegistryProperties properties = new ProtectedNiFiRegistryProperties()
        assert properties.getSensitivePropertyProviders().isEmpty()

        SensitivePropertyProvider mockProvider =
                [unprotect       : { String input ->
                    logger.mock("Mock call to 1#unprotect(${input})")
                    input.reverse()
                },
                 getIdentifierKey: { -> "mockProvider" }] as SensitivePropertyProvider
        properties.addSensitivePropertyProvider(mockProvider)
        assert properties.getSensitivePropertyProviders().size() == 1

        SensitivePropertyProvider mockProvider2 =
                [unprotect       : { String input ->
                    logger.mock("Mock call to 2#unprotect(${input})")
                    input.reverse()
                },
                 getIdentifierKey: { -> "mockProvider" }] as SensitivePropertyProvider

        // Act
        def msg = shouldFail(UnsupportedOperationException) {
            properties.addSensitivePropertyProvider(mockProvider2)
        }
        logger.info(msg)

        // Assert
        assert msg == "Cannot overwrite existing sensitive property provider registered for mockProvider"
        assert properties.getSensitivePropertyProviders().size() == 1
    }

    @Test
    void testGetUnprotectedPropertiesShouldReturnInternalInstanceWhenNoneProtected() {
        // Arrange
        ProtectedNiFiRegistryProperties protectedNiFiProperties = loadFromResourceFile("/conf/nifi-registry.properties")
        logger.info("Loaded ${protectedNiFiProperties.size()} properties from conf/nifi.properties")

        int hashCode = protectedNiFiProperties.getApplicationProperties().hashCode()
        logger.info("Hash code of internal instance: ${hashCode}")

        // Act
        NiFiRegistryProperties unprotectedNiFiProperties = protectedNiFiProperties.getUnprotectedProperties()
        logger.info("Unprotected ${unprotectedNiFiProperties.size()} properties")

        // Assert
        assert unprotectedNiFiProperties.size() == protectedNiFiProperties.size()
        assert unprotectedNiFiProperties.getPropertyKeys().every {
            !unprotectedNiFiProperties.getProperty(it).endsWith(ApplicationPropertiesProtector.PROTECTED_KEY_SUFFIX)
        }
        logger.info("Hash code from returned unprotected instance: ${unprotectedNiFiProperties.hashCode()}")
        assert unprotectedNiFiProperties.hashCode() == hashCode
    }

    @Test
    void testGetUnprotectedPropertiesShouldDecryptProtectedProperties() {
        // Arrange
        ProtectedNiFiRegistryProperties protectedNiFiProperties =
                loadFromResourceFile("/conf/nifi-registry.with_sensitive_props_protected_aes_128.properties", KEY_HEX_128)

        int protectedPropertyCount = protectedNiFiProperties.getProtectedPropertyKeys().size()
        int protectionSchemeCount = protectedNiFiProperties
                .getPropertyKeysIncludingProtectionSchemes()
                .findAll { it.endsWith(ApplicationPropertiesProtector.PROTECTED_KEY_SUFFIX) }
                .size()
        int expectedUnprotectedPropertyCount = protectedNiFiProperties.size()

        String protectedProps = protectedNiFiProperties
                .getProtectedPropertyKeys()
                .collectEntries {
            [(it.key): protectedNiFiProperties.getProperty(it.key)]
        }.entrySet()
                .join("\n")

        logger.info("Detected ${protectedPropertyCount} protected properties and ${protectionSchemeCount} protection scheme properties")
        logger.info("Protected properties: \n${protectedProps}")

        logger.info("Expected unprotected property count: ${expectedUnprotectedPropertyCount}")

        int hashCode = protectedNiFiProperties.getApplicationProperties().hashCode()
        logger.info("Hash code of internal instance: ${hashCode}")

        // Act
        NiFiRegistryProperties unprotectedNiFiProperties = protectedNiFiProperties.getUnprotectedProperties()
        logger.info("Unprotected ${unprotectedNiFiProperties.size()} properties")

        // Assert
        assert unprotectedNiFiProperties.size() == expectedUnprotectedPropertyCount
        assert unprotectedNiFiProperties.getPropertyKeys().every {
            !unprotectedNiFiProperties.getProperty(it).endsWith(ApplicationPropertiesProtector.PROTECTED_KEY_SUFFIX)
        }
        logger.info("Hash code from returned unprotected instance: ${unprotectedNiFiProperties.hashCode()}")
        assert unprotectedNiFiProperties.hashCode() != hashCode
    }

    @Test
    void testGetUnprotectedPropertiesShouldDecryptProtectedPropertiesWith256Bit() {
        // Arrange
        Assume.assumeTrue("JCE unlimited strength crypto policy must be installed for this test", Cipher.getMaxAllowedKeyLength("AES") > 128)
        ProtectedNiFiRegistryProperties protectedNiFiProperties =
                loadFromResourceFile("/conf/nifi-registry.with_sensitive_props_protected_aes_256.properties", KEY_HEX_256)

        int protectedPropertyCount = protectedNiFiProperties.getProtectedPropertyKeys().size()
        int protectionSchemeCount = protectedNiFiProperties
                .getPropertyKeysIncludingProtectionSchemes()
                .findAll { it.endsWith(ApplicationPropertiesProtector.PROTECTED_KEY_SUFFIX) }
                .size()
        int expectedUnprotectedPropertyCount = protectedNiFiProperties.size()

        String protectedProps = protectedNiFiProperties
                .getProtectedPropertyKeys()
                .collectEntries {
            [(it.key): protectedNiFiProperties.getProperty(it.key)]
        }.entrySet()
                .join("\n")

        logger.info("Detected ${protectedPropertyCount} protected properties and ${protectionSchemeCount} protection scheme properties")
        logger.info("Protected properties: \n${protectedProps}")

        logger.info("Expected unprotected property count: ${expectedUnprotectedPropertyCount}")

        int hashCode = protectedNiFiProperties.getApplicationProperties().hashCode()
        logger.info("Hash code of internal instance: ${hashCode}")

        // Act
        NiFiRegistryProperties unprotectedNiFiProperties = protectedNiFiProperties.getUnprotectedProperties()
        logger.info("Unprotected ${unprotectedNiFiProperties.size()} properties")

        // Assert
        assert unprotectedNiFiProperties.size() == expectedUnprotectedPropertyCount
        assert unprotectedNiFiProperties.getPropertyKeys().every {
            !unprotectedNiFiProperties.getProperty(it).endsWith(ApplicationPropertiesProtector.PROTECTED_KEY_SUFFIX)
        }
        logger.info("Hash code from returned unprotected instance: ${unprotectedNiFiProperties.hashCode()}")
        assert unprotectedNiFiProperties.hashCode() != hashCode
    }

    @Test
    void testShouldCalculateSize() {
        // Arrange
        Properties props = [key: "protectedValue", "key.protected": "scheme", "key2": "value2"] as Properties
        NiFiRegistryProperties rawProperties = new NiFiRegistryProperties(props)
        ProtectedNiFiRegistryProperties protectedNiFiProperties = new ProtectedNiFiRegistryProperties(rawProperties)
        logger.info("Raw properties (${rawProperties.size()}): ${rawProperties.getPropertyKeys().join(", ")}")

        // Act
        int protectedSize = protectedNiFiProperties.size()
        logger.info("Protected properties (${protectedNiFiProperties.size()}): " +
                "${protectedNiFiProperties.getPropertyKeys().join(", ")}")

        // Assert
        assert protectedSize == rawProperties.size() - 1
    }

    @Test
    void testGetPropertyKeysShouldMatchSize() {
        // Arrange
        Properties props = [key: "protectedValue", "key.protected": "scheme", "key2": "value2"] as Properties
        NiFiRegistryProperties rawProperties = new NiFiRegistryProperties(props)
        ProtectedNiFiRegistryProperties protectedNiFiProperties = new ProtectedNiFiRegistryProperties(rawProperties)
        logger.info("Raw properties (${rawProperties.size()}): ${rawProperties.getPropertyKeys().join(", ")}")

        // Act
        def filteredKeys = protectedNiFiProperties.getPropertyKeys()
        logger.info("Protected properties (${protectedNiFiProperties.size()}): ${filteredKeys.join(", ")}")

        // Assert
        assert protectedNiFiProperties.size() == rawProperties.size() - 1
        assert filteredKeys == rawProperties.getPropertyKeys() - "key.protected"
    }

    @Test
    void testShouldGetPropertyKeysIncludingProtectionSchemes() {
        // Arrange
        Properties props = [key: "protectedValue", "key.protected": "scheme", "key2": "value2"] as Properties
        NiFiRegistryProperties rawProperties = new NiFiRegistryProperties(props)
        ProtectedNiFiRegistryProperties protectedNiFiProperties = new ProtectedNiFiRegistryProperties(rawProperties)
        logger.info("Raw properties (${rawProperties.size()}): ${rawProperties.getPropertyKeys().join(", ")}")

        // Act
        def allKeys = protectedNiFiProperties.getPropertyKeysIncludingProtectionSchemes()
        logger.info("Protected properties with schemes (${allKeys.size()}): ${allKeys.join(", ")}")

        // Assert
        assert allKeys.size() == rawProperties.size()
        assert allKeys == rawProperties.getPropertyKeys()
    }

}
