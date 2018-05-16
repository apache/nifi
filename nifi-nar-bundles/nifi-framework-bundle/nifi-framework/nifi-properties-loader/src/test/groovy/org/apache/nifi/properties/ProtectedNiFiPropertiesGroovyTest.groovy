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

import org.apache.nifi.util.NiFiProperties
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

import java.security.Security

@RunWith(JUnit4.class)
class ProtectedNiFiPropertiesGroovyTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(ProtectedNiFiPropertiesGroovyTest.class)

    final def DEFAULT_SENSITIVE_PROPERTIES = [
            "nifi.sensitive.props.key",
            "nifi.security.keystorePasswd",
            "nifi.security.keyPasswd",
            "nifi.security.truststorePasswd",
            "nifi.provenance.repository.encryption.key"
    ]

    final def COMMON_ADDITIONAL_SENSITIVE_PROPERTIES = [
            "nifi.sensitive.props.algorithm",
            "nifi.kerberos.service.principal",
            "nifi.kerberos.krb5.file",
            "nifi.kerberos.keytab.location"
    ]

    private static final String KEY_HEX = "0123456789ABCDEFFEDCBA9876543210" * 2

    private static String originalPropertiesPath = System.getProperty(NiFiProperties.PROPERTIES_FILE_PATH)

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
        if (originalPropertiesPath) {
            System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, originalPropertiesPath)
        }
    }

    private static ProtectedNiFiProperties loadFromFile(String propertiesFilePath) {
        String filePath
        try {
            filePath = ProtectedNiFiPropertiesGroovyTest.class.getResource(propertiesFilePath).toURI().getPath()
        } catch (URISyntaxException ex) {
            throw new RuntimeException("Cannot load properties file due to "
                    + ex.getLocalizedMessage(), ex)
        }

        File file = new File(filePath)

        if (file == null || !file.exists() || !file.canRead()) {
            String path = (file == null ? "missing file" : file.getAbsolutePath())
            logger.error("Cannot read from '{}' -- file is missing or not readable", path)
            throw new IllegalArgumentException("NiFi properties file missing or unreadable")
        }

        Properties rawProperties = new Properties()

        InputStream inStream = null
        try {
            inStream = new BufferedInputStream(new FileInputStream(file))
            rawProperties.load(inStream)
            logger.info("Loaded {} properties from {}", rawProperties.size(), file.getAbsolutePath())

            ProtectedNiFiProperties protectedNiFiProperties = new ProtectedNiFiProperties(rawProperties)

            // If it has protected keys, inject the SPP
            if (protectedNiFiProperties.hasProtectedKeys()) {
                protectedNiFiProperties.addSensitivePropertyProvider(new AESSensitivePropertyProvider(KEY_HEX))
            }

            return protectedNiFiProperties
        } catch (final Exception ex) {
            logger.error("Cannot load properties file due to " + ex.getLocalizedMessage())
            throw new RuntimeException("Cannot load properties file due to "
                    + ex.getLocalizedMessage(), ex)
        } finally {
            if (null != inStream) {
                try {
                    inStream.close()
                } catch (final Exception ex) {
                    /**
                     * do nothing *
                     */
                }
            }
        }
    }

    @Test
    void testConstructorShouldCreateNewInstance() throws Exception {
        // Arrange

        // Act
        NiFiProperties niFiProperties = new StandardNiFiProperties()
        logger.info("niFiProperties has ${niFiProperties.size()} properties: ${niFiProperties.getPropertyKeys()}")

        // Assert
        assert niFiProperties.size() == 0
        assert niFiProperties.getPropertyKeys() == [] as Set
    }

    @Test
    void testConstructorShouldAcceptRawProperties() throws Exception {
        // Arrange
        Properties rawProperties = new Properties()
        rawProperties.setProperty("key", "value")
        logger.info("rawProperties has ${rawProperties.size()} properties: ${rawProperties.stringPropertyNames()}")
        assert rawProperties.size() == 1

        // Act
        NiFiProperties niFiProperties = new StandardNiFiProperties(rawProperties)
        logger.info("niFiProperties has ${niFiProperties.size()} properties: ${niFiProperties.getPropertyKeys()}")

        // Assert
        assert niFiProperties.size() == 1
        assert niFiProperties.getPropertyKeys() == ["key"] as Set
    }

    @Test
    void testConstructorShouldAcceptNiFiProperties() throws Exception {
        // Arrange
        Properties rawProperties = new Properties()
        rawProperties.setProperty("key", "value")
        rawProperties.setProperty("key.protected", "value2")
        NiFiProperties niFiProperties = new StandardNiFiProperties(rawProperties)
        logger.info("niFiProperties has ${niFiProperties.size()} properties: ${niFiProperties.getPropertyKeys()}")
        assert niFiProperties.size() == 2

        // Act
        ProtectedNiFiProperties protectedNiFiProperties = new ProtectedNiFiProperties(niFiProperties)
        logger.info("protectedNiFiProperties has ${protectedNiFiProperties.size()} properties: ${protectedNiFiProperties.getPropertyKeys()}")

        // Assert
        def allKeys = protectedNiFiProperties.getPropertyKeysIncludingProtectionSchemes()
        assert allKeys == ["key", "key.protected"] as Set
        assert allKeys.size() == niFiProperties.size()

    }

    @Test
    void testShouldAllowMultipleInstances() throws Exception {
        // Arrange
        Properties rawProperties = new Properties()
        rawProperties.setProperty("key", "value")
        logger.info("rawProperties has ${rawProperties.size()} properties: ${rawProperties.stringPropertyNames()}")
        assert rawProperties.size() == 1

        // Act
        NiFiProperties niFiProperties = new StandardNiFiProperties(rawProperties)
        logger.info("niFiProperties has ${niFiProperties.size()} properties: ${niFiProperties.getPropertyKeys()}")
        NiFiProperties emptyProperties = new StandardNiFiProperties()
        logger.info("emptyProperties has ${emptyProperties.size()} properties: ${emptyProperties.getPropertyKeys()}")

        // Assert
        assert niFiProperties.size() == 1
        assert niFiProperties.getPropertyKeys() == ["key"] as Set

        assert emptyProperties.size() == 0
        assert emptyProperties.getPropertyKeys() == [] as Set
    }

    @Test
    void testShouldDetectIfPropertyIsSensitive() throws Exception {
        // Arrange
        final String INSENSITIVE_PROPERTY_KEY = "nifi.ui.banner.text"
        final String SENSITIVE_PROPERTY_KEY = "nifi.security.keystorePasswd"

        ProtectedNiFiProperties properties = loadFromFile("/conf/nifi.properties")

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
        logger.expected("${DEFAULT_SENSITIVE_PROPERTIES.size()} default sensitive properties: ${DEFAULT_SENSITIVE_PROPERTIES.join(", ")}")

        ProtectedNiFiProperties properties = loadFromFile("/conf/nifi.properties")

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
        def completeSensitiveProperties = DEFAULT_SENSITIVE_PROPERTIES + ["nifi.ui.banner.text", "nifi.version"]
        logger.expected("${completeSensitiveProperties.size()} total sensitive properties: ${completeSensitiveProperties.join(", ")}")

        ProtectedNiFiProperties properties = loadFromFile("/conf/nifi_with_additional_sensitive_keys.properties")

        // Act
        List retrievedSensitiveProperties = properties.getSensitivePropertyKeys()
        logger.info("${retrievedSensitiveProperties.size()} retrieved sensitive properties: ${retrievedSensitiveProperties.join(", ")}")

        // Assert
        assert retrievedSensitiveProperties.size() == completeSensitiveProperties.size()
        assert retrievedSensitiveProperties.containsAll(completeSensitiveProperties)
    }

    // TODO: Add negative tests (fuzz additional.keys property, etc.)

    @Test
    void testGetAdditionalSensitivePropertiesShouldNotIncludeSelf() throws Exception {
        // Arrange
        def completeSensitiveProperties = DEFAULT_SENSITIVE_PROPERTIES + ["nifi.ui.banner.text", "nifi.version"]
        logger.expected("${completeSensitiveProperties.size()} total sensitive properties: ${completeSensitiveProperties.join(", ")}")

        ProtectedNiFiProperties properties = loadFromFile("/conf/nifi_with_additional_sensitive_keys.properties")

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
        final String KEYSTORE_PASSWORD_KEY = "nifi.security.keystorePasswd"
        final String EXPECTED_KEYSTORE_PASSWORD = "thisIsABadKeystorePassword"

        ProtectedNiFiProperties properties = loadFromFile("/conf/nifi_with_sensitive_properties_unprotected.properties")

        boolean isSensitive = properties.isPropertySensitive(KEYSTORE_PASSWORD_KEY)
        boolean isProtected = properties.isPropertyProtected(KEYSTORE_PASSWORD_KEY)
        logger.info("The property is ${isSensitive ? "sensitive" : "not sensitive"} and ${isProtected ? "protected" : "not protected"}")

        // Act
        String retrievedKeystorePassword = properties.getProperty(KEYSTORE_PASSWORD_KEY)
        logger.info("${KEYSTORE_PASSWORD_KEY}: ${retrievedKeystorePassword}")

        // Assert
        assert retrievedKeystorePassword == EXPECTED_KEYSTORE_PASSWORD
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
        final String TRUSTSTORE_PASSWORD_KEY = "nifi.security.truststorePasswd"
        final String EXPECTED_TRUSTSTORE_PASSWORD = ""

        ProtectedNiFiProperties properties = loadFromFile("/conf/nifi_with_sensitive_properties_unprotected.properties")

        boolean isSensitive = properties.isPropertySensitive(TRUSTSTORE_PASSWORD_KEY)
        boolean isProtected = properties.isPropertyProtected(TRUSTSTORE_PASSWORD_KEY)
        logger.info("The property is ${isSensitive ? "sensitive" : "not sensitive"} and ${isProtected ? "protected" : "not protected"}")

        // Act
        NiFiProperties unprotectedProperties = properties.getUnprotectedProperties()
        String retrievedTruststorePassword = unprotectedProperties.getProperty(TRUSTSTORE_PASSWORD_KEY)
        logger.info("${TRUSTSTORE_PASSWORD_KEY}: ${retrievedTruststorePassword}")

        // Assert
        assert retrievedTruststorePassword == EXPECTED_TRUSTSTORE_PASSWORD
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
        final String KEYSTORE_PASSWORD_KEY = "nifi.security.keystorePasswd"
        final String EXPECTED_KEYSTORE_PASSWORD = "thisIsABadKeystorePassword"

        ProtectedNiFiProperties properties = loadFromFile("/conf/nifi_with_sensitive_properties_protected_aes.properties")

        boolean isSensitive = properties.isPropertySensitive(KEYSTORE_PASSWORD_KEY)
        boolean isProtected = properties.isPropertyProtected(KEYSTORE_PASSWORD_KEY)
        logger.info("The property is ${isSensitive ? "sensitive" : "not sensitive"} and ${isProtected ? "protected" : "not protected"}")

        // Act
        NiFiProperties unprotectedProperties = properties.getUnprotectedProperties()
        String retrievedKeystorePassword = unprotectedProperties.getProperty(KEYSTORE_PASSWORD_KEY)
        logger.info("${KEYSTORE_PASSWORD_KEY}: ${retrievedKeystorePassword}")

        // Assert
        assert retrievedKeystorePassword == EXPECTED_KEYSTORE_PASSWORD
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
        final String KEYSTORE_PASSWORD_KEY = "nifi.security.keystorePasswd"

        // Raw properties
        Properties rawProperties = new Properties()
        rawProperties.load(new File("src/test/resources/conf/nifi_with_sensitive_properties_protected_unknown.properties").newInputStream())
        final String RAW_KEYSTORE_PASSWORD = rawProperties.getProperty(KEYSTORE_PASSWORD_KEY)
        logger.info("Raw value for ${KEYSTORE_PASSWORD_KEY}: ${RAW_KEYSTORE_PASSWORD}")

        ProtectedNiFiProperties properties = loadFromFile("/conf/nifi_with_sensitive_properties_protected_unknown.properties")

        boolean isSensitive = properties.isPropertySensitive(KEYSTORE_PASSWORD_KEY)
        boolean isProtected = properties.isPropertyProtected(KEYSTORE_PASSWORD_KEY)

        // While the value is "protected", the scheme is not registered, so treat it as raw
        logger.info("The property is ${isSensitive ? "sensitive" : "not sensitive"} and ${isProtected ? "protected" : "not protected"}")

        // Act
        NiFiProperties unprotectedProperties = properties.getUnprotectedProperties()
        String retrievedKeystorePassword = unprotectedProperties.getProperty(KEYSTORE_PASSWORD_KEY)
        logger.info("${KEYSTORE_PASSWORD_KEY}: ${retrievedKeystorePassword}")

        // Assert
        assert retrievedKeystorePassword == RAW_KEYSTORE_PASSWORD
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
        final String KEYSTORE_PASSWORD_KEY = "nifi.security.keystorePasswd"

        // Raw properties
        Properties rawProperties = new Properties()
        rawProperties.load(new File("src/test/resources/conf/nifi_with_sensitive_properties_protected_aes_single_malformed.properties").newInputStream())
        final String RAW_KEYSTORE_PASSWORD = rawProperties.getProperty(KEYSTORE_PASSWORD_KEY)
        logger.info("Raw value for ${KEYSTORE_PASSWORD_KEY}: ${RAW_KEYSTORE_PASSWORD}")

        ProtectedNiFiProperties properties = loadFromFile("/conf/nifi_with_sensitive_properties_protected_aes_single_malformed.properties")

        boolean isSensitive = properties.isPropertySensitive(KEYSTORE_PASSWORD_KEY)
        boolean isProtected = properties.isPropertyProtected(KEYSTORE_PASSWORD_KEY)
        logger.info("The property is ${isSensitive ? "sensitive" : "not sensitive"} and ${isProtected ? "protected" : "not protected"}")

        // Act
        def msg = shouldFail(SensitivePropertyProtectionException) {
            NiFiProperties unprotectedProperties = properties.getUnprotectedProperties()
            String retrievedKeystorePassword = unprotectedProperties.getProperty(KEYSTORE_PASSWORD_KEY)
            logger.info("${KEYSTORE_PASSWORD_KEY}: ${retrievedKeystorePassword}")
        }
        logger.expected(msg)

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
        Properties rawProperties = new Properties()
        rawProperties.load(new File("src/test/resources/conf/nifi_with_sensitive_properties_protected_aes_multiple_malformed.properties").newInputStream())

        ProtectedNiFiProperties properties = loadFromFile("/conf/nifi_with_sensitive_properties_protected_aes_multiple_malformed.properties")

        // Iterate over the protected keys and track the ones that fail to decrypt
        SensitivePropertyProvider spp = new AESSensitivePropertyProvider(KEY_HEX)
        Set<String> malformedKeys = properties.getProtectedPropertyKeys()
                .findAll { String key, String scheme -> scheme == spp.identifierKey }
                .keySet().collect { String key ->
            try {
                String rawValue = spp.unprotect(properties.getProperty(key))
                return
            } catch (SensitivePropertyProtectionException e) {
                logger.expected("Caught a malformed value for ${key}")
                return key
            }
        }

        logger.expected("Malformed keys: ${malformedKeys.join(", ")}")

        // Act
        def e = groovy.test.GroovyAssert.shouldFail(SensitivePropertyProtectionException) {
            NiFiProperties unprotectedProperties = properties.getUnprotectedProperties()
        }
        logger.expected(e.getMessage())

        // Assert
        assert e instanceof MultipleSensitivePropertyProtectionException
        assert e.getMessage() =~ "Failed to unprotect keys"
        assert e.getFailedKeys() == malformedKeys

    }

    /**
     * In the default (no protection enabled) scenario, a call to retrieve a sensitive property (which is empty) should return the raw value transparently.
     * @throws Exception
     */
    @Test
    void testShouldGetEmptyUnprotectedValueOfSensitivePropertyWithDefault() throws Exception {
        // Arrange
        final String TRUSTSTORE_PASSWORD_KEY = "nifi.security.truststorePasswd"
        final String EXPECTED_TRUSTSTORE_PASSWORD = ""
        final String DEFAULT_VALUE = "defaultValue"

        // Raw properties
        Properties rawProperties = new Properties()
        rawProperties.load(new File("src/test/resources/conf/nifi_with_sensitive_properties_unprotected.properties").newInputStream())
        final String RAW_TRUSTSTORE_PASSWORD = rawProperties.getProperty(TRUSTSTORE_PASSWORD_KEY)
        logger.info("Raw value for ${TRUSTSTORE_PASSWORD_KEY}: ${RAW_TRUSTSTORE_PASSWORD}")
        assert RAW_TRUSTSTORE_PASSWORD == EXPECTED_TRUSTSTORE_PASSWORD

        ProtectedNiFiProperties properties = loadFromFile("/conf/nifi_with_sensitive_properties_unprotected.properties")

        boolean isSensitive = properties.isPropertySensitive(TRUSTSTORE_PASSWORD_KEY)
        boolean isProtected = properties.isPropertyProtected(TRUSTSTORE_PASSWORD_KEY)
        logger.info("The property is ${isSensitive ? "sensitive" : "not sensitive"} and ${isProtected ? "protected" : "not protected"}")

        // Act
        String retrievedTruststorePassword = properties.getProperty(TRUSTSTORE_PASSWORD_KEY, DEFAULT_VALUE)
        logger.info("${TRUSTSTORE_PASSWORD_KEY}: ${retrievedTruststorePassword}")

        // Assert
        assert retrievedTruststorePassword == DEFAULT_VALUE
        assert isSensitive
        assert !isProtected
    }

    /**
     * In the protection enabled scenario, a call to retrieve a sensitive property should return the raw value transparently.
     * @throws Exception
     */
    @Test
    void testShouldGetUnprotectedValueOfSensitivePropertyWhenProtectedWithDefault() throws Exception {
        // Arrange
        final String KEYSTORE_PASSWORD_KEY = "nifi.security.keystorePasswd"
        final String EXPECTED_KEYSTORE_PASSWORD = "thisIsABadKeystorePassword"
        final String DEFAULT_VALUE = "defaultValue"

        // Raw properties
        Properties rawProperties = new Properties()
        rawProperties.load(new File("src/test/resources/conf/nifi_with_sensitive_properties_protected_aes.properties").newInputStream())
        final String RAW_KEYSTORE_PASSWORD = rawProperties.getProperty(KEYSTORE_PASSWORD_KEY)
        logger.info("Raw value for ${KEYSTORE_PASSWORD_KEY}: ${RAW_KEYSTORE_PASSWORD}")

        ProtectedNiFiProperties properties = loadFromFile("/conf/nifi_with_sensitive_properties_protected_aes.properties")

        boolean isSensitive = properties.isPropertySensitive(KEYSTORE_PASSWORD_KEY)
        boolean isProtected = properties.isPropertyProtected(KEYSTORE_PASSWORD_KEY)
        logger.info("The property is ${isSensitive ? "sensitive" : "not sensitive"} and ${isProtected ? "protected" : "not protected"}")

        // Act
        NiFiProperties unprotectedProperties = properties.getUnprotectedProperties()
        String retrievedKeystorePassword = unprotectedProperties.getProperty(KEYSTORE_PASSWORD_KEY, DEFAULT_VALUE)
        logger.info("${KEYSTORE_PASSWORD_KEY}: ${retrievedKeystorePassword}")

        // Assert
        assert retrievedKeystorePassword == EXPECTED_KEYSTORE_PASSWORD
        assert isSensitive
        assert isProtected
    }

    // TODO: Test getProtected with multiple providers

    /**
     * In the protection enabled scenario, a call to retrieve a sensitive property should handle if the internal cache of providers is empty.
     * @throws Exception
     */
    @Test
    void testGetValueOfSensitivePropertyShouldHandleInvalidatedInternalCache() throws Exception {
        // Arrange
        final String KEYSTORE_PASSWORD_KEY = "nifi.security.keystorePasswd"
        final String EXPECTED_KEYSTORE_PASSWORD = "thisIsABadKeystorePassword"

        ProtectedNiFiProperties properties = loadFromFile("/conf/nifi_with_sensitive_properties_protected_aes.properties")

        final String RAW_PASSWORD = properties.getProperty(KEYSTORE_PASSWORD_KEY)
        logger.info("Read raw value from properties: ${RAW_PASSWORD}")

        // Overwrite the internal cache
        properties.localProviderCache = [:]

        boolean isSensitive = properties.isPropertySensitive(KEYSTORE_PASSWORD_KEY)
        boolean isProtected = properties.isPropertyProtected(KEYSTORE_PASSWORD_KEY)
        logger.info("The property is ${isSensitive ? "sensitive" : "not sensitive"} and ${isProtected ? "protected" : "not protected"}")

        // Act
        NiFiProperties unprotectedProperties = properties.getUnprotectedProperties()
        String retrievedKeystorePassword = unprotectedProperties.getProperty(KEYSTORE_PASSWORD_KEY)
        logger.info("${KEYSTORE_PASSWORD_KEY}: ${retrievedKeystorePassword}")

        // Assert
        assert retrievedKeystorePassword == RAW_PASSWORD
        assert isSensitive
        assert isProtected
    }

    @Test
    void testShouldDetectIfPropertyIsProtected() throws Exception {
        // Arrange
        final String UNPROTECTED_PROPERTY_KEY = "nifi.security.truststorePasswd"
        final String PROTECTED_PROPERTY_KEY = "nifi.security.keystorePasswd"

        ProtectedNiFiProperties properties = loadFromFile("/conf/nifi_with_sensitive_properties_protected_aes.properties")

        // Act
        boolean unprotectedPasswordIsSensitive = properties.isPropertySensitive(UNPROTECTED_PROPERTY_KEY)
        boolean unprotectedPasswordIsProtected = properties.isPropertyProtected(UNPROTECTED_PROPERTY_KEY)
        logger.info("${UNPROTECTED_PROPERTY_KEY} is ${unprotectedPasswordIsSensitive ? "SENSITIVE" : "NOT SENSITIVE"}")
        logger.info("${UNPROTECTED_PROPERTY_KEY} is ${unprotectedPasswordIsProtected ? "PROTECTED" : "NOT PROTECTED"}")
        boolean protectedPasswordIsSensitive = properties.isPropertySensitive(PROTECTED_PROPERTY_KEY)
        boolean protectedPasswordIsProtected = properties.isPropertyProtected(PROTECTED_PROPERTY_KEY)
        logger.info("${PROTECTED_PROPERTY_KEY} is ${protectedPasswordIsSensitive ? "SENSITIVE" : "NOT SENSITIVE"}")
        logger.info("${PROTECTED_PROPERTY_KEY} is ${protectedPasswordIsProtected ? "PROTECTED" : "NOT PROTECTED"}")

        // Assert
        assert unprotectedPasswordIsSensitive
        assert !unprotectedPasswordIsProtected

        assert protectedPasswordIsSensitive
        assert protectedPasswordIsProtected
    }

    @Test
    void testShouldDetectIfPropertyWithEmptyProtectionSchemeIsProtected() throws Exception {
        // Arrange
        final String UNPROTECTED_PROPERTY_KEY = "nifi.sensitive.props.key"

        ProtectedNiFiProperties properties = loadFromFile("/conf/nifi_with_sensitive_properties_unprotected_extra_line.properties")

        // Act
        boolean unprotectedPasswordIsSensitive = properties.isPropertySensitive(UNPROTECTED_PROPERTY_KEY)
        boolean unprotectedPasswordIsProtected = properties.isPropertyProtected(UNPROTECTED_PROPERTY_KEY)
        logger.info("${UNPROTECTED_PROPERTY_KEY} is ${unprotectedPasswordIsSensitive ? "SENSITIVE" : "NOT SENSITIVE"}")
        logger.info("${UNPROTECTED_PROPERTY_KEY} is ${unprotectedPasswordIsProtected ? "PROTECTED" : "NOT PROTECTED"}")

        // Assert
        assert unprotectedPasswordIsSensitive
        assert !unprotectedPasswordIsProtected
    }

    @Test
    void testShouldGetPercentageOfSensitivePropertiesProtected_0() throws Exception {
        // Arrange
        ProtectedNiFiProperties properties = loadFromFile("/conf/nifi.properties")

        logger.info("Sensitive property keys: ${properties.getSensitivePropertyKeys()}")
        logger.info("Protected property keys: ${properties.getProtectedPropertyKeys().keySet()}")

        // Act
        double percentProtected = properties.getPercentOfSensitivePropertiesProtected()
        logger.info("${percentProtected}% (${properties.getProtectedPropertyKeys().size()} of ${properties.getPopulatedSensitivePropertyKeys().size()}) protected")

        // Assert
        assert percentProtected == 0.0
    }

    @Test
    void testShouldGetPercentageOfSensitivePropertiesProtected_75() throws Exception {
        // Arrange
        ProtectedNiFiProperties properties = loadFromFile("/conf/nifi_with_sensitive_properties_protected_aes.properties")

        logger.info("Sensitive property keys: ${properties.getSensitivePropertyKeys()}")
        logger.info("Protected property keys: ${properties.getProtectedPropertyKeys().keySet()}")

        // Act
        double percentProtected = properties.getPercentOfSensitivePropertiesProtected()
        logger.info("${percentProtected}% (${properties.getProtectedPropertyKeys().size()} of ${properties.getPopulatedSensitivePropertyKeys().size()}) protected")

        // Assert
        assert percentProtected == 75.0
    }

    @Test
    void testShouldGetPercentageOfSensitivePropertiesProtected_100() throws Exception {
        // Arrange
        ProtectedNiFiProperties properties = loadFromFile("/conf/nifi_with_all_sensitive_properties_protected_aes.properties")

        logger.info("Sensitive property keys: ${properties.getSensitivePropertyKeys()}")
        logger.info("Protected property keys: ${properties.getProtectedPropertyKeys().keySet()}")

        // Act
        double percentProtected = properties.getPercentOfSensitivePropertiesProtected()
        logger.info("${percentProtected}% (${properties.getProtectedPropertyKeys().size()} of ${properties.getPopulatedSensitivePropertyKeys().size()}) protected")

        // Assert
        assert percentProtected == 100.0
    }

    @Test
    void testInstanceWithNoProtectedPropertiesShouldNotLoadSPP() throws Exception {
        // Arrange
        ProtectedNiFiProperties properties = loadFromFile("/conf/nifi.properties")
        assert properties.@localProviderCache?.isEmpty()

        logger.info("Has protected properties: ${properties.hasProtectedKeys()}")
        assert !properties.hasProtectedKeys()

        // Act
        Map localCache = properties.@localProviderCache
        logger.info("Internal cache ${localCache} has ${localCache.size()} providers loaded")

        // Assert
        assert localCache.isEmpty()
    }

    @Test
    void testShouldAddSensitivePropertyProvider() throws Exception {
        // Arrange
        ProtectedNiFiProperties properties = new ProtectedNiFiProperties()
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
        ProtectedNiFiProperties properties = new ProtectedNiFiProperties()
        assert properties.getSensitivePropertyProviders().isEmpty()

        // Act
        def msg = shouldFail(IllegalArgumentException) {
            properties.addSensitivePropertyProvider(null)
        }
        logger.expected(msg)

        // Assert
        assert properties.getSensitivePropertyProviders().size() == 0
        assert msg == "Cannot add null SensitivePropertyProvider"
    }

    @Test
    void testShouldNotAllowOverwriteOfProvider() throws Exception {
        // Arrange
        ProtectedNiFiProperties properties = new ProtectedNiFiProperties()
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
        logger.expected(msg)

        // Assert
        assert msg == "Cannot overwrite existing sensitive property provider registered for mockProvider"
        assert properties.getSensitivePropertyProviders().size() == 1
    }

    @Test
    void testGetUnprotectedPropertiesShouldReturnInternalInstanceWhenNoneProtected() {
        // Arrange
        String noProtectedPropertiesPath = "/conf/nifi.properties"
        ProtectedNiFiProperties protectedNiFiProperties = loadFromFile(noProtectedPropertiesPath)
        logger.info("Loaded ${protectedNiFiProperties.size()} properties from ${noProtectedPropertiesPath}")

        int hashCode = protectedNiFiProperties.internalNiFiProperties.hashCode()
        logger.info("Hash code of internal instance: ${hashCode}")

        // Act
        NiFiProperties unprotectedNiFiProperties = protectedNiFiProperties.getUnprotectedProperties()
        logger.info("Unprotected ${unprotectedNiFiProperties.size()} properties")

        // Assert
        assert unprotectedNiFiProperties.size() == protectedNiFiProperties.size()
        assert unprotectedNiFiProperties.getPropertyKeys().every {
            !unprotectedNiFiProperties.getProperty(it).endsWith(".protected")
        }
        logger.info("Hash code from returned unprotected instance: ${unprotectedNiFiProperties.hashCode()}")
        assert unprotectedNiFiProperties.hashCode() == hashCode
    }

    @Test
    void testGetUnprotectedPropertiesShouldDecryptProtectedProperties() {
        // Arrange
        String noProtectedPropertiesPath = "/conf/nifi_with_sensitive_properties_protected_aes.properties"
        ProtectedNiFiProperties protectedNiFiProperties = loadFromFile(noProtectedPropertiesPath)
        logger.info("Loaded ${protectedNiFiProperties.size()} properties from ${noProtectedPropertiesPath}")

        int protectedPropertyCount = protectedNiFiProperties.getProtectedPropertyKeys().size()
        int protectionSchemeCount = protectedNiFiProperties
                .getPropertyKeys().findAll { it.endsWith(".protected") }
                .size()
        int expectedUnprotectedPropertyCount = protectedNiFiProperties.size() - protectionSchemeCount

        String protectedProps = protectedNiFiProperties
                .getProtectedPropertyKeys()
                .collectEntries {
            [(it.key): protectedNiFiProperties.getProperty(it.key)]
        }.entrySet()
                .join("\n")

        logger.info("Detected ${protectedPropertyCount} protected properties and ${protectionSchemeCount} protection scheme properties")
        logger.info("Protected properties: \n${protectedProps}")

        logger.info("Expected unprotected property count: ${expectedUnprotectedPropertyCount}")

        int hashCode = protectedNiFiProperties.internalNiFiProperties.hashCode()
        logger.info("Hash code of internal instance: ${hashCode}")

        // Act
        NiFiProperties unprotectedNiFiProperties = protectedNiFiProperties.getUnprotectedProperties()
        logger.info("Unprotected ${unprotectedNiFiProperties.size()} properties")

        // Assert
        assert unprotectedNiFiProperties.size() == expectedUnprotectedPropertyCount
        assert unprotectedNiFiProperties.getPropertyKeys().every {
            !unprotectedNiFiProperties.getProperty(it).endsWith(".protected")
        }
        logger.info("Hash code from returned unprotected instance: ${unprotectedNiFiProperties.hashCode()}")
        assert unprotectedNiFiProperties.hashCode() != hashCode
    }

    @Test
    void testShouldCalculateSize() {
        // Arrange
        Properties rawProperties = [key: "protectedValue", "key.protected": "scheme", "key2": "value2"] as Properties
        ProtectedNiFiProperties protectedNiFiProperties = new ProtectedNiFiProperties(rawProperties)
        logger.info("Raw properties (${rawProperties.size()}): ${rawProperties.keySet().join(", ")}")

        // Act
        int protectedSize = protectedNiFiProperties.size()
        logger.info("Protected properties (${protectedNiFiProperties.size()}): ${protectedNiFiProperties.getPropertyKeys().join(", ")}")

        // Assert
        assert protectedSize == rawProperties.size() - 1
    }

    @Test
    void testGetPropertyKeysShouldMatchSize() {
        // Arrange
        Properties rawProperties = [key: "protectedValue", "key.protected": "scheme", "key2": "value2"] as Properties
        ProtectedNiFiProperties protectedNiFiProperties = new ProtectedNiFiProperties(rawProperties)
        logger.info("Raw properties (${rawProperties.size()}): ${rawProperties.keySet().join(", ")}")

        // Act
        def filteredKeys = protectedNiFiProperties.getPropertyKeys()
        logger.info("Protected properties (${protectedNiFiProperties.size()}): ${filteredKeys.join(", ")}")

        // Assert
        assert protectedNiFiProperties.size() == rawProperties.size() - 1
        assert filteredKeys == rawProperties.keySet() - "key.protected"
    }

    @Test
    void testShouldGetPropertyKeysIncludingProtectionSchemes() {
        // Arrange
        Properties rawProperties = [key: "protectedValue", "key.protected": "scheme", "key2": "value2"] as Properties
        ProtectedNiFiProperties protectedNiFiProperties = new ProtectedNiFiProperties(rawProperties)
        logger.info("Raw properties (${rawProperties.size()}): ${rawProperties.keySet().join(", ")}")

        // Act
        def allKeys = protectedNiFiProperties.getPropertyKeysIncludingProtectionSchemes()
        logger.info("Protected properties with schemes (${allKeys.size()}): ${allKeys.join(", ")}")

        // Assert
        assert allKeys.size() == rawProperties.size()
        assert allKeys == rawProperties.keySet()
    }

    // TODO: Add tests for protectPlainProperties
}
