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
import org.apache.nifi.properties.SensitivePropertyProtectionException
import org.apache.nifi.properties.SensitivePropertyProvider
import org.apache.nifi.properties.scheme.StandardProtectionScheme
import org.apache.nifi.properties.StandardSensitivePropertyProviderFactory
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

import javax.crypto.Cipher

@RunWith(JUnit4.class)
class ProtectedNiFiRegistryPropertiesGroovyTest extends GroovyTestCase {
    private static final String KEYSTORE_PASSWORD_KEY = NiFiRegistryProperties.SECURITY_KEYSTORE_PASSWD
    private static final String KEY_PASSWORD_KEY = NiFiRegistryProperties.SECURITY_KEY_PASSWD
    private static final String TRUSTSTORE_PASSWORD_KEY = NiFiRegistryProperties.SECURITY_TRUSTSTORE_PASSWD
    private static final String OIDC_CLIENT_SECRET = NiFiRegistryProperties.SECURITY_USER_OIDC_CLIENT_SECRET

    private static final def DEFAULT_SENSITIVE_PROPERTIES = [
            KEYSTORE_PASSWORD_KEY,
            KEY_PASSWORD_KEY,
            TRUSTSTORE_PASSWORD_KEY,
            OIDC_CLIENT_SECRET
    ]

    private static final String KEY_HEX_128 = "0123456789ABCDEFFEDCBA9876543210"
    private static final String KEY_HEX_256 = KEY_HEX_128 * 2
    private static final String KEY_HEX = Cipher.getMaxAllowedKeyLength("AES") < 256 ? KEY_HEX_128 : KEY_HEX_256

    private static ProtectedNiFiRegistryProperties loadFromResourceFile(String propertiesFilePath) {
        return loadFromResourceFile(propertiesFilePath, KEY_HEX)
    }

    private static ProtectedNiFiRegistryProperties loadFromResourceFile(String propertiesFilePath, String keyHex) {
        File file = fileForResource(propertiesFilePath)

        if (file == null || !file.exists() || !file.canRead()) {
            throw new IllegalArgumentException("NiFi Registry properties file missing or unreadable")
        }

        FileReader reader = new FileReader(file)

        try {
            final Properties props = new Properties()
            props.load(reader)
            NiFiRegistryProperties properties = new NiFiRegistryProperties(props)

            ProtectedNiFiRegistryProperties protectedNiFiProperties = new ProtectedNiFiRegistryProperties(properties)

            // If it has protected keys, inject the SPP
            if (protectedNiFiProperties.hasProtectedKeys()) {
                protectedNiFiProperties.addSensitivePropertyProvider(StandardSensitivePropertyProviderFactory.withKey(keyHex)
                        .getProvider(new StandardProtectionScheme("aes/gcm")))
            }

            return protectedNiFiProperties
        } catch (final Exception ex) {
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
        final String INSENSITIVE_PROPERTY_KEY = "nifi.registry.web.http.port"
        final String SENSITIVE_PROPERTY_KEY = "nifi.registry.security.keystorePasswd"

        ProtectedNiFiRegistryProperties properties = loadFromResourceFile("/conf/nifi-registry.properties")

        boolean bannerIsSensitive = properties.isPropertySensitive(INSENSITIVE_PROPERTY_KEY)
        boolean passwordIsSensitive = properties.isPropertySensitive(SENSITIVE_PROPERTY_KEY)

        assert !bannerIsSensitive
        assert passwordIsSensitive
    }

    @Test
    void testShouldGetDefaultSensitiveProperties() throws Exception {
        ProtectedNiFiRegistryProperties properties = loadFromResourceFile("/conf/nifi-registry.properties")

        List defaultSensitiveProperties = properties.getSensitivePropertyKeys()

        assert defaultSensitiveProperties.size() == DEFAULT_SENSITIVE_PROPERTIES.size()
        assert defaultSensitiveProperties.containsAll(DEFAULT_SENSITIVE_PROPERTIES)
    }

    @Test
    void testShouldGetAdditionalSensitiveProperties() throws Exception {
        def completeSensitiveProperties = DEFAULT_SENSITIVE_PROPERTIES + ["nifi.registry.web.http.port", "nifi.registry.web.http.host"]
        ProtectedNiFiRegistryProperties properties = loadFromResourceFile("/conf/nifi-registry.with_additional_sensitive_keys.properties")
        List retrievedSensitiveProperties = properties.getSensitivePropertyKeys()

        assert retrievedSensitiveProperties.size() == completeSensitiveProperties.size()
        assert retrievedSensitiveProperties.containsAll(completeSensitiveProperties)
    }

    @Test
    void testGetAdditionalSensitivePropertiesShouldNotIncludeSelf() throws Exception {
        def completeSensitiveProperties = DEFAULT_SENSITIVE_PROPERTIES + ["nifi.registry.web.http.port", "nifi.registry.web.http.host"]

        ProtectedNiFiRegistryProperties properties = loadFromResourceFile("/conf/nifi-registry.with_additional_sensitive_keys.properties")

        List retrievedSensitiveProperties = properties.getSensitivePropertyKeys()

        assert retrievedSensitiveProperties.size() == completeSensitiveProperties.size()
        assert retrievedSensitiveProperties.containsAll(completeSensitiveProperties)
    }

    /**
     * In the default (no protection enabled) scenario, a call to retrieve a sensitive property should return the raw value transparently.
     * @throws Exception
     */
    @Test
    void testShouldGetUnprotectedValueOfSensitiveProperty() throws Exception {
        final String expectedKeystorePassword = "thisIsABadKeystorePassword"

        ProtectedNiFiRegistryProperties properties = loadFromResourceFile("/conf/nifi-registry.with_sensitive_props_unprotected.properties")

        boolean isSensitive = properties.isPropertySensitive(KEYSTORE_PASSWORD_KEY)
        boolean isProtected = properties.isPropertyProtected(KEYSTORE_PASSWORD_KEY)

        String retrievedKeystorePassword = properties.getProperty(KEYSTORE_PASSWORD_KEY)

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
        final String expectedTruststorePassword = ""

        ProtectedNiFiRegistryProperties properties = loadFromResourceFile("/conf/nifi-registry.with_sensitive_props_unprotected.properties")

        boolean isSensitive = properties.isPropertySensitive(TRUSTSTORE_PASSWORD_KEY)
        boolean isProtected = properties.isPropertyProtected(TRUSTSTORE_PASSWORD_KEY)

        NiFiRegistryProperties unprotectedProperties = properties.getUnprotectedProperties()
        String retrievedTruststorePassword = unprotectedProperties.getProperty(TRUSTSTORE_PASSWORD_KEY)

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
        final String expectedKeystorePassword = "thisIsABadPassword"

        ProtectedNiFiRegistryProperties properties = loadFromResourceFile("/conf/nifi-registry.with_sensitive_props_protected_aes_128.properties", KEY_HEX_128)

        boolean isSensitive = properties.isPropertySensitive(KEYSTORE_PASSWORD_KEY)
        boolean isProtected = properties.isPropertyProtected(KEYSTORE_PASSWORD_KEY)

        NiFiRegistryProperties unprotectedProperties = properties.getUnprotectedProperties()
        String retrievedKeystorePassword = unprotectedProperties.getProperty(KEYSTORE_PASSWORD_KEY)

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
        Properties rawProperties = new Properties()
        rawProperties.load(new FileReader(fileForResource("/conf/nifi-registry.with_sensitive_props_protected_unknown.properties")))

        ProtectedNiFiRegistryProperties properties = loadFromResourceFile("/conf/nifi-registry.with_sensitive_props_protected_unknown.properties")

        assert properties.isPropertySensitive(KEYSTORE_PASSWORD_KEY)
        assert properties.isPropertyProtected(KEYSTORE_PASSWORD_KEY)

        shouldFail(SensitivePropertyProtectionException) {
            properties.getUnprotectedProperties()
        }
    }

    /**
     * In the protection enabled scenario, a call to retrieve a sensitive property should handle if the property is unable to be unprotected due to a malformed value.
     * @throws Exception
     */
    @Test
    void testGetValueOfSensitivePropertyShouldHandleSingleMalformedValue() throws Exception {
        ProtectedNiFiRegistryProperties properties = loadFromResourceFile("/conf/nifi-registry.with_sensitive_props_protected_aes_single_malformed.properties", KEY_HEX_128)
        boolean isSensitive = properties.isPropertySensitive(KEYSTORE_PASSWORD_KEY)
        boolean isProtected = properties.isPropertyProtected(KEYSTORE_PASSWORD_KEY)

        shouldFail(SensitivePropertyProtectionException) {
            properties.getUnprotectedProperties()
        }

        assert isSensitive
        assert isProtected
    }

    /**
     * In the protection enabled scenario, a call to retrieve a sensitive property should handle if the property is unable to be unprotected due to a malformed value.
     * @throws Exception
     */
    @Test
    void testGetValueOfSensitivePropertyShouldHandleMultipleMalformedValues() throws Exception {
        ProtectedNiFiRegistryProperties properties =
                loadFromResourceFile("/conf/nifi-registry.with_sensitive_props_protected_aes_multiple_malformed.properties", KEY_HEX_128)

        groovy.test.GroovyAssert.shouldFail(SensitivePropertyProtectionException) {
            properties.getUnprotectedProperties()
        }
    }

    @Test
    void testShouldDetectIfPropertyIsProtected() throws Exception {
        final String unprotectedPropertyKey = TRUSTSTORE_PASSWORD_KEY
        final String protectedPropertyKey = KEYSTORE_PASSWORD_KEY

        ProtectedNiFiRegistryProperties properties =
                loadFromResourceFile("/conf/nifi-registry.with_sensitive_props_protected_aes_128.properties", KEY_HEX_128)

        boolean unprotectedPasswordIsSensitive = properties.isPropertySensitive(unprotectedPropertyKey)
        boolean unprotectedPasswordIsProtected = properties.isPropertyProtected(unprotectedPropertyKey)
        boolean protectedPasswordIsSensitive = properties.isPropertySensitive(protectedPropertyKey)
        boolean protectedPasswordIsProtected = properties.isPropertyProtected(protectedPropertyKey)

        assert unprotectedPasswordIsSensitive
        assert !unprotectedPasswordIsProtected

        assert protectedPasswordIsSensitive
        assert protectedPasswordIsProtected
    }

    @Test
    void testShouldDetectIfPropertyWithEmptyProtectionSchemeIsProtected() throws Exception {
        final String unprotectedPropertyKey = KEY_PASSWORD_KEY
        ProtectedNiFiRegistryProperties properties =
                loadFromResourceFile("/conf/nifi-registry.with_sensitive_props_unprotected_extra_line.properties")

        boolean unprotectedPasswordIsSensitive = properties.isPropertySensitive(unprotectedPropertyKey)
        boolean unprotectedPasswordIsProtected = properties.isPropertyProtected(unprotectedPropertyKey)

        assert unprotectedPasswordIsSensitive
        assert !unprotectedPasswordIsProtected
    }

    @Test
    void testShouldGetPercentageOfSensitivePropertiesProtected_0() throws Exception {
        ProtectedNiFiRegistryProperties properties = loadFromResourceFile("/conf/nifi-registry.properties")
        double percentProtected = getPercentOfSensitivePropertiesProtected(properties)

        assert percentProtected == 0.0D
    }

    private static double getPercentOfSensitivePropertiesProtected(final ProtectedNiFiRegistryProperties properties) {
        return (int) Math.round(properties.getProtectedPropertyKeys().size() / ((double) properties.getPopulatedSensitivePropertyKeys().size()) * 100)
    }

    @Test
    void testShouldGetPercentageOfSensitivePropertiesProtected_75() throws Exception {
        ProtectedNiFiRegistryProperties properties =
                loadFromResourceFile("/conf/nifi-registry.with_sensitive_props_protected_aes_128.properties", KEY_HEX_128)

        double percentProtected = getPercentOfSensitivePropertiesProtected(properties)
        assert percentProtected == 75.0D
    }

    @Test
    void testShouldGetPercentageOfSensitivePropertiesProtected_100() throws Exception {
        ProtectedNiFiRegistryProperties properties =
                loadFromResourceFile("/conf/nifi-registry.with_sensitive_props_fully_protected_aes_128.properties", KEY_HEX_128)

        double percentProtected = getPercentOfSensitivePropertiesProtected(properties)

        assert percentProtected == 100.0D
    }

    @Test
    void testShouldAddSensitivePropertyProvider() throws Exception {
        ProtectedNiFiRegistryProperties properties = new ProtectedNiFiRegistryProperties()

        SensitivePropertyProvider mockProvider =
                [unprotect       : { String input ->
                    input.reverse()
                },
                 getIdentifierKey: { -> "mockProvider" }] as SensitivePropertyProvider

        properties.addSensitivePropertyProvider(mockProvider)
    }

    @Test
    void testShouldNotAllowOverwriteOfProvider() throws Exception {
        // Arrange
        ProtectedNiFiRegistryProperties properties = new ProtectedNiFiRegistryProperties()

        SensitivePropertyProvider mockProvider =
                [unprotect       : { String input ->
                    input.reverse()
                },
                 getIdentifierKey: { -> "mockProvider" }] as SensitivePropertyProvider
        properties.addSensitivePropertyProvider(mockProvider)

        SensitivePropertyProvider mockProvider2 =
                [unprotect       : { String input ->
                    input.reverse()
                },
                 getIdentifierKey: { -> "mockProvider" }] as SensitivePropertyProvider

        shouldFail(UnsupportedOperationException) {
            properties.addSensitivePropertyProvider(mockProvider2)
        }
    }

    @Test
    void testGetUnprotectedPropertiesShouldReturnInternalInstanceWhenNoneProtected() {
        ProtectedNiFiRegistryProperties protectedNiFiProperties = loadFromResourceFile("/conf/nifi-registry.properties")

        int hashCode = protectedNiFiProperties.getApplicationProperties().hashCode()

        NiFiRegistryProperties unprotectedNiFiProperties = protectedNiFiProperties.getUnprotectedProperties()

        assert unprotectedNiFiProperties.size() == protectedNiFiProperties.size()
        assert unprotectedNiFiProperties.getPropertyKeys().every {
            !unprotectedNiFiProperties.getProperty(it).endsWith(ApplicationPropertiesProtector.PROTECTED_KEY_SUFFIX)
        }
        assert unprotectedNiFiProperties.hashCode() == hashCode
    }

    @Test
    void testGetUnprotectedPropertiesShouldDecryptProtectedProperties() {
        ProtectedNiFiRegistryProperties protectedNiFiProperties =
                loadFromResourceFile("/conf/nifi-registry.with_sensitive_props_protected_aes_128.properties", KEY_HEX_128)

        int expectedUnprotectedPropertyCount = protectedNiFiProperties.size()

        int hashCode = protectedNiFiProperties.getApplicationProperties().hashCode()

        NiFiRegistryProperties unprotectedNiFiProperties = protectedNiFiProperties.getUnprotectedProperties()

        assert unprotectedNiFiProperties.size() == expectedUnprotectedPropertyCount
        assert unprotectedNiFiProperties.getPropertyKeys().every {
            !unprotectedNiFiProperties.getProperty(it).endsWith(ApplicationPropertiesProtector.PROTECTED_KEY_SUFFIX)
        }
        assert unprotectedNiFiProperties.hashCode() != hashCode
    }

    @Test
    void testGetUnprotectedPropertiesShouldDecryptProtectedPropertiesWith256Bit() {
        ProtectedNiFiRegistryProperties protectedNiFiProperties =
                loadFromResourceFile("/conf/nifi-registry.with_sensitive_props_protected_aes_256.properties", KEY_HEX_256)

        int expectedUnprotectedPropertyCount = protectedNiFiProperties.size()

        int hashCode = protectedNiFiProperties.getApplicationProperties().hashCode()

        NiFiRegistryProperties unprotectedNiFiProperties = protectedNiFiProperties.getUnprotectedProperties()

        assert unprotectedNiFiProperties.size() == expectedUnprotectedPropertyCount
        assert unprotectedNiFiProperties.getPropertyKeys().every {
            !unprotectedNiFiProperties.getProperty(it).endsWith(ApplicationPropertiesProtector.PROTECTED_KEY_SUFFIX)
        }
        assert unprotectedNiFiProperties.hashCode() != hashCode

        assert unprotectedNiFiProperties.getProperty(OIDC_CLIENT_SECRET) == "thisIsABadOidcSecret"
        assert unprotectedNiFiProperties.getProperty(KEY_PASSWORD_KEY) == "thisIsABadKeyPassword"
        assert unprotectedNiFiProperties.getProperty(KEYSTORE_PASSWORD_KEY) == "thisIsABadKeystorePassword"
    }

    @Test
    void testShouldCalculateSize() {
        Properties props = [key: "protectedValue", "key.protected": "scheme", "key2": "value2"] as Properties
        NiFiRegistryProperties rawProperties = new NiFiRegistryProperties(props)
        ProtectedNiFiRegistryProperties protectedNiFiProperties = new ProtectedNiFiRegistryProperties(rawProperties)

        int protectedSize = protectedNiFiProperties.size()
        assert protectedSize == rawProperties.size() - 1
    }

    @Test
    void testGetPropertyKeysShouldMatchSize() {
        Properties props = [key: "protectedValue", "key.protected": "scheme", "key2": "value2"] as Properties
        NiFiRegistryProperties rawProperties = new NiFiRegistryProperties(props)
        ProtectedNiFiRegistryProperties protectedNiFiProperties = new ProtectedNiFiRegistryProperties(rawProperties)

        def filteredKeys = protectedNiFiProperties.getPropertyKeys()

        assert protectedNiFiProperties.size() == rawProperties.size() - 1
        assert filteredKeys == rawProperties.getPropertyKeys() - "key.protected"
    }

    @Test
    void testShouldGetPropertyKeysIncludingProtectionSchemes() {
        Properties props = [key: "protectedValue", "key.protected": "scheme", "key2": "value2"] as Properties
        NiFiRegistryProperties rawProperties = new NiFiRegistryProperties(props)
        ProtectedNiFiRegistryProperties protectedNiFiProperties = new ProtectedNiFiRegistryProperties(rawProperties)

        def allKeys = protectedNiFiProperties.getPropertyKeysIncludingProtectionSchemes()

        assert allKeys.size() == rawProperties.size()
        assert allKeys == rawProperties.getPropertyKeys()
    }

}
