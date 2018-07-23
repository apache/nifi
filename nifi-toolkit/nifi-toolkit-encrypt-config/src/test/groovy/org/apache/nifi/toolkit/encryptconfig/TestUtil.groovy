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
package org.apache.nifi.toolkit.encryptconfig

import groovy.util.slurpersupport.GPathResult
import org.apache.commons.lang3.SystemUtils
import org.apache.nifi.properties.AESSensitivePropertyProvider
import org.apache.nifi.toolkit.encryptconfig.util.NiFiRegistryAuthorizersXmlEncryptor
import org.apache.nifi.toolkit.encryptconfig.util.NiFiRegistryIdentityProvidersXmlEncryptor

import javax.crypto.Cipher
import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermission

class TestUtil {

    static final String RESOURCE_REGISTRY_BOOTSTRAP_DEFAULT = absolutePathForResource('/nifi-registry/bootstrap_default.conf')
    static final String RESOURCE_REGISTRY_BOOTSTRAP_NO_KEY = absolutePathForResource('/nifi-registry/bootstrap_without_master_key.conf')
    static final String RESOURCE_REGISTRY_BOOTSTRAP_EMPTY_KEY = absolutePathForResource('/nifi-registry/bootstrap_with_empty_master_key.conf')
    static final String RESOURCE_REGISTRY_BOOTSTRAP_KEY_128 = absolutePathForResource('/nifi-registry/bootstrap_with_master_key_128.conf')
    static final String RESOURCE_REGISTRY_BOOTSTRAP_KEY_FROM_PASSWORD_128 = absolutePathForResource('/nifi-registry/bootstrap_with_master_key_from_password_128.conf')

    static final String RESOURCE_REGISTRY_PROPERTIES_COMMENTED = absolutePathForResource('/nifi-registry/nifi-registry-commented.properties')
    static final String RESOURCE_REGISTRY_PROPERTIES_EMPTY = absolutePathForResource('/nifi-registry/nifi-registry-empty.properties')
    static final String RESOURCE_REGISTRY_PROPERTIES_POPULATED_UNPROTECTED = absolutePathForResource('/nifi-registry/nifi-registry-populated-unprotected.properties')
    static final String RESOURCE_REGISTRY_PROPERTIES_POPULATED_PROTECTED_KEY_128 = absolutePathForResource('/nifi-registry/nifi-registry-populated-protected-key-128.properties')
    static final String RESOURCE_REGISTRY_PROPERTIES_POPULATED_PROTECTED_KEY_256 = absolutePathForResource('/nifi-registry/nifi-registry-populated-protected-key-256.properties')
    static final String RESOURCE_REGISTRY_PROPERTIES_POPULATED_PROTECTED_PASSWORD_256 = absolutePathForResource('/nifi-registry/nifi-registry-populated-protected-password-256.properties')

    static final String RESOURCE_REGISTRY_AUTHORIZERS_COMMENTED = absolutePathForResource('/nifi-registry/authorizers-commented.xml')
    static final String RESOURCE_REGISTRY_AUTHORIZERS_EMPTY = absolutePathForResource('/nifi-registry/authorizers-empty.xml')
    static final String RESOURCE_REGISTRY_AUTHORIZERS_POPULATED_UNPROTECTED = absolutePathForResource('/nifi-registry/authorizers-populated-unprotected.xml')

    static final String RESOURCE_REGISTRY_IDENTITY_PROVIDERS_COMMENTED = absolutePathForResource('/nifi-registry/identity-providers-commented.xml')
    static final String RESOURCE_REGISTRY_IDENTITY_PROVIDERS_EMPTY = absolutePathForResource('/nifi-registry/identity-providers-empty.xml')
    static final String RESOURCE_REGISTRY_IDENTITY_PROVIDERS_POPULATED_UNPROTECTED = absolutePathForResource('/nifi-registry/identity-providers-populated-unprotected.xml')

    static final String[] RESOURCE_REGISTRY_PROPERTIES_SENSITIVE_PROPS = [
            "nifi.registry.security.keystorePasswd",
            "nifi.registry.security.keyPasswd",
            "nifi.registry.security.truststorePasswd",
            "nifi.registry.dummy.sensitive.property.1",
            "nifi.registry.dummy.sensitive.property.2"
    ]

    private static final int RESOURCE_REGISTRY_IDENTITY_PROVIDERS_PASSWORD_LINE_COUNT = 3
    private static final int RESOURCE_REGISTRY_AUTHORIZERS_PASSWORD_LINE_COUNT = 3
    private final String PASSWORD_PROP_REGEX = "<property[^>]* name=\".* Password\""

    static final String KEY_HEX_128 = "0123456789ABCDEFFEDCBA9876543210"
    static final String KEY_HEX_256 = KEY_HEX_128 * 2
    static final String KEY_HEX = isUnlimitedStrengthCryptoAvailable() ? KEY_HEX_256 : KEY_HEX_128

    static final String PASSWORD = "thisIsABadPassword"
    // From ToolUtilities.deriveKeyFromPassword("thisIsABadPassword")
    static final String PASSWORD_KEY_HEX_256 = "2C576A9585DB862F5ECBEE5B4FFFCCA14B18D8365968D7081651006507AD2BDE"
    static final String PASSWORD_KEY_HEX_128 = "2C576A9585DB862F5ECBEE5B4FFFCCA1"
    static final String PASSWORD_KEY_HEX = isUnlimitedStrengthCryptoAvailable() ? PASSWORD_KEY_HEX_256 : PASSWORD_KEY_HEX_128

    static final String PROTECTION_SCHEME_128 = "aes/gcm/128"
    static final String PROTECTION_SCHEME_256 = "aes/gcm/256"
    static final String PROTECTION_SCHEME = isUnlimitedStrengthCryptoAvailable() ? PROTECTION_SCHEME_256 : PROTECTION_SCHEME_128

    private static final String DEFAULT_TMP_DIR = "target/tmp/"

    /**
     * @return boolean indicating if the current Java Runtime Environment supports unlimited strength crypto functions
     */
    static boolean isUnlimitedStrengthCryptoAvailable() {
        Cipher.getMaxAllowedKeyLength("AES") > 128
    }

    private static absolutePathForResource(String relativeResourcePath) {
        return TestUtil.class.getResource(relativeResourcePath).getPath()
    }

    static File setupTmpDir(String tmpDirPath = DEFAULT_TMP_DIR) {
        File tmpDir = new File(tmpDirPath)
        tmpDir.mkdirs()
        setFilePermissions(tmpDir, [PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE, PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_WRITE, PosixFilePermission.GROUP_EXECUTE, PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_WRITE, PosixFilePermission.OTHERS_EXECUTE])
        tmpDir
    }

    static void cleanupTmpDir(String tmpDirPath = DEFAULT_TMP_DIR) {
        File tmpDir = new File(tmpDirPath)
        tmpDir.delete()
    }

    static String generateTmpFilePath() {
        File tmpDir = setupTmpDir()
        return "${tmpDir.getAbsolutePath()}/${UUID.randomUUID().toString()}.tmp_file"
    }

    static File generateTmpFile() {
        File tmpFile = new File(generateTmpFilePath())
        tmpFile
    }

    static String copyFileToTempFile(String filePath) {
        File tmpFile = generateTmpFile()
        tmpFile.text = new File(filePath).text
        return tmpFile.getAbsolutePath()
    }

    /**
     * OS-agnostic method for setting file permissions. On POSIX-compliant systems, accurately sets the provided permissions. On Windows, sets the corresponding permissions for the file owner only.
     *
     * @param file the file to modify
     * @param permissions the desired permissions
     */
    static void setFilePermissions(File file, List<PosixFilePermission> permissions = []) {
        if (SystemUtils.IS_OS_WINDOWS) {
            file?.setReadable(permissions.contains(PosixFilePermission.OWNER_READ))
            file?.setWritable(permissions.contains(PosixFilePermission.OWNER_WRITE))
            file?.setExecutable(permissions.contains(PosixFilePermission.OWNER_EXECUTE))
        } else {
            Files.setPosixFilePermissions(file?.toPath(), permissions as Set)
        }
    }

    /**
     * Make assertions that a properties file is protected correctly given a known starting point.
     *
     * @param pathToOriginalUnprotectedProperties - location of the original, plaintext properties file
     * @param pathToProtectedPropertiesToVerify - location of the protected properties file
     * @param sensitivePropertiesToVerify - the properties that should be considered sensitive
     * @param expectedProtectionSchemeToVerify - the expected protection cipher identifier
     * @return true if all assertion checks pass, otherwise assertion error is thrown
     */
    static boolean assertPropertiesAreProtected(
            String pathToOriginalUnprotectedProperties,
            String pathToProtectedPropertiesToVerify,
            String[] sensitivePropertiesToVerify,
            String expectedProtectionScheme = PROTECTION_SCHEME) {

        Properties unprotectedProperties = new Properties()
        unprotectedProperties.load(new FileReader(pathToOriginalUnprotectedProperties))

        String[] populatedSensitiveProperties = sensitivePropertiesToVerify.findAll {
            unprotectedProperties.getProperty(it) != null && unprotectedProperties.getProperty(it).toString().length() > 0
        }
        def populatedSensitivePropertiesCount = populatedSensitiveProperties.length

        Properties protectedProperties = new Properties()
        protectedProperties.load(new FileReader(pathToProtectedPropertiesToVerify))

        // For each populated, sensitive property, one additional "*.protected" property should have been added
        assert unprotectedProperties.size() + populatedSensitivePropertiesCount == protectedProperties.size()

        // For each populated, sensitive property, ensure its value differs from its original value, and
        // that no two protected property values match (due to IV, which is unique per-property)
        Set<String> distinctValues = new HashSet<>()
        populatedSensitiveProperties.every { key ->
            def originalValue = unprotectedProperties.getProperty(key)
            def protectedValue = protectedProperties.getProperty(key)
            def protectionScheme = protectedProperties.getProperty("${key}.protected")

            assert null != protectedValue
            assert protectedValue.length() > 0
            assert originalValue != protectedValue
            assert expectedProtectionScheme == protectionScheme

            assert !distinctValues.contains(protectedValue)
            distinctValues.add(protectedValue)
        }

        return true
    }

    /**
     * Make assertions that a NiFi Registry Authorizers XML file is protected correctly given a known starting point.
     *
     * @param pathToOriginalUnprotectedXml - location of the original, plaintext XML file
     * @param pathToProtectedXmlToVerify - location of the protected XML file
     * @param expectedProtectionScheme - expected scheme/cipher used to encrypt
     * @param expectedKey - key used to encrypt
     *
     * @return true if all assertions pass
     * @throws AssertionError if any assertion fails
     */
    static boolean assertRegistryAuthorizersXmlIsProtected(
            String pathToOriginalUnprotectedXml,
            String pathToProtectedXmlToVerify,
            String expectedProtectionScheme = PROTECTION_SCHEME,
            String expectedKey = KEY_HEX) {

        return assertXmlIsProtected(
                pathToOriginalUnprotectedXml,
                pathToProtectedXmlToVerify,
                expectedProtectionScheme,
                expectedKey,
                { rootNode ->
                    try {
                        rootNode.userGroupProvider.find {
                            it.'class'.text() == NiFiRegistryAuthorizersXmlEncryptor.LDAP_USER_GROUP_PROVIDER_CLASS
                        }.property.findAll {
                            it.@name =~ "Password"
                        }
                    } catch (Exception ignored) {
                        null
                    }

                }
        )
    }

    /**
     * Make assertions that a NiFi Registry Identity Providers XML file is protected correctly given a known starting point.
     *
     * @param pathToOriginalUnprotectedXml - location of the original, plaintext XML file
     * @param pathToProtectedXmlToVerify - location of the protected XML file
     * @param expectedProtectionScheme - expected scheme/cipher used to encrypt
     * @param expectedKey - key used to encrypt
     *
     * @return true if all assertions pass
     * @throws AssertionError if any assertion fails
     */
    static boolean assertRegistryIdentityProvidersXmlIsProtected(
            String pathToOriginalUnprotectedXml,
            String pathToProtectedXmlToVerify,
            String expectedProtectionScheme = PROTECTION_SCHEME,
            String expectedKey = KEY_HEX) {

        return assertXmlIsProtected(
                pathToOriginalUnprotectedXml,
                pathToProtectedXmlToVerify,
                expectedProtectionScheme,
                expectedKey,
                { rootNode ->
                    try {
                        rootNode.provider.find {
                            it.'class'.text() == NiFiRegistryIdentityProvidersXmlEncryptor.LDAP_PROVIDER_CLASS
                        }.property.findAll {
                            it.@name =~ "Password"
                        }
                    } catch (Exception ignored) {
                        null
                    }

                }
        )
    }

    /**
     * Make assertions that an XML file is protected correctly given a known starting point.
     *
     * @param pathToOriginalUnprotectedXml - location of the original, plaintext XML file
     * @param pathToProtectedXmlToVerify - location of the protected XML file
     * @param expectedProtectionScheme - expected scheme/cipher used to encrypt
     * @param expectedKey - key used to encrypt
     * @param callbackToGetNodesToVerify - closure that returns GPathResult[] of all sensitive nodes that
     *                                     should be protected given a GPathResult for the root of the XML document
     *
     * @return true if all assertions pass
     * @throws AssertionError if any assertion fails
     */
    static boolean assertXmlIsProtected(
            String pathToOriginalUnprotectedXml,
            String pathToProtectedXmlToVerify,
            String expectedProtectionScheme = PROTECTION_SCHEME,
            String expectedKey = KEY_HEX,
            callbackToGetNodesToVerify) {

        String originalUnprotectedXml = new File(pathToOriginalUnprotectedXml).text
        String protectedXml = new File(pathToProtectedXmlToVerify).text
        def originalDoc = new XmlParser().parseText(originalUnprotectedXml)
        def protectedDoc = new XmlParser().parseText(protectedXml)

        def sensitiveProperties = callbackToGetNodesToVerify(originalDoc)
        assert sensitiveProperties && sensitiveProperties.size > 0  // necessary as so many key assertions are based on at least one sensitive prop
        def populatedSensitiveProperties = sensitiveProperties.findAll { node ->
            node.text()
        }
        def plaintextValues = populatedSensitiveProperties.collect {
            it.text()
        }

        if (populatedSensitiveProperties.size() == 0) {
            return assertFilesAreEqual(pathToOriginalUnprotectedXml, pathToProtectedXmlToVerify)
        }

        def protectedSensitiveProperties = callbackToGetNodesToVerify(protectedDoc).findAll { node ->
            node.@encryption != "none" && node.@encryption != "" }

        assert populatedSensitiveProperties.size() == protectedSensitiveProperties.size()

        AESSensitivePropertyProvider spp = new AESSensitivePropertyProvider(expectedKey)

        protectedSensitiveProperties.each {
            String value = it.text()
            String propertyValue = value
            assert it.@encryption == expectedProtectionScheme
            assert !plaintextValues.contains(propertyValue)
            assert plaintextValues.contains(spp.unprotect(propertyValue))
        }

        return true
    }

    /**
     * Asserts the contents of files are equal, ignoring blank lines and starting / trailing whitespace
     *
     * @param pathToExpected - path to file with the expected content
     * @param pathToActual - path to file with the actual content
     * @return true if assertions pass
     */
    static boolean assertFilesAreEqual(String pathToExpected, String pathToActual) {
        List<String> expectedLines = new File(pathToExpected).readLines().findAll{
            it.trim().length() > 0
        }.collect{ it.trim() }
        List<String> actualLines = new File(pathToActual).readLines().findAll{
            it.trim().length() > 0
        }.collect{ it.trim() }

        return assertLinesAreEqual(expectedLines, actualLines)
    }

    /**
     * Asserts the contents of a bootstrap.conf file match that of an an expected bootstrap.conf.
     *
     * @param pathToExpectedBootstrap
     * @param pathToActualBootstrap
     * @param includeComments - if false, comment lines in the bootstrap.conf files will be ignored
     * @return true if assertions pass
     */
    static boolean assertBootstrapFilesAreEqual(String pathToExpectedBootstrap, String pathToActualBootstrap, boolean includeComments) {
        return assertConfOrPropertiesFilesAreEqual(pathToExpectedBootstrap, pathToActualBootstrap, includeComments)
    }

    /**
     * Asserts the contents of a properties file match that of an an expected properties file.
     *
     * @param pathToExpectedProperties
     * @param pathToActualProperties
     * @param includeComments - if false, comment lines in the properties files will be ignored
     * @return true if assertions pass
     */
    static boolean assertPropertiesFilesAreEqual(String pathToExpectedProperties, String pathToActualProperties, boolean includeComments) {
        return assertConfOrPropertiesFilesAreEqual(pathToExpectedProperties, pathToActualProperties, includeComments)
    }

    private static boolean assertConfOrPropertiesFilesAreEqual(String expected, String actual, boolean includeComments) {
        List<String> expectedLines = new File(expected).readLines().findAll{
            (it.trim().length() > 0 && (includeComments || !it.startsWith("#")))
        }.collect{ it.trim() }
        List<String> actualLines = new File(actual).readLines().findAll{
            (it.trim().length() > 0 && (includeComments || !it.startsWith("#")))
        }.collect{ it.trim() }

        return assertLinesAreEqual(expectedLines, actualLines)
    }

    private static boolean assertLinesAreEqual(List<String> expectedLines, List<String> actualLines) {

        assert actualLines != null
        assert actualLines.size() == expectedLines.size()
        assert actualLines == expectedLines

        return true
    }

}
