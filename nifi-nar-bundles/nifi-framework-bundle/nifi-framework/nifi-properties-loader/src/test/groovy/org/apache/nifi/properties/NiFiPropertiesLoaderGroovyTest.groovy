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
import org.junit.Ignore
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.crypto.Cipher
import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermission
import java.security.Security

@RunWith(JUnit4.class)
class NiFiPropertiesLoaderGroovyTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(NiFiPropertiesLoaderGroovyTest.class)

    final def DEFAULT_SENSITIVE_PROPERTIES = [
            "nifi.sensitive.props.key",
            "nifi.security.keystorePasswd",
            "nifi.security.keyPasswd",
            "nifi.security.truststorePasswd"
    ]

    final def COMMON_ADDITIONAL_SENSITIVE_PROPERTIES = [
            "nifi.sensitive.props.algorithm",
            "nifi.kerberos.service.principal",
            "nifi.kerberos.krb5.file",
            "nifi.kerberos.keytab.location"
    ]

    private static final String KEY_HEX_128 = "0123456789ABCDEFFEDCBA9876543210"
    private static final String KEY_HEX_256 = KEY_HEX_128 * 2
    public static final String KEY_HEX = isUnlimitedStrengthCryptoAvailable() ? KEY_HEX_256 : KEY_HEX_128

    private static final String PASSWORD_KEY_HEX_128 = "2C576A9585DB862F5ECBEE5B4FFFCCA1"

    private static String originalPropertiesPath = System.getProperty(NiFiProperties.PROPERTIES_FILE_PATH)
    private
    final Set<PosixFilePermission> ownerReadWrite = [PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_READ]

    private static boolean isUnlimitedStrengthCryptoAvailable() {
        Cipher.getMaxAllowedKeyLength("AES") > 128
    }

    @BeforeClass
    public static void setUpOnce() throws Exception {
        Security.addProvider(new BouncyCastleProvider())

        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
        // Clear the sensitive property providers between runs
//        if (ProtectedNiFiProperties.@localProviderCache) {
//            ProtectedNiFiProperties.@localProviderCache = [:]
//        }
        NiFiPropertiesLoader.@sensitivePropertyProviderFactory = null
    }

    @AfterClass
    public static void tearDownOnce() {
        if (originalPropertiesPath) {
            System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, originalPropertiesPath)
        }
    }

    @Test
    public void testConstructorShouldCreateNewInstance() throws Exception {
        // Arrange

        // Act
        NiFiPropertiesLoader niFiPropertiesLoader = new NiFiPropertiesLoader()

        // Assert
        assert !niFiPropertiesLoader.@instance
        assert !niFiPropertiesLoader.@keyHex
    }

    @Test
    public void testShouldCreateInstanceWithKey() throws Exception {
        // Arrange

        // Act
        NiFiPropertiesLoader niFiPropertiesLoader = NiFiPropertiesLoader.withKey(KEY_HEX)

        // Assert
        assert !niFiPropertiesLoader.@instance
        assert niFiPropertiesLoader.@keyHex == KEY_HEX
    }

    @Test
    public void testShouldGetDefaultProviderKey() throws Exception {
        // Arrange
        final String EXPECTED_PROVIDER_KEY = "aes/gcm/${Cipher.getMaxAllowedKeyLength("AES") > 128 ? 256 : 128}"
        logger.info("Expected provider key: ${EXPECTED_PROVIDER_KEY}")

        // Act
        String defaultKey = NiFiPropertiesLoader.getDefaultProviderKey()
        logger.info("Default key: ${defaultKey}")
        // Assert
        assert defaultKey == EXPECTED_PROVIDER_KEY
    }

    @Test
    public void testShouldInitializeSensitivePropertyProviderFactory() throws Exception {
        // Arrange
        NiFiPropertiesLoader niFiPropertiesLoader = new NiFiPropertiesLoader()

        // Act
        niFiPropertiesLoader.initializeSensitivePropertyProviderFactory()

        // Assert
        assert niFiPropertiesLoader.@sensitivePropertyProviderFactory
    }

    @Test
    public void testShouldLoadUnprotectedPropertiesFromFile() throws Exception {
        // Arrange
        File unprotectedFile = new File("src/test/resources/conf/nifi.properties")
        NiFiPropertiesLoader niFiPropertiesLoader = new NiFiPropertiesLoader()

        // Act
        NiFiProperties niFiProperties = niFiPropertiesLoader.load(unprotectedFile)

        // Assert
        assert niFiProperties.size() > 0

        // Ensure it is not a ProtectedNiFiProperties
        assert niFiProperties instanceof StandardNiFiProperties
    }

    @Test
    public void testShouldNotLoadUnprotectedPropertiesFromNullFile() throws Exception {
        // Arrange
        NiFiPropertiesLoader niFiPropertiesLoader = new NiFiPropertiesLoader()

        // Act
        def msg = shouldFail(IllegalArgumentException) {
            NiFiProperties niFiProperties = niFiPropertiesLoader.load(null as File)
        }
        logger.expected(msg)

        // Assert
        assert msg == "NiFi properties file missing or unreadable"
    }

    @Test
    public void testShouldNotLoadUnprotectedPropertiesFromMissingFile() throws Exception {
        // Arrange
        File missingFile = new File("src/test/resources/conf/nifi_missing.properties")
        assert !missingFile.exists()

        NiFiPropertiesLoader niFiPropertiesLoader = new NiFiPropertiesLoader()

        // Act
        def msg = shouldFail(IllegalArgumentException) {
            NiFiProperties niFiProperties = niFiPropertiesLoader.load(missingFile)
        }
        logger.expected(msg)

        // Assert
        assert msg == "NiFi properties file missing or unreadable"
    }

    @Test
    public void testShouldNotLoadUnprotectedPropertiesFromUnreadableFile() throws Exception {
        // Arrange
        File unreadableFile = new File("src/test/resources/conf/nifi_no_permissions.properties")
        Files.setPosixFilePermissions(unreadableFile.toPath(), [] as Set)
        assert !unreadableFile.canRead()

        NiFiPropertiesLoader niFiPropertiesLoader = new NiFiPropertiesLoader()

        // Act
        def msg = shouldFail(IllegalArgumentException) {
            NiFiProperties niFiProperties = niFiPropertiesLoader.load(unreadableFile)
        }
        logger.expected(msg)

        // Assert
        assert msg == "NiFi properties file missing or unreadable"

        // Clean up to allow for indexing, etc.
        Files.setPosixFilePermissions(unreadableFile.toPath(), ownerReadWrite)
    }

    @Test
    public void testShouldLoadUnprotectedPropertiesFromPath() throws Exception {
        // Arrange
        File unprotectedFile = new File("src/test/resources/conf/nifi.properties")
        NiFiPropertiesLoader niFiPropertiesLoader = new NiFiPropertiesLoader()

        // Act
        NiFiProperties niFiProperties = niFiPropertiesLoader.load(unprotectedFile.path)

        // Assert
        assert niFiProperties.size() > 0

        // Ensure it is not a ProtectedNiFiProperties
        assert niFiProperties instanceof StandardNiFiProperties
    }

    @Test
    public void testShouldLoadUnprotectedPropertiesFromProtectedFile() throws Exception {
        // Arrange
        File protectedFile = new File("src/test/resources/conf/nifi_with_sensitive_properties_protected_aes.properties")
        NiFiPropertiesLoader niFiPropertiesLoader = NiFiPropertiesLoader.withKey(KEY_HEX)

        final def EXPECTED_PLAIN_VALUES = [
                (NiFiProperties.SENSITIVE_PROPS_KEY): "thisIsABadSensitiveKeyPassword",
                (NiFiProperties.SECURITY_KEYSTORE_PASSWD): "thisIsABadKeystorePassword",
                (NiFiProperties.SECURITY_KEY_PASSWD): "thisIsABadKeyPassword",
        ]

        // This method is covered in tests above, so safe to use here to retrieve protected properties
        ProtectedNiFiProperties protectedNiFiProperties = niFiPropertiesLoader.readProtectedPropertiesFromDisk(protectedFile)
        int totalKeysCount = protectedNiFiProperties.getPropertyKeysIncludingProtectionSchemes().size()
        int protectedKeysCount = protectedNiFiProperties.getProtectedPropertyKeys().size()
        logger.info("Read ${totalKeysCount} total properties (${protectedKeysCount} protected) from ${protectedFile.canonicalPath}")

        // Act
        NiFiProperties niFiProperties = niFiPropertiesLoader.load(protectedFile)

        // Assert
        assert niFiProperties.size() == totalKeysCount - protectedKeysCount

        // Ensure that any key marked as protected above is different in this instance
        protectedNiFiProperties.getProtectedPropertyKeys().keySet().each { String key ->
            String plainValue = niFiProperties.getProperty(key)
            String protectedValue = protectedNiFiProperties.getProperty(key)

            logger.info("Checking that [${protectedValue}] -> [${plainValue}] == [${EXPECTED_PLAIN_VALUES[key]}]")

            assert plainValue == EXPECTED_PLAIN_VALUES[key]
            assert plainValue != protectedValue
            assert plainValue.length() <= protectedValue.length()
        }

        // Ensure it is not a ProtectedNiFiProperties
        assert niFiProperties instanceof StandardNiFiProperties
    }

    @Test
    public void testShouldExtractKeyFromBootstrapFile() throws Exception {
        // Arrange
        def defaultNiFiPropertiesFilePath = "src/test/resources/bootstrap_tests/conf/nifi.properties"
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, defaultNiFiPropertiesFilePath)

        // Act
        String key = NiFiPropertiesLoader.extractKeyFromBootstrapFile()

        // Assert
        assert key == KEY_HEX
    }

    @Test
    public void testShouldNotExtractKeyFromBootstrapFileWithoutKeyLine() throws Exception {
        // Arrange
        def defaultNiFiPropertiesFilePath = "src/test/resources/bootstrap_tests/missing_key_line/nifi.properties"
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, defaultNiFiPropertiesFilePath)

        // Act
        String key = NiFiPropertiesLoader.extractKeyFromBootstrapFile()

        // Assert
        assert key == ""
    }

    @Test
    public void testShouldNotExtractKeyFromBootstrapFileWithoutKey() throws Exception {
        // Arrange
        def defaultNiFiPropertiesFilePath = "src/test/resources/bootstrap_tests/missing_key_line/nifi.properties"
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, defaultNiFiPropertiesFilePath)

        // Act
        String key = NiFiPropertiesLoader.extractKeyFromBootstrapFile()

        // Assert
        assert key == ""
    }

    @Test
    public void testShouldNotExtractKeyFromMissingBootstrapFile() throws Exception {
        // Arrange
        def defaultNiFiPropertiesFilePath = "src/test/resources/bootstrap_tests/missing_bootstrap/nifi.properties"
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, defaultNiFiPropertiesFilePath)

        // Act
        def msg = shouldFail(IOException) {
            String key = NiFiPropertiesLoader.extractKeyFromBootstrapFile()
        }
        logger.expected(msg)

        // Assert
        assert msg == "Cannot read from bootstrap.conf"
    }

    @Test
    public void testShouldNotExtractKeyFromUnreadableBootstrapFile() throws Exception {
        // Arrange
        File unreadableFile = new File("src/test/resources/bootstrap_tests/unreadable_bootstrap/bootstrap.conf")
        Set<PosixFilePermission> originalPermissions = Files.getPosixFilePermissions(unreadableFile.toPath())
        Files.setPosixFilePermissions(unreadableFile.toPath(), [] as Set)
        assert !unreadableFile.canRead()

        def defaultNiFiPropertiesFilePath = "src/test/resources/bootstrap_tests/unreadable_bootstrap/nifi.properties"
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, defaultNiFiPropertiesFilePath)

        // Act
        def msg = shouldFail(IOException) {
            String key = NiFiPropertiesLoader.extractKeyFromBootstrapFile()
        }
        logger.expected(msg)

        // Assert
        assert msg == "Cannot read from bootstrap.conf"

        // Clean up to allow for indexing, etc.
        Files.setPosixFilePermissions(unreadableFile.toPath(), originalPermissions)
    }

    @Ignore("Unreadable conf directory breaks build")
    @Test
    public void testShouldNotExtractKeyFromUnreadableConfDir() throws Exception {
        // Arrange
        File unreadableDir = new File("src/test/resources/bootstrap_tests/unreadable_conf")
        Set<PosixFilePermission> originalPermissions = Files.getPosixFilePermissions(unreadableDir.toPath())
        Files.setPosixFilePermissions(unreadableDir.toPath(), [] as Set)
        assert !unreadableDir.canRead()

        def defaultNiFiPropertiesFilePath = "src/test/resources/bootstrap_tests/unreadable_conf/nifi.properties"
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, defaultNiFiPropertiesFilePath)

        // Act
        def msg = shouldFail(IOException) {
            String key = NiFiPropertiesLoader.extractKeyFromBootstrapFile()
        }
        logger.expected(msg)

        // Assert
        assert msg == "Cannot read from bootstrap.conf"

        // Clean up to allow for indexing, etc.
        Files.setPosixFilePermissions(unreadableDir.toPath(), originalPermissions)
    }

    @Test
    public void testShouldLoadUnprotectedPropertiesFromProtectedDefaultFileAndUseBootstrapKey() throws Exception {
        // Arrange
        File protectedFile = new File("src/test/resources/bootstrap_tests/conf/nifi_with_sensitive_properties_protected_aes.properties")
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, protectedFile.path)
        NiFiPropertiesLoader niFiPropertiesLoader = NiFiPropertiesLoader.withKey(KEY_HEX)

        NiFiProperties normalReadProperties = niFiPropertiesLoader.load(protectedFile)
        logger.info("Read ${normalReadProperties.size()} total properties from ${protectedFile.canonicalPath}")

        // Act
        NiFiProperties niFiProperties = NiFiPropertiesLoader.loadDefaultWithKeyFromBootstrap()

        // Assert
        assert niFiProperties.size() == normalReadProperties.size()


        def readPropertiesAndValues = niFiProperties.getPropertyKeys().collectEntries {
            [(it): niFiProperties.getProperty(it)]
        }
        def expectedPropertiesAndValues = normalReadProperties.getPropertyKeys().collectEntries {
            [(it): normalReadProperties.getProperty(it)]
        }
        assert readPropertiesAndValues == expectedPropertiesAndValues
    }

    @Test
    public void testShouldUpdateKeyInFactory() throws Exception {
        // Arrange
        File originalKeyFile = new File("src/test/resources/conf/nifi_with_sensitive_properties_protected_aes_128.properties")
        File passwordKeyFile = new File("src/test/resources/conf/nifi_with_sensitive_properties_protected_aes_128_password.properties")
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, originalKeyFile.path)
        NiFiPropertiesLoader niFiPropertiesLoader = NiFiPropertiesLoader.withKey(KEY_HEX_128)

        NiFiProperties niFiProperties = niFiPropertiesLoader.load(originalKeyFile)
        logger.info("Read ${niFiProperties.size()} total properties from ${originalKeyFile.canonicalPath}")

        // Act
        NiFiPropertiesLoader passwordNiFiPropertiesLoader = NiFiPropertiesLoader.withKey(PASSWORD_KEY_HEX_128)

        NiFiProperties passwordProperties = passwordNiFiPropertiesLoader.load(passwordKeyFile)
        logger.info("Read ${passwordProperties.size()} total properties from ${passwordKeyFile.canonicalPath}")

        // Assert
        assert niFiProperties.size() == passwordProperties.size()


        def readPropertiesAndValues = niFiProperties.getPropertyKeys().collectEntries {
            [(it): niFiProperties.getProperty(it)]
        }
        def readPasswordPropertiesAndValues = passwordProperties.getPropertyKeys().collectEntries {
            [(it): passwordProperties.getProperty(it)]
        }

        assert readPropertiesAndValues == readPasswordPropertiesAndValues
    }
}
