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

import org.apache.commons.lang3.SystemUtils
import org.apache.nifi.util.NiFiBootstrapUtils
import org.apache.nifi.util.NiFiProperties
import org.apache.nifi.util.file.FileUtils
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.junit.After
import org.junit.AfterClass
import org.junit.Assume
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
    static void setUpOnce() throws Exception {
        Assume.assumeTrue("Test only runs on *nix", !SystemUtils.IS_OS_WINDOWS)
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
        // Clear the sensitive property providers between runs
//        if (ProtectedNiFiProperties.@localProviderCache) {
//            ProtectedNiFiProperties.@localProviderCache = [:]
//        }
    }

    @AfterClass
    static void tearDownOnce() {
        if (originalPropertiesPath) {
            System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, originalPropertiesPath)
        }
    }

    @Test
    void testConstructorShouldCreateNewInstance() throws Exception {
        // Arrange

        // Act
        NiFiPropertiesLoader niFiPropertiesLoader = new NiFiPropertiesLoader()

        // Assert
        assert !niFiPropertiesLoader.@instance
        assert !niFiPropertiesLoader.@keyHex
    }

    @Test
    void testShouldCreateInstanceWithKey() throws Exception {
        // Arrange

        // Act
        NiFiPropertiesLoader niFiPropertiesLoader = NiFiPropertiesLoader.withKey(KEY_HEX)

        // Assert
        assert !niFiPropertiesLoader.@instance
        assert niFiPropertiesLoader.@keyHex == KEY_HEX
    }

    @Test
    void testShouldLoadUnprotectedPropertiesFromFile() throws Exception {
        // Arrange
        File unprotectedFile = new File("src/test/resources/conf/nifi.properties")
        NiFiPropertiesLoader niFiPropertiesLoader = new NiFiPropertiesLoader()

        // Act
        NiFiProperties niFiProperties = niFiPropertiesLoader.load(unprotectedFile)

        // Assert
        assert niFiProperties.size() > 0

        // Ensure it is not a ProtectedNiFiProperties
        assert !(niFiProperties instanceof ProtectedNiFiProperties)
    }

    @Test
    void testShouldLoadUnprotectedPropertiesFromFileWithoutBootstrap() throws Exception {
        // Arrange
        File unprotectedFile = new File("src/test/resources/conf/nifi.properties")

        // Set the system property to the test file
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, unprotectedFile.absolutePath)
        logger.info("Set ${NiFiProperties.PROPERTIES_FILE_PATH} to ${unprotectedFile.absolutePath}")

        // Act
        NiFiProperties niFiProperties = NiFiPropertiesLoader.loadDefaultWithKeyFromBootstrap()

        // Assert
        assert niFiProperties.size() > 0

        // Ensure it is not a ProtectedNiFiProperties
        assert !(niFiProperties instanceof ProtectedNiFiProperties)
    }

    @Test
    void testShouldNotLoadProtectedPropertiesFromFileWithoutBootstrap() throws Exception {
        // Arrange
        File protectedFile = new File("src/test/resources/conf/nifi_with_sensitive_properties_protected_aes.properties")

        // Set the system property to the test file
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, protectedFile.absolutePath)
        logger.info("Set ${NiFiProperties.PROPERTIES_FILE_PATH} to ${protectedFile.absolutePath}")

        // Act
        def msg = shouldFail(SensitivePropertyProtectionException) {
            NiFiProperties niFiProperties = NiFiPropertiesLoader.loadDefaultWithKeyFromBootstrap()
        }
        logger.expected(msg)

        // Assert
        assert msg =~ "Could not read root key from bootstrap.conf"
    }

    @Test
    void testShouldLoadUnprotectedPropertiesFromPathWithGeneratedSensitivePropertiesKey() throws Exception {
        // Arrange
        final File propertiesFile = File.createTempFile("nifi.without.key", ".properties")
        propertiesFile.deleteOnExit()
        final OutputStream outputStream = new FileOutputStream(propertiesFile)
        final InputStream inputStream = getClass().getResourceAsStream("/conf/nifi.without.key.properties")
        FileUtils.copy(inputStream, outputStream)

        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, propertiesFile.absolutePath);
        NiFiPropertiesLoader niFiPropertiesLoader = new NiFiPropertiesLoader()

        // Act
        NiFiProperties niFiProperties = niFiPropertiesLoader.get()

        // Assert
        final String sensitivePropertiesKey = niFiProperties.getProperty(NiFiProperties.SENSITIVE_PROPS_KEY)
        assert sensitivePropertiesKey.length() == 32
    }

    @Test
    void testShouldNotLoadUnprotectedPropertiesFromPathWithBlankKeyForClusterNode() throws Exception {
        // Arrange
        final File propertiesFile = File.createTempFile("nifi.without.key", ".properties")
        propertiesFile.deleteOnExit()
        final OutputStream outputStream = new FileOutputStream(propertiesFile)
        final InputStream inputStream = getClass().getResourceAsStream("/conf/nifi.cluster.without.key.properties")
        FileUtils.copy(inputStream, outputStream)

        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, propertiesFile.absolutePath);
        NiFiPropertiesLoader niFiPropertiesLoader = new NiFiPropertiesLoader()

        // Act
        shouldFail(SensitivePropertyProtectionException) {
            niFiPropertiesLoader.get()
        }
    }

    @Test
    void testShouldNotLoadUnprotectedPropertiesFromNullFile() throws Exception {
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
    void testShouldNotLoadUnprotectedPropertiesFromMissingFile() throws Exception {
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
    void testShouldNotLoadUnprotectedPropertiesFromUnreadableFile() throws Exception {
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
    void testShouldLoadUnprotectedPropertiesFromPath() throws Exception {
        // Arrange
        File unprotectedFile = new File("src/test/resources/conf/nifi.properties")
        NiFiPropertiesLoader niFiPropertiesLoader = new NiFiPropertiesLoader()

        // Act
        NiFiProperties niFiProperties = niFiPropertiesLoader.load(unprotectedFile.path)

        // Assert
        assert niFiProperties.size() > 0

        // Ensure it is not a ProtectedNiFiProperties
        assert !(niFiProperties instanceof ProtectedNiFiProperties)
    }

    @Test
    void testShouldLoadUnprotectedPropertiesFromProtectedFile() throws Exception {
        // Arrange
        File protectedFile = new File("src/test/resources/conf/nifi_with_sensitive_properties_protected_aes.properties")

        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, protectedFile.path)
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
        assert !(niFiProperties instanceof ProtectedNiFiProperties)
    }

    @Test
    void testShouldExtractKeyFromBootstrapFile() throws Exception {
        // Arrange
        def defaultNiFiPropertiesFilePath = "src/test/resources/bootstrap_tests/conf/nifi.properties"
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, defaultNiFiPropertiesFilePath)

        // Act
        String key = NiFiBootstrapUtils.extractKeyFromBootstrapFile()

        // Assert
        assert key == KEY_HEX
    }

    @Test
    void testShouldNotExtractKeyFromBootstrapFileWithoutKeyLine() throws Exception {
        // Arrange
        def defaultNiFiPropertiesFilePath = "src/test/resources/bootstrap_tests/missing_key_line/nifi.properties"
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, defaultNiFiPropertiesFilePath)

        // Act
        String key = NiFiBootstrapUtils.extractKeyFromBootstrapFile()

        // Assert
        assert key == ""
    }

    @Test
    void testShouldNotExtractKeyFromBootstrapFileWithoutKey() throws Exception {
        // Arrange
        def defaultNiFiPropertiesFilePath = "src/test/resources/bootstrap_tests/missing_key_line/nifi.properties"
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, defaultNiFiPropertiesFilePath)

        // Act
        String key = NiFiBootstrapUtils.extractKeyFromBootstrapFile()

        // Assert
        assert key == ""
    }

    @Test
    void testShouldNotExtractKeyFromMissingBootstrapFile() throws Exception {
        // Arrange
        def defaultNiFiPropertiesFilePath = "src/test/resources/bootstrap_tests/missing_bootstrap/nifi.properties"
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, defaultNiFiPropertiesFilePath)

        // Act
        def msg = shouldFail(IOException) {
            String key = NiFiBootstrapUtils.extractKeyFromBootstrapFile()
        }
        logger.expected(msg)

        // Assert
        assert msg =~ "Cannot read from .*bootstrap.conf"
    }

    @Test
    void testShouldNotExtractKeyFromUnreadableBootstrapFile() throws Exception {
        // Arrange
        File unreadableFile = new File("src/test/resources/bootstrap_tests/unreadable_bootstrap/bootstrap.conf")
        Set<PosixFilePermission> originalPermissions = Files.getPosixFilePermissions(unreadableFile.toPath())
        Files.setPosixFilePermissions(unreadableFile.toPath(), [] as Set)
        assert !unreadableFile.canRead()

        def defaultNiFiPropertiesFilePath = "src/test/resources/bootstrap_tests/unreadable_bootstrap/nifi.properties"
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, defaultNiFiPropertiesFilePath)

        // Act
        def msg = shouldFail(IOException) {
            String key = NiFiBootstrapUtils.extractKeyFromBootstrapFile()
        }
        logger.expected(msg)

        // Assert
        assert msg =~ "Cannot read from .*bootstrap.conf"

        // Clean up to allow for indexing, etc.
        Files.setPosixFilePermissions(unreadableFile.toPath(), originalPermissions)
    }

    @Ignore("Unreadable conf directory breaks build")
    @Test
    void testShouldNotExtractKeyFromUnreadableConfDir() throws Exception {
        // Arrange
        File unreadableDir = new File("src/test/resources/bootstrap_tests/unreadable_conf")
        Set<PosixFilePermission> originalPermissions = Files.getPosixFilePermissions(unreadableDir.toPath())
        Files.setPosixFilePermissions(unreadableDir.toPath(), [] as Set)
        assert !unreadableDir.canRead()

        def defaultNiFiPropertiesFilePath = "src/test/resources/bootstrap_tests/unreadable_conf/nifi.properties"
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, defaultNiFiPropertiesFilePath)

        // Act
        def msg = shouldFail(IOException) {
            String key = NiFiBootstrapUtils.extractKeyFromBootstrapFile()
        }
        logger.expected(msg)

        // Assert
        assert msg == "Cannot read from bootstrap.conf"

        // Clean up to allow for indexing, etc.
        Files.setPosixFilePermissions(unreadableDir.toPath(), originalPermissions)
    }

    @Test
    void testShouldLoadUnprotectedPropertiesFromProtectedDefaultFileAndUseBootstrapKey() throws Exception {
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
    void testShouldUpdateKeyInFactory() throws Exception {
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
