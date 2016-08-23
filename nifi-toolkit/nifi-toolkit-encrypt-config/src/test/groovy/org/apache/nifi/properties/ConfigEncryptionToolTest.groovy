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

import ch.qos.logback.classic.spi.LoggingEvent
import ch.qos.logback.core.AppenderBase
import org.apache.commons.codec.binary.Hex
import org.apache.nifi.toolkit.tls.commandLine.CommandLineParseException
import org.apache.nifi.util.NiFiProperties
import org.apache.nifi.util.console.TextDevice
import org.apache.nifi.util.console.TextDevices
import org.bouncycastle.crypto.generators.SCrypt
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.Test
import org.junit.contrib.java.lang.system.Assertion
import org.junit.contrib.java.lang.system.ExpectedSystemExit
import org.junit.contrib.java.lang.system.SystemOutRule
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.crypto.Cipher
import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermission
import java.security.KeyException
import java.security.Security

@RunWith(JUnit4.class)
class ConfigEncryptionToolTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(ConfigEncryptionToolTest.class)

    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none()

    @Rule
    public final SystemOutRule systemOutRule = new SystemOutRule().enableLog()

    private static final String KEY_HEX_128 = "0123456789ABCDEFFEDCBA9876543210"
    private static final String KEY_HEX_256 = KEY_HEX_128 * 2
    public static final String KEY_HEX = isUnlimitedStrengthCryptoAvailable() ? KEY_HEX_256 : KEY_HEX_128
    private static final String PASSWORD = "thisIsABadPassword"

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
        TestAppender.reset()
    }

    private static boolean isUnlimitedStrengthCryptoAvailable() {
        Cipher.getMaxAllowedKeyLength("AES") > 128
    }

    private static void printProperties(NiFiProperties properties) {
        if (!(properties instanceof ProtectedNiFiProperties)) {
            properties = new ProtectedNiFiProperties(properties)
        }

        (properties as ProtectedNiFiProperties).getPropertyKeysIncludingProtectionSchemes().sort().each { String key ->
            logger.info("${key}\t\t${properties.getProperty(key)}")
        }
    }

    @Test
    void testShouldPrintHelpMessage() {
        // Arrange
        def flags = ["-h", "--help"]
        ConfigEncryptionTool tool = new ConfigEncryptionTool()

        // Act
        flags.each { String arg ->
            def msg = shouldFail(CommandLineParseException) {
                tool.parse([arg] as String[])
            }

            // Assert
            assert msg == null
            assert systemOutRule.getLog().contains("usage: org.apache.nifi.properties.ConfigEncryptionTool [")
        }
    }

    @Test
    void testShouldParseBootstrapConfArgument() {
        // Arrange
        def flags = ["-b", "--bootstrapConf"]
        String bootstrapPath = "src/test/resources/bootstrap.conf"
        ConfigEncryptionTool tool = new ConfigEncryptionTool()

        // Act
        flags.each { String arg ->
            tool.parse([arg, bootstrapPath] as String[])
            logger.info("Parsed bootstrap.conf location: ${tool.bootstrapConfPath}")

            // Assert
            assert tool.bootstrapConfPath == bootstrapPath
        }
    }

    @Test
    void testParseShouldPopulateDefaultBootstrapConfArgument() {
        // Arrange
        String bootstrapPath = "conf/bootstrap.conf"
        ConfigEncryptionTool tool = new ConfigEncryptionTool()

        // Act
        tool.parse([] as String[])
        logger.info("Parsed bootstrap.conf location: ${tool.bootstrapConfPath}")

        // Assert
        assert new File(tool.bootstrapConfPath).getPath() == new File(bootstrapPath).getPath()
    }

    @Test
    void testShouldParseNiFiPropertiesArgument() {
        // Arrange
        def flags = ["-n", "--niFiProperties"]
        String niFiPropertiesPath = "src/test/resources/nifi.properties"
        ConfigEncryptionTool tool = new ConfigEncryptionTool()

        // Act
        flags.each { String arg ->
            tool.parse([arg, niFiPropertiesPath] as String[])
            logger.info("Parsed nifi.properties location: ${tool.niFiPropertiesPath}")

            // Assert
            assert tool.niFiPropertiesPath == niFiPropertiesPath
        }
    }

    @Test
    void testParseShouldPopulateDefaultNiFiPropertiesArgument() {
        // Arrange
        String niFiPropertiesPath = "conf/nifi.properties"
        ConfigEncryptionTool tool = new ConfigEncryptionTool()

        // Act
        tool.parse([] as String[])
        logger.info("Parsed nifi.properties location: ${tool.niFiPropertiesPath}")

        // Assert
        assert new File(tool.niFiPropertiesPath).getPath() == new File(niFiPropertiesPath).getPath()
    }

    @Test
    void testShouldParseOutputNiFiPropertiesArgument() {
        // Arrange
        def flags = ["-o", "--outputNiFiProperties"]
        String niFiPropertiesPath = "src/test/resources/nifi.properties"
        ConfigEncryptionTool tool = new ConfigEncryptionTool()

        // Act
        flags.each { String arg ->
            tool.parse([arg, niFiPropertiesPath] as String[])
            logger.info("Parsed output nifi.properties location: ${tool.outputNiFiPropertiesPath}")

            // Assert
            assert tool.outputNiFiPropertiesPath == niFiPropertiesPath
        }
    }

    @Test
    void testParseShouldPopulateDefaultOutputNiFiPropertiesArgument() {
        // Arrange
        String niFiPropertiesPath = "conf/nifi.properties"
        ConfigEncryptionTool tool = new ConfigEncryptionTool()

        // Act
        tool.parse([] as String[])
        logger.info("Parsed output nifi.properties location: ${tool.outputNiFiPropertiesPath}")

        // Assert
        assert new File(tool.outputNiFiPropertiesPath).getPath() == new File(niFiPropertiesPath).getPath()
    }

    @Test
    void testParseShouldWarnIfNiFiPropertiesWillBeOverwritten() {
        // Arrange
        String niFiPropertiesPath = "conf/nifi.properties"
        ConfigEncryptionTool tool = new ConfigEncryptionTool()

        // Act
        tool.parse("-n ${niFiPropertiesPath} -o ${niFiPropertiesPath}".split(" ") as String[])
        logger.info("Parsed nifi.properties location: ${tool.niFiPropertiesPath}")
        logger.info("Parsed output nifi.properties location: ${tool.outputNiFiPropertiesPath}")

        // Assert
        assert !TestAppender.events.isEmpty()
        assert TestAppender.events.first().message =~ "The source nifi.properties and destination nifi.properties are identical \\[.*\\] so the original will be overwritten"
    }

    @Test
    void testShouldParseKeyArgument() {
        // Arrange
        def flags = ["-k", "--key"]
        ConfigEncryptionTool tool = new ConfigEncryptionTool()

        // Act
        flags.each { String arg ->
            tool.parse([arg, KEY_HEX] as String[])
            logger.info("Parsed key: ${tool.keyHex}")

            // Assert
            assert tool.keyHex == KEY_HEX
        }
    }

    @Test
    void testShouldLoadNiFiProperties() {
        // Arrange
        String niFiPropertiesPath = "src/test/resources/nifi_with_sensitive_properties_unprotected.properties"
        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        String[] args = ["-n", niFiPropertiesPath] as String[]

        String oldFilePath = System.getProperty(NiFiProperties.PROPERTIES_FILE_PATH)

        tool.parse(args)
        logger.info("Parsed nifi.properties location: ${tool.niFiPropertiesPath}")

        // Act
        NiFiProperties properties = tool.loadNiFiProperties()
        logger.info("Loaded NiFiProperties from ${tool.niFiPropertiesPath}")

        // Assert
        assert properties
        assert properties.size() > 0

        // The system variable was reset to the original value
        assert System.getProperty(NiFiProperties.PROPERTIES_FILE_PATH) == oldFilePath
    }

    @Test
    void testShouldReadKeyFromConsole() {
        // Arrange
        List<String> keyValues = [
                "0123 4567",
                KEY_HEX,
                "   ${KEY_HEX}   ",
                "non-hex-chars",
        ]

        // Act
        keyValues.each { String key ->
            TextDevice mockConsoleDevice = TextDevices.streamDevice(new ByteArrayInputStream(key.bytes), new ByteArrayOutputStream())
            String readKey = ConfigEncryptionTool.readKeyFromConsole(mockConsoleDevice)
            logger.info("Read key: [${readKey}]")

            // Assert
            assert readKey == key
        }
    }

    @Test
    void testShouldReadPasswordFromConsole() {
        // Arrange
        List<String> passwords = [
                "0123 4567",
                PASSWORD,
                "   ${PASSWORD}   ",
                "non-hex-chars",
        ]

        // Act
        passwords.each { String pw ->
            logger.info("Using password: [${PASSWORD}]")
            TextDevice mockConsoleDevice = TextDevices.streamDevice(new ByteArrayInputStream(pw.bytes), new ByteArrayOutputStream())
            String readPassword = ConfigEncryptionTool.readPasswordFromConsole(mockConsoleDevice)
            logger.info("Read password: [${readPassword}]")

            // Assert
            assert readPassword == pw
        }
    }

    @Test
    void testShouldReadPasswordFromConsoleIfNoKeyPresent() {
        // Arrange
        def args = [] as String[]
        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        tool.parse(args)
        logger.info("Using password flag: ${tool.usingPassword}")
        logger.info("Password: ${tool.password}")
        logger.info("Key hex:  ${tool.keyHex}")

        assert tool.usingPassword
        assert !tool.password
        assert !tool.keyHex

        TextDevice mockConsoleDevice = TextDevices.streamDevice(new ByteArrayInputStream(PASSWORD.bytes), new ByteArrayOutputStream())

        // Mocked for deterministic output and performance in test -- SCrypt is not under test here
        SCrypt.metaClass.'static'.generate = { byte[] pw, byte[] s, int N, int r, int p, int dkLen ->
            logger.mock("Mock SCrypt.generate(${Hex.encodeHexString(pw)}, ${Hex.encodeHexString(s)}, ${N}, ${r}, ${p}, ${dkLen})")
            logger.mock("Returning ${KEY_HEX[0..<dkLen * 2]}")
            Hex.decodeHex(KEY_HEX[0..<dkLen * 2] as char[])
        }

        // Act
        String readKey = tool.getKey(mockConsoleDevice)
        logger.info("Read key: [${readKey}]")

        // Assert
        assert readKey == KEY_HEX
        assert tool.keyHex == readKey
        assert !tool.password
        assert !tool.usingPassword

        SCrypt.metaClass.'static' = null
    }

    @Test
    void testShouldReadKeyFromConsoleIfFlagProvided() {
        // Arrange
        def args = ["-r"] as String[]
        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        tool.parse(args)
        logger.info("Using password flag: ${tool.usingPassword}")
        logger.info("Password: ${tool.password}")
        logger.info("Key hex:  ${tool.keyHex}")

        assert !tool.usingPassword
        assert !tool.password
        assert !tool.keyHex

        TextDevice mockConsoleDevice = TextDevices.streamDevice(new ByteArrayInputStream(KEY_HEX.bytes), new ByteArrayOutputStream())

        // Act
        String readKey = tool.getKey(mockConsoleDevice)
        logger.info("Read key: [${readKey}]")

        // Assert
        assert readKey == KEY_HEX
        assert tool.keyHex == readKey
        assert !tool.password
        assert !tool.usingPassword
    }

    @Test
    void testShouldIgnoreRawKeyFlagIfKeyProvided() {
        // Arrange
        def args = ["-r", "-k", KEY_HEX] as String[]
        ConfigEncryptionTool tool = new ConfigEncryptionTool()

        // Act
        tool.parse(args)
        logger.info("Using password flag: ${tool.usingPassword}")
        logger.info("Password: ${tool.password}")
        logger.info("Key hex:  ${tool.keyHex}")

        // Assert
        assert !tool.usingPassword
        assert !tool.password
        assert tool.keyHex == KEY_HEX

        assert !TestAppender.events.isEmpty()
        assert TestAppender.events.collect {
            it.message
        }.contains("If the key or password is provided in the arguments, '-r'/'--useRawKey' is ignored")
    }

    @Test
    void testShouldIgnoreRawKeyFlagIfPasswordProvided() {
        // Arrange
        def args = ["-r", "-p", PASSWORD] as String[]
        ConfigEncryptionTool tool = new ConfigEncryptionTool()

        // Act
        tool.parse(args)
        logger.info("Using password flag: ${tool.usingPassword}")
        logger.info("Password: ${tool.password}")
        logger.info("Key hex:  ${tool.keyHex}")

        // Assert
        assert tool.usingPassword
        assert tool.password == PASSWORD
        assert !tool.keyHex

        assert !TestAppender.events.isEmpty()
        assert TestAppender.events.collect {
            it.message
        }.contains("If the key or password is provided in the arguments, '-r'/'--useRawKey' is ignored")
    }

    @Test
    void testShouldParseKey() {
        // Arrange
        Map<String, String> keyValues = [
                (KEY_HEX)                         : KEY_HEX,
                "   ${KEY_HEX}   "                : KEY_HEX,
                "xxx${KEY_HEX}zzz"                : KEY_HEX,
                ((["0123", "4567"] * 4).join("-")): "01234567" * 4,
                ((["89ab", "cdef"] * 4).join(" ")): "89ABCDEF" * 4,
                (KEY_HEX.toLowerCase())           : KEY_HEX,
                (KEY_HEX[0..<32])                 : KEY_HEX[0..<32],
        ]

        if (isUnlimitedStrengthCryptoAvailable()) {
            keyValues.put(KEY_HEX[0..<48], KEY_HEX[0..<48])
        }

        // Act
        keyValues.each { String key, final String EXPECTED_KEY ->
            logger.info("Reading key: [${key}]")
            String parsedKey = ConfigEncryptionTool.parseKey(key)
            logger.info("Parsed key:  [${parsedKey}]")

            // Assert
            assert parsedKey == EXPECTED_KEY
        }
    }

    @Test
    void testParseKeyShouldThrowExceptionForInvalidKeys() {
        // Arrange
        List<String> keyValues = [
                "0123 4567",
                "non-hex-chars",
                KEY_HEX[0..<-1],
                "&ITD SF^FI&&%SDIF"
        ]

        def validKeyLengths = ConfigEncryptionTool.getValidKeyLengths()
        def bitLengths = validKeyLengths.collect { it / 4 }
        String secondHalf = /\[${validKeyLengths.join(", ")}\] bits / +
                /\(\[${bitLengths.join(", ")}\]/ + / hex characters\)/.toString()

        // Act
        keyValues.each { String key ->
            logger.info("Reading key: [${key}]")
            def msg = shouldFail(KeyException) {
                String parsedKey = ConfigEncryptionTool.parseKey(key)
                logger.info("Parsed key:  [${parsedKey}]")
            }
            logger.expected(msg)
            int trimmedKeySize = key.replaceAll("[^0-9a-fA-F]", "").size()

            // Assert
            assert msg =~ "The key \\(${trimmedKeySize} hex chars\\) must be of length ${secondHalf}"
        }
    }

    @Test
    void testShouldDeriveKeyFromPassword() {
        // Arrange

        // Mocked for deterministic output and performance in test -- SCrypt is not under test here
        SCrypt.metaClass.'static'.generate = { byte[] pw, byte[] s, int N, int r, int p, int dkLen ->
            logger.mock("Mock SCrypt.generate(${Hex.encodeHexString(pw)}, ${Hex.encodeHexString(s)}, ${N}, ${r}, ${p}, ${dkLen})")
            logger.mock("Returning ${KEY_HEX[0..<dkLen * 2]}")
            Hex.decodeHex(KEY_HEX[0..<dkLen * 2] as char[])
        }

        logger.info("Using password: [${PASSWORD}]")

        // Act
        String derivedKey = ConfigEncryptionTool.deriveKeyFromPassword(PASSWORD)
        logger.info("Derived key:  [${derivedKey}]")

        // Assert
        assert derivedKey == KEY_HEX

        SCrypt.metaClass.'static' = null
    }

    @Test
    void testShouldActuallyDeriveKeyFromPassword() {
        // Arrange
        logger.info("Using password: [${PASSWORD}]")

        // Act
        String derivedKey = ConfigEncryptionTool.deriveKeyFromPassword(PASSWORD)
        logger.info("Derived key:  [${derivedKey}]")

        // Assert
        assert derivedKey.length() == (Cipher.getMaxAllowedKeyLength("AES") > 128 ? 64 : 32)
    }

    @Test
    void testDeriveKeyFromPasswordShouldTrimPassword() {
        // Arrange
        final String PASSWORD_SPACES = "   ${PASSWORD}   "

        def attemptedPasswords = []

        // Mocked for deterministic output and performance in test -- SCrypt is not under test here
        SCrypt.metaClass.'static'.generate = { byte[] pw, byte[] s, int N, int r, int p, int dkLen ->
            logger.mock("Mock SCrypt.generate(${Hex.encodeHexString(pw)}, ${Hex.encodeHexString(s)}, ${N}, ${r}, ${p}, ${dkLen})")
            attemptedPasswords << new String(pw)
            logger.mock("Returning ${KEY_HEX[0..<dkLen * 2]}")
            Hex.decodeHex(KEY_HEX[0..<dkLen * 2] as char[])
        }

        // Act
        [PASSWORD, PASSWORD_SPACES].each { String password ->
            logger.info("Using password: [${password}]")
            String derivedKey = ConfigEncryptionTool.deriveKeyFromPassword(password)
            logger.info("Derived key:  [${derivedKey}]")
        }

        // Assert
        assert attemptedPasswords.size() == 2
        assert attemptedPasswords.every { it == PASSWORD }

        SCrypt.metaClass.'static' = null
    }

    @Test
    void testDeriveKeyFromPasswordShouldThrowExceptionForInvalidPasswords() {
        // Arrange
        List<String> passwords = [
                (null),
                "",
                "      ",
                "shortpass",
                "shortwith    "
        ]

        // Act
        passwords.each { String password ->
            logger.info("Reading password: [${password}]")
            def msg = shouldFail(KeyException) {
                String derivedKey = ConfigEncryptionTool.deriveKeyFromPassword(password)
                logger.info("Derived key:  [${derivedKey}]")
            }
            logger.expected(msg)

            // Assert
            assert msg == "Cannot derive key from empty/short password -- password must be at least 12 characters"
        }
    }

    @Test
    void testShouldHandleKeyAndPasswordFlag() {
        // Arrange
        def args = ["-k", KEY_HEX, "-p", PASSWORD]
        logger.info("Using args: ${args}")

        // Act
        def msg = shouldFail(CommandLineParseException) {
            new ConfigEncryptionTool().parse(args as String[])
        }
        logger.expected(msg)

        // Assert
        assert msg == "Only one of password and key can be used"
    }

    @Test
    void testShouldNotLoadMissingNiFiProperties() {
        // Arrange
        String niFiPropertiesPath = "src/test/resources/non_existent_nifi.properties"
        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        String[] args = ["-n", niFiPropertiesPath] as String[]

        String oldFilePath = System.getProperty(NiFiProperties.PROPERTIES_FILE_PATH)

        tool.parse(args)
        logger.info("Parsed nifi.properties location: ${tool.niFiPropertiesPath}")

        // Act
        def msg = shouldFail(CommandLineParseException) {
            NiFiProperties properties = tool.loadNiFiProperties()
            logger.info("Loaded NiFiProperties from ${tool.niFiPropertiesPath}")
        }

        // Assert
        assert msg == "Cannot load NiFiProperties from [${niFiPropertiesPath}]".toString()

        // The system variable was reset to the original value
        assert System.getProperty(NiFiProperties.PROPERTIES_FILE_PATH) == oldFilePath
    }

    @Test
    void testLoadNiFiPropertiesShouldHandleReadFailure() {
        // Arrange
        File inputPropertiesFile = new File("src/test/resources/nifi_with_sensitive_properties_unprotected.properties")
        File workingFile = new File("tmp_nifi.properties")
        workingFile.delete()

        Files.copy(inputPropertiesFile.toPath(), workingFile.toPath())
        // Empty set of permissions
        Files.setPosixFilePermissions(workingFile.toPath(), [] as Set)
        logger.info("Set POSIX permissions to ${Files.getPosixFilePermissions(workingFile.toPath())}")

        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        String[] args = ["-n", workingFile.path, "-k", KEY_HEX]
        tool.parse(args)

        // Act
        def msg = shouldFail(IOException) {
            tool.loadNiFiProperties()
            logger.info("Read nifi.properties")
        }
        logger.expected(msg)

        // Assert
        assert msg == "Cannot load NiFiProperties from [${workingFile.path}]".toString()

        workingFile.deleteOnExit()
    }

    @Test
    void testShouldEncryptSensitiveProperties() {
        // Arrange
        String niFiPropertiesPath = "src/test/resources/nifi_with_sensitive_properties_unprotected.properties"
        String newPropertiesPath = "src/test/resources/tmp_encrypted_nifi.properties"
        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        String[] args = ["-n", niFiPropertiesPath, "-o", newPropertiesPath] as String[]

        tool.parse(args)
        logger.info("Parsed nifi.properties location: ${tool.niFiPropertiesPath}")

        tool.keyHex = KEY_HEX

        NiFiProperties plainNiFiProperties = tool.loadNiFiProperties()
        ProtectedNiFiProperties protectedWrapper = new ProtectedNiFiProperties(plainNiFiProperties)
        assert !protectedWrapper.hasProtectedKeys()

        // Act
        NiFiProperties encryptedProperties = tool.encryptSensitiveProperties(plainNiFiProperties)
        logger.info("Encrypted sensitive properties")

        // Assert
        ProtectedNiFiProperties protectedWrapperAroundEncrypted = new ProtectedNiFiProperties(encryptedProperties)
        assert protectedWrapperAroundEncrypted.hasProtectedKeys()

        // Ensure that all non-empty sensitive properties are marked as protected
        final Set<String> EXPECTED_PROTECTED_KEYS = protectedWrapperAroundEncrypted
                .getSensitivePropertyKeys().findAll { String k ->
            plainNiFiProperties.getProperty(k)
        } as Set<String>
        assert protectedWrapperAroundEncrypted.getProtectedPropertyKeys().keySet() == EXPECTED_PROTECTED_KEYS
    }

    @Test
    void testShouldUpdateBootstrapContentsWithKey() {
        // Arrange
        final String EXPECTED_KEY_LINE = ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX + KEY_HEX

        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        tool.keyHex = KEY_HEX

        List<String> originalLines = [
                ConfigEncryptionTool.BOOTSTRAP_KEY_COMMENT,
                "${ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX}="
        ]

        // Act
        List<String> updatedLines = tool.updateBootstrapContentsWithKey(originalLines)
        logger.info("Updated bootstrap.conf lines: ${updatedLines}")

        // Assert
        assert updatedLines.size() == originalLines.size()
        assert updatedLines.first() == originalLines.first()
        assert updatedLines.last() == EXPECTED_KEY_LINE
    }

    @Test
    void testUpdateBootstrapContentsWithKeyShouldOverwriteExistingKey() {
        // Arrange
        final String EXPECTED_KEY_LINE = ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX + KEY_HEX

        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        tool.keyHex = KEY_HEX

        List<String> originalLines = [
                ConfigEncryptionTool.BOOTSTRAP_KEY_COMMENT,
                "${ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX}=badKey"
        ]

        // Act
        List<String> updatedLines = tool.updateBootstrapContentsWithKey(originalLines)
        logger.info("Updated bootstrap.conf lines: ${updatedLines}")

        // Assert
        assert updatedLines.size() == originalLines.size()
        assert updatedLines.first() == originalLines.first()
        assert updatedLines.last() == EXPECTED_KEY_LINE
    }

    @Test
    void testShouldUpdateBootstrapContentsWithKeyAndComment() {
        // Arrange
        final String EXPECTED_KEY_LINE = ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX + KEY_HEX

        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        tool.keyHex = KEY_HEX

        List<String> originalLines = [
                "${ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX}="
        ]

        // Act
        List<String> updatedLines = tool.updateBootstrapContentsWithKey(originalLines.clone() as List<String>)
        logger.info("Updated bootstrap.conf lines: ${updatedLines}")

        // Assert
        assert updatedLines.size() == originalLines.size() + 1
        assert updatedLines.first() == ConfigEncryptionTool.BOOTSTRAP_KEY_COMMENT
        assert updatedLines.last() == EXPECTED_KEY_LINE
    }

    @Test
    void testUpdateBootstrapContentsWithKeyShouldAddLines() {
        // Arrange
        final String EXPECTED_KEY_LINE = ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX + KEY_HEX

        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        tool.keyHex = KEY_HEX

        List<String> originalLines = []

        // Act
        List<String> updatedLines = tool.updateBootstrapContentsWithKey(originalLines.clone() as List<String>)
        logger.info("Updated bootstrap.conf lines: ${updatedLines}")

        // Assert
        assert updatedLines.size() == originalLines.size() + 3
        assert updatedLines.first() == "\n"
        assert updatedLines[1] == ConfigEncryptionTool.BOOTSTRAP_KEY_COMMENT
        assert updatedLines.last() == EXPECTED_KEY_LINE
    }

    @Test
    void testShouldWriteKeyToBootstrapConf() {
        // Arrange
        File emptyKeyFile = new File("src/test/resources/bootstrap_with_empty_master_key.conf")
        File workingFile = new File("tmp_bootstrap.conf")
        workingFile.delete()

        Files.copy(emptyKeyFile.toPath(), workingFile.toPath())
        final List<String> originalLines = workingFile.readLines()
        String originalKeyLine = originalLines.find { it.startsWith(ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX) }
        logger.info("Original key line from bootstrap.conf: ${originalKeyLine}")

        final String EXPECTED_KEY_LINE = ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX + KEY_HEX

        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        String[] args = ["-b", workingFile.path, "-k", KEY_HEX]
        tool.parse(args)

        // Act
        tool.writeKeyToBootstrapConf()
        logger.info("Updated bootstrap.conf")

        // Assert
        final List<String> updatedLines = workingFile.readLines()
        String updatedLine = updatedLines.find { it.startsWith(ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX) }
        logger.info("Updated key line: ${updatedLine}")

        assert updatedLine == EXPECTED_KEY_LINE
        assert originalLines.size() == updatedLines.size()

        workingFile.deleteOnExit()
    }

    @Test
    void testWriteKeyToBootstrapConfShouldHandleReadFailure() {
        // Arrange
        File emptyKeyFile = new File("src/test/resources/bootstrap_with_empty_master_key.conf")
        File workingFile = new File("tmp_bootstrap.conf")
        workingFile.delete()

        Files.copy(emptyKeyFile.toPath(), workingFile.toPath())
        // Empty set of permissions
        Files.setPosixFilePermissions(workingFile.toPath(), [] as Set)
        logger.info("Set POSIX permissions to ${Files.getPosixFilePermissions(workingFile.toPath())}")

        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        String[] args = ["-b", workingFile.path, "-k", KEY_HEX]
        tool.parse(args)

        // Act
        def msg = shouldFail(IOException) {
            tool.writeKeyToBootstrapConf()
            logger.info("Updated bootstrap.conf")
        }
        logger.expected(msg)

        // Assert
        assert msg == "The bootstrap.conf file at tmp_bootstrap.conf must exist and be readable and writable by the user running this tool"

        workingFile.deleteOnExit()
    }

    @Test
    void testWriteKeyToBootstrapConfShouldHandleWriteFailure() {
        // Arrange
        File emptyKeyFile = new File("src/test/resources/bootstrap_with_empty_master_key.conf")
        File workingFile = new File("tmp_bootstrap.conf")
        workingFile.delete()

        Files.copy(emptyKeyFile.toPath(), workingFile.toPath())
        // Read-only set of permissions
        Files.setPosixFilePermissions(workingFile.toPath(), [PosixFilePermission.OWNER_READ, PosixFilePermission.GROUP_READ, PosixFilePermission.OTHERS_READ] as Set)
        logger.info("Set POSIX permissions to ${Files.getPosixFilePermissions(workingFile.toPath())}")

        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        String[] args = ["-b", workingFile.path, "-k", KEY_HEX]
        tool.parse(args)

        // Act
        def msg = shouldFail(IOException) {
            tool.writeKeyToBootstrapConf()
            logger.info("Updated bootstrap.conf")
        }
        logger.expected(msg)

        // Assert
        assert msg == "The bootstrap.conf file at tmp_bootstrap.conf must exist and be readable and writable by the user running this tool"

        workingFile.deleteOnExit()
    }

    @Test
    void testShouldEncryptNiFiPropertiesWithEmptyProtectionScheme() {
        // Arrange
        String originalNiFiPropertiesPath = "src/test/resources/nifi_with_sensitive_properties_unprotected_and_empty_protection_schemes.properties"

        File originalFile = new File(originalNiFiPropertiesPath)
        List<String> originalLines = originalFile.readLines()
        logger.info("Read ${originalLines.size()} lines from ${originalNiFiPropertiesPath}")
        logger.info("\n" + originalLines[0..3].join("\n") + "...")

        NiFiProperties plainProperties = NiFiPropertiesLoader.withKey(KEY_HEX).load(originalNiFiPropertiesPath)
        logger.info("Loaded NiFiProperties from ${originalNiFiPropertiesPath}")

        ProtectedNiFiProperties protectedWrapper = new ProtectedNiFiProperties(plainProperties)
        logger.info("Loaded ${plainProperties.size()} properties")
        logger.info("There are ${protectedWrapper.getSensitivePropertyKeys().size()} sensitive properties")

        SensitivePropertyProvider spp = new AESSensitivePropertyProvider(KEY_HEX)
        int protectedPropertyCount = protectedWrapper.protectedPropertyKeys.size()
        logger.info("Counted ${protectedPropertyCount} protected keys")
        assert protectedPropertyCount < protectedWrapper.getSensitivePropertyKeys().size()

        ConfigEncryptionTool tool = new ConfigEncryptionTool(keyHex: KEY_HEX)

        // Act
        NiFiProperties encryptedProperties = tool.encryptSensitiveProperties(plainProperties)

        // Assert
        ProtectedNiFiProperties encryptedWrapper = new ProtectedNiFiProperties(encryptedProperties)
        encryptedWrapper.getProtectedPropertyKeys().every { String key, String protectionScheme ->
            logger.info("${key} is protected by ${protectionScheme}")
            assert protectionScheme == spp.identifierKey
        }

        printProperties(encryptedWrapper)

        assert encryptedWrapper.getProtectedPropertyKeys().size() == encryptedWrapper.getSensitivePropertyKeys().findAll {
            encryptedWrapper.getProperty(it)
        }.size()
    }

    @Test
    void testShouldSerializeNiFiProperties() {
        // Arrange
        Properties rawProperties = [key: "value", key2: "value2"] as Properties
        NiFiProperties properties = new StandardNiFiProperties(rawProperties)
        logger.info("Loaded ${properties.size()} properties")

        // Act
        List<String> lines = ConfigEncryptionTool.serializeNiFiProperties(properties)
        logger.info("Serialized NiFiProperties to ${lines.size()} lines")
        logger.info("\n" + lines.join("\n"))

        // Assert

        // The serialization could have occurred > 1 second ago, causing a rolling date/time mismatch, so use regex
        // Format -- #Fri Aug 19 16:51:16 PDT 2016
        String datePattern = /#\w{3} \w{3} \d{2} \d{2}:\d{2}:\d{2} \w{3} \d{4}/

        // One extra line for the date
        assert lines.size() == properties.size() + 1
        assert lines.first() =~ datePattern

        rawProperties.keySet().every { String key ->
            assert lines.contains("${key}=${properties.getProperty(key)}".toString())
        }
    }

    @Test
    void testShouldSerializeNiFiPropertiesAndPreserveFormat() {
        // Arrange
        String originalNiFiPropertiesPath = "src/test/resources/nifi_with_few_sensitive_properties_unprotected.properties"

        File originalFile = new File(originalNiFiPropertiesPath)
        List<String> originalLines = originalFile.readLines()
        logger.info("Read ${originalLines.size()} lines from ${originalNiFiPropertiesPath}")
        logger.info("\n" + originalLines[0..3].join("\n") + "...")

        NiFiProperties plainProperties = NiFiPropertiesLoader.withKey(KEY_HEX).load(originalNiFiPropertiesPath)
        logger.info("Loaded NiFiProperties from ${originalNiFiPropertiesPath}")

        ProtectedNiFiProperties protectedWrapper = new ProtectedNiFiProperties(plainProperties)
        logger.info("Loaded ${plainProperties.size()} properties")
        logger.info("There are ${protectedWrapper.getSensitivePropertyKeys().size()} sensitive properties")

        protectedWrapper.addSensitivePropertyProvider(new AESSensitivePropertyProvider(KEY_HEX))
        NiFiProperties protectedProperties = protectedWrapper.protectPlainProperties()
        int protectedPropertyCount = ProtectedNiFiProperties.countProtectedProperties(protectedProperties)
        logger.info("Counted ${protectedPropertyCount} protected keys")

        // Act
        List<String> lines = ConfigEncryptionTool.serializeNiFiPropertiesAndPreserveFormat(protectedProperties, originalFile)
        logger.info("Serialized NiFiProperties to ${lines.size()} lines")
        lines.eachWithIndex { String entry, int i ->
            logger.debug("${(i + 1).toString().padLeft(3)}: ${entry}")
        }

        // Assert

        // Added n new lines for the encrypted properties
        assert lines.size() == originalLines.size() + protectedPropertyCount

        protectedProperties.getPropertyKeys().every { String key ->
            assert lines.contains("${key}=${protectedProperties.getProperty(key)}".toString())
        }

        logger.info("Updated nifi.properties:")
        logger.info("\n" * 2 + lines.join("\n"))
    }

    @Test
    void testShouldSerializeNiFiPropertiesAndPreserveFormatWithExistingProtectionSchemes() {
        // Arrange
        String originalNiFiPropertiesPath = "src/test/resources/nifi_with_few_sensitive_properties_protected_aes.properties"

        File originalFile = new File(originalNiFiPropertiesPath)
        List<String> originalLines = originalFile.readLines()
        logger.info("Read ${originalLines.size()} lines from ${originalNiFiPropertiesPath}")
        logger.info("\n" + originalLines[0..3].join("\n") + "...")

        ProtectedNiFiProperties protectedProperties = NiFiPropertiesLoader.withKey(KEY_HEX).readProtectedPropertiesFromDisk(new File(originalNiFiPropertiesPath))
        logger.info("Loaded NiFiProperties from ${originalNiFiPropertiesPath}")

        logger.info("Loaded ${protectedProperties.getPropertyKeys().size()} properties")
        logger.info("There are ${protectedProperties.getSensitivePropertyKeys().size()} sensitive properties")
        logger.info("There are ${protectedProperties.getProtectedPropertyKeys().size()} protected properties")
        int originalProtectedPropertyCount = protectedProperties.getProtectedPropertyKeys().size()

        protectedProperties.addSensitivePropertyProvider(new AESSensitivePropertyProvider(KEY_HEX))
        NiFiProperties encryptedProperties = protectedProperties.protectPlainProperties()
        int protectedPropertyCount = ProtectedNiFiProperties.countProtectedProperties(encryptedProperties)
        logger.info("Counted ${protectedPropertyCount} protected keys")

        int protectedCountChange = protectedPropertyCount - originalProtectedPropertyCount
        logger.info("Expected line count change: ${protectedCountChange}")

        // Act
        List<String> lines = ConfigEncryptionTool.serializeNiFiPropertiesAndPreserveFormat(protectedProperties, originalFile)
        logger.info("Serialized NiFiProperties to ${lines.size()} lines")
        lines.eachWithIndex { String entry, int i ->
            logger.debug("${(i + 1).toString().padLeft(3)}: ${entry}")
        }

        // Assert

        // Added n new lines for the encrypted properties
        assert lines.size() == originalLines.size() + protectedCountChange

        protectedProperties.getPropertyKeys().every { String key ->
            assert lines.contains("${key}=${protectedProperties.getProperty(key)}".toString())
        }

        logger.info("Updated nifi.properties:")
        logger.info("\n" * 2 + lines.join("\n"))
    }

    @Test
    void testShouldSerializeNiFiPropertiesAndPreserveFormatWithNewPropertyAtEOF() {
        // Arrange
        String originalNiFiPropertiesPath = "src/test/resources/nifi_with_few_sensitive_properties_unprotected.properties"

        File originalFile = new File(originalNiFiPropertiesPath)
        List<String> originalLines = originalFile.readLines()
        logger.info("Read ${originalLines.size()} lines from ${originalNiFiPropertiesPath}")
        logger.info("\n" + originalLines[0..3].join("\n") + "...")

        NiFiProperties plainProperties = NiFiPropertiesLoader.withKey(KEY_HEX).load(originalNiFiPropertiesPath)
        logger.info("Loaded NiFiProperties from ${originalNiFiPropertiesPath}")

        ProtectedNiFiProperties protectedWrapper = new ProtectedNiFiProperties(plainProperties)
        logger.info("Loaded ${plainProperties.size()} properties")
        logger.info("There are ${protectedWrapper.getSensitivePropertyKeys().size()} sensitive properties")

        // Set a value for the sensitive property which is the last line in the file

        // Groovy access to avoid duplicating entire object to add one value
        (plainProperties as StandardNiFiProperties).@rawProperties.setProperty(NiFiProperties.SECURITY_TRUSTSTORE_PASSWD, "thisIsABadTruststorePassword")

        protectedWrapper.addSensitivePropertyProvider(new AESSensitivePropertyProvider(KEY_HEX))
        NiFiProperties protectedProperties = protectedWrapper.protectPlainProperties()
        int protectedPropertyCount = ProtectedNiFiProperties.countProtectedProperties(protectedProperties)
        logger.info("Counted ${protectedPropertyCount} protected keys")

        // Act
        List<String> lines = ConfigEncryptionTool.serializeNiFiPropertiesAndPreserveFormat(protectedProperties, originalFile)
        logger.info("Serialized NiFiProperties to ${lines.size()} lines")
        lines.eachWithIndex { String entry, int i ->
            logger.debug("${(i + 1).toString().padLeft(3)}: ${entry}")
        }

        // Assert

        // Added n new lines for the encrypted properties
        assert lines.size() == originalLines.size() + protectedPropertyCount

        protectedProperties.getPropertyKeys().every { String key ->
            assert lines.contains("${key}=${protectedProperties.getProperty(key)}".toString())
        }

        logger.info("Updated nifi.properties:")
        logger.info("\n" * 2 + lines.join("\n"))
    }

    @Test
    void testShouldWriteNiFiProperties() {
        // Arrange
        File inputPropertiesFile = new File("src/test/resources/nifi_with_sensitive_properties_unprotected.properties")
        File workingFile = new File("tmp_nifi.properties")
        workingFile.delete()

        final List<String> originalLines = inputPropertiesFile.readLines()

        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        String[] args = ["-n", inputPropertiesFile.path, "-o", workingFile.path, "-k", KEY_HEX]
        tool.parse(args)
        NiFiProperties niFiProperties = tool.loadNiFiProperties()
        tool.@niFiProperties = niFiProperties
        logger.info("Loaded ${niFiProperties.size()} properties from ${inputPropertiesFile.path}")

        // Act
        tool.writeNiFiProperties()
        logger.info("Wrote to ${workingFile.path}")

        // Assert
        final List<String> updatedLines = workingFile.readLines()
        niFiProperties.getPropertyKeys().every { String key ->
            assert updatedLines.contains("${key}=${niFiProperties.getProperty(key)}".toString())
        }

        assert originalLines == updatedLines

        logger.info("Updated nifi.properties:")
        logger.info("\n" * 2 + updatedLines.join("\n"))

        workingFile.deleteOnExit()
    }

    @Test
    void testShouldWriteNiFiPropertiesInSameLocation() {
        // Arrange
        File inputPropertiesFile = new File("src/test/resources/nifi_with_sensitive_properties_unprotected.properties")
        File workingFile = new File("tmp_nifi.properties")
        workingFile.delete()
        Files.copy(inputPropertiesFile.toPath(), workingFile.toPath())

        final List<String> originalLines = inputPropertiesFile.readLines()

        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        String[] args = ["-n", workingFile.path, "-k", KEY_HEX]
        tool.parse(args)
        NiFiProperties niFiProperties = tool.loadNiFiProperties()
        tool.@niFiProperties = niFiProperties
        logger.info("Loaded ${niFiProperties.size()} properties from ${workingFile.path}")

        // Act
        tool.writeNiFiProperties()
        logger.info("Wrote to ${workingFile.path}")

        // Assert
        final List<String> updatedLines = workingFile.readLines()
        niFiProperties.getPropertyKeys().every { String key ->
            assert updatedLines.contains("${key}=${niFiProperties.getProperty(key)}".toString())
        }

        assert originalLines == updatedLines

        logger.info("Updated nifi.properties:")
        logger.info("\n" * 2 + updatedLines.join("\n"))

        assert TestAppender.events.collect {
            it.message
        }.contains("The source nifi.properties and destination nifi.properties are identical [${workingFile.path}] so the original will be overwritten".toString())

        workingFile.deleteOnExit()
    }

    @Test
    void testWriteNiFiPropertiesShouldHandleWriteFailureWhenFileExists() {
        // Arrange
        File inputPropertiesFile = new File("src/test/resources/nifi_with_sensitive_properties_unprotected.properties")
        File workingFile = new File("tmp_nifi.properties")
        workingFile.delete()

        Files.copy(inputPropertiesFile.toPath(), workingFile.toPath())
        // Read-only set of permissions
        Files.setPosixFilePermissions(workingFile.toPath(), [PosixFilePermission.OWNER_READ, PosixFilePermission.GROUP_READ, PosixFilePermission.OTHERS_READ] as Set)
        logger.info("Set POSIX permissions to ${Files.getPosixFilePermissions(workingFile.toPath())}")

        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        String[] args = ["-n", inputPropertiesFile.path, "-o", workingFile.path, "-k", KEY_HEX]
        tool.parse(args)
        NiFiProperties niFiProperties = tool.loadNiFiProperties()
        tool.@niFiProperties = niFiProperties
        logger.info("Loaded ${niFiProperties.size()} properties from ${inputPropertiesFile.path}")

        // Act
        def msg = shouldFail(IOException) {
            tool.writeNiFiProperties()
            logger.info("Wrote to ${workingFile.path}")
        }
        logger.expected(msg)

        // Assert
        assert msg == "The nifi.properties file at ${workingFile.path} must be writable by the user running this tool".toString()

        workingFile.deleteOnExit()
    }

    @Test
    void testWriteNiFiPropertiesShouldHandleWriteFailureWhenFileDoesNotExist() {
        // Arrange
        File inputPropertiesFile = new File("src/test/resources/nifi_with_sensitive_properties_unprotected.properties")
        File tmpDir = new File("target/tmp/")
        tmpDir.mkdirs()
        File workingFile = new File("target/tmp/tmp_nifi.properties")
        workingFile.delete()

        // Read-only set of permissions
        Files.setPosixFilePermissions(tmpDir.toPath(), [PosixFilePermission.OWNER_READ, PosixFilePermission.GROUP_READ, PosixFilePermission.OTHERS_READ] as Set)
        logger.info("Set POSIX permissions on parent directory to ${Files.getPosixFilePermissions(tmpDir.toPath())}")

        ConfigEncryptionTool tool = new ConfigEncryptionTool()
        String[] args = ["-n", inputPropertiesFile.path, "-o", workingFile.path, "-k", KEY_HEX]
        tool.parse(args)
        NiFiProperties niFiProperties = tool.loadNiFiProperties()
        tool.@niFiProperties = niFiProperties
        logger.info("Loaded ${niFiProperties.size()} properties from ${inputPropertiesFile.path}")

        // Act
        def msg = shouldFail(IOException) {
            tool.writeNiFiProperties()
            logger.info("Wrote to ${workingFile.path}")
        }
        logger.expected(msg)

        // Assert
        assert msg == "The nifi.properties file at ${workingFile.path} must be writable by the user running this tool".toString()

        workingFile.deleteOnExit()
        Files.setPosixFilePermissions(tmpDir.toPath(), [PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE] as Set)
        tmpDir.deleteOnExit()
    }

    @Test
    void testShouldPerformFullOperation() {
        // Arrange
        exit.expectSystemExitWithStatus(0)

        File tmpDir = new File("target/tmp/")
        tmpDir.mkdirs()
        Files.setPosixFilePermissions(tmpDir.toPath(), [PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE, PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_WRITE, PosixFilePermission.GROUP_EXECUTE, PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_WRITE, PosixFilePermission.OTHERS_EXECUTE] as Set)

        File emptyKeyFile = new File("src/test/resources/bootstrap_with_empty_master_key.conf")
        File bootstrapFile = new File("target/tmp/tmp_bootstrap.conf")
        bootstrapFile.delete()

        Files.copy(emptyKeyFile.toPath(), bootstrapFile.toPath())
        final List<String> originalBootstrapLines = bootstrapFile.readLines()
        String originalKeyLine = originalBootstrapLines.find {
            it.startsWith(ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX)
        }
        logger.info("Original key line from bootstrap.conf: ${originalKeyLine}")
        assert originalKeyLine == ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX

        final String EXPECTED_KEY_LINE = ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX + KEY_HEX

        File inputPropertiesFile = new File("src/test/resources/nifi_with_sensitive_properties_unprotected.properties")
        File outputPropertiesFile = new File("target/tmp/tmp_nifi.properties")
        outputPropertiesFile.delete()

        NiFiProperties inputProperties = new NiFiPropertiesLoader().load(inputPropertiesFile)
        logger.info("Loaded ${inputProperties.size()} properties from input file")
        ProtectedNiFiProperties protectedInputProperties = new ProtectedNiFiProperties(inputProperties)
        def originalSensitiveValues = protectedInputProperties.getSensitivePropertyKeys().collectEntries { String key -> [(key): protectedInputProperties.getProperty(key)] }
        logger.info("Original sensitive values: ${originalSensitiveValues}")

        String[] args = ["-n", inputPropertiesFile.path, "-b", bootstrapFile.path, "-o", outputPropertiesFile.path, "-k", KEY_HEX]

        exit.checkAssertionAfterwards(new Assertion() {
            public void checkAssertion() {
                final List<String> updatedPropertiesLines = outputPropertiesFile.readLines()
                logger.info("Updated nifi.properties:")
                logger.info("\n" * 2 + updatedPropertiesLines.join("\n"))

                // Check that the output values for sensitive properties are not the same as the original (i.e. it was encrypted)
                NiFiProperties updatedProperties = new NiFiPropertiesLoader().readProtectedPropertiesFromDisk(outputPropertiesFile)
                assert updatedProperties.size() >= inputProperties.size()
                originalSensitiveValues.every { String key, String originalValue ->
                    assert updatedProperties.getProperty(key) != originalValue
                }

                // Check that the new NiFiProperties instance matches the output file (values still encrypted)
                updatedProperties.getPropertyKeys().every { String key ->
                    assert updatedPropertiesLines.contains("${key}=${updatedProperties.getProperty(key)}".toString())
                }

                // Check that the key was persisted to the bootstrap.conf
                final List<String> updatedBootstrapLines = bootstrapFile.readLines()
                String updatedKeyLine = updatedBootstrapLines.find {
                    it.startsWith(ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX)
                }
                logger.info("Updated key line: ${updatedKeyLine}")

                assert updatedKeyLine == EXPECTED_KEY_LINE
                assert originalBootstrapLines.size() == updatedBootstrapLines.size()

                // Clean up
                outputPropertiesFile.deleteOnExit()
                bootstrapFile.deleteOnExit()
                tmpDir.deleteOnExit()
            }
        });

        // Act
        ConfigEncryptionTool.main(args)
        logger.info("Invoked #main with ${args.join(" ")}")

        // Assert

        // Assertions defined above
    }

    @Test
    void testShouldPerformFullOperationWithPassword() {
        // Arrange
        exit.expectSystemExitWithStatus(0)

        File tmpDir = new File("target/tmp/")
        tmpDir.mkdirs()
        Files.setPosixFilePermissions(tmpDir.toPath(), [PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE, PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_WRITE, PosixFilePermission.GROUP_EXECUTE, PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_WRITE, PosixFilePermission.OTHERS_EXECUTE] as Set)

        File emptyKeyFile = new File("src/test/resources/bootstrap_with_empty_master_key.conf")
        File bootstrapFile = new File("target/tmp/tmp_bootstrap.conf")
        bootstrapFile.delete()

        Files.copy(emptyKeyFile.toPath(), bootstrapFile.toPath())
        final List<String> originalBootstrapLines = bootstrapFile.readLines()
        String originalKeyLine = originalBootstrapLines.find {
            it.startsWith(ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX)
        }
        logger.info("Original key line from bootstrap.conf: ${originalKeyLine}")
        assert originalKeyLine == ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX

        final String EXPECTED_KEY_HEX = ConfigEncryptionTool.deriveKeyFromPassword(PASSWORD)
        logger.info("Derived key from password [${PASSWORD}]: ${EXPECTED_KEY_HEX}")

        final String EXPECTED_KEY_LINE = ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX + EXPECTED_KEY_HEX

        File inputPropertiesFile = new File("src/test/resources/nifi_with_sensitive_properties_unprotected.properties")
        File outputPropertiesFile = new File("target/tmp/tmp_nifi.properties")
        outputPropertiesFile.delete()

        NiFiProperties inputProperties = new NiFiPropertiesLoader().load(inputPropertiesFile)
        logger.info("Loaded ${inputProperties.size()} properties from input file")
        ProtectedNiFiProperties protectedInputProperties = new ProtectedNiFiProperties(inputProperties)
        def originalSensitiveValues = protectedInputProperties.getSensitivePropertyKeys().collectEntries { String key -> [(key): protectedInputProperties.getProperty(key)] }
        logger.info("Original sensitive values: ${originalSensitiveValues}")

        String[] args = ["-n", inputPropertiesFile.path, "-b", bootstrapFile.path, "-o", outputPropertiesFile.path, "-p", PASSWORD]

        exit.checkAssertionAfterwards(new Assertion() {
            public void checkAssertion() {
                final List<String> updatedPropertiesLines = outputPropertiesFile.readLines()
                logger.info("Updated nifi.properties:")
                logger.info("\n" * 2 + updatedPropertiesLines.join("\n"))

                // Check that the output values for sensitive properties are not the same as the original (i.e. it was encrypted)
                NiFiProperties updatedProperties = new NiFiPropertiesLoader().readProtectedPropertiesFromDisk(outputPropertiesFile)
                assert updatedProperties.size() >= inputProperties.size()
                originalSensitiveValues.every { String key, String originalValue ->
                    assert updatedProperties.getProperty(key) != originalValue
                }

                // Check that the new NiFiProperties instance matches the output file (values still encrypted)
                updatedProperties.getPropertyKeys().every { String key ->
                    assert updatedPropertiesLines.contains("${key}=${updatedProperties.getProperty(key)}".toString())
                }

                // Check that the key was persisted to the bootstrap.conf
                final List<String> updatedBootstrapLines = bootstrapFile.readLines()
                String updatedKeyLine = updatedBootstrapLines.find {
                    it.startsWith(ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX)
                }
                logger.info("Updated key line: ${updatedKeyLine}")

                assert updatedKeyLine == EXPECTED_KEY_LINE
                assert originalBootstrapLines.size() == updatedBootstrapLines.size()

                // Clean up
                outputPropertiesFile.deleteOnExit()
                bootstrapFile.deleteOnExit()
                tmpDir.deleteOnExit()
            }
        });

        // Act
        ConfigEncryptionTool.main(args)
        logger.info("Invoked #main with ${args.join(" ")}")

        // Assert

        // Assertions defined above
    }

    @Test
    void testShouldPerformFullOperationMultipleTimes() {
        // Arrange
        exit.expectSystemExitWithStatus(0)

        File tmpDir = new File("target/tmp/")
        tmpDir.mkdirs()
        Files.setPosixFilePermissions(tmpDir.toPath(), [PosixFilePermission.OWNER_READ, PosixFilePermission.OWNER_WRITE, PosixFilePermission.OWNER_EXECUTE, PosixFilePermission.GROUP_READ, PosixFilePermission.GROUP_WRITE, PosixFilePermission.GROUP_EXECUTE, PosixFilePermission.OTHERS_READ, PosixFilePermission.OTHERS_WRITE, PosixFilePermission.OTHERS_EXECUTE] as Set)

        File emptyKeyFile = new File("src/test/resources/bootstrap_with_empty_master_key.conf")
        File bootstrapFile = new File("target/tmp/tmp_bootstrap.conf")
        bootstrapFile.delete()

        Files.copy(emptyKeyFile.toPath(), bootstrapFile.toPath())
        final List<String> originalBootstrapLines = bootstrapFile.readLines()
        String originalKeyLine = originalBootstrapLines.find {
            it.startsWith(ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX)
        }
        logger.info("Original key line from bootstrap.conf: ${originalKeyLine}")
        assert originalKeyLine == ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX

        final String EXPECTED_KEY_HEX = ConfigEncryptionTool.deriveKeyFromPassword(PASSWORD)
        logger.info("Derived key from password [${PASSWORD}]: ${EXPECTED_KEY_HEX}")

        final String EXPECTED_KEY_LINE = ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX + EXPECTED_KEY_HEX

        File inputPropertiesFile = new File("src/test/resources/nifi_with_sensitive_properties_unprotected.properties")
        File outputPropertiesFile = new File("target/tmp/tmp_nifi.properties")
        outputPropertiesFile.delete()

        NiFiProperties inputProperties = new NiFiPropertiesLoader().load(inputPropertiesFile)
        logger.info("Loaded ${inputProperties.size()} properties from input file")
        ProtectedNiFiProperties protectedInputProperties = new ProtectedNiFiProperties(inputProperties)
        def originalSensitiveValues = protectedInputProperties.getSensitivePropertyKeys().collectEntries { String key -> [(key): protectedInputProperties.getProperty(key)] }
        logger.info("Original sensitive values: ${originalSensitiveValues}")

        String[] args = ["-n", inputPropertiesFile.path, "-b", bootstrapFile.path, "-o", outputPropertiesFile.path, "-p", PASSWORD, "-v"]

        def msg = shouldFail {
            logger.info("Invoked #main first time with ${args.join(" ")}")
            ConfigEncryptionTool.main(args)
        }
        logger.expected(msg)

        // Act
        args = ["-n", outputPropertiesFile.path, "-b", bootstrapFile.path, "-p", PASSWORD, "-v"]

        // Add a new property to be encrypted
        outputPropertiesFile.text = outputPropertiesFile.text.replace("nifi.sensitive.props.additional.keys=", "nifi.sensitive.props.additional.keys=nifi.ui.banner.text")

        exit.checkAssertionAfterwards(new Assertion() {
            public void checkAssertion() {
                final List<String> updatedPropertiesLines = outputPropertiesFile.readLines()
                logger.info("Updated nifi.properties:")
                logger.info("\n" * 2 + updatedPropertiesLines.join("\n"))

                // Check that the output values for sensitive properties are not the same as the original (i.e. it was encrypted)
                NiFiProperties updatedProperties = new NiFiPropertiesLoader().readProtectedPropertiesFromDisk(outputPropertiesFile)
                assert updatedProperties.size() >= inputProperties.size()
                originalSensitiveValues.every { String key, String originalValue ->
                    assert updatedProperties.getProperty(key) != originalValue
                }

                // Check that the new NiFiProperties instance matches the output file (values still encrypted)
                updatedProperties.getPropertyKeys().every { String key ->
                    assert updatedPropertiesLines.contains("${key}=${updatedProperties.getProperty(key)}".toString())
                }

                // Check that the key was persisted to the bootstrap.conf
                final List<String> updatedBootstrapLines = bootstrapFile.readLines()
                String updatedKeyLine = updatedBootstrapLines.find {
                    it.startsWith(ConfigEncryptionTool.BOOTSTRAP_KEY_PREFIX)
                }
                logger.info("Updated key line: ${updatedKeyLine}")

                assert updatedKeyLine == EXPECTED_KEY_LINE
                assert originalBootstrapLines.size() == updatedBootstrapLines.size()

                // Clean up
                outputPropertiesFile.deleteOnExit()
                bootstrapFile.deleteOnExit()
                tmpDir.deleteOnExit()
            }
        });

        logger.info("Invoked #main second time with ${args.join(" ")}")
        ConfigEncryptionTool.main(args)

        // Assert

        // Assertions defined above
    }
}

public class TestAppender extends AppenderBase<LoggingEvent> {
    static List<LoggingEvent> events = new ArrayList<>();

    @Override
    protected void append(LoggingEvent e) {
        synchronized (events) {
            events.add(e);
        }
    }

    public static void reset() {
        synchronized (events) {
            events.clear();
        }
    }
}