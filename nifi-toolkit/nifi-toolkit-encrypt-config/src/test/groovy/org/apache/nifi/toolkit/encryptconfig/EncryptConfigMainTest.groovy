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

import org.apache.nifi.properties.AESSensitivePropertyProvider
import org.apache.nifi.properties.ConfigEncryptionTool
import org.apache.nifi.properties.NiFiPropertiesLoader
import org.apache.nifi.toolkit.encryptconfig.util.BootstrapUtil
import org.apache.nifi.util.NiFiProperties
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.Test
import org.junit.contrib.java.lang.system.Assertion
import org.junit.contrib.java.lang.system.ExpectedSystemExit
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import java.nio.file.Files
import java.security.Security

import static org.apache.nifi.toolkit.encryptconfig.TestUtil.*

@RunWith(JUnit4.class)
class EncryptConfigMainTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(EncryptConfigMainTest.class)

    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none()

    @BeforeClass
    static void setUpOnce() throws Exception {
        Security.addProvider(new BouncyCastleProvider())

        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }

        setupTmpDir()
    }

    @Test
    void testDetermineModeFromArgsWithLegacyMode() {
        // Arrange
        def argsList = "-b conf/bootstrap.conf -n conf/nifi.properties".split(" ").toList()

        // Act
        def toolMode = EncryptConfigMain.determineModeFromArgs(argsList)

        // Assert
        toolMode != null
        toolMode instanceof LegacyMode
    }

    @Test
    void testDetermineModeFromArgsWithNifiRegistryMode() {
        // Arrange
        def argsList = "--nifiRegistry".split(" ").toList()
        // Act
        def toolMode = EncryptConfigMain.determineModeFromArgs(argsList)
        // Assert
        toolMode != null
        toolMode instanceof NiFiRegistryMode
        !argsList.contains("--nifiRegistry")

        /* Test when --nifiRegistry is not first flag */

        // Arrange
        argsList = "-b conf/bootstrap.conf -p --nifiRegistry -r conf/nifi-registry.properties".split(" ").toList()
        // Act
        toolMode = EncryptConfigMain.determineModeFromArgs(argsList)
        // Assert
        toolMode != null
        toolMode instanceof NiFiRegistryMode
        !argsList.contains("--nifiRegistry")
    }

    @Test
    void testDetermineModeFromArgsWithNifiRegistryDecryptMode() {
        // Arrange
        def argsList = "--nifiRegistry --decrypt".split(" ").toList()
        // Act
        def toolMode = EncryptConfigMain.determineModeFromArgs(argsList)
        // Assert
        toolMode != null
        toolMode instanceof NiFiRegistryDecryptMode
        !argsList.contains("--nifiRegistry")
        !argsList.contains("--decrypt")

        /* Test when --decrypt comes before --nifiRegistry  */

        // Arrange
        argsList = "--b conf/bootstrap.conf --decrypt --nifiRegistry".split(" ").toList()
        // Act
        toolMode = EncryptConfigMain.determineModeFromArgs(argsList)
        // Assert
        toolMode != null
        toolMode instanceof NiFiRegistryDecryptMode
        !argsList.contains("--nifiRegistry")
        !argsList.contains("--decrypt")
    }

    @Test
    void testDetermineModeFromArgsReturnsNullOnDecryptWithoutNifiRegistryPresent() {
        // Arrange
        def argsList = "--decrypt".split(" ").toList()

        // Act
        def toolMode = EncryptConfigMain.determineModeFromArgs(argsList)

        // Assert
        toolMode == null
    }

    @Test
    void testShouldPerformFullOperationForNiFiPropertiesAndLoginIdentityProvidersAndAuthorizers() {
        // Arrange
        exit.expectSystemExitWithStatus(0)

        File tmpDir = setupTmpDir()

        File emptyKeyFile = new File("src/test/resources/bootstrap_with_empty_master_key.conf")
        File bootstrapFile = new File("target/tmp/tmp_bootstrap.conf")
        bootstrapFile.delete()

        Files.copy(emptyKeyFile.toPath(), bootstrapFile.toPath())
        final List<String> originalBootstrapLines = bootstrapFile.readLines()
        String originalKeyLine = originalBootstrapLines.find {
            it.startsWith("${BootstrapUtil.NIFI_BOOTSTRAP_KEY_PROPERTY}=")
        }
        logger.info("Original key line from bootstrap.conf: ${originalKeyLine}")
        assert originalKeyLine == "${BootstrapUtil.NIFI_BOOTSTRAP_KEY_PROPERTY}="

        final String EXPECTED_KEY_LINE = "${BootstrapUtil.NIFI_BOOTSTRAP_KEY_PROPERTY}=${KEY_HEX}"

        // Set up the NFP file
        File inputPropertiesFile = new File("src/test/resources/nifi_with_sensitive_properties_unprotected.properties")
        File outputPropertiesFile = new File("target/tmp/tmp_nifi.properties")
        outputPropertiesFile.delete()

        NiFiProperties inputProperties = new NiFiPropertiesLoader().load(inputPropertiesFile)
        logger.info("Loaded ${inputProperties.size()} properties from input file")

        // Set up the LIP file
        File inputLIPFile = new File("src/test/resources/login-identity-providers-populated.xml")
        File outputLIPFile = new File("target/tmp/tmp-lip.xml")
        outputLIPFile.delete()

        String originalLipXmlContent = inputLIPFile.text
        logger.info("Original LIP XML content: ${originalLipXmlContent}")

        // Set up the Authorizers file
        File inputAuthorizersFile = new File("src/test/resources/authorizers-populated.xml")
        File outputAuthorizersFile = new File("target/tmp/tmp-authorizers.xml")
        outputAuthorizersFile.delete()

        String originalAuthorizersXmlContent = inputAuthorizersFile.text
        logger.info("Original Authorizers XML content: ${originalAuthorizersXmlContent}")

        String[] args = [
                "-n", inputPropertiesFile.path,
                "-l", inputLIPFile.path,
                "-a", inputAuthorizersFile.path,
                "-b", bootstrapFile.path,
                "-o", outputPropertiesFile.path,
                "-i", outputLIPFile.path,
                "-u", outputAuthorizersFile.path,
                "-k", KEY_HEX,
                "-v"]

        AESSensitivePropertyProvider spp = new AESSensitivePropertyProvider(KEY_HEX)

        exit.checkAssertionAfterwards(new Assertion() {
            void checkAssertion() {

                /*** NiFi Properties Assertions ***/

                final List<String> updatedPropertiesLines = outputPropertiesFile.readLines()
                logger.info("Updated nifi.properties:")
                logger.info("\n" * 2 + updatedPropertiesLines.join("\n"))

                // Check that the output values for sensitive properties are not the same as the original (i.e. it was encrypted)
                NiFiProperties updatedProperties = new NiFiPropertiesLoader().readProtectedPropertiesFromDisk(outputPropertiesFile)
                assert updatedProperties.size() >= inputProperties.size()

                // Check that the new NiFiProperties instance matches the output file (values still encrypted)
                updatedProperties.getPropertyKeys().every { String key ->
                    assert updatedPropertiesLines.contains("${key}=${updatedProperties.getProperty(key)}".toString())
                }

                /*** Login Identity Providers Assertions ***/

                final String updatedLipXmlContent = outputLIPFile.text
                logger.info("Updated LIP XML content: ${updatedLipXmlContent}")
                // Check that the output values for sensitive properties are not the same as the original (i.e. it was encrypted)
                def originalLipParsedXml = new XmlSlurper().parseText(originalLipXmlContent)
                def updatedLipParsedXml = new XmlSlurper().parseText(updatedLipXmlContent)
                assert originalLipParsedXml != updatedLipParsedXml
                assert originalLipParsedXml.'**'.findAll { it.@encryption } != updatedLipParsedXml.'**'.findAll {
                    it.@encryption
                }
                def lipEncryptedValues = updatedLipParsedXml.provider.find {
                    it.identifier == 'ldap-provider'
                }.property.findAll {
                    it.@name =~ "Password" && it.@encryption =~ "aes/gcm/\\d{3}"
                }
                lipEncryptedValues.each {
                    assert spp.unprotect(it.text()) == PASSWORD
                }
                // Check that the comments are still there
                def lipTrimmedLines = inputLIPFile.readLines().collect { it.trim() }.findAll { it }
                def lipTrimmedSerializedLines = updatedLipXmlContent.split("\n").collect { it.trim() }.findAll { it }
                assert lipTrimmedLines.size() == lipTrimmedSerializedLines.size()

                /*** Authorizers Assertions ***/

                final String updatedAuthorizersXmlContent = outputAuthorizersFile.text
                logger.info("Updated Authorizers XML content: ${updatedAuthorizersXmlContent}")
                // Check that the output values for sensitive properties are not the same as the original (i.e. it was encrypted)
                def originalAuthorizersParsedXml = new XmlSlurper().parseText(originalAuthorizersXmlContent)
                def updatedAuthorizersParsedXml = new XmlSlurper().parseText(updatedAuthorizersXmlContent)
                assert originalAuthorizersParsedXml != updatedAuthorizersParsedXml
                assert originalAuthorizersParsedXml.'**'.findAll { it.@encryption } != updatedAuthorizersParsedXml.'**'.findAll {
                    it.@encryption
                }
                def authorizersEncryptedValues = updatedAuthorizersParsedXml.userGroupProvider.find {
                    it.identifier == 'ldap-user-group-provider'
                }.property.findAll {
                    it.@name =~ "Password" && it.@encryption =~ "aes/gcm/\\d{3}"
                }
                authorizersEncryptedValues.each {
                    assert spp.unprotect(it.text()) == PASSWORD
                }
                // Check that the comments are still there
                def authorizersTrimmedLines = inputAuthorizersFile.readLines().collect { it.trim() }.findAll { it }
                def authorizersTrimmedSerializedLines = updatedAuthorizersXmlContent.split("\n").collect { it.trim() }.findAll { it }
                assert authorizersTrimmedLines.size() == authorizersTrimmedSerializedLines.size()

                /*** Bootstrap assertions ***/

                // Check that the key was persisted to the bootstrap.conf
                final List<String> updatedBootstrapLines = bootstrapFile.readLines()
                String updatedKeyLine = updatedBootstrapLines.find {
                    it.startsWith(BootstrapUtil.NIFI_BOOTSTRAP_KEY_PROPERTY)
                }
                logger.info("Updated key line: ${updatedKeyLine}")

                assert updatedKeyLine == EXPECTED_KEY_LINE
                assert originalBootstrapLines.size() == updatedBootstrapLines.size()

                // Clean up
                outputPropertiesFile.deleteOnExit()
                outputLIPFile.deleteOnExit()
                outputAuthorizersFile.deleteOnExit()
                bootstrapFile.deleteOnExit()
                tmpDir.deleteOnExit()
            }
        })

        // Act
        EncryptConfigMain.main(args)
        logger.info("Invoked #main with ${args.join(" ")}")

        // Assert

        // Assertions defined above
    }

}
