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
package org.apache.nifi.fingerprint

import org.apache.nifi.encrypt.PropertyEncryptor
import org.apache.nifi.encrypt.SensitiveValueEncoder
import org.apache.nifi.nar.ExtensionManager
import org.apache.nifi.nar.StandardExtensionDiscoveringManager
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
class FingerprintFactoryGroovyTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(FingerprintFactoryGroovyTest.class)

    private static PropertyEncryptor mockEncryptor = [
            encrypt: { String plaintext -> plaintext.reverse() },
            decrypt: { String cipherText -> cipherText.reverse() }] as PropertyEncryptor
    private static SensitiveValueEncoder mockSensitiveValueEncoder = [
            getEncoded: { String plaintext -> "[MASKED] (${plaintext.sha256()})".toString() }] as SensitiveValueEncoder
    private static ExtensionManager extensionManager = new StandardExtensionDiscoveringManager()

    private static String originalPropertiesPath = System.getProperty(NiFiProperties.PROPERTIES_FILE_PATH)
    private static final String NIFI_PROPERTIES_PATH = "src/test/resources/conf/nifi.properties"

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

    /**
     * The flow fingerprint should not disclose sensitive property values.
     */
    @Test
    void testCreateFingerprintShouldNotDiscloseSensitivePropertyValues() {
        // Arrange
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, NIFI_PROPERTIES_PATH)

        // Create flow
        String initialFlowXML = new File("src/test/resources/nifi/fingerprint/initial.xml").text
        logger.info("Read initial flow: ${initialFlowXML[0..<100]}...")

        // Create the FingerprintFactory with collaborators
        FingerprintFactory fingerprintFactory = new FingerprintFactory(mockEncryptor, extensionManager, mockSensitiveValueEncoder)

        // Act

        // Create the fingerprint from the flow
        String fingerprint = fingerprintFactory.createFingerprint(initialFlowXML.bytes)
        logger.info("Generated flow fingerprint: ${fingerprint}")

        // Assert

        // Assert the fingerprint does not contain the password
        assert !(fingerprint =~ "originalPlaintextPassword")
        def maskedValue = (fingerprint =~ /\[MASKED\] \([\w\/\+=]+\)/)
        assert maskedValue
        logger.info("Masked value: ${maskedValue[0]}")
    }
}
