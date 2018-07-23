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

import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.junit.Assume
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import spock.lang.Specification

import java.security.Security

import static org.apache.nifi.toolkit.encryptconfig.TestUtil.*

class NiFiRegistryDecryptModeSpec extends Specification {
    private static final Logger logger = LoggerFactory.getLogger(NiFiRegistryDecryptModeSpec.class)

    ByteArrayOutputStream toolStdOutContent
    PrintStream origSystemOut

    // runs before every feature method
    def setup() {
        origSystemOut = System.out
        toolStdOutContent = new ByteArrayOutputStream();
        System.setOut(new PrintStream(toolStdOutContent));
    }

    // runs after every feature method
    def cleanup() {
        toolStdOutContent.flush()
        System.setOut(origSystemOut);
        toolStdOutContent.close()
    }

    // runs before the first feature method
    def setupSpec() {
        Security.addProvider(new BouncyCastleProvider())
        setupTmpDir()
    }

    // runs after the last feature method
    def cleanupSpec() {
        cleanupTmpDir()
    }

    def "decrypt protected nifi-registry.properties file using -k"() {

        setup:
        NiFiRegistryDecryptMode tool = new NiFiRegistryDecryptMode()
        def inRegistryProperties1 = copyFileToTempFile(RESOURCE_REGISTRY_PROPERTIES_POPULATED_PROTECTED_KEY_128)
        File outRegistryProperties1 = generateTmpFile()

        when: "run with args: -k <key> -r <file>"
        tool.run("-k ${KEY_HEX_128} -r ${inRegistryProperties1}".split(" "))
        toolStdOutContent.flush()
        outRegistryProperties1.text = toolStdOutContent.toString()
        then: "decrypted properties file was printed to std out"
        assertPropertiesFilesAreEqual(RESOURCE_REGISTRY_PROPERTIES_POPULATED_UNPROTECTED, outRegistryProperties1.getAbsolutePath(), true)
        and: "input properties file is still encrypted"
        assertPropertiesFilesAreEqual(RESOURCE_REGISTRY_PROPERTIES_POPULATED_PROTECTED_KEY_128, inRegistryProperties1, true)

    }

    def "decrypt protected nifi-registry.properties file using -p [256-bit]"() {

        Assume.assumeTrue("Test only runs when unlimited strength crypto is available", isUnlimitedStrengthCryptoAvailable())

        setup:
        NiFiRegistryDecryptMode tool = new NiFiRegistryDecryptMode()
        def inRegistryProperties1 = copyFileToTempFile(RESOURCE_REGISTRY_PROPERTIES_POPULATED_PROTECTED_PASSWORD_256)
        File outRegistryProperties1 = generateTmpFile()

        when: "run with args: -p <password> -r <file>"
        tool.run("-p ${PASSWORD} -r ${inRegistryProperties1}".split(" "))
        toolStdOutContent.flush()
        outRegistryProperties1.text = toolStdOutContent.toString()
        then: "decrypted properties file was printed to std out"
        assertPropertiesFilesAreEqual(RESOURCE_REGISTRY_PROPERTIES_POPULATED_UNPROTECTED, outRegistryProperties1.getAbsolutePath(), true)
        and: "input properties file is still encrypted"
        assertPropertiesFilesAreEqual(RESOURCE_REGISTRY_PROPERTIES_POPULATED_PROTECTED_PASSWORD_256, inRegistryProperties1, true)

    }

    def "decrypt protected nifi-registry.properties file using -b"() {

        setup:
        NiFiRegistryDecryptMode tool = new NiFiRegistryDecryptMode()
        def inRegistryProperties = copyFileToTempFile(RESOURCE_REGISTRY_PROPERTIES_POPULATED_PROTECTED_KEY_128)
        def inBootstrap = copyFileToTempFile(RESOURCE_REGISTRY_BOOTSTRAP_KEY_128)
        File outRegistryProperties = generateTmpFile()

        when: "run with args: -b <file> -r <file>"
        tool.run("-b ${inBootstrap} -r ${inRegistryProperties}".split(" "))
        toolStdOutContent.flush()
        outRegistryProperties.text = toolStdOutContent.toString()
        then: "decrypted properties file was printed to std out"
        assertPropertiesFilesAreEqual(RESOURCE_REGISTRY_PROPERTIES_POPULATED_UNPROTECTED, outRegistryProperties.getAbsolutePath(), true)
        and: "input properties file is still encrypted"
        assertPropertiesFilesAreEqual(RESOURCE_REGISTRY_PROPERTIES_POPULATED_PROTECTED_KEY_128, inRegistryProperties, true)

    }

}
