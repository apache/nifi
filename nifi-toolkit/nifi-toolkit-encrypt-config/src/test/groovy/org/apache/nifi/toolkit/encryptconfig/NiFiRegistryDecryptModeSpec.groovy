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
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import spock.lang.Specification

import java.security.Security

import static org.apache.nifi.toolkit.encryptconfig.TestUtil.*

class NiFiRegistryDecryptModeSpec extends Specification {
    private static final Logger logger = LoggerFactory.getLogger(NiFiRegistryDecryptModeSpec.class)

    // runs before every feature method
    def setup() {}

    // runs after every feature method
    def cleanup() {}

    // runs before the first feature method
    def setupSpec() {
        Security.addProvider(new BouncyCastleProvider())
        setupTmpDir()
    }

    // runs after the last feature method
    def cleanupSpec() {
        cleanupTmpDir()
    }

    def "decrypt protected nifi-registry.properties file"() {

        setup:
        NiFiRegistryDecryptMode tool = new NiFiRegistryDecryptMode()
        def inRegistryProperties1 = copyFileToTempFile(RESOURCE_REGISTRY_PROPERTIES_POPULATED_PROTECTED_KEY_128)
        def inRegistryProperties2 = copyFileToTempFile(RESOURCE_REGISTRY_PROPERTIES_POPULATED_PROTECTED_KEY_128)
        def outRegistryProperties2 = generateTmpFilePath()
        def inRegistryProperties3 = copyFileToTempFile(RESOURCE_REGISTRY_PROPERTIES_POPULATED_PROTECTED_KEY_128)
        def outRegistryProperties3 = generateTmpFilePath()
        def inRegistryProperties4 = copyFileToTempFile(RESOURCE_REGISTRY_PROPERTIES_POPULATED_PROTECTED_KEY_128)
        def outRegistryProperties4 = generateTmpFilePath()

        when: "run with args: --oldKey <key> -r <file>"
        tool.run("--oldKey ${KEY_HEX_128} -r ${inRegistryProperties1}".split(" "))
        then: "input properties file is still encrypted (output goes to stdout)"
        assertPropertiesFilesAreEqual(RESOURCE_REGISTRY_PROPERTIES_POPULATED_PROTECTED_KEY_128, inRegistryProperties1, true)

        when: "run with args: --oldKey <key> -r <file> -R <file>"
        tool.run("--oldKey ${KEY_HEX_128} -r ${inRegistryProperties2} -R ${outRegistryProperties2}".split(" "))
        then: "output properties file is decrypted"
        assertPropertiesFilesAreEqual(RESOURCE_REGISTRY_PROPERTIES_POPULATED_UNPROTECTED, outRegistryProperties2, true)
        and: "input properties file is still encrypted"
        assertPropertiesFilesAreEqual(RESOURCE_REGISTRY_PROPERTIES_POPULATED_PROTECTED_KEY_128, inRegistryProperties2, true)

        when: "run with args: -k <key> -r <file> -R <file>"
        tool.run("-k ${KEY_HEX_128} -r ${inRegistryProperties3} -R ${outRegistryProperties3}".split(" "))
        then: "output properties file is decrypted"
        assertPropertiesFilesAreEqual(RESOURCE_REGISTRY_PROPERTIES_POPULATED_UNPROTECTED, outRegistryProperties3, true)

        when: "run with args: -b <file> -r <file> -R <file>"
        tool.run("-k ${KEY_HEX_128} -r ${inRegistryProperties4} -R ${outRegistryProperties4}".split(" "))
        then: "output properties file is decrypted"
        assertPropertiesFilesAreEqual(RESOURCE_REGISTRY_PROPERTIES_POPULATED_UNPROTECTED, outRegistryProperties4, true)

    }

}
