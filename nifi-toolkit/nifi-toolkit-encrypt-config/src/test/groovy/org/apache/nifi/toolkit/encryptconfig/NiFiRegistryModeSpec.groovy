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

import org.apache.nifi.toolkit.encryptconfig.util.BootstrapUtil
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import spock.lang.Specification

import java.security.Security

import static org.apache.nifi.toolkit.encryptconfig.TestUtil.*

class NiFiRegistryModeSpec extends Specification {
    private static final Logger logger = LoggerFactory.getLogger(NiFiRegistryModeSpec.class)

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

    def "writing key to bootstrap.conf file"() {

        setup:
        NiFiRegistryMode tool = new NiFiRegistryMode()
        def inBootstrapConf1 = copyFileToTempFile(RESOURCE_REGISTRY_BOOTSTRAP_DEFAULT)
        def inBootstrapConf2 = copyFileToTempFile(RESOURCE_REGISTRY_BOOTSTRAP_DEFAULT)
        def inBootstrapConf3 = copyFileToTempFile(RESOURCE_REGISTRY_BOOTSTRAP_DEFAULT)
        def outBootstrapConf3 = generateTmpFilePath()
        def inBootstrapConf4 = copyFileToTempFile(RESOURCE_REGISTRY_BOOTSTRAP_DEFAULT)
        def outBootstrapConf4 = generateTmpFilePath()
        def inBootstrapConf5 = copyFileToTempFile(RESOURCE_REGISTRY_BOOTSTRAP_KEY_128)
        def outBootstrapConf5 = generateTmpFilePath()

        when: "run with args: -k <key> -b <file>"
        tool.run("-k ${KEY_HEX_128} -b ${inBootstrapConf1}".split(" "))
        then: "key is written to input bootstrap.conf"
        assertBootstrapFilesAreEqual(RESOURCE_REGISTRY_BOOTSTRAP_KEY_128, inBootstrapConf1, true)

        when: "run with args: -p <password> -b <file>"
        tool.run("-p ${PASSWORD} -b ${inBootstrapConf2}".split(" "))
        then: "key derived from password is written to input bootstrap.conf"
        PASSWORD_KEY_HEX == readKeyFromBootstrap(inBootstrapConf2)

        when: "run with args: -k <key> -b <file> -B <file>"
        tool.run("-k ${KEY_HEX_128} -b ${inBootstrapConf3} -B ${outBootstrapConf3}".split(" "))
        then: "key is written to output bootstrap.conf"
        assertBootstrapFilesAreEqual(RESOURCE_REGISTRY_BOOTSTRAP_KEY_128, outBootstrapConf3, true)
        and: "input bootstrap.conf is unchanged"
        assertBootstrapFilesAreEqual(RESOURCE_REGISTRY_BOOTSTRAP_DEFAULT, inBootstrapConf3, true)

        when: "run with args: -p <key> -b <file> -B <file>"
        tool.run("-p ${PASSWORD} -b ${inBootstrapConf4} -B ${outBootstrapConf4}".split(" "))
        then: "key derived from password is written to output bootstrap.conf"
        PASSWORD_KEY_HEX == readKeyFromBootstrap(outBootstrapConf4)
        and: "input bootstrap.conf is unchanged"
        assertBootstrapFilesAreEqual(RESOURCE_REGISTRY_BOOTSTRAP_DEFAULT, inBootstrapConf4, true)

        when: "run with args: -b <file> -B <file>"
        tool.run("-b ${inBootstrapConf5} -B ${outBootstrapConf5}".split(" "))
        then: "key from input file is copied to output file"
        KEY_HEX_128 == readKeyFromBootstrap(outBootstrapConf5)
        assertBootstrapFilesAreEqual(inBootstrapConf5, outBootstrapConf5, true)
        and: "input bootstrap.conf is unchanged"
        assertBootstrapFilesAreEqual(RESOURCE_REGISTRY_BOOTSTRAP_KEY_128, inBootstrapConf5, true)

    }

    def "encrypt unprotected nifi-registry.properties file"() {

        setup:
        NiFiRegistryMode tool = new NiFiRegistryMode()
        def inBootstrapConf1 = copyFileToTempFile(RESOURCE_REGISTRY_BOOTSTRAP_DEFAULT)
        def inRegistryProperties1 = copyFileToTempFile(RESOURCE_REGISTRY_PROPERTIES_POPULATED_UNPROTECTED)
        def inBootstrapConf2 = copyFileToTempFile(RESOURCE_REGISTRY_BOOTSTRAP_DEFAULT)
        def inRegistryProperties2 = copyFileToTempFile(RESOURCE_REGISTRY_PROPERTIES_POPULATED_UNPROTECTED)
        def outRegistryProperties2 = generateTmpFilePath()
        def inBootstrapConf3 = copyFileToTempFile(RESOURCE_REGISTRY_BOOTSTRAP_DEFAULT)
        def inRegistryProperties3 = copyFileToTempFile(RESOURCE_REGISTRY_PROPERTIES_POPULATED_UNPROTECTED)
        def inRegistryProperties4 = copyFileToTempFile(RESOURCE_REGISTRY_PROPERTIES_POPULATED_UNPROTECTED)

        when: "run with args: -k <key> -b <file> -r <file>"
        tool.run("-k ${KEY_HEX} -b ${inBootstrapConf1} -r ${inRegistryProperties1}".split(" "))
        then: "properties file is protected in place"
        assertNiFiRegistryUnprotectedPropertiesAreProtected(inRegistryProperties1)
        and: "key is written to input bootstrap.conf"
        KEY_HEX == readKeyFromBootstrap(inBootstrapConf1)

        when: "run with args: -k <key> -b <file> -r <file> -R <file>"
        tool.run("-k ${KEY_HEX} -b ${inBootstrapConf2} -r ${inRegistryProperties2} -R ${outRegistryProperties2}".split(" "))
        then: "output properties file is protected"
        assertNiFiRegistryUnprotectedPropertiesAreProtected(outRegistryProperties2)
        and: "input properties file is unchanged"
        assertPropertiesFilesAreEqual(RESOURCE_REGISTRY_PROPERTIES_POPULATED_UNPROTECTED, inRegistryProperties2, true)
        and: "key is written to output bootstrap.conf"
        KEY_HEX == readKeyFromBootstrap(inBootstrapConf2)

        when: "run with args: -p <password> -b <file> -r <file>"
        tool.run("-p ${PASSWORD} -b ${inBootstrapConf3} -r ${inRegistryProperties3}".split(" "))
        then: "properties file is protected in place"
        assertNiFiRegistryUnprotectedPropertiesAreProtected(inRegistryProperties3)
        and: "key is written to input bootstrap.conf"
        PASSWORD_KEY_HEX == readKeyFromBootstrap(inBootstrapConf3)

        when: "run with args: -b <file_with_key> -r <file>"
        tool.run("-b ${RESOURCE_REGISTRY_BOOTSTRAP_KEY_128} -r ${inRegistryProperties4}".split(" "))
        then: "properties file is protected in place using key from bootstrap"
        assertNiFiRegistryUnprotectedPropertiesAreProtected(inRegistryProperties4, PROTECTION_SCHEME_128)

    }

    def "encrypt nifi-registry.properties with no sensitive properties is a no-op"() {

        setup:
        NiFiRegistryMode tool = new NiFiRegistryMode()
        def inBootstrapConf1 = copyFileToTempFile(RESOURCE_REGISTRY_BOOTSTRAP_DEFAULT)
        def inRegistryProperties1 = copyFileToTempFile(RESOURCE_REGISTRY_PROPERTIES_COMMENTED)
        def inBootstrapConf2 = copyFileToTempFile(RESOURCE_REGISTRY_BOOTSTRAP_DEFAULT)
        def inRegistryProperties2 = copyFileToTempFile(RESOURCE_REGISTRY_PROPERTIES_EMPTY)
        def outRegistryProperties2 = generateTmpFilePath()

        when: "run with args: -k <key> -b <file> -r <file_with_no_sensitive_props>"
        tool.run("-k ${KEY_HEX} -b ${inBootstrapConf1} -r ${inRegistryProperties1}".split(" "))
        then: "properties file is unchanged"
        assertPropertiesFilesAreEqual(RESOURCE_REGISTRY_PROPERTIES_COMMENTED, inRegistryProperties1, true)
        and: "key is written to input bootstrap.conf"
        KEY_HEX == readKeyFromBootstrap(inBootstrapConf1)

        when: "run with args: -k <key> -b <file> -r <file_with_empty_sensitive_props> -R <file>"
        tool.run("-k ${KEY_HEX} -b ${inBootstrapConf2} -r ${inRegistryProperties2} -R ${outRegistryProperties2}".split(" "))
        then: "input properties file is unchanged and output properties file matches input"
        assertPropertiesFilesAreEqual(RESOURCE_REGISTRY_PROPERTIES_EMPTY, inRegistryProperties2, true)
        assertPropertiesFilesAreEqual(inRegistryProperties2, outRegistryProperties2, true)
        and: "key is written to output bootstrap.conf"
        KEY_HEX == readKeyFromBootstrap(inBootstrapConf2)

    }

    def "encrypt unprotected authorizers.xml file"() {

        setup:
        NiFiRegistryMode tool = new NiFiRegistryMode()
        def inBootstrapConf1 = copyFileToTempFile(RESOURCE_REGISTRY_BOOTSTRAP_DEFAULT)
        def inAuthorizers1 = copyFileToTempFile(RESOURCE_REGISTRY_AUTHORIZERS_POPULATED_UNPROTECTED)
        def inBootstrapConf2 = copyFileToTempFile(RESOURCE_REGISTRY_BOOTSTRAP_DEFAULT)
        def inAuthorizers2 = copyFileToTempFile(RESOURCE_REGISTRY_AUTHORIZERS_POPULATED_UNPROTECTED)
        def outAuthorizers2 = generateTmpFilePath()

        when: "run with args: -k <key> -b <file> -a <file>"
        tool.run("-k ${KEY_HEX} -b ${inBootstrapConf1} -a ${inAuthorizers1}".split(" "))
        then: "authorizers file is protected in place"
        assertRegistryAuthorizersXmlIsProtected(inAuthorizers1)
        and: "key is written to input bootstrap.conf"
        KEY_HEX == readKeyFromBootstrap(inBootstrapConf1)

        when: "run with args: -k <key> -b <file> -a <file> -A <file>"
        tool.run("-k ${KEY_HEX} -b ${inBootstrapConf2} -a ${inAuthorizers2} -A ${outAuthorizers2}".split(" "))
        then: "authorizers file is protected in place"
        assertRegistryAuthorizersXmlIsProtected(outAuthorizers2)
        and: "key is written to input bootstrap.conf"
        KEY_HEX == readKeyFromBootstrap(inBootstrapConf2)

    }

    def "encrypt authorizers.xml with no sensitive properties is a no-op"() {

        setup:
        NiFiRegistryMode tool = new NiFiRegistryMode()
        def inBootstrapConf1 = copyFileToTempFile(RESOURCE_REGISTRY_BOOTSTRAP_DEFAULT)
        def inAuthorizersXml1 = copyFileToTempFile(RESOURCE_REGISTRY_AUTHORIZERS_COMMENTED)
        def inBootstrapConf2 = copyFileToTempFile(RESOURCE_REGISTRY_BOOTSTRAP_DEFAULT)
        def inAuthorizersXml2 = copyFileToTempFile(RESOURCE_REGISTRY_AUTHORIZERS_EMPTY)
        def outAuthorizers2 = generateTmpFilePath()

        when: "run with args: -k <key> -b <file> -a <file_with_no_sensitive_props>"
        tool.run("-k ${KEY_HEX} -b ${inBootstrapConf1} -a ${inAuthorizersXml1}".split(" "))
        then: "authorizers file is unchanged"
        assertFilesAreEqual(RESOURCE_REGISTRY_AUTHORIZERS_COMMENTED, inAuthorizersXml1)
        and: "key is written to input bootstrap.conf"
        KEY_HEX == readKeyFromBootstrap(inBootstrapConf1)

        when: "run with args: -k <key> -b <file> -a <file_with_empty_sensitive_props> -A <file>"
        tool.run("-k ${KEY_HEX} -b ${inBootstrapConf2} -a ${inAuthorizersXml2} -A ${outAuthorizers2}".split(" "))
        then: "input authorizers file is unchanged and output authorizers matches input"
        assertFilesAreEqual(RESOURCE_REGISTRY_AUTHORIZERS_EMPTY, inAuthorizersXml2)
        assertFilesAreEqual(inAuthorizersXml2, outAuthorizers2)
        and: "key is written to output bootstrap.conf"
        KEY_HEX == readKeyFromBootstrap(inBootstrapConf2)

    }

    def "encrypt unprotected identity-providers.xml file"() {

        setup:
        NiFiRegistryMode tool = new NiFiRegistryMode()
        def inBootstrapConf1 = copyFileToTempFile(RESOURCE_REGISTRY_BOOTSTRAP_DEFAULT)
        def inIdentityProviders1 = copyFileToTempFile(RESOURCE_REGISTRY_IDENTITY_PROVIDERS_POPULATED_UNPROTECTED)
        def inBootstrapConf2 = copyFileToTempFile(RESOURCE_REGISTRY_BOOTSTRAP_DEFAULT)
        def inIdentityProviders2 = copyFileToTempFile(RESOURCE_REGISTRY_IDENTITY_PROVIDERS_POPULATED_UNPROTECTED)
        def outIdentityProviders2 = generateTmpFilePath()

        when: "run with args: -k <key> -b <file> -i <file>"
        tool.run("-k ${KEY_HEX} -b ${inBootstrapConf1} -i ${inIdentityProviders1}".split(" "))
        then: "identity providers file is protected in place"
        assertRegistryIdentityProvidersXmlIsProtected(inIdentityProviders1)
        and: "key is written to input bootstrap.conf"
        KEY_HEX == readKeyFromBootstrap(inBootstrapConf1)

        when: "run with args: -k <key> -b <file> -i <file> -I <file>"
        tool.run("-k ${KEY_HEX} -b ${inBootstrapConf2} -i ${inIdentityProviders2} -I ${outIdentityProviders2}".split(" "))
        then: "identity providers file is protected in place"
        assertRegistryIdentityProvidersXmlIsProtected(outIdentityProviders2)
        and: "key is written to input bootstrap.conf"
        KEY_HEX == readKeyFromBootstrap(inBootstrapConf2)

    }

    def "encrypt identity-providers.xml with no sensitive properties is a no-op"() {

        setup:
        NiFiRegistryMode tool = new NiFiRegistryMode()
        def inBootstrapConf1 = copyFileToTempFile(RESOURCE_REGISTRY_BOOTSTRAP_DEFAULT)
        def inIdentityProviders1 = copyFileToTempFile(RESOURCE_REGISTRY_IDENTITY_PROVIDERS_COMMENTED)
        def inBootstrapConf2 = copyFileToTempFile(RESOURCE_REGISTRY_BOOTSTRAP_DEFAULT)
        def inIdentityProviders2 = copyFileToTempFile(RESOURCE_REGISTRY_IDENTITY_PROVIDERS_EMPTY)
        def outIdentityProviders2 = generateTmpFilePath()

        when: "run with args: -k <key> -b <file> -i <file_with_no_sensitive_props>"
        tool.run("-k ${KEY_HEX} -b ${inBootstrapConf1} -i ${inIdentityProviders1}".split(" "))
        then: "identity providers file is unchanged"
        assertFilesAreEqual(RESOURCE_REGISTRY_IDENTITY_PROVIDERS_COMMENTED, inIdentityProviders1)
        and: "key is written to input bootstrap.conf"
        KEY_HEX == readKeyFromBootstrap(inBootstrapConf1)

        when: "run with args: -k <key> -b <file> -i <file_with_empty_sensitive_props> -I <file>"
        tool.run("-k ${KEY_HEX} -b ${inBootstrapConf2} -i ${inIdentityProviders2} -I ${outIdentityProviders2}".split(" "))
        then: "identity providers file is unchanged and output identity providers matches input"
        assertFilesAreEqual(RESOURCE_REGISTRY_IDENTITY_PROVIDERS_EMPTY, inIdentityProviders2)
        assertFilesAreEqual(inIdentityProviders2, outIdentityProviders2)
        and: "key is written to output bootstrap.conf"
        KEY_HEX == readKeyFromBootstrap(inBootstrapConf2)

    }

    def "encrypt full configuration with properties, authorizers, and identity providers"() {

        setup:
        NiFiRegistryMode tool = new NiFiRegistryMode()
        def inBootstrapConf1 = copyFileToTempFile(RESOURCE_REGISTRY_BOOTSTRAP_DEFAULT)
        def inRegistryProperties1 = copyFileToTempFile(RESOURCE_REGISTRY_PROPERTIES_POPULATED_UNPROTECTED)
        def inAuthorizers1 = copyFileToTempFile(RESOURCE_REGISTRY_AUTHORIZERS_POPULATED_UNPROTECTED)
        def inIdentityProviders1 = copyFileToTempFile(RESOURCE_REGISTRY_IDENTITY_PROVIDERS_POPULATED_UNPROTECTED)

        when: "run with args: -k <key> -b <file> -r <file> -a <file_with_no_sensitive_props> -i <file_with_no_sensitive_props>"
        tool.run("-k ${KEY_HEX} -b ${inBootstrapConf1} -r ${inRegistryProperties1} -a ${inAuthorizers1} -i ${inIdentityProviders1}".split(" "))
        then: "all files are protected"
        assertNiFiRegistryUnprotectedPropertiesAreProtected(inRegistryProperties1)
        assertRegistryAuthorizersXmlIsProtected(inAuthorizers1)
        assertRegistryIdentityProvidersXmlIsProtected(inIdentityProviders1)
        and: "key is written to input bootstrap.conf"
        KEY_HEX == readKeyFromBootstrap(inBootstrapConf1)

    }
    
    //-- Helper Methods

    private static String readKeyFromBootstrap(String bootstrapPath) {
        return BootstrapUtil.extractKeyFromBootstrapFile(bootstrapPath, BootstrapUtil.REGISTRY_BOOTSTRAP_KEY_PROPERTY)
    }

    private static boolean assertNiFiRegistryUnprotectedPropertiesAreProtected(
            String pathToProtectedProperties,
            String expectedProtectionScheme = PROTECTION_SCHEME) {
        return assertPropertiesAreProtected(
                RESOURCE_REGISTRY_PROPERTIES_POPULATED_UNPROTECTED,
                pathToProtectedProperties,
                RESOURCE_REGISTRY_PROPERTIES_SENSITIVE_PROPS,
                expectedProtectionScheme)
    }

    static boolean assertRegistryAuthorizersXmlIsProtected(
            String pathToProtectedXmlToVerify,
            String expectedProtectionScheme = PROTECTION_SCHEME,
            String expectedKey = KEY_HEX) {
        return assertRegistryAuthorizersXmlIsProtected(
                RESOURCE_REGISTRY_AUTHORIZERS_POPULATED_UNPROTECTED,
                pathToProtectedXmlToVerify,
                expectedProtectionScheme,
                expectedKey)
    }

    static boolean assertRegistryIdentityProvidersXmlIsProtected(
            String pathToProtectedXmlToVerify,
            String expectedProtectionScheme = PROTECTION_SCHEME,
            String expectedKey = KEY_HEX) {
        return assertRegistryIdentityProvidersXmlIsProtected(
                RESOURCE_REGISTRY_IDENTITY_PROVIDERS_POPULATED_UNPROTECTED,
                pathToProtectedXmlToVerify,
                expectedProtectionScheme,
                expectedKey)
    }


}
