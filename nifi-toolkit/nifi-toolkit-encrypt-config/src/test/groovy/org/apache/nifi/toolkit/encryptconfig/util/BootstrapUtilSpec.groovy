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
package org.apache.nifi.toolkit.encryptconfig.util

import org.apache.nifi.toolkit.encryptconfig.TestUtil
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import spock.lang.Specification

class BootstrapUtilSpec extends Specification {
    private static final Logger logger = LoggerFactory.getLogger(BootstrapUtilSpec.class)

    // runs before every feature method
    def setup() {}

    // runs after every feature method
    def cleanup() {}

    // runs before the first feature method
    def setupSpec() {
        TestUtil.setupTmpDir()
    }

    // runs after the last feature method
    def cleanupSpec() {
        TestUtil.cleanupTmpDir()
    }

    def "test extractKeyFromBootstrapFile with Registry bootstrap.conf"() {

        setup:
        def bootstrapKeyProperty = BootstrapUtil.REGISTRY_BOOTSTRAP_KEY_PROPERTY


        when: "bootstrap.conf has no key property"
        def actualKeyHex = BootstrapUtil.extractKeyFromBootstrapFile(TestUtil.RESOURCE_REGISTRY_BOOTSTRAP_NO_KEY, bootstrapKeyProperty)

        then: "null is returned"
        actualKeyHex == null


        when: "bootstrap.conf has an empty key property"
        actualKeyHex = BootstrapUtil.extractKeyFromBootstrapFile(TestUtil.RESOURCE_REGISTRY_BOOTSTRAP_EMPTY_KEY, bootstrapKeyProperty)

        then: "null is returned"
        actualKeyHex == null


        when: "bootstrap.conf has a populated key property"
        actualKeyHex = BootstrapUtil.extractKeyFromBootstrapFile(TestUtil.RESOURCE_REGISTRY_BOOTSTRAP_KEY_128, bootstrapKeyProperty)

        then: "key is returned"
        actualKeyHex == TestUtil.KEY_HEX_128


        when: "bootstrap.conf file does not exist"
        BootstrapUtil.extractKeyFromBootstrapFile("__file_does_not_exist__", bootstrapKeyProperty)

        then: "expect an IOException"
        thrown IOException

    }

    def "test writeKeyToBootstrapFile with Registry bootstrap.conf"() {

        setup:
        def bootstrapKeyProperty = BootstrapUtil.REGISTRY_BOOTSTRAP_KEY_PROPERTY
        def outFile1 = TestUtil.generateTmpFilePath()
        def outFile2 = TestUtil.generateTmpFilePath()
        def outFile3 = TestUtil.generateTmpFilePath()
        def expected = TestUtil.RESOURCE_REGISTRY_BOOTSTRAP_KEY_128


        when: "input is default bootstrap.conf"
        BootstrapUtil.writeKeyToBootstrapFile(TestUtil.KEY_HEX_128, bootstrapKeyProperty, outFile1, TestUtil.RESOURCE_REGISTRY_BOOTSTRAP_DEFAULT)

        then: "output file content matches populated bootstrap file"
        TestUtil.assertBootstrapFilesAreEqual(expected, outFile1, true)
        and: "key is readable from output file"
        BootstrapUtil.extractKeyFromBootstrapFile(outFile1, bootstrapKeyProperty) == TestUtil.KEY_HEX_128


        when: "input bootstrap.conf has no key property"
        BootstrapUtil.writeKeyToBootstrapFile(TestUtil.KEY_HEX_128, bootstrapKeyProperty, outFile2, TestUtil.RESOURCE_REGISTRY_BOOTSTRAP_NO_KEY)

        then: "output file content matches pre-populated bootstrap file"
        TestUtil.assertBootstrapFilesAreEqual(expected, outFile2, true)


        when: "input bootstrap.conf has existing, different root key"
        BootstrapUtil.writeKeyToBootstrapFile(TestUtil.KEY_HEX_128, bootstrapKeyProperty, outFile3, TestUtil.RESOURCE_REGISTRY_BOOTSTRAP_KEY_FROM_PASSWORD_128)

        then: "output file content matches pre-populated bootstrap file"
        TestUtil.assertBootstrapFilesAreEqual(expected, outFile3, true)
    }
}
