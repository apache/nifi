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
package org.apache.nifi.security.util


import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@RunWith(JUnit4.class)
class TlsConfigurationTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(TlsConfigurationTest.class)

    @BeforeClass
    static void setUpOnce() {
        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @Before
    void setUp() {
        super.setUp()

    }

    @After
    void tearDown() {

    }

    @Test
    void testShouldParseJavaVersion() {
        // Arrange
        def possibleVersions = ["1.5.0", "1.6.0", "1.7.0.123", "1.8.0.231", "9.0.1", "10.1.2", "11.2.3", "12.3.456"]

        // Act
        def majorVersions = possibleVersions.collect { String version ->
            logger.debug("Attempting to determine major version of ${version}")
            TlsConfiguration.parseJavaVersion(version)
        }
        logger.info("Major versions: ${majorVersions}")

        // Assert
        assert majorVersions == (5..12)
    }

    @Test
    void testShouldGetCurrentSupportedTlsProtocolVersions() {
        // Arrange
        int javaMajorVersion = TlsConfiguration.getJavaVersion()
        logger.debug("Running on Java version: ${javaMajorVersion}")

        // Act
        def tlsVersions = TlsConfiguration.getCurrentSupportedTlsProtocolVersions()
        logger.info("Supported protocol versions for ${javaMajorVersion}: ${tlsVersions}")

        // Assert
        if (javaMajorVersion < 11) {
            assert tlsVersions == ["TLSv1.2"] as String[]
        } else {
            assert tlsVersions == ["TLSv1.3", "TLSv1.2"] as String[]
        }
    }

    @Test
    void testShouldGetMaxCurrentSupportedTlsProtocolVersion() {
        // Arrange
        int javaMajorVersion = TlsConfiguration.getJavaVersion()
        logger.debug("Running on Java version: ${javaMajorVersion}")

        // Act
        def tlsVersion = TlsConfiguration.getHighestCurrentSupportedTlsProtocolVersion()
        logger.info("Highest supported protocol version for ${javaMajorVersion}: ${tlsVersion}")

        // Assert
        if (javaMajorVersion < 11) {
            assert tlsVersion == "TLSv1.2"
        } else {
            assert tlsVersion == "TLSv1.3"
        }
    }
}
