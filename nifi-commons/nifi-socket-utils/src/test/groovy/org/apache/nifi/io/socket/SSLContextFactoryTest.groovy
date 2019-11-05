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
package org.apache.nifi.io.socket

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

import javax.net.ssl.SSLContext
import java.security.Security

@RunWith(JUnit4.class)
class SSLContextFactoryTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(SSLContextFactoryTest.class)

    private static String NF_PROPS_FILE = null

    @BeforeClass
    static void setUpOnce() throws Exception {
        Security.addProvider(new BouncyCastleProvider())

        if (System.getProperty(NiFiProperties.PROPERTIES_FILE_PATH)) {
            NF_PROPS_FILE = System.getProperty(NiFiProperties.PROPERTIES_FILE_PATH)
            System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, null)
        }

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
    static void tearDownOnce() throws Exception {
        if (NF_PROPS_FILE) {
            System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, NF_PROPS_FILE)
        }
    }

    /**
     * Returns a {@link NiFiProperties} object configured with default values for accessing
     * keystores and truststores. The values can be overridden by providing a map parameter.
     *
     * @param overrides an optional Map of overriding configuration values
     * @return the configured NiFiProperties object
     */
    private static NiFiProperties buildNiFiProperties(Map<String, String> overrides = [:]) {
        final Map DEFAULTS = [
                (NiFiProperties.SECURITY_KEYSTORE_PASSWD)  : "keystorepassword",
                (NiFiProperties.SECURITY_KEYSTORE)         : "src/test/resources/samepassword.jks",
                (NiFiProperties.SECURITY_KEYSTORE_TYPE)    : "JKS",
                (NiFiProperties.SECURITY_TRUSTSTORE_PASSWD): "changeit",
                (NiFiProperties.SECURITY_TRUSTSTORE)       : buildCacertsPath(),
                (NiFiProperties.SECURITY_TRUSTSTORE_TYPE)  : "JKS",
        ]
        DEFAULTS.putAll(overrides)
        NiFiProperties.createBasicNiFiProperties(null, DEFAULTS)
    }

    /**
     * Returns the file path to the {@code cacerts} default JRE truststore. Handles Java 8
     * and earlier as well as Java 9 and later directory structures.
     *
     * @return the path to cacerts
     */
    private static String buildCacertsPath() {
        String javaHome = System.getenv("JAVA_HOME")
        if (System.getProperty("java.version").startsWith("1.")) {
            javaHome + "/jre/lib/security/cacerts"
        } else {
            javaHome + "/lib/security/cacerts"
        }
    }

    @Test
    void testShouldVerifyKeystoreWithSameKeyPassword() throws Exception {
        // Arrange

        // Set up the keystore configuration as NiFiProperties object
        NiFiProperties np = buildNiFiProperties()

        // Create the SSLContextFactory with the config
        SSLContextFactory sslcf = new SSLContextFactory(np)

        // Act

        // Access the SSLContextFactory to create an SSLContext
        SSLContext sslContext = sslcf.createSslContext()

        // Assert

        // The SSLContext was accessible and correct
        assert sslContext
    }

    @Test
    void testShouldVerifyKeystoreWithDifferentKeyPassword() throws Exception {
        // Arrange

        // Set up the keystore configuration as NiFiProperties object
        // (prior to NIFI-6830, an UnrecoverableKeyException was thrown due to the wrong password being provided)
        NiFiProperties np = buildNiFiProperties([
                (NiFiProperties.SECURITY_KEYSTORE)  : "src/test/resources/differentpassword.jks",
                (NiFiProperties.SECURITY_KEY_PASSWD): "keypassword",
        ])

        // Create the SSLContextFactory with the config
        SSLContextFactory sslcf = new SSLContextFactory(np)

        // Act

        // Access the SSLContextFactory to create an SSLContext
        SSLContext sslContext = sslcf.createSslContext()

        // Assert

        // The SSLContext was accessible and correct
        assert sslContext
    }
}
