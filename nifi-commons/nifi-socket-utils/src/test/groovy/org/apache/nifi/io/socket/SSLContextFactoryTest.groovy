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
import sun.security.ssl.SSLContextImpl
import sun.security.ssl.SunX509KeyManagerImpl

import javax.net.ssl.SSLContext
import javax.net.ssl.SSLEngine
import javax.security.cert.Certificate
import java.security.Security
import java.security.cert.X509Certificate

@RunWith(JUnit4.class)
class SSLContextFactoryTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(SSLContextFactoryTest.class)

    private static String NF_PROPS_FILE = null
    private final String DEFAULT_ALIAS = "nifi"

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

    private static NiFiProperties buildNiFiProperties(Map<String, String> overrides = [:]) {
        final Map DEFAULTS = [
                (NiFiProperties.SECURITY_KEYSTORE_PASSWD): "keystorepassword",
                (NiFiProperties.SECURITY_KEYSTORE)       : "src/test/resources/samepassword.jks",
                (NiFiProperties.SECURITY_KEYSTORE_TYPE)  : "JKS",
                (NiFiProperties.SECURITY_TRUSTSTORE_PASSWD): "changeit",
                (NiFiProperties.SECURITY_TRUSTSTORE)       : System.getenv("JAVA_HOME") + "/jre/lib/security/cacerts",
                (NiFiProperties.SECURITY_TRUSTSTORE_TYPE)  : "JKS",
        ]
        DEFAULTS.putAll(overrides)
        NiFiProperties.createBasicNiFiProperties(null, DEFAULTS)
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
        SSLContext sslContext =  sslcf.createSslContext()

//        SSLEngine sslEngine = sslContext.createSSLEngine("localhost", 8443)
//
//        // Create connection to this SSLEngine
//
//        URL url = new URL("https://localhost:8443")
//        URLConnection urlConnection = url.openConnection()
//        urlConnection.connect()



//        SSLContextImpl sslci = sslContext.contextSpi as SSLContextImpl
//        SunX509KeyManagerImpl km = (sslci as SSLContextImpl).@keyManager
//        List<Certificate> certificates =  km.getCertificateChain(DEFAULT_ALIAS) as List<Certificate>

        // Assert

        // The SSLContext was accessible and correct
        assert sslContext
//        assert certificates.size() > 0
//        X509Certificate x509Certificate = certificates.first() as X509Certificate
//        assert x509Certificate.getSubjectX500Principal().getName() == "CN=test, OU=nifi, O=apache, City=los angeles, ST=CA, C=US"
    }

    @Test
    void testShouldVerifyKeystoreWithDifferentKeyPassword() throws Exception {
        // Arrange

        // Set up the keystore configuration as NiFiProperties object
        NiFiProperties np = buildNiFiProperties([
                (NiFiProperties.SECURITY_KEYSTORE): "src/test/resources/differentpassword.jks",
                (NiFiProperties.SECURITY_KEY_PASSWD): "keypassword",
        ])

        // Create the SSLContextFactory with the config
        SSLContextFactory sslcf = new SSLContextFactory(np)

        // Act

        // Access the SSLContextFactory to create an SSLContext
        SSLContext sslContext =  sslcf.createSslContext()

        // Assert

        // The SSLContext was accessible and correct
        assert sslContext
    }
}
