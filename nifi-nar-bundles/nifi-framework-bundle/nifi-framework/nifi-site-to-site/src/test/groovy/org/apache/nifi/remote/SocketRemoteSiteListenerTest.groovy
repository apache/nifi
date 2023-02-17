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
package org.apache.nifi.remote

import org.apache.nifi.security.util.KeyStoreUtils
import org.apache.nifi.security.util.KeystoreType
import org.apache.nifi.security.util.SslContextFactory
import org.apache.nifi.security.util.StandardTlsConfiguration
import org.apache.nifi.security.util.TlsConfiguration
import org.apache.nifi.util.NiFiProperties
import org.bouncycastle.jce.provider.BouncyCastleProvider
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.net.ssl.SSLContext
import javax.net.ssl.SSLServerSocket
import java.security.Security

@RunWith(JUnit4.class)
class SocketRemoteSiteListenerTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(SocketRemoteSiteListenerTest.class)

    private static final String KEYSTORE_PATH = "src/test/resources/localhost-ks.jks"
    private static final String KEYSTORE_PASSWORD = "OI7kMpWzzVNVx/JGhTL/0uO4+PWpGJ46uZ/pfepbkwI"
    private static final KeystoreType KEYSTORE_TYPE = KeystoreType.JKS

    private static final String TRUSTSTORE_PATH = "src/test/resources/localhost-ts.jks"
    private static final String TRUSTSTORE_PASSWORD = "wAOR0nQJ2EXvOP0JZ2EaqA/n7W69ILS4sWAHghmIWCc"
    private static final KeystoreType TRUSTSTORE_TYPE = KeystoreType.JKS

    private static final String HOSTNAME = "localhost"
    private static final int PORT = 0

    // The nifi.properties in src/test/resources has 0.x properties and should be removed or updated
    private static final Map<String, String> DEFAULT_PROPS = [
            (NiFiProperties.SECURITY_KEYSTORE)         : KEYSTORE_PATH,
            (NiFiProperties.SECURITY_KEYSTORE_PASSWD)  : KEYSTORE_PASSWORD,
            (NiFiProperties.SECURITY_KEYSTORE_TYPE)    : KEYSTORE_TYPE.getType(),
            (NiFiProperties.SECURITY_TRUSTSTORE)       : TRUSTSTORE_PATH,
            (NiFiProperties.SECURITY_TRUSTSTORE_PASSWD): TRUSTSTORE_PASSWORD,
            (NiFiProperties.SECURITY_TRUSTSTORE_TYPE)  : TRUSTSTORE_TYPE.getType(),
            (NiFiProperties.REMOTE_INPUT_HOST): HOSTNAME,
            (NiFiProperties.REMOTE_INPUT_PORT): PORT as String,
            "nifi.remote.input.secure": "true"
    ]

    private NiFiProperties mockNiFiProperties = NiFiProperties.createBasicNiFiProperties("", DEFAULT_PROPS)

    private static TlsConfiguration tlsConfiguration
    private static SSLContext sslContext

    private SocketRemoteSiteListener srsListener

    @BeforeClass
    static void setUpOnce() throws Exception {
        Security.addProvider(new BouncyCastleProvider())

        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }

        tlsConfiguration = new StandardTlsConfiguration(KEYSTORE_PATH, KEYSTORE_PASSWORD, KEYSTORE_TYPE, TRUSTSTORE_PATH, TRUSTSTORE_PASSWORD, TRUSTSTORE_TYPE)
        sslContext = SslContextFactory.createSslContext(tlsConfiguration)
    }

    @Before
    void setUp() {
    }

    @After
    void tearDown() {
        if (srsListener) {
            srsListener.stop()
        }
    }

    /**
     * Asserts that the protocol versions in the parameters object are correct. In recent versions of Java, this enforces order as well, but in older versions, it just enforces presence.
     *
     * @param enabledProtocols the actual protocols, either in {@code String[]} or {@code Collection<String>} form
     * @param expectedProtocols the specific protocol versions to be present (ordered as desired)
     */
    void assertProtocolVersions(def enabledProtocols, def expectedProtocols) {
        if (TlsConfiguration.getJavaVersion() > 8) {
            assert enabledProtocols == expectedProtocols as String[]
        } else {
            assert enabledProtocols as Set == expectedProtocols as Set
        }
    }

    @Test
    void testShouldCreateSecureServer() {
        // Arrange
        logger.info("Creating SSL Context from TLS Configuration: ${tlsConfiguration}")
        SSLContext sslContext = SslContextFactory.createSslContext(tlsConfiguration)
        logger.info("Created SSL Context: ${KeyStoreUtils.sslContextToString(sslContext)}")

        srsListener = new SocketRemoteSiteListener(PORT, sslContext, mockNiFiProperties)

        // Act
        srsListener.start()

        // Assert

        // serverSocket isn't instance field like CLBS so have to use private method invocation to verify
        SSLServerSocket sslServerSocket = srsListener.createServerSocket() as SSLServerSocket
        logger.info("Created SSL server socket: ${KeyStoreUtils.sslServerSocketToString(sslServerSocket)}" as String)
        assertProtocolVersions(sslServerSocket.enabledProtocols, TlsConfiguration.getCurrentSupportedTlsProtocolVersions())
        assert sslServerSocket.needClientAuth
    }
}
