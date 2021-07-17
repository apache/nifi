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


import org.apache.nifi.security.util.KeystoreType
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

import javax.net.ssl.SSLServerSocket
import java.security.Security

@RunWith(JUnit4.class)
class SocketUtilsTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(SocketUtilsTest.class)

    private static final String KEYSTORE_PATH = "src/test/resources/TlsConfigurationKeystore.jks"
    private static final String KEYSTORE_PASSWORD = "keystorepassword"
    private static final String KEY_PASSWORD = "keypassword"
    private static final KeystoreType KEYSTORE_TYPE = KeystoreType.JKS

    private static final String TRUSTSTORE_PATH = "src/test/resources/TlsConfigurationTruststore.jks"
    private static final String TRUSTSTORE_PASSWORD = "truststorepassword"
    private static final KeystoreType TRUSTSTORE_TYPE = KeystoreType.JKS

    private static final String PROTOCOL = TlsConfiguration.getHighestCurrentSupportedTlsProtocolVersion()

    private static final Map<String, String> DEFAULT_PROPS = [
            (NiFiProperties.SECURITY_KEYSTORE)         : KEYSTORE_PATH,
            (NiFiProperties.SECURITY_KEYSTORE_PASSWD)  : KEYSTORE_PASSWORD,
            (NiFiProperties.SECURITY_KEY_PASSWD)       : KEY_PASSWORD,
            (NiFiProperties.SECURITY_KEYSTORE_TYPE)    : KEYSTORE_TYPE.getType(),
            (NiFiProperties.SECURITY_TRUSTSTORE)       : TRUSTSTORE_PATH,
            (NiFiProperties.SECURITY_TRUSTSTORE_PASSWD): TRUSTSTORE_PASSWORD,
            (NiFiProperties.SECURITY_TRUSTSTORE_TYPE)  : TRUSTSTORE_TYPE.getType(),
    ]

    private NiFiProperties mockNiFiProperties = NiFiProperties.createBasicNiFiProperties(null, DEFAULT_PROPS)

    // A static TlsConfiguration referencing the test resource keystore and truststore
//    private static final TlsConfiguration TLS_CONFIGURATION = new StandardTlsConfiguration(KEYSTORE_PATH, KEYSTORE_PASSWORD, KEY_PASSWORD, KEYSTORE_TYPE, TRUSTSTORE_PATH, TRUSTSTORE_PASSWORD, TRUSTSTORE_TYPE, PROTOCOL)
//    private static final SSLContext sslContext = SslContextFactory.createSslContext(TLS_CONFIGURATION, ClientAuth.NONE)

    @BeforeClass
    static void setUpOnce() throws Exception {
        Security.addProvider(new BouncyCastleProvider())

        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @Before
    void setUp() {

    }

    @After
    void tearDown() {

    }

    @Test
    void testCreateSSLServerSocketShouldRestrictTlsProtocols() {
        // Arrange
        ServerSocketConfiguration mockServerSocketConfiguration = new ServerSocketConfiguration()
        mockServerSocketConfiguration.setTlsConfiguration(StandardTlsConfiguration.fromNiFiProperties(mockNiFiProperties))

        // Act
        SSLServerSocket sslServerSocket = SocketUtils.createSSLServerSocket(0, mockServerSocketConfiguration)
        logger.info("Created SSL server socket: ${sslServerSocket}")

        // Assert
        String[] enabledProtocols = sslServerSocket.getEnabledProtocols()
        logger.info("Enabled protocols: ${enabledProtocols}")
        assert enabledProtocols == TlsConfiguration.getCurrentSupportedTlsProtocolVersions()
        assert !enabledProtocols.contains("TLSv1")
        assert !enabledProtocols.contains("TLSv1.1")
    }

    @Test
    void testCreateServerSocketShouldRestrictTlsProtocols() {
        // Arrange
        ServerSocketConfiguration mockServerSocketConfiguration = new ServerSocketConfiguration()
        mockServerSocketConfiguration.setTlsConfiguration(StandardTlsConfiguration.fromNiFiProperties(mockNiFiProperties))

        // Act
        SSLServerSocket sslServerSocket = SocketUtils.createServerSocket(0, mockServerSocketConfiguration) as SSLServerSocket
        logger.info("Created SSL server socket: ${sslServerSocket}")

        // Assert
        String[] enabledProtocols = sslServerSocket.getEnabledProtocols()
        logger.info("Enabled protocols: ${enabledProtocols}")
        assert enabledProtocols == TlsConfiguration.getCurrentSupportedTlsProtocolVersions()
        assert !enabledProtocols.contains("TLSv1")
        assert !enabledProtocols.contains("TLSv1.1")
    }
}