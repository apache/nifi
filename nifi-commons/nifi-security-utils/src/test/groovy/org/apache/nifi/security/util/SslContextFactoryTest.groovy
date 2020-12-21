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
class SslContextFactoryTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(SslContextFactoryTest.class)

    private static final String KEYSTORE_PATH = "src/test/resources/TlsConfigurationKeystore.jks"
    private static final String KEYSTORE_PASSWORD = "keystorepassword"
    private static final String KEY_PASSWORD = "keypassword"
    private static final KeystoreType KEYSTORE_TYPE = KeystoreType.JKS

    private static final String TRUSTSTORE_PATH = "src/test/resources/TlsConfigurationTruststore.jks"
    private static final String TRUSTSTORE_PASSWORD = "truststorepassword"
    private static final KeystoreType TRUSTSTORE_TYPE = KeystoreType.JKS

    private static final String CONFIGURED_TLS_PROTOCOL = "TLSv1.2"

    private static final Map<String, String> DEFAULT_PROPS = [
            (NiFiProperties.SECURITY_KEYSTORE)         : KEYSTORE_PATH,
            (NiFiProperties.SECURITY_KEYSTORE_PASSWD)  : KEYSTORE_PASSWORD,
            (NiFiProperties.SECURITY_KEY_PASSWD)       : KEY_PASSWORD,
            (NiFiProperties.SECURITY_KEYSTORE_TYPE)    : KEYSTORE_TYPE.getType(),
            (NiFiProperties.SECURITY_TRUSTSTORE)       : TRUSTSTORE_PATH,
            (NiFiProperties.SECURITY_TRUSTSTORE_PASSWD): TRUSTSTORE_PASSWORD,
            (NiFiProperties.SECURITY_TRUSTSTORE_TYPE)  : TRUSTSTORE_TYPE.getType(),
    ]

    private NiFiProperties mockNiFiProperties = NiFiProperties.createBasicNiFiProperties("", DEFAULT_PROPS)

    private TlsConfiguration tlsConfiguration

    @BeforeClass
    static void setUpOnce() throws Exception {
        Security.addProvider(new BouncyCastleProvider())

        logger.metaClass.methodMissing = { String name, args ->
            logger.info("[${name?.toUpperCase()}] ${(args as List).join(" ")}")
        }
    }

    @Before
    void setUp() {
        tlsConfiguration = new StandardTlsConfiguration(KEYSTORE_PATH, KEYSTORE_PASSWORD, KEY_PASSWORD, KEYSTORE_TYPE, TRUSTSTORE_PATH, TRUSTSTORE_PASSWORD, TRUSTSTORE_TYPE)
    }

    @After
    void tearDown() {

    }

    /**
     * Asserts that the protocol versions are correct. In recent versions of Java, this enforces order as well, but in older versions, it just enforces presence.
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
    void testShouldCreateSslContextFromTlsConfiguration() {
        // Arrange
        logger.info("Creating SSL Context from TLS Configuration: ${tlsConfiguration}")

        // Act
        SSLContext sslContext = SslContextFactory.createSslContext(tlsConfiguration)
        logger.info("Created SSL Context: ${KeyStoreUtils.sslContextToString(sslContext)}")

        // Assert
        assert sslContext.protocol == tlsConfiguration.protocol

        // Check a socket created from this context
        assertSocketProtocols(sslContext)
    }

    @Test
    void testCreateSslContextFromTlsConfigurationShouldHandleEmptyKeyPassword() {
        // Arrange

        // Change the keystore to one with the same keystore and key password, but don't provide the key password
        Map missingKeyPasswordProps = DEFAULT_PROPS + [
                (NiFiProperties.SECURITY_KEYSTORE)  : "src/test/resources/samepassword.jks",
                (NiFiProperties.SECURITY_KEY_PASSWD): "",
        ]
        NiFiProperties propertiesWithoutKeyPassword = NiFiProperties.createBasicNiFiProperties("", missingKeyPasswordProps)
        TlsConfiguration configWithoutKeyPassword = StandardTlsConfiguration.fromNiFiProperties(propertiesWithoutKeyPassword)
        logger.info("Creating SSL Context from TLS Configuration: ${configWithoutKeyPassword}")

        // Act
        SSLContext sslContext = SslContextFactory.createSslContext(configWithoutKeyPassword)
        logger.info("Created SSL Context: ${KeyStoreUtils.sslContextToString(sslContext)}")

        // Assert
        assert sslContext.protocol == configWithoutKeyPassword.protocol

        // Check a socket created from this context
        assertSocketProtocols(sslContext)
    }

    /**
     * This test ensures that silent failures don't occur -- if some keystore/truststore properties
     * are populated but not enough to be valid, throw an exception on failure.
     */
    @Test
    void testCreateSslContextFromTlsConfigurationShouldFailOnInvalidProperties() {
        // Arrange

        // Set up configurations missing the keystore path and truststore path
        Map missingKeystorePathProps = DEFAULT_PROPS + [
                (NiFiProperties.SECURITY_KEYSTORE): "",
        ]
        NiFiProperties propsNoKeystorePath = NiFiProperties.createBasicNiFiProperties("", missingKeystorePathProps)
        TlsConfiguration configNoKeystorePath = StandardTlsConfiguration.fromNiFiProperties(propsNoKeystorePath)
        logger.info("Creating SSL Context from TLS Configuration: ${configNoKeystorePath}")

        Map missingTruststorePathProps = DEFAULT_PROPS + [
                (NiFiProperties.SECURITY_TRUSTSTORE)     : "",
                // Remove the keystore properties to ensure the right conditional is tested
                (NiFiProperties.SECURITY_KEYSTORE)       : "",
                (NiFiProperties.SECURITY_KEYSTORE_PASSWD): "",
                (NiFiProperties.SECURITY_KEY_PASSWD)     : "",
                (NiFiProperties.SECURITY_KEYSTORE_TYPE)  : "",
        ]
        NiFiProperties propsNoTruststorePath = NiFiProperties.createBasicNiFiProperties("", missingTruststorePathProps)
        TlsConfiguration configNoTruststorePath = StandardTlsConfiguration.fromNiFiProperties(propsNoTruststorePath)
        logger.info("Creating SSL Context from TLS Configuration: ${configNoTruststorePath}")

        // Act
        def noKeystorePathMsg = shouldFail(TlsException) {
            SSLContext sslContext = SslContextFactory.createSslContext(configNoKeystorePath)
            logger.info("Created SSL Context missing keystore path: ${KeyStoreUtils.sslContextToString(sslContext)}")
        }

        def noTruststorePathMsg = shouldFail(TlsException) {
            SSLContext sslContext = SslContextFactory.createSslContext(configNoTruststorePath)
            logger.info("Created SSL Context missing truststore path: ${KeyStoreUtils.sslContextToString(sslContext)}")
        }

        // Assert
        assert noKeystorePathMsg =~ "The keystore properties are not valid"
        assert noTruststorePathMsg =~ "The truststore properties are not valid"
    }

    /**
     * This is a regression test to ensure that a truststore without a password is allowed (legacy truststores did not require a password).
     */
    @Test
    void testCreateSslContextFromTlsConfigurationShouldHandleEmptyTruststorePassword() {
        // Arrange

        // Change the truststore to one with no password
        Map truststoreNoPasswordProps = DEFAULT_PROPS + [
                (NiFiProperties.SECURITY_TRUSTSTORE)       : "src/test/resources/no-password-truststore.jks",
                (NiFiProperties.SECURITY_TRUSTSTORE_PASSWD): "",
        ]
        NiFiProperties propertiesNoTruststorePassword = NiFiProperties.createBasicNiFiProperties("", truststoreNoPasswordProps)
        TlsConfiguration configNoTruststorePassword = StandardTlsConfiguration.fromNiFiProperties(propertiesNoTruststorePassword)
        logger.info("Creating SSL Context from TLS Configuration: ${configNoTruststorePassword}")

        // Act
        SSLContext sslContext = SslContextFactory.createSslContext(configNoTruststorePassword)
        logger.info("Created SSL Context: ${KeyStoreUtils.sslContextToString(sslContext)}")

        // Assert
        assert sslContext.protocol == configNoTruststorePassword.protocol

        // Check a socket created from this context
        assertSocketProtocols(sslContext)
    }

    /**
     * This test is for legacy expectations in nifi-framework-core. That {@code SslContextFactory}
     * implementation threw an exception if the keystore properties were present and the truststore
     * properties were not.
     */
    @Test
    void testCreateSslContextFromTlsConfigurationShouldHandleKeystorePropsWithoutTruststoreProps() {
        // Arrange

        // Change the keystore to one with the same keystore and key password, but don't provide the key password
        Map keystoreOnlyProps = DEFAULT_PROPS.findAll { k, v -> k.contains("keystore") }
        NiFiProperties keystoreNiFiProperties = NiFiProperties.createBasicNiFiProperties("", keystoreOnlyProps)
        TlsConfiguration keystoreOnlyConfig = StandardTlsConfiguration.fromNiFiProperties(keystoreNiFiProperties)
        logger.info("Creating SSL Context from TLS Configuration: ${keystoreOnlyConfig}")

        // Act
        def msg = shouldFail(TlsException) {
            SSLContext sslContext = SslContextFactory.createSslContext(keystoreOnlyConfig)
            logger.info("Created SSL Context: ${KeyStoreUtils.sslContextToString(sslContext)}")
        }
        logger.expected(msg)

        // Assert
        assert msg =~ "Truststore properties are required if keystore properties are present"
    }

    /**
     * This test is for legacy expectations in nifi-framework-core. That {@code SslContextFactory}
     * implementation returned {@code null} if none of the properties were populated.
     */
    @Test
    void testCreateSslContextFromTlsConfigurationShouldHandleEmptyConfiguration() {
        // Arrange
        TlsConfiguration emptyConfig = new StandardTlsConfiguration()
        logger.info("Creating SSL Context from TLS Configuration: ${emptyConfig}")

        // Act
        SSLContext sslContext = SslContextFactory.createSslContext(emptyConfig)

        // Assert
        assert !sslContext
    }

    /**
     * Asserts an {@link SSLServerSocket} from the provided {@link SSLContext} has the proper TLS protocols set.
     *
     * @param sslContext the context under test
     */
    void assertSocketProtocols(SSLContext sslContext) {
        SSLServerSocket sslSocket = sslContext.serverSocketFactory.createServerSocket() as SSLServerSocket
        logger.info("Created SSL (server) socket: ${sslSocket}")
        assert sslSocket.enabledProtocols.contains(CONFIGURED_TLS_PROTOCOL)

        // Override the SSL parameters protocol version
        SSLServerSocket customSslSocket = sslContext.serverSocketFactory.createServerSocket() as SSLServerSocket
        def customParameters = customSslSocket.getSSLParameters()
        customParameters.setProtocols([CONFIGURED_TLS_PROTOCOL] as String[])
        customSslSocket.setSSLParameters(customParameters)
        assertProtocolVersions(customSslSocket.enabledProtocols, [CONFIGURED_TLS_PROTOCOL])
    }
}