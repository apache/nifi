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

import java.security.Security

@RunWith(JUnit4.class)
class StandardTlsConfigurationTest extends GroovyTestCase {
    private static final Logger logger = LoggerFactory.getLogger(StandardTlsConfigurationTest.class)

    private static final String KEYSTORE_PATH = "src/test/resources/TlsConfigurationKeystore.jks"
    private static final String KEYSTORE_PASSWORD = "keystorepassword"
    private static final String KEY_PASSWORD = "keypassword"
    private static final KeystoreType KEYSTORE_TYPE = KeystoreType.JKS

    private static final String TRUSTSTORE_PATH = "src/test/resources/TlsConfigurationTruststore.jks"
    private static final String TRUSTSTORE_PASSWORD = "truststorepassword"
    private static final KeystoreType TRUSTSTORE_TYPE = KeystoreType.JKS

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
    void setUp() throws Exception {
        tlsConfiguration = new StandardTlsConfiguration(KEYSTORE_PATH, KEYSTORE_PASSWORD, KEY_PASSWORD, KEYSTORE_TYPE, TRUSTSTORE_PATH, TRUSTSTORE_PASSWORD, TRUSTSTORE_TYPE)
    }

    @After
    void tearDown() throws Exception {
    }

    @Test
    void testShouldCreateFromNiFiProperties() {
        // Arrange

        // Act
        TlsConfiguration fromProperties = StandardTlsConfiguration.fromNiFiProperties(mockNiFiProperties)
        logger.info("Created TlsConfiguration: ${fromProperties}")

        // Assert
        assert fromProperties == tlsConfiguration
    }

    @Test
    void testCreateFromNiFiPropertiesShouldHandleNullKeystoreTypes() {
        // Arrange
        def noKeystoreTypesProps = NiFiProperties.createBasicNiFiProperties("", DEFAULT_PROPS +
                [(NiFiProperties.SECURITY_KEYSTORE_TYPE)  : "",
                 (NiFiProperties.SECURITY_TRUSTSTORE_TYPE): ""
                ])

        // Act
        TlsConfiguration fromProperties = StandardTlsConfiguration.fromNiFiProperties(noKeystoreTypesProps)
        logger.info("Created TlsConfiguration: ${fromProperties}")

        // Assert
        assert fromProperties.keystoreType == null
        assert fromProperties.truststoreType == null
    }

    @Test
    void testShouldGetFunctionalKeyPassword() {
        // Arrange
        TlsConfiguration withKeyPassword = tlsConfiguration

        // A container where the keystore password is explicitly set as the key password as well
        TlsConfiguration withoutKeyPassword = new StandardTlsConfiguration(KEYSTORE_PATH, KEYSTORE_PASSWORD, KEYSTORE_TYPE, TRUSTSTORE_PATH, TRUSTSTORE_PASSWORD, TRUSTSTORE_TYPE)

        // A container where null is explicitly set as the key password
        TlsConfiguration withNullPassword = new StandardTlsConfiguration(KEYSTORE_PATH, KEYSTORE_PASSWORD, null, KEYSTORE_TYPE, TRUSTSTORE_PATH, TRUSTSTORE_PASSWORD, TRUSTSTORE_TYPE)

        // Act
        String actualKeyPassword = withKeyPassword.getKeyPassword()
        String functionalKeyPassword = withKeyPassword.getFunctionalKeyPassword()

        String duplicateKeystorePassword = withoutKeyPassword.getKeyPassword()
        String duplicateFunctionalKeyPassword = withoutKeyPassword.getFunctionalKeyPassword()

        String missingKeyPassword = withNullPassword.getKeyPassword()
        String keystorePassword = withNullPassword.getFunctionalKeyPassword()

        // Assert
        assert actualKeyPassword == KEY_PASSWORD
        assert functionalKeyPassword == KEY_PASSWORD

        assert duplicateKeystorePassword == KEYSTORE_PASSWORD
        assert duplicateFunctionalKeyPassword == KEYSTORE_PASSWORD

        assert missingKeyPassword == null
        assert keystorePassword == KEYSTORE_PASSWORD
    }

    @Test
    void testShouldCheckKeystorePopulation() {
        // Arrange
        TlsConfiguration empty = new StandardTlsConfiguration()
        TlsConfiguration noKeystorePassword = new StandardTlsConfiguration(KEYSTORE_PATH, "", KEY_PASSWORD, KEYSTORE_TYPE, TRUSTSTORE_PATH, TRUSTSTORE_PASSWORD, TRUSTSTORE_TYPE)

        // Act
        boolean normalIsPopulated = tlsConfiguration.isKeystorePopulated()
        boolean emptyIsPopulated = empty.isKeystorePopulated()
        boolean noPasswordIsPopulated = noKeystorePassword.isKeystorePopulated()

        // Assert
        assert normalIsPopulated
        assert !emptyIsPopulated
        assert !noPasswordIsPopulated
    }

    @Test
    void testShouldCheckTruststorePopulation() {
        // Arrange
        TlsConfiguration empty = new StandardTlsConfiguration()
        TlsConfiguration noTruststorePassword = new StandardTlsConfiguration(KEYSTORE_PATH, KEYSTORE_PASSWORD, KEY_PASSWORD, KEYSTORE_TYPE, TRUSTSTORE_PATH, "", TRUSTSTORE_TYPE)

        // Act
        boolean normalIsPopulated = tlsConfiguration.isTruststorePopulated()
        boolean emptyIsPopulated = empty.isTruststorePopulated()
        boolean noPasswordIsPopulated = noTruststorePassword.isTruststorePopulated()

        // Assert
        assert normalIsPopulated
        assert !emptyIsPopulated
        assert noPasswordIsPopulated
    }

    @Test
    void testShouldValidateKeystoreConfiguration() {
        // Arrange
        TlsConfiguration empty = new StandardTlsConfiguration()
        TlsConfiguration wrongPassword = new StandardTlsConfiguration(KEYSTORE_PATH, KEYSTORE_PASSWORD.reverse(), KEY_PASSWORD.reverse(), KEYSTORE_TYPE, TRUSTSTORE_PATH, TRUSTSTORE_PASSWORD.reverse(), TRUSTSTORE_TYPE)
        TlsConfiguration invalid = new StandardTlsConfiguration(KEYSTORE_PATH.reverse(), KEYSTORE_PASSWORD.reverse(), KEY_PASSWORD.reverse(), KEYSTORE_TYPE, TRUSTSTORE_PATH.reverse(), TRUSTSTORE_PASSWORD.reverse(), TRUSTSTORE_TYPE)

        // Act
        boolean normalIsValid = tlsConfiguration.isKeystoreValid()
        boolean emptyIsValid = empty.isKeystoreValid()
        boolean wrongPasswordIsValid = wrongPassword.isKeystoreValid()
        boolean invalidIsValid = invalid.isKeystoreValid()

        // Assert
        assert normalIsValid
        assert !emptyIsValid
        assert !wrongPasswordIsValid
        assert !invalidIsValid
    }

    @Test
    void testShouldValidateTruststoreConfiguration() {
        // Arrange
        TlsConfiguration empty = new StandardTlsConfiguration()
        TlsConfiguration wrongPassword = new StandardTlsConfiguration(KEYSTORE_PATH, KEYSTORE_PASSWORD.reverse(), KEY_PASSWORD.reverse(), KEYSTORE_TYPE, TRUSTSTORE_PATH, TRUSTSTORE_PASSWORD.reverse(), TRUSTSTORE_TYPE)
        TlsConfiguration invalid = new StandardTlsConfiguration(KEYSTORE_PATH.reverse(), KEYSTORE_PASSWORD.reverse(), KEY_PASSWORD.reverse(), KEYSTORE_TYPE, TRUSTSTORE_PATH.reverse(), TRUSTSTORE_PASSWORD.reverse(), TRUSTSTORE_TYPE)

        // Act
        boolean normalIsValid = tlsConfiguration.isTruststoreValid()
        boolean emptyIsValid = empty.isTruststoreValid()
        boolean wrongPasswordIsValid = wrongPassword.isTruststoreValid()
        boolean invalidIsValid = invalid.isTruststoreValid()

        // Assert
        assert normalIsValid
        assert !emptyIsValid
        assert !wrongPasswordIsValid
        assert !invalidIsValid
    }

    @Test
    void testShouldReturnLegacyAndCurrentEnabledProtocolsForSsl() {
        TlsConfiguration configuration = getTlsConfiguration(TlsConfiguration.SSL_PROTOCOL)

        String[] enabledProtocols = configuration.enabledProtocols
        assert enabledProtocols.toList().containsAll(TlsConfiguration.LEGACY_TLS_PROTOCOL_VERSIONS)
        assert enabledProtocols.toList().containsAll(TlsConfiguration.getCurrentSupportedTlsProtocolVersions())
    }

    @Test
    void testShouldReturnCurrentEnabledProtocolsForTls() {
        TlsConfiguration configuration = getTlsConfiguration(TlsConfiguration.TLS_PROTOCOL)

        String[] enabledProtocols = configuration.enabledProtocols
        assert !enabledProtocols.toList().containsAll(TlsConfiguration.LEGACY_TLS_PROTOCOL_VERSIONS)
        assert enabledProtocols.toList().containsAll(TlsConfiguration.getCurrentSupportedTlsProtocolVersions())
    }

    @Test
    void testShouldReturnConfiguredEnabledProtocols() {
        String currentProtocol = TlsConfiguration.getHighestCurrentSupportedTlsProtocolVersion()
        TlsConfiguration configuration = getTlsConfiguration(currentProtocol)

        String[] enabledProtocols = configuration.enabledProtocols
        assert enabledProtocols == [currentProtocol]
    }

    @Test
    void testShouldReturnEmptyEnabledProtocolsForNullProtocol() {
        TlsConfiguration configuration = getTlsConfiguration(null)

        String[] enabledProtocols = configuration.enabledProtocols
        assert enabledProtocols.toList().isEmpty()
    }

    TlsConfiguration getTlsConfiguration(String protocol) {
        new StandardTlsConfiguration(KEYSTORE_PATH, KEYSTORE_PASSWORD, KEY_PASSWORD, KEYSTORE_TYPE, TRUSTSTORE_PATH, TRUSTSTORE_PASSWORD, TRUSTSTORE_TYPE, protocol)
    }
}
