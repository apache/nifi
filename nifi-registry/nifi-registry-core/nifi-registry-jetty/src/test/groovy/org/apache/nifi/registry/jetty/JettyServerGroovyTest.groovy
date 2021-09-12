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
package org.apache.nifi.registry.jetty

import org.apache.nifi.registry.properties.NiFiRegistryProperties
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.util.ssl.SslContextFactory
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.junit.jupiter.MockitoExtension

import static org.junit.jupiter.api.Assertions.assertNotNull

@ExtendWith(MockitoExtension.class)
@Disabled("This test did not run because there is no src/test/java folder. It is broken because setProperties was removed from the NiFiRegistryProperties object. It needs to be refactored.")
class JettyServerGroovyTest {
    private static final keyPassword = "keyPassword"
    private static final keystorePassword = "keystorePassword"
    private static final truststorePassword = "truststorePassword"
    private static final matchingPassword = "thePassword"

    @Test
    void testCreateSslContextFactoryWithKeystoreAndKeypassword() throws Exception {

        // Arrange
        NiFiRegistryProperties properties = new NiFiRegistryProperties()
        properties.setProperty(NiFiRegistryProperties.SECURITY_TRUSTSTORE, "src/test/resources/truststore.jks")
        properties.setProperty(NiFiRegistryProperties.SECURITY_TRUSTSTORE_PASSWD, truststorePassword)
        properties.setProperty(NiFiRegistryProperties.SECURITY_TRUSTSTORE_TYPE, "JKS")
        properties.setProperty(NiFiRegistryProperties.SECURITY_KEYSTORE, "src/test/resources/keystoreDifferentPasswords.jks")
        properties.setProperty(NiFiRegistryProperties.SECURITY_KEY_PASSWD, keyPassword)
        properties.setProperty(NiFiRegistryProperties.SECURITY_KEYSTORE_PASSWD, keystorePassword)
        properties.setProperty(NiFiRegistryProperties.SECURITY_KEYSTORE_TYPE, "JKS")

        Server internalServer = new Server()
        JettyServer testServer = new JettyServer(internalServer, properties)

        // Act
        SslContextFactory sslContextFactory = testServer.createSslContextFactory()
        sslContextFactory.start()

        // Assert
        assertNotNull(sslContextFactory)
        assertNotNull(sslContextFactory.getSslContext())
    }

    @Test
    void testCreateSslContextFactoryWithOnlyKeystorePassword() throws Exception {

        // Arrange
        NiFiRegistryProperties properties = new NiFiRegistryProperties()
        properties.setProperty(NiFiRegistryProperties.SECURITY_TRUSTSTORE, "src/test/resources/truststore.jks")
        properties.setProperty(NiFiRegistryProperties.SECURITY_TRUSTSTORE_PASSWD, truststorePassword)
        properties.setProperty(NiFiRegistryProperties.SECURITY_TRUSTSTORE_TYPE, "JKS")
        properties.setProperty(NiFiRegistryProperties.SECURITY_KEYSTORE, "src/test/resources/keystoreSamePassword.jks")
        properties.setProperty(NiFiRegistryProperties.SECURITY_KEYSTORE_PASSWD, matchingPassword)
        properties.setProperty(NiFiRegistryProperties.SECURITY_KEYSTORE_TYPE, "JKS")

        Server internalServer = new Server()
        JettyServer testServer = new JettyServer(internalServer, properties)

        // Act
        SslContextFactory sslContextFactory = testServer.createSslContextFactory()
        sslContextFactory.start()

        // Assert
        assertNotNull(sslContextFactory)
        assertNotNull(sslContextFactory.getSslContext())
    }

    @Test
    void testCreateSslContextFactoryWithMatchingPasswordsDefined() throws Exception {

        // Arrange
        NiFiRegistryProperties properties = new NiFiRegistryProperties()

        properties.setProperty(NiFiRegistryProperties.SECURITY_TRUSTSTORE, "src/test/resources/truststore.jks")
        properties.setProperty(NiFiRegistryProperties.SECURITY_TRUSTSTORE_PASSWD, truststorePassword)
        properties.setProperty(NiFiRegistryProperties.SECURITY_TRUSTSTORE_TYPE, "JKS")
        properties.setProperty(NiFiRegistryProperties.SECURITY_KEYSTORE, "src/test/resources/keystoreSamePassword.jks")
        properties.setProperty(NiFiRegistryProperties.SECURITY_KEY_PASSWD, matchingPassword)
        properties.setProperty(NiFiRegistryProperties.SECURITY_KEYSTORE_PASSWD, matchingPassword)
        properties.setProperty(NiFiRegistryProperties.SECURITY_KEYSTORE_TYPE, "JKS")

        Server internalServer = new Server()
        JettyServer testServer = new JettyServer(internalServer, properties)

        // Act
        SslContextFactory sslContextFactory = testServer.createSslContextFactory()
        sslContextFactory.start()

        // Assert
        assertNotNull(sslContextFactory)
        assertNotNull(sslContextFactory.getSslContext())
    }

    @Test
    void testCreateSslContextFactoryWithNoKeystorePasswordFails() throws Exception {

        // Arrange
//        exception.expect(IllegalArgumentException.class)
//        exception.expectMessage("The keystore password cannot be null or empty")

        NiFiRegistryProperties properties = new NiFiRegistryProperties()
        properties.setProperty(NiFiRegistryProperties.SECURITY_TRUSTSTORE, "src/test/resources/truststore.jks")
        properties.setProperty(NiFiRegistryProperties.SECURITY_TRUSTSTORE_PASSWD, truststorePassword)
        properties.setProperty(NiFiRegistryProperties.SECURITY_TRUSTSTORE_TYPE, "JKS")
        properties.setProperty(NiFiRegistryProperties.SECURITY_KEYSTORE, "src/test/resources/keystoreSamePassword.jks")
        properties.setProperty(NiFiRegistryProperties.SECURITY_KEYSTORE_TYPE, "JKS")

        Server internalServer = new Server()
        JettyServer testServer = new JettyServer(internalServer, properties)

        // Act but expect exception
        SslContextFactory sslContextFactory = testServer.createSslContextFactory()
    }
}
