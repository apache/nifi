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
package org.apache.nifi.cdc.mysql.processors.ssl;

import com.github.shyiko.mysql.binlog.network.SSLMode;
import org.apache.nifi.security.util.KeystoreType;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.security.util.TlsPlatform;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class StandardConnectionPropertiesProviderTest {
    private static final String KEY_STORE_PATH = "keystore.p12";

    private static final KeystoreType KEY_STORE_TYPE = KeystoreType.PKCS12;

    private static final String KEY_STORE_PASSWORD = String.class.getName();

    private static final String TRUST_STORE_PATH = "cacerts";

    private static final KeystoreType TRUST_STORE_TYPE = KeystoreType.PKCS12;

    private static final String TRUST_STORE_PASSWORD = Integer.class.getName();

    @Mock
    TlsConfiguration tlsConfiguration;

    @Test
    void testGetConnectionPropertiesSslModeDisabled() {
        final StandardConnectionPropertiesProvider provider = new StandardConnectionPropertiesProvider(SSLMode.DISABLED, null);

        final Map<String, String> properties = provider.getConnectionProperties();

        assertNotNull(properties);

        final String useSsl = properties.get(SecurityProperty.USE_SSL.getProperty());
        assertEquals(Boolean.FALSE.toString(), useSsl);
    }

    @Test
    void testGetConnectionPropertiesSslModePreferred() {
        final StandardConnectionPropertiesProvider provider = new StandardConnectionPropertiesProvider(SSLMode.PREFERRED, null);

        final Map<String, String> properties = provider.getConnectionProperties();

        assertNotNull(properties);

        final String useSsl = properties.get(SecurityProperty.USE_SSL.getProperty());
        assertEquals(Boolean.TRUE.toString(), useSsl);

        final String requireSsl = properties.get(SecurityProperty.REQUIRE_SSL.getProperty());
        assertEquals(Boolean.FALSE.toString(), requireSsl);

        final String protocols = properties.get(SecurityProperty.ENABLED_TLS_PROTOCOLS.getProperty());
        assertNotNull(protocols);
    }

    @Test
    void testGetConnectionPropertiesSslModeRequired() {
        final StandardConnectionPropertiesProvider provider = new StandardConnectionPropertiesProvider(SSLMode.REQUIRED, null);

        final Map<String, String> properties = provider.getConnectionProperties();

        assertNotNull(properties);

        final String useSsl = properties.get(SecurityProperty.USE_SSL.getProperty());
        assertEquals(Boolean.TRUE.toString(), useSsl);

        final String requireSsl = properties.get(SecurityProperty.REQUIRE_SSL.getProperty());
        assertEquals(Boolean.TRUE.toString(), requireSsl);

        final String protocols = properties.get(SecurityProperty.ENABLED_TLS_PROTOCOLS.getProperty());
        assertNotNull(protocols);
    }

    @Test
    void testGetConnectionPropertiesSslModeVerifyIdentity() {
        final StandardConnectionPropertiesProvider provider = new StandardConnectionPropertiesProvider(SSLMode.VERIFY_IDENTITY, null);

        final Map<String, String> properties = provider.getConnectionProperties();

        assertNotNull(properties);

        final String useSsl = properties.get(SecurityProperty.USE_SSL.getProperty());
        assertEquals(Boolean.TRUE.toString(), useSsl);

        final String requireSsl = properties.get(SecurityProperty.REQUIRE_SSL.getProperty());
        assertEquals(Boolean.TRUE.toString(), requireSsl);

        final String protocols = properties.get(SecurityProperty.ENABLED_TLS_PROTOCOLS.getProperty());
        assertNotNull(protocols);

        final String verifyServerCertificate = properties.get(SecurityProperty.VERIFY_SERVER_CERTIFICATE.getProperty());
        assertEquals(Boolean.TRUE.toString(), verifyServerCertificate);
    }

    @Test
    void testGetConnectionPropertiesSslModeRequiredTlsConfiguration() {
        final String latestProtocol = TlsPlatform.getLatestProtocol();
        when(tlsConfiguration.getEnabledProtocols()).thenReturn(new String[]{latestProtocol});
        when(tlsConfiguration.isKeystorePopulated()).thenReturn(true);
        when(tlsConfiguration.getKeystorePath()).thenReturn(KEY_STORE_PATH);
        when(tlsConfiguration.getKeystoreType()).thenReturn(KEY_STORE_TYPE);
        when(tlsConfiguration.getKeystorePassword()).thenReturn(KEY_STORE_PASSWORD);
        when(tlsConfiguration.isTruststorePopulated()).thenReturn(true);
        when(tlsConfiguration.getTruststorePath()).thenReturn(TRUST_STORE_PATH);
        when(tlsConfiguration.getTruststoreType()).thenReturn(TRUST_STORE_TYPE);
        when(tlsConfiguration.getTruststorePassword()).thenReturn(TRUST_STORE_PASSWORD);

        final StandardConnectionPropertiesProvider provider = new StandardConnectionPropertiesProvider(SSLMode.REQUIRED, tlsConfiguration);

        final Map<String, String> properties = provider.getConnectionProperties();

        assertNotNull(properties);

        final String useSsl = properties.get(SecurityProperty.USE_SSL.getProperty());
        assertEquals(Boolean.TRUE.toString(), useSsl);

        final String requireSsl = properties.get(SecurityProperty.REQUIRE_SSL.getProperty());
        assertEquals(Boolean.TRUE.toString(), requireSsl);

        final String protocols = properties.get(SecurityProperty.ENABLED_TLS_PROTOCOLS.getProperty());
        assertEquals(latestProtocol, protocols);

        final String clientCertificateUrl = properties.get(SecurityProperty.CLIENT_CERTIFICATE_KEY_STORE_URL.getProperty());
        assertEquals(KEY_STORE_PATH, clientCertificateUrl);

        final String clientCertificateType = properties.get(SecurityProperty.CLIENT_CERTIFICATE_KEY_STORE_TYPE.getProperty());
        assertEquals(KEY_STORE_TYPE.getType(), clientCertificateType);

        final String clientCertificatePassword = properties.get(SecurityProperty.CLIENT_CERTIFICATE_KEY_STORE_PASSWORD.getProperty());
        assertEquals(KEY_STORE_PASSWORD, clientCertificatePassword);

        final String trustCertificateUrl = properties.get(SecurityProperty.TRUST_CERTIFICATE_KEY_STORE_URL.getProperty());
        assertEquals(TRUST_STORE_PATH, trustCertificateUrl);

        final String trustCertificateType = properties.get(SecurityProperty.TRUST_CERTIFICATE_KEY_STORE_TYPE.getProperty());
        assertEquals(TRUST_STORE_TYPE.getType(), trustCertificateType);

        final String trustCertificatePassword = properties.get(SecurityProperty.TRUST_CERTIFICATE_KEY_STORE_PASSWORD.getProperty());
        assertEquals(TRUST_STORE_PASSWORD, trustCertificatePassword);
    }
}
