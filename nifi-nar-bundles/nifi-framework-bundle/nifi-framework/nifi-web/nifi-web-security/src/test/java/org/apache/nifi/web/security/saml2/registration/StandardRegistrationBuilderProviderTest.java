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
package org.apache.nifi.web.security.saml2.registration;

import okhttp3.HttpUrl;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.commons.io.IOUtils;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.security.util.TemporaryKeyStoreBuilder;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.security.util.TlsException;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.security.saml2.SamlConfigurationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.security.saml2.provider.service.registration.RelyingPartyRegistration;
import org.springframework.security.saml2.provider.service.registration.Saml2MessageBinding;

import javax.net.ssl.SSLSocketFactory;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StandardRegistrationBuilderProviderTest {
    private static final String LOCALHOST = "localhost";

    private static final String METADATA_PATH = "/saml/sso-circle-meta.xml";

    private static final int HTTP_NOT_FOUND = 404;

    private static final boolean PROXY_DISABLED = false;

    private MockWebServer mockWebServer;

    @BeforeEach
    void startServer() throws IOException {
        mockWebServer = new MockWebServer();
        mockWebServer.start();
    }

    @AfterEach
    void shutdownServer() throws IOException {
        mockWebServer.shutdown();
    }

    @Test
    void testGetRegistrationBuilderFileUrl() {
        final NiFiProperties properties = getProperties(getFileMetadataUrl());

        assertRegistrationFound(properties);
    }

    @Test
    void testGetRegistrationBuilderHttpUrl() throws IOException {
        final String metadata = getMetadata();
        final MockResponse response = new MockResponse().setBody(metadata);
        mockWebServer.enqueue(response);
        final String metadataUrl = getMetadataUrl();

        final NiFiProperties properties = getProperties(metadataUrl);

        assertRegistrationFound(properties);
    }

    @Test
    void testGetRegistrationBuilderHttpUrlNotFound() {
        final MockResponse response = new MockResponse().setResponseCode(HTTP_NOT_FOUND);
        mockWebServer.enqueue(response);
        final String metadataUrl = getMetadataUrl();

        final NiFiProperties properties = getProperties(metadataUrl);

        final StandardRegistrationBuilderProvider provider = new StandardRegistrationBuilderProvider(properties);

        final SamlConfigurationException exception = assertThrows(SamlConfigurationException.class, provider::getRegistrationBuilder);
        assertTrue(exception.getMessage().contains(Integer.toString(HTTP_NOT_FOUND)));
    }

    @Test
    void testGetRegistrationBuilderHttpsUrl() throws IOException, TlsException {
        final TlsConfiguration tlsConfiguration = new TemporaryKeyStoreBuilder().build();
        final SSLSocketFactory sslSocketFactory = Objects.requireNonNull(SslContextFactory.createSSLSocketFactory(tlsConfiguration));
        mockWebServer.useHttps(sslSocketFactory, PROXY_DISABLED);

        final String metadata = getMetadata();
        final MockResponse response = new MockResponse().setBody(metadata);
        mockWebServer.enqueue(response);
        final String metadataUrl = getMetadataUrl();

        final NiFiProperties properties = getProperties(metadataUrl, tlsConfiguration);

        assertRegistrationFound(properties);
    }

    private String getMetadataUrl() {
        final HttpUrl url = mockWebServer.url(METADATA_PATH).newBuilder().host(LOCALHOST).build();
        return url.toString();
    }

    private void assertRegistrationFound(final NiFiProperties properties) {
        final StandardRegistrationBuilderProvider provider = new StandardRegistrationBuilderProvider(properties);
        final RelyingPartyRegistration.Builder builder = provider.getRegistrationBuilder();

        final RelyingPartyRegistration registration = builder.build();
        assertEquals(Saml2MessageBinding.POST, registration.getAssertionConsumerServiceBinding());
    }

    private NiFiProperties getProperties(final String metadataUrl) {
        final Properties properties = new Properties();
        properties.setProperty(NiFiProperties.SECURITY_USER_SAML_IDP_METADATA_URL, metadataUrl);
        return NiFiProperties.createBasicNiFiProperties(null, properties);
    }

    private NiFiProperties getProperties(final String metadataUrl, final TlsConfiguration tlsConfiguration) {
        final Properties properties = new Properties();
        properties.setProperty(NiFiProperties.SECURITY_USER_SAML_IDP_METADATA_URL, metadataUrl);
        properties.setProperty(NiFiProperties.SECURITY_USER_SAML_HTTP_CLIENT_TRUSTSTORE_STRATEGY, StandardRegistrationBuilderProvider.NIFI_TRUST_STORE_STRATEGY);

        properties.setProperty(NiFiProperties.SECURITY_KEYSTORE, tlsConfiguration.getKeystorePath());
        properties.setProperty(NiFiProperties.SECURITY_KEYSTORE_TYPE, tlsConfiguration.getKeystoreType().getType());
        properties.setProperty(NiFiProperties.SECURITY_KEYSTORE_PASSWD, tlsConfiguration.getKeystorePassword());
        properties.setProperty(NiFiProperties.SECURITY_KEY_PASSWD, tlsConfiguration.getKeyPassword());
        properties.setProperty(NiFiProperties.SECURITY_TRUSTSTORE, tlsConfiguration.getTruststorePath());
        properties.setProperty(NiFiProperties.SECURITY_TRUSTSTORE_TYPE, tlsConfiguration.getTruststoreType().getType());
        properties.setProperty(NiFiProperties.SECURITY_TRUSTSTORE_PASSWD, tlsConfiguration.getTruststorePassword());

        return NiFiProperties.createBasicNiFiProperties(null, properties);
    }

    final String getMetadata() throws IOException {
        try (final InputStream inputStream = Objects.requireNonNull(getClass().getResourceAsStream(METADATA_PATH))) {
            return IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        }
    }

    private String getFileMetadataUrl() {
        final URL resource = Objects.requireNonNull(getClass().getResource(METADATA_PATH));
        return resource.toString();
    }
}
