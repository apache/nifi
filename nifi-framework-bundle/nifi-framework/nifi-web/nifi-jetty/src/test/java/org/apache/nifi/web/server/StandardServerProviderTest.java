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
package org.apache.nifi.web.server;

import org.apache.nifi.jetty.configuration.connector.ApplicationLayerProtocol;
import org.apache.nifi.security.cert.builder.StandardCertificateBuilder;
import org.apache.nifi.security.ssl.EphemeralKeyStoreBuilder;
import org.apache.nifi.security.ssl.StandardSslContextBuilder;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.server.handler.HeaderWriterHandler;
import org.eclipse.jetty.http.HttpStatus;
import org.eclipse.jetty.rewrite.handler.RewriteHandler;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.RequestLog;
import org.eclipse.jetty.server.Server;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.web.util.UriComponentsBuilder;

import javax.net.ssl.SSLContext;
import javax.security.auth.x500.X500Principal;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StandardServerProviderTest {

    private static final String RANDOM_PORT = "0";

    private static final String SSL_PROTOCOL = "ssl";

    private static final Duration TIMEOUT = Duration.ofSeconds(15);

    private static final String ALIAS = "entry-0";

    private static final char[] PROTECTION_PARAMETER = new char[]{};

    private static final String LOCALHOST_NAME = "localhost";

    private static final X500Principal LOCALHOST_SUBJECT = new X500Principal("CN=%s, O=NiFi".formatted(LOCALHOST_NAME));

    private static final String LOCALHOST_ADDRESS = "127.0.0.1";

    private static final String LOCALHOST_HTTP_PORT = "localhost:80";

    private static final String HOST_HEADER = "Host";

    private static final String PUBLIC_HOST = "nifi.apache.org";

    private static final String PUBLIC_UNKNOWN_HOST = "nifi.staged.apache.org";

    private static final String ALLOW_RESTRICTED_HEADERS_PROPERTY = "jdk.httpclient.allowRestrictedHeaders";

    private static SSLContext sslContext;

    @BeforeAll
    static void setConfiguration() throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
        final X509Certificate certificate = new StandardCertificateBuilder(keyPair, LOCALHOST_SUBJECT, Duration.ofHours(1))
                .setDnsSubjectAlternativeNames(List.of(PUBLIC_HOST))
                .build();
        final KeyStore keyStore = new EphemeralKeyStoreBuilder().build();
        keyStore.setKeyEntry(ALIAS, keyPair.getPrivate(), PROTECTION_PARAMETER, new Certificate[]{certificate});
        sslContext = new StandardSslContextBuilder()
                .keyStore(keyStore)
                .trustStore(keyStore)
                .keyPassword(PROTECTION_PARAMETER)
                .build();

        // Allow Restricted Headers for testing TLS SNI
        System.setProperty(ALLOW_RESTRICTED_HEADERS_PROPERTY, HOST_HEADER);
    }

    @Test
    void testGetServer() {
        final Properties applicationProperties = new Properties();
        applicationProperties.setProperty(NiFiProperties.WEB_HTTP_PORT, RANDOM_PORT);
        final NiFiProperties properties = NiFiProperties.createBasicNiFiProperties(null, applicationProperties);

        final StandardServerProvider provider = new StandardServerProvider(null);

        final Server server = provider.getServer(properties);

        assertStandardConfigurationFound(server);
        assertHttpConnectorFound(server);
    }

    @Test
    void testGetServerHttps() {
        final Properties applicationProperties = new Properties();
        applicationProperties.setProperty(NiFiProperties.WEB_HTTPS_PORT, RANDOM_PORT);
        final NiFiProperties properties = NiFiProperties.createBasicNiFiProperties(null, applicationProperties);

        final StandardServerProvider provider = new StandardServerProvider(sslContext);

        final Server server = provider.getServer(properties);

        assertStandardConfigurationFound(server);
        assertHttpsConnectorFound(server);
    }

    @Test
    void testGetServerStart() throws Exception {
        final Properties applicationProperties = new Properties();
        applicationProperties.setProperty(NiFiProperties.WEB_HTTP_PORT, RANDOM_PORT);
        final NiFiProperties properties = NiFiProperties.createBasicNiFiProperties(null, applicationProperties);

        final StandardServerProvider provider = new StandardServerProvider(null);

        final Server server = provider.getServer(properties);

        assertStandardConfigurationFound(server);
        assertHttpConnectorFound(server);

        try {
            server.start();

            assertFalse(server.isFailed());
        } finally {
            server.stop();
        }
    }

    @Timeout(15)
    @Test
    void testGetServerHttpsRequestsCompleted() throws Exception {
        final Properties applicationProperties = new Properties();
        applicationProperties.setProperty(NiFiProperties.WEB_HTTPS_PORT, RANDOM_PORT);
        applicationProperties.setProperty(NiFiProperties.WEB_PROXY_HOST, PUBLIC_HOST);
        final NiFiProperties properties = NiFiProperties.createBasicNiFiProperties(null, applicationProperties);

        final StandardServerProvider provider = new StandardServerProvider(sslContext);

        final Server server = provider.getServer(properties);

        assertStandardConfigurationFound(server);
        assertHttpsConnectorFound(server);

        try {
            server.start();

            assertFalse(server.isFailed());

            while (server.isStarting()) {
                TimeUnit.MILLISECONDS.sleep(250);
            }

            assertTrue(server.isStarted());

            final URI uri = server.getURI();
            assertHttpsRequestsCompleted(uri);
        } finally {
            server.stop();
        }
    }

    void assertHttpsRequestsCompleted(final URI serverUri) throws IOException, InterruptedException {
        try (HttpClient httpClient = HttpClient.newBuilder()
                .connectTimeout(TIMEOUT)
                .sslContext(sslContext)
                .build()
        ) {
            final URI localhostUri = UriComponentsBuilder.fromUri(serverUri).host(LOCALHOST_NAME).build().toUri();

            assertRedirectRequestsCompleted(httpClient, localhostUri);
            assertBadRequestsCompleted(httpClient, localhostUri);
            assertMisdirectedRequestsCompleted(httpClient, localhostUri);
        }
    }

    void assertRedirectRequestsCompleted(final HttpClient httpClient, final URI localhostUri) throws IOException, InterruptedException {
        final HttpRequest localhostRequest = HttpRequest.newBuilder(localhostUri)
                .version(HttpClient.Version.HTTP_2)
                .build();
        assertResponseStatusCode(httpClient, localhostRequest, HttpStatus.MOVED_TEMPORARILY_302);

        final HttpRequest alternativeNameRequest = HttpRequest.newBuilder(localhostUri)
                .version(HttpClient.Version.HTTP_1_1)
                .header(HOST_HEADER, PUBLIC_HOST)
                .build();
        assertResponseStatusCode(httpClient, alternativeNameRequest, HttpStatus.MOVED_TEMPORARILY_302);
    }

    void assertBadRequestsCompleted(final HttpClient httpClient, final URI localhostUri) throws IOException, InterruptedException {
        final HttpRequest publicHostHeaderRequest = HttpRequest.newBuilder(localhostUri)
                .header(HOST_HEADER, PUBLIC_UNKNOWN_HOST)
                .version(HttpClient.Version.HTTP_1_1)
                .build();
        assertResponseStatusCode(httpClient, publicHostHeaderRequest, HttpStatus.BAD_REQUEST_400);

        final HttpRequest localhostAddressRequest = HttpRequest.newBuilder(localhostUri)
                .header(HOST_HEADER, LOCALHOST_ADDRESS)
                .version(HttpClient.Version.HTTP_1_1)
                .build();
        assertResponseStatusCode(httpClient, localhostAddressRequest, HttpStatus.BAD_REQUEST_400);
    }

    void assertMisdirectedRequestsCompleted(final HttpClient httpClient, final URI localhostUri) throws IOException, InterruptedException {
        final HttpRequest localhostPortRequest = HttpRequest.newBuilder(localhostUri)
                .version(HttpClient.Version.HTTP_1_1)
                .header(HOST_HEADER, LOCALHOST_HTTP_PORT)
                .build();
        assertResponseStatusCode(httpClient, localhostPortRequest, HttpStatus.MISDIRECTED_REQUEST_421);
    }

    void assertResponseStatusCode(final HttpClient httpClient, final HttpRequest request, final int statusCodeExpected) throws IOException, InterruptedException {
        final HttpResponse<Void> response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());
        assertEquals(statusCodeExpected, response.statusCode());
    }

    void assertHttpConnectorFound(final Server server) {
        final Connector[] connectors = server.getConnectors();
        assertNotNull(connectors);
        final Connector connector = connectors[0];
        final List<String> protocols = connector.getProtocols();
        assertEquals(ApplicationLayerProtocol.HTTP_1_1.getProtocol(), protocols.getFirst());
    }

    void assertHttpsConnectorFound(final Server server) {
        final Connector[] connectors = server.getConnectors();
        assertNotNull(connectors);
        final Connector connector = connectors[0];
        final List<String> protocols = connector.getProtocols();
        assertEquals(SSL_PROTOCOL, protocols.getFirst());
    }

    void assertStandardConfigurationFound(final Server server) {
        assertNotNull(server);
        assertHandlersFound(server);

        final RequestLog requestLog = server.getRequestLog();
        assertNotNull(requestLog);
    }

    void assertHandlersFound(final Server server) {
        final Handler serverHandler = server.getHandler();
        assertInstanceOf(Handler.Collection.class, serverHandler);

        Handler defaultHandler = server.getDefaultHandler();
        assertInstanceOf(RewriteHandler.class, defaultHandler);

        final Handler.Collection handlerCollection = (Handler.Collection) serverHandler;
        final HeaderWriterHandler headerWriterHandler = handlerCollection.getDescendant(HeaderWriterHandler.class);
        assertNotNull(headerWriterHandler);
    }
}
