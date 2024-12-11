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
package org.apache.nifi.web.client.provider.service;

import okhttp3.Credentials;
import okhttp3.HttpUrl;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.nifi.proxy.ProxyConfiguration;
import org.apache.nifi.proxy.ProxyConfigurationService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.cert.builder.StandardCertificateBuilder;
import org.apache.nifi.security.ssl.EphemeralKeyStoreBuilder;
import org.apache.nifi.security.ssl.StandardSslContextBuilder;
import org.apache.nifi.security.ssl.StandardTrustManagerBuilder;
import org.apache.nifi.ssl.SSLContextProvider;
import org.apache.nifi.util.NoOpProcessor;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.HttpResponseStatus;
import org.apache.nifi.web.client.api.HttpUriBuilder;
import org.apache.nifi.web.client.api.WebClientService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.x500.X500Principal;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URI;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StandardWebClientServiceProviderTest {
    private static final String SERVICE_ID = StandardWebClientServiceProvider.class.getSimpleName();

    private static final String SSL_CONTEXT_SERVICE_ID = SSLContextProvider.class.getSimpleName();

    private static final String PROXY_SERVICE_ID = ProxyConfigurationService.class.getSimpleName();

    private static final String LOCALHOST = "localhost";

    private static final String HTTPS = "https";

    private static final int PORT = 8443;

    private static final String PATH_SEGMENT = "resources";

    private static final String PARAMETER_NAME = "search";

    private static final String PARAMETER_VALUE = "search";

    private static final String ROOT_PATH = "/";

    private static final URI LOCALHOST_URI = URI.create(String.format("%s://%s:%d/%s?%s=%s", HTTPS, LOCALHOST, PORT, PATH_SEGMENT, PARAMETER_NAME, PARAMETER_VALUE));

    private static final String PROXY_AUTHENTICATE_HEADER = "Proxy-Authenticate";

    private static final String PROXY_AUTHENTICATE_BASIC_REALM = "Basic realm=\"Authentication Required\"";

    private static final String PROXY_AUTHORIZATION_HEADER = "Proxy-Authorization";

    private static final boolean TUNNEL_PROXY_DISABLED = false;

    static SSLContext sslContext;

    static X509TrustManager trustManager;

    @Mock
    SSLContextProvider sslContextProvider;

    @Mock
    ProxyConfigurationService proxyConfigurationService;

    TestRunner runner;

    MockWebServer mockWebServer;

    StandardWebClientServiceProvider provider;

    @BeforeAll
    static void setTlsConfiguration() throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
        final X509Certificate certificate = new StandardCertificateBuilder(keyPair, new X500Principal("CN=localhost"), Duration.ofHours(1)).build();
        final KeyStore keyStore = new EphemeralKeyStoreBuilder()
                .addPrivateKeyEntry(new KeyStore.PrivateKeyEntry(keyPair.getPrivate(), new Certificate[]{certificate}))
                .build();
        final char[] protectionParameter = new char[]{};

        sslContext = new StandardSslContextBuilder()
                .trustStore(keyStore)
                .keyStore(keyStore)
                .keyPassword(protectionParameter)
                .build();

        trustManager = new StandardTrustManagerBuilder().trustStore(keyStore).build();
    }

    @BeforeEach
    void setRunner() throws InitializationException {
        mockWebServer = new MockWebServer();

        runner = TestRunners.newTestRunner(NoOpProcessor.class);

        provider = new StandardWebClientServiceProvider();
        runner.addControllerService(SERVICE_ID, provider);
    }

    @AfterEach
    void shutdownServer() throws IOException {
        mockWebServer.shutdown();
    }

    @Test
    void testEnable() {
        runner.enableControllerService(provider);
    }

    @Test
    void testGetHttpUriBuilder() {
        runner.enableControllerService(provider);

        final HttpUriBuilder httpUriBuilder = provider.getHttpUriBuilder();

        final URI uri = httpUriBuilder.scheme(HTTPS)
                .host(LOCALHOST)
                .port(PORT)
                .addPathSegment(PATH_SEGMENT)
                .addQueryParameter(PARAMETER_NAME, PARAMETER_VALUE)
                .build();

        assertEquals(LOCALHOST_URI, uri);
    }

    @Test
    void testGetWebServiceClientGetUri() throws InterruptedException {
        runner.enableControllerService(provider);

        final WebClientService webClientService = provider.getWebClientService();

        assertNotNull(webClientService);

        assertGetUriCompleted(webClientService);
    }

    @Test
    void testGetWebServiceClientSslContextServiceConfiguredGetUri() throws InitializationException, InterruptedException {
        when(sslContextProvider.getIdentifier()).thenReturn(SSL_CONTEXT_SERVICE_ID);
        when(sslContextProvider.createTrustManager()).thenReturn(trustManager);
        when(sslContextProvider.createContext()).thenReturn(sslContext);

        runner.addControllerService(SSL_CONTEXT_SERVICE_ID, sslContextProvider);
        runner.enableControllerService(sslContextProvider);

        runner.setProperty(provider, StandardWebClientServiceProvider.SSL_CONTEXT_SERVICE, SSL_CONTEXT_SERVICE_ID);
        runner.enableControllerService(provider);

        final WebClientService webClientService = provider.getWebClientService();

        assertNotNull(webClientService);

        final SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();
        mockWebServer.useHttps(sslSocketFactory, TUNNEL_PROXY_DISABLED);

        assertGetUriCompleted(webClientService);
    }

    @Test
    void testGetWebServiceClientProxyConfigurationGetUri() throws InitializationException, InterruptedException {
        final Proxy proxy = mockWebServer.toProxyAddress();
        final InetSocketAddress proxyAddress = (InetSocketAddress) proxy.address();

        final ProxyConfiguration proxyConfiguration = new ProxyConfiguration();
        proxyConfiguration.setProxyType(Proxy.Type.HTTP);
        proxyConfiguration.setProxyServerHost(proxyAddress.getHostName());
        proxyConfiguration.setProxyServerPort(proxyAddress.getPort());

        final String username = String.class.getSimpleName();
        final String password = String.class.getName();
        proxyConfiguration.setProxyUserName(username);
        proxyConfiguration.setProxyUserPassword(password);

        when(proxyConfigurationService.getIdentifier()).thenReturn(PROXY_SERVICE_ID);
        when(proxyConfigurationService.getConfiguration()).thenReturn(proxyConfiguration);

        mockWebServer.enqueue(new MockResponse()
                .setResponseCode(HttpResponseStatus.PROXY_AUTHENTICATION_REQUIRED.getCode())
                .setHeader(PROXY_AUTHENTICATE_HEADER, PROXY_AUTHENTICATE_BASIC_REALM)
        );

        runner.addControllerService(PROXY_SERVICE_ID, proxyConfigurationService);
        runner.enableControllerService(proxyConfigurationService);

        runner.setProperty(provider, ProxyConfigurationService.PROXY_CONFIGURATION_SERVICE, PROXY_SERVICE_ID);
        runner.enableControllerService(provider);

        final WebClientService webClientService = provider.getWebClientService();

        assertNotNull(webClientService);

        assertGetUriCompleted(webClientService);

        final RecordedRequest proxyAuthorizationRequest = mockWebServer.takeRequest();
        final String proxyAuthorization = proxyAuthorizationRequest.getHeader(PROXY_AUTHORIZATION_HEADER);
        final String credentials = Credentials.basic(username, password);
        assertEquals(credentials, proxyAuthorization);
    }

    private void assertGetUriCompleted(final WebClientService webClientService) throws InterruptedException {
        final URI uri = mockWebServer.url(ROOT_PATH).newBuilder().host(LOCALHOST).build().uri();

        final HttpResponseStatus httpResponseStatus = HttpResponseStatus.OK;
        final MockResponse mockResponse = new MockResponse().setResponseCode(httpResponseStatus.getCode());
        mockWebServer.enqueue(mockResponse);

        final HttpResponseEntity httpResponseEntity = webClientService.get().uri(uri).retrieve();

        assertNotNull(httpResponseEntity);
        assertEquals(httpResponseStatus.getCode(), httpResponseEntity.statusCode());

        final RecordedRequest request = mockWebServer.takeRequest();
        final HttpUrl requestUrl = request.getRequestUrl();
        assertNotNull(requestUrl);

        final URI requestUri = requestUrl.uri();
        assertEquals(uri.getPort(), requestUri.getPort());
    }
}
