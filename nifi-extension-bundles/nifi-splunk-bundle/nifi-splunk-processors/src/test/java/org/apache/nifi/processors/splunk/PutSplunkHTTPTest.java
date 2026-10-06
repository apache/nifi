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
package org.apache.nifi.processors.splunk;

import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import mockwebserver3.RecordedRequest;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.cert.builder.StandardCertificateBuilder;
import org.apache.nifi.security.ssl.EphemeralKeyStoreBuilder;
import org.apache.nifi.security.ssl.StandardSslContextBuilder;
import org.apache.nifi.security.ssl.StandardTrustManagerBuilder;
import org.apache.nifi.ssl.SSLContextProvider;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Optional;
import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.x500.X500Principal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PutSplunkHTTPTest {
    private static final String SSL_CONTEXT_SERVICE_ID = "ssl-context-service";

    private static final String KEY_ALGORITHM = "RSA";

    private static final String HTTPS_SCHEME = "https";

    private static final String COLLECTOR_PATH = "/services/collector/raw";

    private static final String AUTHORIZATION_HEADER = "Authorization";

    private static final String REQUEST_CHANNEL_HEADER = "X-Splunk-Request-Channel";

    private static final String CERTIFICATE_SUBJECT_NAME = "CN=splunk.internal";

    private static final String TOKEN = "Splunk 888c5a81-8777-49a0-a3af-f76e050ab5d9";

    private static final String REQUEST_CHANNEL = "22bd7414-0d77-4c73-936d-c8f5d1b21862";

    private static final String EVENT = "splunk-https-event";

    private static final String ACK_ID = "1234";

    private static final String SUCCESS_RESPONSE = """
            {
                "text": "Success",
                "code": 0,
                "ackId": %s
            }""".formatted(ACK_ID);

    private static final X500Principal CERTIFICATE_SUBJECT = new X500Principal(CERTIFICATE_SUBJECT_NAME);

    private MockWebServer mockWebServer;

    private SSLContext sslContext;

    private X509TrustManager trustManager;

    @BeforeEach
    void startServer() throws Exception {
        final KeyPair keyPair = KeyPairGenerator.getInstance(KEY_ALGORITHM).generateKeyPair();
        final X509Certificate certificate = new StandardCertificateBuilder(keyPair, CERTIFICATE_SUBJECT, Duration.ofHours(1)).build();
        final KeyStore keyStore = new EphemeralKeyStoreBuilder()
                .addPrivateKeyEntry(new KeyStore.PrivateKeyEntry(keyPair.getPrivate(), new Certificate[]{certificate}))
                .addCertificate(certificate)
                .build();
        final char[] protectionParameter = new char[]{};

        sslContext = new StandardSslContextBuilder()
                .trustStore(keyStore)
                .keyStore(keyStore)
                .keyPassword(protectionParameter)
                .build();
        trustManager = new StandardTrustManagerBuilder().trustStore(keyStore).build();

        mockWebServer = new MockWebServer();
        mockWebServer.useHttps(sslContext.getSocketFactory());
        mockWebServer.start();
    }

    @AfterEach
    void shutdownServer() {
        mockWebServer.close();
    }

    @Test
    void testHttpsPostTrustsConfiguredHost() throws InitializationException, InterruptedException {
        mockWebServer.enqueue(new MockResponse.Builder()
                .code(HttpURLConnection.HTTP_OK)
                .body(SUCCESS_RESPONSE)
                .build());

        final SSLContextProvider sslContextProvider = mock(SSLContextProvider.class);
        when(sslContextProvider.getIdentifier()).thenReturn(SSL_CONTEXT_SERVICE_ID);
        when(sslContextProvider.createContext()).thenReturn(sslContext);
        when(sslContextProvider.createTrustManager()).thenReturn(trustManager);
        when(sslContextProvider.createKeyManager()).thenReturn(Optional.empty());

        final TestRunner runner = TestRunners.newTestRunner(PutSplunkHTTP.class);
        runner.addControllerService(SSL_CONTEXT_SERVICE_ID, sslContextProvider);
        runner.enableControllerService(sslContextProvider);
        runner.setProperty(SplunkAPICall.SSL_CONTEXT_SERVICE, SSL_CONTEXT_SERVICE_ID);
        runner.setProperty(SplunkAPICall.SCHEME, HTTPS_SCHEME);
        runner.setProperty(SplunkAPICall.HOSTNAME, mockWebServer.getHostName());
        runner.setProperty(SplunkAPICall.PORT, Integer.toString(mockWebServer.getPort()));
        runner.setProperty(SplunkAPICall.TOKEN, TOKEN);
        runner.setProperty(SplunkAPICall.REQUEST_CHANNEL, REQUEST_CHANNEL);
        runner.setProperty(PutSplunkHTTP.CHARSET, StandardCharsets.UTF_8.name());

        runner.enqueue(EVENT);
        runner.run();

        runner.assertAllFlowFilesTransferred(PutSplunkHTTP.RELATIONSHIP_SUCCESS, 1);
        final MockFlowFile outgoingFlowFile = runner.getFlowFilesForRelationship(PutSplunkHTTP.RELATIONSHIP_SUCCESS).getFirst();
        assertEquals(ACK_ID, outgoingFlowFile.getAttribute(SplunkAPICall.ACKNOWLEDGEMENT_ID_ATTRIBUTE));

        final RecordedRequest request = mockWebServer.takeRequest();
        assertEquals(COLLECTOR_PATH, request.getTarget());
        assertEquals(TOKEN, request.getHeaders().get(AUTHORIZATION_HEADER));
        assertEquals(REQUEST_CHANNEL, request.getHeaders().get(REQUEST_CHANNEL_HEADER));
        assertNotNull(request.getBody());
        assertEquals(EVENT, request.getBody().utf8());
    }
}
