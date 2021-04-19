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
package org.apache.nifi.processors.standard;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.servlet.http.HttpServletResponse;
import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.remote.io.socket.NetworkUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.util.KeyStoreUtils;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.security.util.StandardTlsConfiguration;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.ssl.RestrictedSSLContextService;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.web.util.ssl.SslContextUtils;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.thread.ThreadPool;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import static org.apache.nifi.processors.standard.ListenHTTP.RELATIONSHIP_SUCCESS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class TestListenHTTP {

    private static final String SSL_CONTEXT_SERVICE_IDENTIFIER = "ssl-context";

    private static final MediaType APPLICATION_OCTET_STREAM = MediaType.get("application/octet-stream");
    private static final String HTTP_BASE_PATH = "basePath";

    private final static String PORT_VARIABLE = "HTTP_PORT";
    private final static String HTTP_SERVER_PORT_EL = "${" + PORT_VARIABLE + "}";

    private final static String BASEPATH_VARIABLE = "HTTP_BASEPATH";
    private final static String HTTP_SERVER_BASEPATH_EL = "${" + BASEPATH_VARIABLE + "}";
    private static final String MULTIPART_ATTRIBUTE = "http.multipart.name";

    private static final String TLS_1_3 = "TLSv1.3";
    private static final String TLS_1_2 = "TLSv1.2";
    private static final String LOCALHOST = "localhost";

    private static final int SOCKET_CONNECT_TIMEOUT = 100;
    private static final long SERVER_START_TIMEOUT = 1200000;
    private static final Duration CLIENT_CALL_TIMEOUT = Duration.ofSeconds(10);
    public static final String LOCALHOST_DN = "CN=localhost";

    private static TlsConfiguration tlsConfiguration;
    private static TlsConfiguration serverConfiguration;
    private static TlsConfiguration serverTls_1_3_Configuration;
    private static TlsConfiguration serverNoTruststoreConfiguration;
    private static SSLContext serverKeyStoreSslContext;
    private static SSLContext serverKeyStoreNoTrustStoreSslContext;
    private static SSLContext keyStoreSslContext;
    private static SSLContext trustStoreSslContext;
    private static X509TrustManager trustManager;

    private ListenHTTP proc;
    private TestRunner runner;

    private int availablePort;

    @BeforeClass
    public static void setUpSuite() throws GeneralSecurityException, IOException {
        // generate new keystore and truststore
        tlsConfiguration = KeyStoreUtils.createTlsConfigAndNewKeystoreTruststore();

        serverConfiguration = new StandardTlsConfiguration(
                tlsConfiguration.getKeystorePath(),
                tlsConfiguration.getKeystorePassword(),
                tlsConfiguration.getKeyPassword(),
                tlsConfiguration.getKeystoreType(),
                tlsConfiguration.getTruststorePath(),
                tlsConfiguration.getTruststorePassword(),
                tlsConfiguration.getTruststoreType(),
                TLS_1_2
        );
        serverTls_1_3_Configuration = new StandardTlsConfiguration(
                tlsConfiguration.getKeystorePath(),
                tlsConfiguration.getKeystorePassword(),
                tlsConfiguration.getKeyPassword(),
                tlsConfiguration.getKeystoreType(),
                tlsConfiguration.getTruststorePath(),
                tlsConfiguration.getTruststorePassword(),
                tlsConfiguration.getTruststoreType(),
                TLS_1_3
        );
        serverNoTruststoreConfiguration = new StandardTlsConfiguration(
                tlsConfiguration.getKeystorePath(),
                tlsConfiguration.getKeystorePassword(),
                tlsConfiguration.getKeyPassword(),
                tlsConfiguration.getKeystoreType(),
                null,
                null,
                null,
                TLS_1_2
        );

        serverKeyStoreSslContext = SslContextUtils.createSslContext(serverConfiguration);
        trustManager = SslContextFactory.getX509TrustManager(serverConfiguration);
        serverKeyStoreNoTrustStoreSslContext = SslContextFactory.createSslContext(serverNoTruststoreConfiguration, new TrustManager[]{trustManager});

        keyStoreSslContext = SslContextUtils.createSslContext(new StandardTlsConfiguration(
                tlsConfiguration.getKeystorePath(),
                tlsConfiguration.getKeystorePassword(),
                tlsConfiguration.getKeystoreType(),
                tlsConfiguration.getTruststorePath(),
                tlsConfiguration.getTruststorePassword(),
                tlsConfiguration.getTruststoreType())
        );
        trustStoreSslContext = SslContextUtils.createSslContext(new StandardTlsConfiguration(
                null,
                null,
                null,
                tlsConfiguration.getTruststorePath(),
                tlsConfiguration.getTruststorePassword(),
                tlsConfiguration.getTruststoreType())
        );
    }

    @AfterClass
    public static void afterClass() throws Exception {
        if (tlsConfiguration != null) {
            try {
                if (StringUtils.isNotBlank(tlsConfiguration.getKeystorePath())) {
                    Files.deleteIfExists(Paths.get(tlsConfiguration.getKeystorePath()));
                }
            } catch (IOException e) {
                throw new IOException("There was an error deleting a keystore: " + e.getMessage(), e);
            }

            try {
                if (StringUtils.isNotBlank(tlsConfiguration.getTruststorePath())) {
                    Files.deleteIfExists(Paths.get(tlsConfiguration.getTruststorePath()));
                }
            } catch (IOException e) {
                throw new IOException("There was an error deleting a truststore: " + e.getMessage(), e);
            }
        }
    }

    @Before
    public void setup() throws IOException {
        proc = new ListenHTTP();
        runner = TestRunners.newTestRunner(proc);
        availablePort = NetworkUtils.availablePort();
        runner.setVariable(PORT_VARIABLE, Integer.toString(availablePort));
        runner.setVariable(BASEPATH_VARIABLE, HTTP_BASE_PATH);
    }

    @After
    public void shutdownServer() {
        proc.shutdownHttpServer();
    }

    @Test
    public void testPOSTRequestsReceivedWithoutEL() throws Exception {
        runner.setProperty(ListenHTTP.PORT, Integer.toString(availablePort));
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);

        testPOSTRequestsReceived(HttpServletResponse.SC_OK, false, false);
    }

    @Test
    public void testPOSTRequestsReceivedReturnCodeWithoutEL() throws Exception {
        runner.setProperty(ListenHTTP.PORT, Integer.toString(availablePort));
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.RETURN_CODE, Integer.toString(HttpServletResponse.SC_NO_CONTENT));

        testPOSTRequestsReceived(HttpServletResponse.SC_NO_CONTENT, false, false);
    }

    @Test
    public void testPOSTRequestsReceivedWithEL() throws Exception {
        runner.setProperty(ListenHTTP.PORT, HTTP_SERVER_PORT_EL);
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_SERVER_BASEPATH_EL);
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_OK, false, false);
    }

    @Test
    public void testPOSTRequestsReturnCodeReceivedWithEL() throws Exception {
        runner.setProperty(ListenHTTP.PORT, HTTP_SERVER_PORT_EL);
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_SERVER_BASEPATH_EL);
        runner.setProperty(ListenHTTP.RETURN_CODE, Integer.toString(HttpServletResponse.SC_NO_CONTENT));
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_NO_CONTENT, false, false);
    }

    @Test
    public void testSecurePOSTRequestsReceivedWithoutEL() throws Exception {
        configureProcessorSslContextService(ListenHTTP.ClientAuthentication.AUTO, serverNoTruststoreConfiguration);

        runner.setProperty(ListenHTTP.PORT, Integer.toString(availablePort));
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_OK, true, false);
    }

    @Test
    public void testSecurePOSTRequestsReturnCodeReceivedWithoutEL() throws Exception {
        configureProcessorSslContextService(ListenHTTP.ClientAuthentication.AUTO, serverNoTruststoreConfiguration);

        runner.setProperty(ListenHTTP.PORT, Integer.toString(availablePort));
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.RETURN_CODE, Integer.toString(HttpServletResponse.SC_NO_CONTENT));
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_NO_CONTENT, true, false);
    }

    @Test
    public void testSecurePOSTRequestsReceivedWithEL() throws Exception {
        configureProcessorSslContextService(ListenHTTP.ClientAuthentication.AUTO, serverNoTruststoreConfiguration);

        runner.setProperty(ListenHTTP.PORT, HTTP_SERVER_PORT_EL);
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_SERVER_BASEPATH_EL);
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_OK, true, false);
    }

    @Test
    public void testSecurePOSTRequestsReturnCodeReceivedWithEL() throws Exception {
        configureProcessorSslContextService(ListenHTTP.ClientAuthentication.AUTO, serverNoTruststoreConfiguration);

        runner.setProperty(ListenHTTP.PORT, Integer.toString(availablePort));
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.RETURN_CODE, Integer.toString(HttpServletResponse.SC_NO_CONTENT));
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_NO_CONTENT, true, false);
    }

    @Test
    public void testSecureTwoWaySslPOSTRequestsReceivedWithoutEL() throws Exception {
        configureProcessorSslContextService(ListenHTTP.ClientAuthentication.REQUIRED, serverConfiguration);

        runner.setProperty(ListenHTTP.PORT, Integer.toString(availablePort));
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_OK, true, true);
    }

    @Test
    public void testSecureTwoWaySslPOSTRequestsReceivedWithUnauthorizedSubjectDn() throws Exception {
        configureProcessorSslContextService(ListenHTTP.ClientAuthentication.REQUIRED, serverConfiguration);

        runner.setProperty(ListenHTTP.PORT, Integer.toString(availablePort));
        runner.setProperty(ListenHTTP.AUTHORIZED_DN_PATTERN, "CN=other");
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_FORBIDDEN, true, true);
    }

    @Test
    public void testSecureTwoWaySslPOSTRequestsReceivedWithAuthorizedIssuerDn() throws Exception {
        configureProcessorSslContextService(ListenHTTP.ClientAuthentication.REQUIRED, serverConfiguration);

        runner.setProperty(ListenHTTP.PORT, Integer.toString(availablePort));
        runner.setProperty(ListenHTTP.AUTHORIZED_DN_PATTERN, LOCALHOST_DN);
        runner.setProperty(ListenHTTP.AUTHORIZED_ISSUER_DN_PATTERN, LOCALHOST_DN);
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_OK, true, true);
    }

    @Test
    public void testSecureTwoWaySslPOSTRequestsReceivedWithUnauthorizedIssuerDn() throws Exception {
        configureProcessorSslContextService(ListenHTTP.ClientAuthentication.REQUIRED, serverConfiguration);

        runner.setProperty(ListenHTTP.PORT, Integer.toString(availablePort));
        runner.setProperty(ListenHTTP.AUTHORIZED_DN_PATTERN, LOCALHOST_DN); // Although subject is authorized, issuer is not
        runner.setProperty(ListenHTTP.AUTHORIZED_ISSUER_DN_PATTERN, "CN=other");
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_FORBIDDEN, true, true);
    }

    @Test
    public void testSecureTwoWaySslPOSTRequestsReturnCodeReceivedWithoutEL() throws Exception {
        configureProcessorSslContextService(ListenHTTP.ClientAuthentication.REQUIRED, serverConfiguration);

        runner.setProperty(ListenHTTP.PORT, Integer.toString(availablePort));
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.RETURN_CODE, Integer.toString(HttpServletResponse.SC_NO_CONTENT));
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_NO_CONTENT, true, true);
    }

    @Test
    public void testSecureTwoWaySslPOSTRequestsReceivedWithEL() throws Exception {
        configureProcessorSslContextService(ListenHTTP.ClientAuthentication.REQUIRED, serverConfiguration);

        runner.setProperty(ListenHTTP.PORT, HTTP_SERVER_PORT_EL);
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_SERVER_BASEPATH_EL);
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_OK, true, true);
    }

    @Test
    public void testSecureTwoWaySslPOSTRequestsReturnCodeReceivedWithEL() throws Exception {
        configureProcessorSslContextService(ListenHTTP.ClientAuthentication.REQUIRED, serverConfiguration);

        runner.setProperty(ListenHTTP.PORT, Integer.toString(availablePort));
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.RETURN_CODE, Integer.toString(HttpServletResponse.SC_NO_CONTENT));
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_NO_CONTENT, true, true);
    }

    @Test
    public void testSecureServerSupportsCurrentTlsProtocolVersion() throws Exception {
        configureProcessorSslContextService(ListenHTTP.ClientAuthentication.AUTO, serverNoTruststoreConfiguration);
        startSecureServer();

        final SSLSocketFactory sslSocketFactory = trustStoreSslContext.getSocketFactory();
        final SSLSocket sslSocket = (SSLSocket) sslSocketFactory.createSocket(LOCALHOST, availablePort);
        final String currentProtocol = serverNoTruststoreConfiguration.getProtocol();
        sslSocket.setEnabledProtocols(new String[]{currentProtocol});

        sslSocket.startHandshake();
        final SSLSession sslSession = sslSocket.getSession();
        assertEquals("SSL Session Protocol not matched", currentProtocol, sslSession.getProtocol());
    }

    @Test
    public void testSecureServerTrustStoreConfiguredClientAuthenticationRequired() throws Exception {
        configureProcessorSslContextService(ListenHTTP.ClientAuthentication.REQUIRED, serverConfiguration);
        startSecureServer();
        assertThrows(SSLException.class, () -> postMessage(null, true, false));
    }

    @Test
    public void testSecureServerTrustStoreNotConfiguredClientAuthenticationNotRequired() throws Exception {
        configureProcessorSslContextService(ListenHTTP.ClientAuthentication.AUTO, serverNoTruststoreConfiguration);
        startSecureServer();
        final int responseCode = postMessage(null, true, true);
        assertEquals(HttpServletResponse.SC_NO_CONTENT, responseCode);
    }

    @Test
    public void testSecureServerRejectsUnsupportedTlsProtocolVersion() throws Exception {
        final String currentProtocol = TlsConfiguration.getHighestCurrentSupportedTlsProtocolVersion();
        final String protocolMessage = String.format("TLS Protocol required [%s] found [%s]", TLS_1_3, currentProtocol);
        Assume.assumeTrue(protocolMessage, TLS_1_3.equals(currentProtocol));

        configureProcessorSslContextService(ListenHTTP.ClientAuthentication.AUTO, serverTls_1_3_Configuration);

        runner.setProperty(ListenHTTP.PORT, Integer.toString(availablePort));
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.RETURN_CODE, Integer.toString(HttpServletResponse.SC_NO_CONTENT));
        runner.assertValid();

        startWebServer();
        final SSLSocketFactory sslSocketFactory = trustStoreSslContext.getSocketFactory();
        final SSLSocket sslSocket = (SSLSocket) sslSocketFactory.createSocket(LOCALHOST, availablePort);
        sslSocket.setEnabledProtocols(new String[]{TLS_1_2});

        assertThrows(SSLHandshakeException.class, sslSocket::startHandshake);
    }

    @Test
    public void testMaxThreadPoolSizeTooLow() {
        // GIVEN, WHEN
        runner.setProperty(ListenHTTP.PORT, Integer.toString(availablePort));
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.MAX_THREAD_POOL_SIZE, "7");

        // THEN
        runner.assertNotValid();
    }

    @Test
    public void testMaxThreadPoolSizeTooHigh() {
        // GIVEN, WHEN
        runner.setProperty(ListenHTTP.PORT, Integer.toString(availablePort));
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.MAX_THREAD_POOL_SIZE, "1001");

        // THEN
        runner.assertNotValid();
    }

    @Test
    public void testMaxThreadPoolSizeOkLowerBound() {
        // GIVEN, WHEN
        runner.setProperty(ListenHTTP.PORT, Integer.toString(availablePort));
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.MAX_THREAD_POOL_SIZE, "8");

        // THEN
        runner.assertValid();
    }

    @Test
    public void testMaxThreadPoolSizeOkUpperBound() {
        // GIVEN, WHEN
        runner.setProperty(ListenHTTP.PORT, Integer.toString(availablePort));
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.MAX_THREAD_POOL_SIZE, "1000");

        // THEN
        runner.assertValid();
    }

    @Test
    public void testMaxThreadPoolSizeSpecifiedInThePropertyIsSetInTheServerInstance() {
        // GIVEN
        int maxThreadPoolSize = 201;
        runner.setProperty(ListenHTTP.PORT, Integer.toString(availablePort));
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.MAX_THREAD_POOL_SIZE, Integer.toString(maxThreadPoolSize));

        // WHEN
        startWebServer();

        // THEN
        Server server = proc.getServer();
        ThreadPool threadPool = server.getThreadPool();
        ThreadPool.SizedThreadPool sizedThreadPool = (ThreadPool.SizedThreadPool) threadPool;
        assertEquals(maxThreadPoolSize, sizedThreadPool.getMaxThreads());
    }

    private void startSecureServer() {
        runner.setProperty(ListenHTTP.PORT, Integer.toString(availablePort));
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.RETURN_CODE, Integer.toString(HttpServletResponse.SC_NO_CONTENT));
        runner.assertValid();
        startWebServer();
    }

    private int postMessage(String message, boolean secure, boolean clientAuthRequired) throws Exception {
        final OkHttpClient okHttpClient = getOkHttpClient(secure, clientAuthRequired);
        final Request.Builder requestBuilder = new Request.Builder();
        final String url = buildUrl(secure);
        requestBuilder.url(url);

        final byte[] bytes = message == null ? new byte[]{} : message.getBytes(StandardCharsets.UTF_8);
        final RequestBody requestBody = RequestBody.create(bytes, APPLICATION_OCTET_STREAM);
        final Request request = requestBuilder.post(requestBody).build();

        try (final Response response = okHttpClient.newCall(request).execute()) {
            return response.code();
        }
    }

    private OkHttpClient getOkHttpClient(final boolean secure, final boolean clientAuthRequired) {
        final OkHttpClient.Builder builder = new OkHttpClient.Builder();
        if (secure) {
            if (clientAuthRequired) {
                builder.sslSocketFactory(keyStoreSslContext.getSocketFactory(), trustManager);
            } else {
                builder.sslSocketFactory(trustStoreSslContext.getSocketFactory(), trustManager);
            }
        }

        builder.callTimeout(CLIENT_CALL_TIMEOUT);

        return builder.build();
    }

    private String buildUrl(final boolean secure) {
        return String.format("%s://localhost:%s/%s", secure ? "https" : "http", availablePort, HTTP_BASE_PATH);
    }

    private void testPOSTRequestsReceived(int returnCode, boolean secure, boolean twoWaySsl) throws Exception {
        final List<String> messages = new ArrayList<>();
        messages.add("payload 1");
        messages.add("");
        messages.add(null);
        messages.add("payload 2");

        startWebServerAndSendMessages(messages, returnCode, secure, twoWaySsl);

        List<MockFlowFile> mockFlowFiles = runner.getFlowFilesForRelationship(RELATIONSHIP_SUCCESS);

        if (returnCode < 400) { // Only if we actually expect success
            runner.assertTransferCount(RELATIONSHIP_SUCCESS, 4);

            mockFlowFiles.get(0).assertContentEquals("payload 1");
            mockFlowFiles.get(1).assertContentEquals("");
            mockFlowFiles.get(2).assertContentEquals("");
            mockFlowFiles.get(3).assertContentEquals("payload 2");

            if (twoWaySsl) {
                mockFlowFiles.get(0).assertAttributeEquals("restlistener.remote.user.dn", LOCALHOST_DN);
                mockFlowFiles.get(0).assertAttributeEquals("restlistener.remote.issuer.dn", LOCALHOST_DN);
            }
        }
    }

    private void startWebServer() {
        final ProcessSessionFactory processSessionFactory = runner.getProcessSessionFactory();
        final ProcessContext context = runner.getProcessContext();
        proc.onTrigger(context, processSessionFactory);

        final int port = context.getProperty(ListenHTTP.PORT).evaluateAttributeExpressions().asInteger();
        final InetSocketAddress socketAddress = new InetSocketAddress(LOCALHOST, port);
        final Socket socket = new Socket();
        boolean connected = false;
        long elapsed = 0;

        while (!connected && elapsed < SERVER_START_TIMEOUT) {
            final long started = System.currentTimeMillis();
            try {
                socket.connect(socketAddress, SOCKET_CONNECT_TIMEOUT);
                connected = true;
                runner.getLogger().debug("Server Socket Connected after {} ms", new Object[]{elapsed});
                socket.close();
            } catch (final Exception e) {
                runner.getLogger().debug("Server Socket Connect Failed: [{}] {}", new Object[]{e.getClass(), e.getMessage()});
            }
            final long connectElapsed = System.currentTimeMillis() - started;
            elapsed += connectElapsed;
        }

        if (!connected) {
            final String message = String.format("HTTP Server Port [%d] not listening after %d ms", port, SERVER_START_TIMEOUT);
            throw new IllegalStateException(message);
        }
    }

    private void startWebServerAndSendMessages(final List<String> messages, final int expectedStatusCode, final boolean secure, final boolean clientAuthRequired) throws Exception {
        startWebServer();

        for (final String message : messages) {
            final int statusCode = postMessage(message, secure, clientAuthRequired);
            assertEquals("HTTP Status Code not matched", expectedStatusCode, statusCode);
        }
    }

    private void configureProcessorSslContextService(final ListenHTTP.ClientAuthentication clientAuthentication,
                                                                  final TlsConfiguration tlsConfiguration) throws InitializationException {
        final RestrictedSSLContextService sslContextService = Mockito.mock(RestrictedSSLContextService.class);
        Mockito.when(sslContextService.getIdentifier()).thenReturn(SSL_CONTEXT_SERVICE_IDENTIFIER);
        Mockito.when(sslContextService.createTlsConfiguration()).thenReturn(tlsConfiguration);

        if (ListenHTTP.ClientAuthentication.REQUIRED.equals(clientAuthentication)) {
            Mockito.when(sslContextService.createContext()).thenReturn(serverKeyStoreSslContext);
        } else {
            Mockito.when(sslContextService.createContext()).thenReturn(serverKeyStoreNoTrustStoreSslContext);
        }
        runner.addControllerService(SSL_CONTEXT_SERVICE_IDENTIFIER, sslContextService);

        runner.setProperty(ListenHTTP.CLIENT_AUTHENTICATION, clientAuthentication.name());
        runner.setProperty(ListenHTTP.SSL_CONTEXT_SERVICE, SSL_CONTEXT_SERVICE_IDENTIFIER);

        runner.enableControllerService(sslContextService);
    }

    @Test
    public void testMultipartFormDataRequest() throws IOException {
        runner.setProperty(ListenHTTP.PORT, Integer.toString(availablePort));
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.RETURN_CODE, Integer.toString(HttpServletResponse.SC_OK));

        final SSLContextService sslContextService = runner.getControllerService(SSL_CONTEXT_SERVICE_IDENTIFIER, SSLContextService.class);
        final boolean isSecure = (sslContextService != null);
        startWebServer();

        File file1 = createTextFile("Hello", "World");
        File file2 = createTextFile("{ \"name\":\"John\", \"age\":30 }");

        MultipartBody multipartBody = new MultipartBody.Builder().setType(MultipartBody.FORM)
                .addFormDataPart("p1", "v1")
                .addFormDataPart("p2", "v2")
                .addFormDataPart("file1", "my-file-text.txt", RequestBody.create(file1, MediaType.parse("text/plain")))
                .addFormDataPart("file2", "my-file-data.json", RequestBody.create(file2, MediaType.parse("application/json")))
                .addFormDataPart("file3", "my-file-binary.bin", RequestBody.create(generateRandomBinaryData(), MediaType.parse("application/octet-stream")))
                .build();

        Request request =
                new Request.Builder()
                        .url(buildUrl(isSecure))
                        .post(multipartBody)
                        .build();

        final OkHttpClient client = getOkHttpClient(false, false);
        try (Response response = client.newCall(request).execute()) {
            Files.deleteIfExists(Paths.get(String.valueOf(file1)));
            Files.deleteIfExists(Paths.get(String.valueOf(file2)));
            Assert.assertTrue(String.format("Unexpected code: %s, body: %s", response.code(), response.body()), response.isSuccessful());
        }

        runner.assertAllFlowFilesTransferred(ListenHTTP.RELATIONSHIP_SUCCESS, 5);
        List<MockFlowFile> flowFilesForRelationship = runner.getFlowFilesForRelationship(ListenHTTP.RELATIONSHIP_SUCCESS);
        // Part fragments are not processed in the order we submitted them.
        // We cannot rely on the order we sent them in.
        MockFlowFile mff = findFlowFile(flowFilesForRelationship, "p1");
        mff.assertAttributeEquals("http.multipart.name", "p1");
        mff.assertAttributeExists("http.multipart.size");
        mff.assertAttributeEquals("http.multipart.fragments.sequence.number", "1");
        mff.assertAttributeEquals("http.multipart.fragments.total.number", "5");
        mff.assertAttributeExists("http.headers.multipart.content-disposition");

        mff = findFlowFile(flowFilesForRelationship, "p2");
        mff.assertAttributeEquals("http.multipart.name", "p2");
        mff.assertAttributeExists("http.multipart.size");
        mff.assertAttributeExists("http.multipart.fragments.sequence.number");
        mff.assertAttributeEquals("http.multipart.fragments.total.number", "5");
        mff.assertAttributeExists("http.headers.multipart.content-disposition");

        mff = findFlowFile(flowFilesForRelationship, "file1");
        mff.assertAttributeEquals("http.multipart.name", "file1");
        mff.assertAttributeEquals("http.multipart.filename", "my-file-text.txt");
        mff.assertAttributeEquals("http.headers.multipart.content-type", "text/plain");
        mff.assertAttributeExists("http.multipart.size");
        mff.assertAttributeExists("http.multipart.fragments.sequence.number");
        mff.assertAttributeEquals("http.multipart.fragments.total.number", "5");
        mff.assertAttributeExists("http.headers.multipart.content-disposition");

        mff = findFlowFile(flowFilesForRelationship, "file2");
        mff.assertAttributeEquals("http.multipart.name", "file2");
        mff.assertAttributeEquals("http.multipart.filename", "my-file-data.json");
        mff.assertAttributeEquals("http.headers.multipart.content-type", "application/json");
        mff.assertAttributeExists("http.multipart.size");
        mff.assertAttributeExists("http.multipart.fragments.sequence.number");
        mff.assertAttributeEquals("http.multipart.fragments.total.number", "5");
        mff.assertAttributeExists("http.headers.multipart.content-disposition");

        mff = findFlowFile(flowFilesForRelationship, "file3");
        mff.assertAttributeEquals("http.multipart.name", "file3");
        mff.assertAttributeEquals("http.multipart.filename", "my-file-binary.bin");
        mff.assertAttributeEquals("http.headers.multipart.content-type", "application/octet-stream");
        mff.assertAttributeExists("http.multipart.size");
        mff.assertAttributeExists("http.multipart.fragments.sequence.number");
        mff.assertAttributeEquals("http.multipart.fragments.total.number", "5");
        mff.assertAttributeExists("http.headers.multipart.content-disposition");
    }

    private byte[] generateRandomBinaryData() {
        byte[] bytes = new byte[100];
        new Random().nextBytes(bytes);
        return bytes;
    }

    private File createTextFile(String...lines) throws IOException {
        final File textFile = Files.createTempFile(TestListenHTTP.class.getSimpleName(), ".txt").toFile();
        textFile.deleteOnExit();

        Files.write(textFile.toPath(), Arrays.asList(lines));
        return textFile;
    }

    protected MockFlowFile findFlowFile(final List<MockFlowFile> flowFiles, final String attributeValue) {
        final Optional<MockFlowFile> foundFlowFile = flowFiles.stream().filter(flowFile -> flowFile.getAttribute(MULTIPART_ATTRIBUTE).equals(attributeValue)).findFirst();
        return foundFlowFile.orElseThrow(() -> new NullPointerException(MULTIPART_ATTRIBUTE));
    }
}
