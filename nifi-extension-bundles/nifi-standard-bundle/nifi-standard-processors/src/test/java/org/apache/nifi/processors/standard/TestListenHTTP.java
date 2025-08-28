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

import jakarta.servlet.http.HttpServletResponse;
import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okio.BufferedSink;
import okio.GzipSink;
import okio.Okio;
import org.apache.nifi.flowfile.attributes.StandardFlowFileMediaType;
import org.apache.nifi.processors.standard.http.ContentEncodingStrategy;
import org.apache.nifi.processors.standard.http.HttpProtocolStrategy;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.cert.builder.StandardCertificateBuilder;
import org.apache.nifi.security.ssl.EphemeralKeyStoreBuilder;
import org.apache.nifi.security.ssl.StandardSslContextBuilder;
import org.apache.nifi.security.ssl.StandardTrustManagerBuilder;
import org.apache.nifi.security.util.TlsPlatform;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.ssl.SSLContextProvider;
import org.apache.nifi.util.FlowFilePackagerV3;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.eclipse.jetty.server.NetworkConnector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.thread.ThreadPool;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.x500.X500Principal;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.stream.IntStream;

import static org.apache.nifi.processors.standard.ListenHTTP.RELATIONSHIP_SUCCESS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestListenHTTP {

    private static final String SSL_CONTEXT_SERVICE_IDENTIFIER = "ssl-context";

    private static final MediaType APPLICATION_OCTET_STREAM = MediaType.get("application/octet-stream");
    private static final String HTTP_BASE_PATH = "basePath";

    private final static String PORT_VARIABLE = "HTTP_PORT";
    private final static String HTTP_SERVER_PORT_EL = "${" + PORT_VARIABLE + "}";

    private final static String BASEPATH_VARIABLE = "HTTP_BASEPATH";
    private final static String HTTP_SERVER_BASEPATH_EL = "${" + BASEPATH_VARIABLE + "}";
    private static final String MULTIPART_ATTRIBUTE = "http.multipart.name";

    private static final String LOCALHOST = "localhost";

    private static final int SOCKET_CONNECT_TIMEOUT = 100;
    private static final long SERVER_START_TIMEOUT = 1200000;

    private static final String HTTP_POST = "POST";
    private static final String HTTP_TRACE = "TRACE";
    private static final String HTTP_OPTIONS = "OPTIONS";

    private static final Duration CLIENT_CALL_TIMEOUT = Duration.ofSeconds(10);
    private static final String LOCALHOST_DN = "CN=localhost";

    private static SSLContext serverKeyStoreNoTrustStoreSslContext;
    private static SSLContext keyStoreSslContext;
    private static SSLContext trustStoreSslContext;
    private static X509TrustManager trustManager;
    private static X509TrustManager defaultTrustManager;

    private ListenHTTP proc;
    private TestRunner runner;

    @BeforeAll
    public static void setUpSuite() throws GeneralSecurityException {
        final KeyPair keyPair = KeyPairGenerator.getInstance("RSA").generateKeyPair();
        final X509Certificate certificate = new StandardCertificateBuilder(keyPair, new X500Principal("CN=localhost"), Duration.ofHours(1)).build();
        final KeyStore keyStore = new EphemeralKeyStoreBuilder()
                .addPrivateKeyEntry(new KeyStore.PrivateKeyEntry(keyPair.getPrivate(), new Certificate[]{certificate}))
                .build();
        final char[] protectionParameter = new char[]{};
        trustManager = new StandardTrustManagerBuilder().trustStore(keyStore).build();
        serverKeyStoreNoTrustStoreSslContext = new StandardSslContextBuilder()
                .keyStore(keyStore)
                .keyPassword(protectionParameter)
                .build();

        keyStoreSslContext = new StandardSslContextBuilder()
                .trustStore(keyStore)
                .keyStore(keyStore)
                .keyPassword(protectionParameter)
                .build();
        trustStoreSslContext = new StandardSslContextBuilder()
                .trustStore(keyStore)
                .build();

        final TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init((KeyStore) null);
        final TrustManager[] trustManagers = trustManagerFactory.getTrustManagers();
        defaultTrustManager = (X509TrustManager) trustManagers[0];
    }

    @BeforeEach
    public void setup() throws IOException {
        proc = new ListenHTTP();

        runner = TestRunners.newTestRunner(proc);
        runner.setEnvironmentVariableValue(PORT_VARIABLE, "0");
        runner.setEnvironmentVariableValue(BASEPATH_VARIABLE, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.PORT, "0");
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
    }

    @AfterEach
    public void shutdownServer() {
        proc.shutdownHttpServer();
    }

    @Test
    public void testPOSTRequestsReceivedWithoutEL() throws Exception {
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);

        testPOSTRequestsReceived(HttpServletResponse.SC_OK, false, false);
    }

    @Test
    public void testPOSTRequestsReceivedReturnCodeWithoutEL() throws Exception {
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
    public void testSecurePOSTRequestsReceivedWithoutELHttp2AndHttp1() throws Exception {
        configureProcessorSslContextService(ListenHTTP.ClientAuthentication.AUTO);

        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.HTTP_PROTOCOL_STRATEGY, HttpProtocolStrategy.H2_HTTP_1_1);
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_OK, true, false);
    }

    @Test
    public void testSecurePOSTRequestsReturnCodeReceivedWithoutELHttp2() throws Exception {
        configureProcessorSslContextService(ListenHTTP.ClientAuthentication.AUTO);

        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.RETURN_CODE, Integer.toString(HttpServletResponse.SC_NO_CONTENT));
        runner.setProperty(ListenHTTP.HTTP_PROTOCOL_STRATEGY, HttpProtocolStrategy.H2);
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_NO_CONTENT, true, false);
    }

    @Test
    public void testSecurePOSTRequestsReceivedWithEL() throws Exception {
        configureProcessorSslContextService(ListenHTTP.ClientAuthentication.AUTO);

        runner.setProperty(ListenHTTP.PORT, HTTP_SERVER_PORT_EL);
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_SERVER_BASEPATH_EL);
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_OK, true, false);
    }

    @Test
    public void testSecurePOSTRequestsReturnCodeReceivedWithEL() throws Exception {
        configureProcessorSslContextService(ListenHTTP.ClientAuthentication.AUTO);

        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.RETURN_CODE, Integer.toString(HttpServletResponse.SC_NO_CONTENT));
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_NO_CONTENT, true, false);
    }

    @Test
    public void testSecureTwoWaySslPOSTRequestsReceivedWithoutEL() throws Exception {
        configureProcessorSslContextService(ListenHTTP.ClientAuthentication.REQUIRED);

        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_OK, true, true);
    }

    @Test
    public void testSecureTwoWaySslPOSTRequestsReceivedWithUnauthorizedSubjectDn() throws Exception {
        configureProcessorSslContextService(ListenHTTP.ClientAuthentication.REQUIRED);

        runner.setProperty(ListenHTTP.AUTHORIZED_DN_PATTERN, "CN=other");
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_FORBIDDEN, true, true);
    }

    @Test
    public void testSecureTwoWaySslPOSTRequestsReceivedWithAuthorizedIssuerDn() throws Exception {
        configureProcessorSslContextService(ListenHTTP.ClientAuthentication.REQUIRED);

        runner.setProperty(ListenHTTP.AUTHORIZED_DN_PATTERN, LOCALHOST_DN);
        runner.setProperty(ListenHTTP.AUTHORIZED_ISSUER_DN_PATTERN, LOCALHOST_DN);
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_OK, true, true);
    }

    @Test
    public void testSecureTwoWaySslPOSTRequestsReceivedWithUnauthorizedIssuerDn() throws Exception {
        configureProcessorSslContextService(ListenHTTP.ClientAuthentication.REQUIRED);

        runner.setProperty(ListenHTTP.AUTHORIZED_DN_PATTERN, LOCALHOST_DN); // Although subject is authorized, issuer is not
        runner.setProperty(ListenHTTP.AUTHORIZED_ISSUER_DN_PATTERN, "CN=other");
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_FORBIDDEN, true, true);
    }

    @Test
    public void testSecureTwoWaySslPOSTRequestsReturnCodeReceivedWithoutEL() throws Exception {
        configureProcessorSslContextService(ListenHTTP.ClientAuthentication.REQUIRED);

        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.RETURN_CODE, Integer.toString(HttpServletResponse.SC_NO_CONTENT));
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_NO_CONTENT, true, true);
    }

    @Test
    public void testSecureTwoWaySslPOSTRequestsReceivedWithEL() throws Exception {
        configureProcessorSslContextService(ListenHTTP.ClientAuthentication.REQUIRED);

        runner.setProperty(ListenHTTP.PORT, HTTP_SERVER_PORT_EL);
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_SERVER_BASEPATH_EL);
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_OK, true, true);
    }

    @Test
    public void testSecureTwoWaySslPOSTRequestsReturnCodeReceivedWithEL() throws Exception {
        configureProcessorSslContextService(ListenHTTP.ClientAuthentication.REQUIRED);

        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.RETURN_CODE, Integer.toString(HttpServletResponse.SC_NO_CONTENT));
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_NO_CONTENT, true, true);
    }

    @Test
    public void testSecureServerSupportsCurrentTlsProtocolVersion() throws Exception {
        configureProcessorSslContextService(ListenHTTP.ClientAuthentication.AUTO);
        final int listeningPort = startSecureServer();

        final SSLSocketFactory sslSocketFactory = trustStoreSslContext.getSocketFactory();
        try (final SSLSocket sslSocket = (SSLSocket) sslSocketFactory.createSocket(LOCALHOST, listeningPort)) {
            final String currentProtocol = TlsPlatform.getLatestProtocol();
            sslSocket.setEnabledProtocols(new String[]{currentProtocol});

            sslSocket.startHandshake();
            final SSLSession sslSession = sslSocket.getSession();
            assertEquals(currentProtocol, sslSession.getProtocol());
        }
    }

    @Test
    public void testSecureServerTrustStoreConfiguredClientAuthenticationRequired() throws Exception {
        configureProcessorSslContextService(ListenHTTP.ClientAuthentication.REQUIRED);
        final int port = startSecureServer();
        assertThrows(IOException.class, () -> sendMessage(null, true, port, HTTP_BASE_PATH, false, HTTP_POST));
    }

    @Test
    public void testSecureServerTrustStoreNotConfiguredClientAuthenticationNotRequired() throws Exception {
        configureProcessorSslContextService(ListenHTTP.ClientAuthentication.AUTO);
        final int port = startSecureServer();
        final int responseCode = sendMessage(null, true, port, HTTP_BASE_PATH, true, HTTP_POST);
        assertEquals(HttpServletResponse.SC_NO_CONTENT, responseCode);
    }

    @Test
    public void testMaxThreadPoolSizeTooLow() {
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.MAX_THREAD_POOL_SIZE, "7");

        runner.assertNotValid();
    }

    @Test
    public void testMaxThreadPoolSizeTooHigh() {
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.MAX_THREAD_POOL_SIZE, "1001");

        runner.assertNotValid();
    }

    @Test
    public void testMaxThreadPoolSizeOkLowerBound() {
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.MAX_THREAD_POOL_SIZE, "8");

        runner.assertValid();
    }

    @Test
    public void testMaxThreadPoolSizeOkUpperBound() {
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.MAX_THREAD_POOL_SIZE, "1000");

        runner.assertValid();
    }

    @Test
    public void testMaxThreadPoolSizeSpecifiedInThePropertyIsSetInTheServerInstance() {
        int maxThreadPoolSize = 201;
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.MAX_THREAD_POOL_SIZE, Integer.toString(maxThreadPoolSize));

        startWebServer();

        Server server = proc.getServer();
        ThreadPool threadPool = server.getThreadPool();
        ThreadPool.SizedThreadPool sizedThreadPool = (ThreadPool.SizedThreadPool) threadPool;
        assertEquals(maxThreadPoolSize, sizedThreadPool.getMaxThreads());
    }

    @Test
    public void testPOSTRequestsReceivedWithRecordReader() throws Exception {
        final MockRecordParser parser = setupRecordReaderTest();

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("code", RecordFieldType.LONG);

        final List<Integer> keys = Arrays.asList(1, 2, 3, 4);
        final List<String> names = Arrays.asList("rec1", "rec2", "rec3", "rec4");
        final List<Long> codes = Arrays.asList(101L, 102L, 103L, 104L);

        for (int i = 0; i < keys.size(); i++) {
            parser.addRecord(keys.get(i), names.get(i), codes.get(i));
        }

        final String expectedMessage =
                """
                        "1","rec1","101"
                        "2","rec2","102"
                        "3","rec3","103"
                        "4","rec4","104"
                        """;

        startWebServerAndSendMessages(Collections.singletonList(""), HttpServletResponse.SC_OK, false, false, HTTP_POST);
        List<MockFlowFile> mockFlowFiles = runner.getFlowFilesForRelationship(RELATIONSHIP_SUCCESS);

        runner.assertTransferCount(RELATIONSHIP_SUCCESS, 1);
        mockFlowFiles.getFirst().assertContentEquals(expectedMessage);
    }

    @Test
    public void testReturn400WhenInvalidPOSTRequestSentWithRecordReader() throws Exception {
        final MockRecordParser parser = setupRecordReaderTest();
        parser.failAfter(2);

        parser.addSchemaField("id", RecordFieldType.INT);
        parser.addSchemaField("name", RecordFieldType.STRING);
        parser.addSchemaField("code", RecordFieldType.LONG);

        final List<Integer> keys = Arrays.asList(1, 2, 3, 4);
        final List<String> names = Arrays.asList("rec1", "rec2", "rec3", "rec4");
        final List<Long> codes = Arrays.asList(101L, 102L, 103L, 104L);

        for (int i = 0; i < keys.size(); i++) {
            parser.addRecord(keys.get(i), names.get(i), codes.get(i));
        }

        startWebServerAndSendMessages(Collections.singletonList(""), HttpServletResponse.SC_BAD_REQUEST, false, false, HTTP_POST);

        runner.assertTransferCount(RELATIONSHIP_SUCCESS, 0);
    }

    @Test
    public void testPostContentEncodingGzipAccepted() throws IOException {
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.RETURN_CODE, Integer.toString(HttpServletResponse.SC_NO_CONTENT));

        final int port = startWebServer();

        final OkHttpClient okHttpClient = getOkHttpClient(false, false);
        final Request.Builder requestBuilder = new Request.Builder();
        final String url = buildUrl(false, port, HTTP_BASE_PATH);
        requestBuilder.url(url);

        final String message = String.class.getSimpleName();
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        final BufferedSink gzipSink = Okio.buffer(new GzipSink(Okio.sink(outputStream)));
        gzipSink.write(message.getBytes(StandardCharsets.UTF_8));
        gzipSink.close();
        final byte[] compressed = outputStream.toByteArray();

        final RequestBody requestBody = RequestBody.create(compressed, APPLICATION_OCTET_STREAM);
        final Request request = requestBuilder.post(requestBody)
                .addHeader("Content-Encoding", ContentEncodingStrategy.GZIP.getValue().toLowerCase())
                .build();

        try (final Response response = okHttpClient.newCall(request).execute()) {
            assertTrue(response.isSuccessful());

            runner.assertTransferCount(RELATIONSHIP_SUCCESS, 1);
            final MockFlowFile flowFile = runner.getFlowFilesForRelationship(RELATIONSHIP_SUCCESS).getFirst();
            flowFile.assertContentEquals(message);
        }
    }

    @Test
    public void testOptionsNotAllowed() throws Exception {
        final List<String> messages = new ArrayList<>();
        messages.add("payload 1");

        startWebServerAndSendMessages(messages, HttpServletResponse.SC_METHOD_NOT_ALLOWED, false, false, HTTP_OPTIONS);
    }

    @Test
    public void testOptionsNotAllowedOnNonBasePath() throws Exception {
        final int port = startWebServer();
        final int statusCode = sendMessage("payload 1", false, port, "randomPath", false, HTTP_OPTIONS);

        assertEquals(HttpServletResponse.SC_METHOD_NOT_ALLOWED, statusCode, "HTTP Status Code not matched");
    }

    @Test
    public void testTraceNotAllowed() throws Exception {
        final List<String> messages = new ArrayList<>();
        messages.add("payload 1");

        startWebServerAndSendMessages(messages, HttpServletResponse.SC_METHOD_NOT_ALLOWED, false, false, HTTP_TRACE);
    }

    @Test
    public void testTraceNotAllowedOnNonBasePath() throws Exception {
        final int port = startWebServer();
        final int statusCode = sendMessage("payload 1", false, port, "randomPath", false, HTTP_TRACE);

        assertEquals(HttpServletResponse.SC_METHOD_NOT_ALLOWED, statusCode, "HTTP Status Code not matched");
    }

    private MockRecordParser setupRecordReaderTest() throws InitializationException {
        final MockRecordParser parser = new MockRecordParser();
        final MockRecordWriter writer = new MockRecordWriter();

        runner.addControllerService("mockRecordParser", parser);
        runner.setProperty(ListenHTTP.RECORD_READER, "mockRecordParser");
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.addControllerService("mockRecordWriter", writer);
        runner.setProperty(ListenHTTP.RECORD_WRITER, "mockRecordWriter");

        runner.enableControllerService(parser);
        runner.enableControllerService(writer);

        return parser;
    }

    private int startSecureServer() {
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.RETURN_CODE, Integer.toString(HttpServletResponse.SC_NO_CONTENT));
        runner.assertValid();
        return startWebServer();
    }

    private int sendMessage(final String message, final boolean secure, final int port, final String basePath, boolean clientAuthRequired, final String httpMethod) throws IOException {
        final byte[] bytes = message == null ? new byte[]{} : message.getBytes(StandardCharsets.UTF_8);
        final RequestBody requestBody = RequestBody.create(bytes, APPLICATION_OCTET_STREAM);
        final String url = buildUrl(secure, port, basePath);
        final Request.Builder requestBuilder = new Request.Builder();
        final Request request = requestBuilder.method(httpMethod, requestBody)
                .url(url)
                .build();

        final OkHttpClient okHttpClient = getOkHttpClient(secure, clientAuthRequired);
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

    private String buildUrl(final boolean secure, final int port, String basePath) {
        return String.format("%s://localhost:%s/%s", secure ? "https" : "http", port, basePath);
    }

    private void testPOSTRequestsReceived(int returnCode, boolean secure, boolean twoWaySsl) throws Exception {
        final List<String> messages = new ArrayList<>();
        messages.add("payload 1");
        messages.add("");
        messages.add(null);
        messages.add("payload 2");

        startWebServerAndSendMessages(messages, returnCode, secure, twoWaySsl, HTTP_POST);

        List<MockFlowFile> mockFlowFiles = runner.getFlowFilesForRelationship(RELATIONSHIP_SUCCESS);

        if (returnCode < 400) { // Only if we actually expect success
            runner.assertTransferCount(RELATIONSHIP_SUCCESS, 4);

            mockFlowFiles.get(0).assertContentEquals("payload 1");
            mockFlowFiles.get(1).assertContentEquals("");
            mockFlowFiles.get(2).assertContentEquals("");
            mockFlowFiles.get(3).assertContentEquals("payload 2");

            if (twoWaySsl) {
                mockFlowFiles.getFirst().assertAttributeEquals("restlistener.remote.user.dn", LOCALHOST_DN);
                mockFlowFiles.getFirst().assertAttributeEquals("restlistener.remote.issuer.dn", LOCALHOST_DN);
            }
        }
    }

    private int startWebServer() {
        runner.run(1, false);
        final int listeningPort = ((NetworkConnector) proc.getServer().getConnectors()[0]).getLocalPort();

        final InetSocketAddress socketAddress = new InetSocketAddress(LOCALHOST, listeningPort);
        final Socket socket = new Socket();
        boolean connected = false;
        long elapsed = 0;

        while (!connected && elapsed < SERVER_START_TIMEOUT) {
            final long started = System.currentTimeMillis();
            try {
                socket.connect(socketAddress, SOCKET_CONNECT_TIMEOUT);
                connected = true;
                runner.getLogger().debug("Server Socket Connected after {} ms", elapsed);
                socket.close();
            } catch (final Exception e) {
                runner.getLogger().debug("Server Socket Connect Failed", e);
            }
            final long connectElapsed = System.currentTimeMillis() - started;
            elapsed += connectElapsed;
        }

        if (!connected) {
            final String message = String.format("HTTP Server Port [%d] not listening after %d ms", listeningPort, SERVER_START_TIMEOUT);
            throw new IllegalStateException(message);
        }

        return listeningPort;
    }

    private void startWebServerAndSendMessages(final List<String> messages, final int expectedStatusCode, final boolean secure, final boolean clientAuthRequired,
                                               final String httpMethod) throws Exception {
        final int port = startWebServer();

        for (final String message : messages) {
            final int statusCode = sendMessage(message, secure, port, HTTP_BASE_PATH, clientAuthRequired, httpMethod);
            assertEquals(expectedStatusCode, statusCode, "HTTP Status Code not matched");
        }
    }

    private void configureProcessorSslContextService(final ListenHTTP.ClientAuthentication clientAuthentication) throws InitializationException {
        final SSLContextProvider sslContextProvider = mock(SSLContextProvider.class);
        when(sslContextProvider.getIdentifier()).thenReturn(SSL_CONTEXT_SERVICE_IDENTIFIER);

        if (ListenHTTP.ClientAuthentication.REQUIRED.equals(clientAuthentication)) {
            when(sslContextProvider.createContext()).thenReturn(keyStoreSslContext);
            when(sslContextProvider.createTrustManager()).thenReturn(trustManager);
        } else {
            when(sslContextProvider.createContext()).thenReturn(serverKeyStoreNoTrustStoreSslContext);
            when(sslContextProvider.createTrustManager()).thenReturn(defaultTrustManager);
        }
        runner.addControllerService(SSL_CONTEXT_SERVICE_IDENTIFIER, sslContextProvider);

        runner.setProperty(ListenHTTP.CLIENT_AUTHENTICATION, clientAuthentication.name());
        runner.setProperty(ListenHTTP.SSL_CONTEXT_SERVICE, SSL_CONTEXT_SERVICE_IDENTIFIER);

        runner.enableControllerService(sslContextProvider);
    }

    @Test
    public void testMultipartFormDataRequest() throws IOException {
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.RETURN_CODE, Integer.toString(HttpServletResponse.SC_OK));
        runner.setProperty(ListenHTTP.MULTIPART_READ_BUFFER_SIZE, "10 b");

        final SSLContextProvider sslContextProvider = runner.getControllerService(SSL_CONTEXT_SERVICE_IDENTIFIER, SSLContextProvider.class);
        final boolean isSecure = (sslContextProvider != null);
        final int port = startWebServer();

        final File file1 = createTextFile("Hello", "World");
        final File file2 = createTextFile("{ \"name\":\"John\", \"age\":30 }");

        final MultipartBody multipartBody = new MultipartBody.Builder().setType(MultipartBody.FORM)
            .addFormDataPart("p1", "v1")
            .addFormDataPart("p2", "v2")
            .addFormDataPart("file1", "my-file-text.txt", RequestBody.create(file1, MediaType.parse("text/plain")))
            .addFormDataPart("file2", "my-file-data.json", RequestBody.create(file2, MediaType.parse("application/json")))
            .addFormDataPart("file3", "my-file-binary.bin", RequestBody.create(generateRandomBinaryData(), MediaType.parse("application/octet-stream")))
            .build();

        final Request request = new Request.Builder()
            .url(buildUrl(isSecure, port, HTTP_BASE_PATH))
            .post(multipartBody)
            .build();

        final OkHttpClient client = getOkHttpClient(false, false);
        try (Response response = client.newCall(request).execute()) {
            Files.deleteIfExists(Paths.get(String.valueOf(file1)));
            Files.deleteIfExists(Paths.get(String.valueOf(file2)));
            assertTrue(response.isSuccessful(), String.format("Unexpected code: %s, body: %s", response.code(), response.body()));
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
        mff.assertAttributeExists("http.headers.multipart.Content-Disposition");

        mff = findFlowFile(flowFilesForRelationship, "p2");
        mff.assertAttributeEquals("http.multipart.name", "p2");
        mff.assertAttributeExists("http.multipart.size");
        mff.assertAttributeExists("http.multipart.fragments.sequence.number");
        mff.assertAttributeEquals("http.multipart.fragments.total.number", "5");
        mff.assertAttributeExists("http.headers.multipart.Content-Disposition");

        mff = findFlowFile(flowFilesForRelationship, "file1");
        mff.assertAttributeEquals("http.multipart.name", "file1");
        mff.assertAttributeEquals("http.multipart.filename", "my-file-text.txt");
        mff.assertAttributeEquals("http.headers.multipart.Content-Type", "text/plain");
        mff.assertAttributeExists("http.multipart.size");
        mff.assertAttributeExists("http.multipart.fragments.sequence.number");
        mff.assertAttributeEquals("http.multipart.fragments.total.number", "5");
        mff.assertAttributeExists("http.headers.multipart.Content-Disposition");

        mff = findFlowFile(flowFilesForRelationship, "file2");
        mff.assertAttributeEquals("http.multipart.name", "file2");
        mff.assertAttributeEquals("http.multipart.filename", "my-file-data.json");
        mff.assertAttributeEquals("http.headers.multipart.Content-Type", "application/json");
        mff.assertAttributeExists("http.multipart.size");
        mff.assertAttributeExists("http.multipart.fragments.sequence.number");
        mff.assertAttributeEquals("http.multipart.fragments.total.number", "5");
        mff.assertAttributeExists("http.headers.multipart.Content-Disposition");

        mff = findFlowFile(flowFilesForRelationship, "file3");
        mff.assertAttributeEquals("http.multipart.name", "file3");
        mff.assertAttributeEquals("http.multipart.filename", "my-file-binary.bin");
        mff.assertAttributeEquals("http.headers.multipart.Content-Type", "application/octet-stream");
        mff.assertAttributeExists("http.multipart.size");
        mff.assertAttributeExists("http.multipart.fragments.sequence.number");
        mff.assertAttributeEquals("http.multipart.fragments.total.number", "5");
        mff.assertAttributeExists("http.headers.multipart.Content-Disposition");

        final Path tempDirectoryPath = Paths.get(System.getProperty("java.io.tmpdir"));
        final long multiPartTempFiles = Files.find(tempDirectoryPath, 1,
                        (filePath, fileAttributes) -> filePath.getFileName().toString().startsWith("MultiPart")
                ).count();
        final String multiPartMessage = String.format("MultiPart files found in temporary directory [%s]", tempDirectoryPath);
        assertEquals(0, multiPartTempFiles, multiPartMessage);
    }

    @Test
    public void testLargeHTTPRequestHeader() throws Exception {
        runner.setProperty(ListenHTTP.REQUEST_HEADER_MAX_SIZE, "16 KB");

        String largeHeaderValue = "A".repeat(9 * 1024);

        final int port = startWebServer();
        OkHttpClient client = getOkHttpClient(false, false);
        final String url = buildUrl(false, port, HTTP_BASE_PATH);
        Request request = new Request.Builder()
                .url(url)
                .addHeader("Large-Header", largeHeaderValue)
                .method("HEAD", null)
                .build();
        try (Response response = client.newCall(request).execute()) {
            int responseCode = response.code();
            assertEquals(200, responseCode, "Expected 200 response code with large header.");
        }
    }

    @Test
    public void testFlowFilePackageRestoresFilenames() throws IOException {
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.RETURN_CODE, Integer.toString(HttpServletResponse.SC_NO_CONTENT));

        final int port = startWebServer();

        final OkHttpClient okHttpClient = getOkHttpClient(false, false);
        final Request.Builder requestBuilder = new Request.Builder();
        final String url = buildUrl(false, port, HTTP_BASE_PATH);
        requestBuilder.url(url);

        // Create FFv3 package containing two FlowFiles with filenames "file1" and "file2"
        final FlowFilePackagerV3 packager = new FlowFilePackagerV3();
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final byte[] data = "content does not matter".getBytes(StandardCharsets.UTF_8);

        IntStream.rangeClosed(0, 1).forEach(i -> {
            try (ByteArrayInputStream content = new ByteArrayInputStream(data)) {
                Map<String, String> attributes = Map.of("filename", "file" + i);
                packager.packageFlowFile(content, baos, attributes, data.length);
            } catch (IOException e) {
                fail();
            }
        });

        final byte[] flowFileV3Package = baos.toByteArray();
        final RequestBody requestBody = RequestBody.create(flowFileV3Package, MediaType.parse(StandardFlowFileMediaType.VERSION_3.getMediaType()));
        final Request request = requestBuilder.post(requestBody)
                .addHeader("filename", "data.flowfilev3")
                .build();

        try (final Response response = okHttpClient.newCall(request).execute()) {
            assertTrue(response.isSuccessful());
        }

        // Confirm FFv3 package is unpacked and each FlowFile carries the proper filename
        runner.assertTransferCount(RELATIONSHIP_SUCCESS, 2);
        IntStream.rangeClosed(0, 1).forEach(i -> {
            final MockFlowFile flowFile = runner.getFlowFilesForRelationship(RELATIONSHIP_SUCCESS).get(i);
            flowFile.assertAttributeEquals("filename", "file" + i);

        });
    }

    private byte[] generateRandomBinaryData() {
        byte[] bytes = new byte[100];
        new Random().nextBytes(bytes);
        return bytes;
    }

    private File createTextFile(String... lines) throws IOException {
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
