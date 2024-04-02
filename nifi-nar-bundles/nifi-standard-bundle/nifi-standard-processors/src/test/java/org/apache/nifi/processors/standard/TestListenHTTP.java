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

import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okio.BufferedSink;
import okio.GzipSink;
import okio.Okio;
import org.apache.nifi.processors.standard.http.ContentEncodingStrategy;
import org.apache.nifi.processors.standard.http.HttpProtocolStrategy;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.security.util.StandardTlsConfiguration;
import org.apache.nifi.security.util.TemporaryKeyStoreBuilder;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.security.util.TlsPlatform;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.ssl.RestrictedSSLContextService;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.web.util.ssl.SslContextUtils;
import org.eclipse.jetty.server.NetworkConnector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.util.thread.ThreadPool;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIf;
import org.mockito.Mockito;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import jakarta.servlet.http.HttpServletResponse;
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
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;

import static org.apache.nifi.processors.standard.ListenHTTP.RELATIONSHIP_SUCCESS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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

    private static final String HTTP_POST = "POST";
    private static final String HTTP_TRACE = "TRACE";
    private static final String HTTP_OPTIONS = "OPTIONS";

    private static final Duration CLIENT_CALL_TIMEOUT = Duration.ofSeconds(10);
    private static final String LOCALHOST_DN = "CN=localhost";

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


    static boolean isTls13Supported() {
        return TLS_1_3.equals(TlsPlatform.getLatestProtocol());
    }

    @BeforeAll
    public static void setUpSuite() throws GeneralSecurityException {
        // generate new keystore and truststore
        final TlsConfiguration tlsConfiguration = new TemporaryKeyStoreBuilder().build();

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
        configureProcessorSslContextService(ListenHTTP.ClientAuthentication.AUTO, serverNoTruststoreConfiguration);

        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.HTTP_PROTOCOL_STRATEGY, HttpProtocolStrategy.H2_HTTP_1_1);
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_OK, true, false);
    }

    @Test
    public void testSecurePOSTRequestsReturnCodeReceivedWithoutELHttp2() throws Exception {
        configureProcessorSslContextService(ListenHTTP.ClientAuthentication.AUTO, serverNoTruststoreConfiguration);

        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.RETURN_CODE, Integer.toString(HttpServletResponse.SC_NO_CONTENT));
        runner.setProperty(ListenHTTP.HTTP_PROTOCOL_STRATEGY, HttpProtocolStrategy.H2);
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

        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.RETURN_CODE, Integer.toString(HttpServletResponse.SC_NO_CONTENT));
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_NO_CONTENT, true, false);
    }

    @Test
    public void testSecureTwoWaySslPOSTRequestsReceivedWithoutEL() throws Exception {
        configureProcessorSslContextService(ListenHTTP.ClientAuthentication.REQUIRED, serverConfiguration);

        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_OK, true, true);
    }

    @Test
    public void testSecureTwoWaySslPOSTRequestsReceivedWithUnauthorizedSubjectDn() throws Exception {
        configureProcessorSslContextService(ListenHTTP.ClientAuthentication.REQUIRED, serverConfiguration);

        runner.setProperty(ListenHTTP.AUTHORIZED_DN_PATTERN, "CN=other");
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_FORBIDDEN, true, true);
    }

    @Test
    public void testSecureTwoWaySslPOSTRequestsReceivedWithAuthorizedIssuerDn() throws Exception {
        configureProcessorSslContextService(ListenHTTP.ClientAuthentication.REQUIRED, serverConfiguration);

        runner.setProperty(ListenHTTP.AUTHORIZED_DN_PATTERN, LOCALHOST_DN);
        runner.setProperty(ListenHTTP.AUTHORIZED_ISSUER_DN_PATTERN, LOCALHOST_DN);
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_OK, true, true);
    }

    @Test
    public void testSecureTwoWaySslPOSTRequestsReceivedWithUnauthorizedIssuerDn() throws Exception {
        configureProcessorSslContextService(ListenHTTP.ClientAuthentication.REQUIRED, serverConfiguration);

        runner.setProperty(ListenHTTP.AUTHORIZED_DN_PATTERN, LOCALHOST_DN); // Although subject is authorized, issuer is not
        runner.setProperty(ListenHTTP.AUTHORIZED_ISSUER_DN_PATTERN, "CN=other");
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_FORBIDDEN, true, true);
    }

    @Test
    public void testSecureTwoWaySslPOSTRequestsReturnCodeReceivedWithoutEL() throws Exception {
        configureProcessorSslContextService(ListenHTTP.ClientAuthentication.REQUIRED, serverConfiguration);

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

        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.RETURN_CODE, Integer.toString(HttpServletResponse.SC_NO_CONTENT));
        runner.assertValid();

        testPOSTRequestsReceived(HttpServletResponse.SC_NO_CONTENT, true, true);
    }

    @Test
    public void testSecureServerSupportsCurrentTlsProtocolVersion() throws Exception {
        configureProcessorSslContextService(ListenHTTP.ClientAuthentication.AUTO, serverNoTruststoreConfiguration);
        final int listeningPort = startSecureServer();

        final SSLSocketFactory sslSocketFactory = trustStoreSslContext.getSocketFactory();
        try (final SSLSocket sslSocket = (SSLSocket) sslSocketFactory.createSocket(LOCALHOST, listeningPort)) {
            final String currentProtocol = serverNoTruststoreConfiguration.getProtocol();
            sslSocket.setEnabledProtocols(new String[]{currentProtocol});

            sslSocket.startHandshake();
            final SSLSession sslSession = sslSocket.getSession();
            assertEquals(currentProtocol, sslSession.getProtocol());
        }
    }

    @Test
    public void testSecureServerTrustStoreConfiguredClientAuthenticationRequired() throws Exception {
        configureProcessorSslContextService(ListenHTTP.ClientAuthentication.REQUIRED, serverConfiguration);
        final int port = startSecureServer();
        assertThrows(IOException.class, () -> sendMessage(null, true, port, false, HTTP_POST));
    }

    @Test
    public void testSecureServerTrustStoreNotConfiguredClientAuthenticationNotRequired() throws Exception {
        configureProcessorSslContextService(ListenHTTP.ClientAuthentication.AUTO, serverNoTruststoreConfiguration);
        final int port = startSecureServer();
        final int responseCode = sendMessage(null, true, port, true, HTTP_POST);
        assertEquals(HttpServletResponse.SC_NO_CONTENT, responseCode);
    }

    @EnabledIf(value = "isTls13Supported", disabledReason = "TLSv1.3 is not supported")
    @Test
    public void testSecureServerRejectsUnsupportedTlsProtocolVersion() throws Exception {
        configureProcessorSslContextService(ListenHTTP.ClientAuthentication.AUTO, serverTls_1_3_Configuration);

        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.RETURN_CODE, Integer.toString(HttpServletResponse.SC_NO_CONTENT));
        runner.assertValid();

        final int listeningPort = startWebServer();
        final SSLSocketFactory sslSocketFactory = trustStoreSslContext.getSocketFactory();
        try (final SSLSocket sslSocket = (SSLSocket) sslSocketFactory.createSocket(LOCALHOST, listeningPort)) {
            sslSocket.setEnabledProtocols(new String[]{TLS_1_2});

            assertThrows(SSLHandshakeException.class, sslSocket::startHandshake);
        }
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
                "\"1\",\"rec1\",\"101\"\n" +
                "\"2\",\"rec2\",\"102\"\n" +
                "\"3\",\"rec3\",\"103\"\n" +
                "\"4\",\"rec4\",\"104\"\n";

        startWebServerAndSendMessages(Collections.singletonList(""), HttpServletResponse.SC_OK, false, false, HTTP_POST);
        List<MockFlowFile> mockFlowFiles = runner.getFlowFilesForRelationship(RELATIONSHIP_SUCCESS);

        runner.assertTransferCount(RELATIONSHIP_SUCCESS, 1);
        mockFlowFiles.get(0).assertContentEquals(expectedMessage);
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
        final String url = buildUrl(false, port);
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
            final MockFlowFile flowFile = runner.getFlowFilesForRelationship(RELATIONSHIP_SUCCESS).iterator().next();
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
    public void testTraceNotAllowed() throws Exception {
        final List<String> messages = new ArrayList<>();
        messages.add("payload 1");

        startWebServerAndSendMessages(messages, HttpServletResponse.SC_METHOD_NOT_ALLOWED, false, false, HTTP_TRACE);
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

    private int sendMessage(final String message, final boolean secure, final int port, boolean clientAuthRequired, final String httpMethod) throws IOException {
        final byte[] bytes = message == null ? new byte[]{} : message.getBytes(StandardCharsets.UTF_8);
        final RequestBody requestBody = RequestBody.create(bytes, APPLICATION_OCTET_STREAM);
        final String url = buildUrl(secure, port);
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

    private String buildUrl(final boolean secure, final int port) {
        return String.format("%s://localhost:%s/%s", secure ? "https" : "http", port, HTTP_BASE_PATH);
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
                mockFlowFiles.get(0).assertAttributeEquals("restlistener.remote.user.dn", LOCALHOST_DN);
                mockFlowFiles.get(0).assertAttributeEquals("restlistener.remote.issuer.dn", LOCALHOST_DN);
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
                runner.getLogger().debug("Server Socket Connected after {} ms", new Object[]{elapsed});
                socket.close();
            } catch (final Exception e) {
                runner.getLogger().debug("Server Socket Connect Failed: [{}] {}", new Object[]{e.getClass(), e.getMessage()});
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
            final int statusCode = sendMessage(message, secure, port, clientAuthRequired, httpMethod);
            assertEquals(expectedStatusCode, statusCode, "HTTP Status Code not matched");
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
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.RETURN_CODE, Integer.toString(HttpServletResponse.SC_OK));
        runner.setProperty(ListenHTTP.MULTIPART_READ_BUFFER_SIZE, "10 b");

        final SSLContextService sslContextService = runner.getControllerService(SSL_CONTEXT_SERVICE_IDENTIFIER, SSLContextService.class);
        final boolean isSecure = (sslContextService != null);
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
            .url(buildUrl(isSecure, port))
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
