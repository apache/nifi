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

import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.servlet.http.HttpServletResponse;
import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.remote.io.socket.NetworkUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.util.KeyStoreUtils;
import org.apache.nifi.security.util.KeystoreType;
import org.apache.nifi.security.util.SslContextFactory;
import org.apache.nifi.security.util.StandardTlsConfiguration;
import org.apache.nifi.security.util.TlsConfiguration;
import org.apache.nifi.ssl.RestrictedSSLContextService;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
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
import static org.junit.Assert.fail;

public class TestListenHTTP {

    private static final String SSL_CONTEXT_SERVICE_IDENTIFIER = "ssl-context";

    private static final String HTTP_POST_METHOD = "POST";
    private static final String HTTP_BASE_PATH = "basePath";

    private final static String PORT_VARIABLE = "HTTP_PORT";
    private final static String HTTP_SERVER_PORT_EL = "${" + PORT_VARIABLE + "}";

    private final static String BASEPATH_VARIABLE = "HTTP_BASEPATH";
    private final static String HTTP_SERVER_BASEPATH_EL = "${" + BASEPATH_VARIABLE + "}";

    private static final String KEYSTORE = "src/test/resources/keystore.jks";
    private static final String KEYSTORE_PASSWORD = "passwordpassword";
    private static final KeystoreType KEYSTORE_TYPE = KeystoreType.JKS;
    private static final String TRUSTSTORE = "src/test/resources/truststore.jks";
    private static final String TRUSTSTORE_PASSWORD = "passwordpassword";
    private static final KeystoreType TRUSTSTORE_TYPE = KeystoreType.JKS;
    private static final String CLIENT_KEYSTORE = "src/test/resources/client-keystore.p12";
    private static final KeystoreType CLIENT_KEYSTORE_TYPE = KeystoreType.PKCS12;

    private static final String TLS_1_3 = "TLSv1.3";
    private static final String TLS_1_2 = "TLSv1.2";
    private static final String LOCALHOST = "localhost";

    private static final long SEND_REQUEST_SLEEP = 150;
    private static final long RESPONSE_TIMEOUT = 1200000;
    private static final int SOCKET_CONNECT_TIMEOUT = 100;
    private static final long SERVER_START_TIMEOUT = 1200000;

    private static TlsConfiguration tlsConfiguration;
    private static TlsConfiguration serverConfiguration;
    private static TlsConfiguration serverTls_1_3_Configuration;
    private static TlsConfiguration serverNoTruststoreConfiguration;
    private static SSLContext serverKeyStoreSslContext;
    private static SSLContext serverKeyStoreNoTrustStoreSslContext;
    private static SSLContext keyStoreSslContext;
    private static SSLContext trustStoreSslContext;

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

        serverKeyStoreSslContext = SslContextFactory.createSslContext(serverConfiguration);
        final TrustManager[] defaultTrustManagers = SslContextFactory.getTrustManagers(serverNoTruststoreConfiguration);
        serverKeyStoreNoTrustStoreSslContext = SslContextFactory.createSslContext(serverNoTruststoreConfiguration, defaultTrustManagers);

        keyStoreSslContext = SslContextFactory.createSslContext(new StandardTlsConfiguration(
                tlsConfiguration.getKeystorePath(),
                tlsConfiguration.getKeystorePassword(),
                tlsConfiguration.getKeystoreType(),
                tlsConfiguration.getTruststorePath(),
                tlsConfiguration.getTruststorePassword(),
                tlsConfiguration.getTruststoreType())
        );
        trustStoreSslContext = SslContextFactory.createSslContext(new StandardTlsConfiguration(
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
    public void teardown() {
        proc.shutdownHttpServer();
        new File("my-file-text.txt").delete();
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
        final HttpsURLConnection connection = getSecureConnection(trustStoreSslContext);
        assertThrows(SSLException.class, connection::getResponseCode);

        final HttpsURLConnection clientCertificateConnection = getSecureConnection(keyStoreSslContext);
        final int responseCode = clientCertificateConnection.getResponseCode();
        assertEquals(HttpServletResponse.SC_METHOD_NOT_ALLOWED, responseCode);
    }

    @Test
    public void testSecureServerTrustStoreNotConfiguredClientAuthenticationNotRequired() throws Exception {
        configureProcessorSslContextService(ListenHTTP.ClientAuthentication.AUTO, serverNoTruststoreConfiguration);
        startSecureServer();
        final HttpsURLConnection connection = getSecureConnection(trustStoreSslContext);
        final int responseCode = connection.getResponseCode();
        assertEquals(HttpServletResponse.SC_METHOD_NOT_ALLOWED, responseCode);
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

    private void startSecureServer() {
        runner.setProperty(ListenHTTP.PORT, Integer.toString(availablePort));
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.RETURN_CODE, Integer.toString(HttpServletResponse.SC_NO_CONTENT));
        runner.assertValid();
        startWebServer();
    }

    private HttpsURLConnection getSecureConnection(final SSLContext sslContext) throws Exception {
        final URL url = new URL(buildUrl(true));
        final HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();
        connection.setSSLSocketFactory(sslContext.getSocketFactory());
        return connection;
    }

    private int executePOST(String message, boolean secure, boolean twoWaySsl) throws Exception {
        String endpointUrl = buildUrl(secure);
        final URL url = new URL(endpointUrl);
        HttpURLConnection connection;

        if (secure) {
            connection = buildSecureConnection(twoWaySsl, url);
        } else {
            connection = (HttpURLConnection) url.openConnection();
        }
        connection.setRequestMethod(HTTP_POST_METHOD);
        connection.setDoOutput(true);

        final DataOutputStream wr = new DataOutputStream(connection.getOutputStream());

        if (message != null) {
            wr.writeBytes(message);
        }
        wr.flush();
        wr.close();
        return connection.getResponseCode();
    }

    private static HttpsURLConnection buildSecureConnection(boolean twoWaySsl, URL url) throws IOException {
        final HttpsURLConnection connection = (HttpsURLConnection) url.openConnection();
        if (twoWaySsl) {
            // Use a client certificate, do not reuse the server's keystore
            connection.setSSLSocketFactory(keyStoreSslContext.getSocketFactory());
        } else {
            // With one-way SSL, the client still needs a truststore
            connection.setSSLSocketFactory(trustStoreSslContext.getSocketFactory());
        }
        return connection;
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

        runner.assertTransferCount(RELATIONSHIP_SUCCESS, 4);
        mockFlowFiles.get(0).assertContentEquals("payload 1");
        mockFlowFiles.get(1).assertContentEquals("");
        mockFlowFiles.get(2).assertContentEquals("");
        mockFlowFiles.get(3).assertContentEquals("payload 2");
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

    private void startWebServerAndSendRequests(Runnable sendRequestToWebserver, int numberOfExpectedFlowFiles) throws Exception {
        startWebServer();
        new Thread(sendRequestToWebserver).start();

        final ProcessSessionFactory processSessionFactory = runner.getProcessSessionFactory();
        final ProcessContext context = runner.getProcessContext();
        int numTransferred = 0;
        long startTime = System.currentTimeMillis();
        while (numTransferred < numberOfExpectedFlowFiles && (System.currentTimeMillis() - startTime < RESPONSE_TIMEOUT)) {
            proc.onTrigger(context, processSessionFactory);
            numTransferred = runner.getFlowFilesForRelationship(RELATIONSHIP_SUCCESS).size();
            Thread.sleep(SEND_REQUEST_SLEEP);
        }

        runner.assertTransferCount(ListenHTTP.RELATIONSHIP_SUCCESS, numberOfExpectedFlowFiles);
    }

    private void startWebServerAndSendMessages(final List<String> messages, int returnCode, boolean secure, boolean twoWaySsl)
            throws Exception {

        Runnable sendMessagesToWebServer = () -> {
            try {
                for (final String message : messages) {
                    if (executePOST(message, secure, twoWaySsl) != returnCode) {
                        fail("HTTP POST failed.");
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
                fail("Not expecting error here.");
            }
        };

        startWebServerAndSendRequests(sendMessagesToWebServer, messages.size());
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
    public void testMultipartFormDataRequest() throws Exception {

        runner.setProperty(ListenHTTP.PORT, Integer.toString(availablePort));
        runner.setProperty(ListenHTTP.BASE_PATH, HTTP_BASE_PATH);
        runner.setProperty(ListenHTTP.RETURN_CODE, Integer.toString(HttpServletResponse.SC_OK));

        final SSLContextService sslContextService = runner.getControllerService(SSL_CONTEXT_SERVICE_IDENTIFIER, SSLContextService.class);
        final boolean isSecure = (sslContextService != null);

        Runnable sendRequestToWebserver = () -> {
            try {
                File file1 = createTextFile("my-file-text-", ".txt", "Hello", "World");
                File file2 = createTextFile("my-file-text-", ".txt", "{ \"name\":\"John\", \"age\":30 }");

                MultipartBody multipartBody = new MultipartBody.Builder().setType(MultipartBody.FORM)
                        .addFormDataPart("p1", "v1")
                        .addFormDataPart("p2", "v2")
                        .addFormDataPart("file1", "my-file-text.txt", RequestBody.create(MediaType.parse("text/plain"), file1))
                        .addFormDataPart("file2", "my-file-data.json", RequestBody.create(MediaType.parse("application/json"), file2))
                        .addFormDataPart("file3", "my-file-binary.bin", RequestBody.create(MediaType.parse("application/octet-stream"), generateRandomBinaryData(100)))
                        .build();

                Request request =
                        new Request.Builder()
                                .url(buildUrl(isSecure))
                                .post(multipartBody)
                                .build();

                int timeout = 3000;
                OkHttpClient client = new OkHttpClient.Builder()
                        .readTimeout(timeout, TimeUnit.MILLISECONDS)
                        .writeTimeout(timeout, TimeUnit.MILLISECONDS)
                        .build();

                try (Response response = client.newCall(request).execute()) {
                    Files.deleteIfExists(Paths.get(String.valueOf(file1)));
                    Files.deleteIfExists(Paths.get(String.valueOf(file2)));
                    Assert.assertTrue(String.format("Unexpected code: %s, body: %s", response.code(), response.body().string()), response.isSuccessful());
                }
            } catch (final Throwable t) {
                t.printStackTrace();
                Assert.fail(t.toString());
            }
        };


        startWebServerAndSendRequests(sendRequestToWebserver, 5);

        runner.assertAllFlowFilesTransferred(ListenHTTP.RELATIONSHIP_SUCCESS, 5);
        List<MockFlowFile> flowFilesForRelationship = runner.getFlowFilesForRelationship(ListenHTTP.RELATIONSHIP_SUCCESS);
        // Part fragments are not processed in the order we submitted them.
        // We cannot rely on the order we sent them in.
        MockFlowFile mff = findFlowFile(flowFilesForRelationship, "http.multipart.name", "p1");
        mff.assertAttributeEquals("http.multipart.name", "p1");
        mff.assertAttributeExists("http.multipart.size");
        mff.assertAttributeEquals("http.multipart.fragments.sequence.number", "1");
        mff.assertAttributeEquals("http.multipart.fragments.total.number", "5");
        mff.assertAttributeExists("http.headers.multipart.content-disposition");

        mff = findFlowFile(flowFilesForRelationship, "http.multipart.name", "p2");
        mff.assertAttributeEquals("http.multipart.name", "p2");
        mff.assertAttributeExists("http.multipart.size");
        mff.assertAttributeExists("http.multipart.fragments.sequence.number");
        mff.assertAttributeEquals("http.multipart.fragments.total.number", "5");
        mff.assertAttributeExists("http.headers.multipart.content-disposition");

        mff = findFlowFile(flowFilesForRelationship, "http.multipart.name", "file1");
        mff.assertAttributeEquals("http.multipart.name", "file1");
        mff.assertAttributeEquals("http.multipart.filename", "my-file-text.txt");
        mff.assertAttributeEquals("http.headers.multipart.content-type", "text/plain");
        mff.assertAttributeExists("http.multipart.size");
        mff.assertAttributeExists("http.multipart.fragments.sequence.number");
        mff.assertAttributeEquals("http.multipart.fragments.total.number", "5");
        mff.assertAttributeExists("http.headers.multipart.content-disposition");

        mff = findFlowFile(flowFilesForRelationship, "http.multipart.name", "file2");
        mff.assertAttributeEquals("http.multipart.name", "file2");
        mff.assertAttributeEquals("http.multipart.filename", "my-file-data.json");
        mff.assertAttributeEquals("http.headers.multipart.content-type", "application/json");
        mff.assertAttributeExists("http.multipart.size");
        mff.assertAttributeExists("http.multipart.fragments.sequence.number");
        mff.assertAttributeEquals("http.multipart.fragments.total.number", "5");
        mff.assertAttributeExists("http.headers.multipart.content-disposition");

        mff = findFlowFile(flowFilesForRelationship, "http.multipart.name", "file3");
        mff.assertAttributeEquals("http.multipart.name", "file3");
        mff.assertAttributeEquals("http.multipart.filename", "my-file-binary.bin");
        mff.assertAttributeEquals("http.headers.multipart.content-type", "application/octet-stream");
        mff.assertAttributeExists("http.multipart.size");
        mff.assertAttributeExists("http.multipart.fragments.sequence.number");
        mff.assertAttributeEquals("http.multipart.fragments.total.number", "5");
        mff.assertAttributeExists("http.headers.multipart.content-disposition");
    }

    private byte[] generateRandomBinaryData(int i) {
        byte[] bytes = new byte[100];
        new Random().nextBytes(bytes);
        return bytes;
    }

    private File createTextFile(String prefix, String extension, String...lines) throws IOException {
        Path file = Files.createTempFile(prefix, extension);
        try (FileOutputStream fos = new FileOutputStream(file.toFile())) {
            IOUtils.writeLines(Arrays.asList(lines), System.lineSeparator(), fos, Charsets.UTF_8);
        }
        return file.toFile();
    }

    protected MockFlowFile findFlowFile(List<MockFlowFile> flowFilesForRelationship, String attributeName, String attributeValue) {
        Optional<MockFlowFile> optional = Iterables.tryFind(flowFilesForRelationship, ff -> ff.getAttribute(attributeName).equals(attributeValue));
        Assert.assertTrue(optional.isPresent());
        return optional.get();
    }
}
