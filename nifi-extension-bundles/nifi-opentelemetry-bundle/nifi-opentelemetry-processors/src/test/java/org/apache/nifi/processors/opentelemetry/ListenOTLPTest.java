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
package org.apache.nifi.processors.opentelemetry;

import com.google.protobuf.Message;
import io.netty.handler.codec.http.HttpMethod;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsPartialSuccess;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceRequest;
import io.opentelemetry.proto.collector.logs.v1.ExportLogsServiceResponse;
import io.opentelemetry.proto.logs.v1.LogRecord;
import io.opentelemetry.proto.logs.v1.ResourceLogs;
import io.opentelemetry.proto.logs.v1.ScopeLogs;
import org.apache.nifi.processors.opentelemetry.encoding.RequestMapper;
import org.apache.nifi.processors.opentelemetry.encoding.StandardRequestMapper;
import org.apache.nifi.processors.opentelemetry.protocol.TelemetryAttributeName;
import org.apache.nifi.processors.opentelemetry.protocol.TelemetryContentType;
import org.apache.nifi.processors.opentelemetry.protocol.TelemetryRequestType;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.security.cert.builder.StandardCertificateBuilder;
import org.apache.nifi.security.ssl.StandardSslContextBuilder;
import org.apache.nifi.security.ssl.StandardTrustManagerBuilder;
import org.apache.nifi.security.util.ClientAuth;
import org.apache.nifi.security.util.TlsPlatform;
import org.apache.nifi.ssl.SSLContextProvider;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.nifi.web.client.StandardWebClientService;
import org.apache.nifi.web.client.api.HttpEntityHeaders;
import org.apache.nifi.web.client.api.HttpResponseEntity;
import org.apache.nifi.web.client.api.HttpResponseStatus;
import org.apache.nifi.web.client.ssl.TlsContext;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.X509KeyManager;
import javax.net.ssl.X509TrustManager;
import javax.security.auth.x500.X500Principal;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;

@Timeout(15)
@ExtendWith(MockitoExtension.class)
class ListenOTLPTest {

    private static final String KEY_ALGORITHM = "RSA";

    private static final String KEY_ALIAS = "localhost";

    private static final X500Principal CERTIFICATE_ISSUER = new X500Principal("CN=localhost");

    private static final String LOCALHOST = "127.0.0.1";

    private static final String RANDOM_PORT = "0";

    private static final String HTTP_URL_FORMAT = "https://localhost:%d%s";

    private static final String SERVICE_ID = SSLContextProvider.class.getSimpleName();

    private static final String PATH_NOT_FOUND = "/not-found";

    private static final String CONTENT_TYPE_HEADER = "Content-Type";

    private static final String CONTENT_ENCODING_HEADER = "Content-Encoding";

    private static final String GZIP_ENCODING = "gzip";

    private static final String TEXT_PLAIN = "text/plain";

    private static final String JSON_OBJECT_SUCCESS = "{}";

    private static final byte[] JSON_OBJECT_SUCCESS_BYTES = JSON_OBJECT_SUCCESS.getBytes(StandardCharsets.UTF_8);

    private static final String JSON_STRING_VALUE_LOCALHOST = "{\"stringValue\":\"127.0.0.1\"}";

    private static final byte[] EMPTY_BYTES = new byte[]{};

    private static final Duration CONNECT_TIMEOUT = Duration.ofSeconds(10);

    private static final Duration READ_TIMEOUT = Duration.ofSeconds(5);

    private static SSLContext sslContext;

    private static X509TrustManager trustManager;

    private static X509KeyManager keyManager;

    private static StandardWebClientService webClientService;

    @Mock
    private SSLContextProvider sslContextProvider;

    private TestRunner runner;

    private ListenOTLP processor;

    @BeforeAll
    static void setSslContext() throws GeneralSecurityException, IOException {
        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(KEY_ALGORITHM);
        final KeyPair keyPair = keyPairGenerator.generateKeyPair();
        final X509Certificate certificate = new StandardCertificateBuilder(keyPair, CERTIFICATE_ISSUER, Duration.ofDays(1)).build();

        final KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
        trustStore.load(null);

        trustStore.setCertificateEntry(KEY_ALIAS, certificate);
        trustManager = new StandardTrustManagerBuilder().trustStore(trustStore).build();

        final char[] generated = UUID.randomUUID().toString().toCharArray();
        final KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(null);
        keyStore.setKeyEntry(KEY_ALIAS, keyPair.getPrivate(), generated, new Certificate[]{certificate});

        final KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagerFactory.init(keyStore, generated);
        final KeyManager[] keyManagers = keyManagerFactory.getKeyManagers();
        final Optional<KeyManager> firstKeyManager = Arrays.stream(keyManagers).findFirst();
        final KeyManager configuredKeyManager = firstKeyManager.orElse(null);
        keyManager = configuredKeyManager instanceof X509KeyManager ? (X509KeyManager) configuredKeyManager : null;

        sslContext = new StandardSslContextBuilder()
                .keyStore(keyStore)
                .keyPassword(generated)
                .trustStore(trustStore)
                .build();

        webClientService = new StandardWebClientService();
        webClientService.setReadTimeout(READ_TIMEOUT);
        webClientService.setConnectTimeout(CONNECT_TIMEOUT);
        webClientService.setWriteTimeout(READ_TIMEOUT);

        webClientService.setTlsContext(new TlsContext() {
            @Override
            public String getProtocol() {
                return TlsPlatform.getLatestProtocol();
            }

            @Override
            public X509TrustManager getTrustManager() {
                return trustManager;
            }

            @Override
            public Optional<X509KeyManager> getKeyManager() {
                return Optional.ofNullable(keyManager);
            }
        });
    }

    @AfterAll
    static void closeWebClientService() {
        webClientService.close();
    }

    @BeforeEach
    void setRunner() {
        processor = new ListenOTLP();
        runner = TestRunners.newTestRunner(processor);
    }

    @AfterEach
    void stopRunner() {
        runner.stop();
    }

    @Test
    void testRequiredProperties() throws InitializationException {
        runner.assertNotValid();

        runner.setProperty(ListenOTLP.ADDRESS, LOCALHOST);
        setSslContextService();

        runner.assertValid();
    }

    @Test
    void testGetMethodNotAllowed() throws Exception {
        startServer();
        final URI uri = getUri(TelemetryRequestType.LOGS.getPath());

        try (HttpResponseEntity httpResponseEntity = webClientService.get()
                .uri(uri)
                .retrieve()
        ) {
            assertEquals(HttpResponseStatus.METHOD_NOT_ALLOWED.getCode(), httpResponseEntity.statusCode());
        }
    }

    @Test
    void testPostPathNotFound() throws Exception {
        startServer();
        final URI uri = getUri(PATH_NOT_FOUND);

        try (HttpResponseEntity httpResponseEntity = webClientService.post()
                .uri(uri)
                .header(CONTENT_TYPE_HEADER, TelemetryContentType.APPLICATION_JSON.getContentType())
                .body(getRequestBody(), OptionalLong.empty())
                .retrieve()
        ) {
            assertEquals(HttpResponseStatus.NOT_FOUND.getCode(), httpResponseEntity.statusCode());
        }
    }

    @Test
    void testPostUnsupportedMediaType() throws Exception {
        startServer();
        final URI uri = getUri(TelemetryRequestType.LOGS.getPath());

        try (HttpResponseEntity httpResponseEntity = webClientService.post()
                .uri(uri)
                .body(getRequestBody(), OptionalLong.empty())
                .header(CONTENT_TYPE_HEADER, TEXT_PLAIN)
                .retrieve()
        ) {
            assertEquals(HttpURLConnection.HTTP_UNSUPPORTED_TYPE, httpResponseEntity.statusCode());
        }
    }

    @Test
    void testPostEmptyJson() throws Exception {
        startServer();
        final URI uri = getUri(TelemetryRequestType.LOGS.getPath());

        try (HttpResponseEntity httpResponseEntity = webClientService.post()
                .uri(uri)
                .body(getRequestBody(), OptionalLong.empty())
                .header(CONTENT_TYPE_HEADER, TelemetryContentType.APPLICATION_JSON.getContentType())
                .retrieve()
        ) {
            assertResponseSuccess(TelemetryContentType.APPLICATION_JSON, httpResponseEntity);

            final byte[] responseBody = getResponseBody(httpResponseEntity.body());
            assertArrayEquals(JSON_OBJECT_SUCCESS_BYTES, responseBody);
        }
    }

    @Test
    void testPostEmptyProtobuf() throws Exception {
        startServer();
        final URI uri = getUri(TelemetryRequestType.LOGS.getPath());

        try (HttpResponseEntity httpResponseEntity = webClientService.post()
                .uri(uri)
                .body(getRequestBody(), OptionalLong.empty())
                .header(CONTENT_TYPE_HEADER, TelemetryContentType.APPLICATION_PROTOBUF.getContentType())
                .retrieve()
        ) {
            assertResponseSuccess(TelemetryContentType.APPLICATION_PROTOBUF, httpResponseEntity);

            final byte[] responseBody = getResponseBody(httpResponseEntity.body());
            assertArrayEquals(EMPTY_BYTES, responseBody);
        }
    }

    @Test
    void testPostEmptyGrpc() throws Exception {
        startServer();
        final URI uri = getUri(TelemetryRequestType.LOGS.getGrpcPath());

        final byte[] uncompressedZeroMessageSize = new byte[]{0, 0, 0, 0, 0};
        final ByteArrayInputStream requestBody = new ByteArrayInputStream(uncompressedZeroMessageSize);

        try (HttpResponseEntity httpResponseEntity = webClientService.post()
                .uri(uri)
                .body(requestBody, OptionalLong.empty())
                .header(CONTENT_TYPE_HEADER, TelemetryContentType.APPLICATION_GRPC.getContentType())
                .retrieve()
        ) {
            assertResponseSuccess(TelemetryContentType.APPLICATION_GRPC, httpResponseEntity);

            final byte[] responseBody = getResponseBody(httpResponseEntity.body());
            assertArrayEquals(EMPTY_BYTES, responseBody);
        }
    }

    @Test
    void testPostProtobuf() throws Exception {
        startServer();
        final TelemetryRequestType requestType = TelemetryRequestType.LOGS;
        final URI uri = getUri(requestType.getPath());

        final ResourceLogs resourceLogs = ResourceLogs.newBuilder().build();
        final ExportLogsServiceRequest request = ExportLogsServiceRequest.newBuilder()
                .addResourceLogs(resourceLogs)
                .build();
        final byte[] protobufRequest = request.toByteArray();
        final ByteArrayInputStream requestBody = new ByteArrayInputStream(protobufRequest);

        final TelemetryContentType contentType = TelemetryContentType.APPLICATION_PROTOBUF;
        try (HttpResponseEntity httpResponseEntity = webClientService.post()
                .uri(uri)
                .body(requestBody, OptionalLong.of(protobufRequest.length))
                .header(CONTENT_TYPE_HEADER, contentType.getContentType())
                .retrieve()
        ) {
            assertResponseSuccess(contentType, httpResponseEntity);

            final byte[] responseBody = getResponseBody(httpResponseEntity.body());
            assertArrayEquals(EMPTY_BYTES, responseBody);
        }

        runner.run(1, false, false);

        assertFlowFileFound(requestType);
    }

    @Test
    void testPostJsonCompressed() throws Exception {
        startServer();
        final TelemetryRequestType requestType = TelemetryRequestType.LOGS;
        final URI uri = getUri(requestType.getPath());

        final ResourceLogs resourceLogs = ResourceLogs.newBuilder().build();
        final ExportLogsServiceRequest request = ExportLogsServiceRequest.newBuilder()
                .addResourceLogs(resourceLogs)
                .build();
        final byte[] requestSerialized = getRequestSerialized(request);
        final byte[] requestBody = getRequestCompressed(requestSerialized);
        final ByteArrayInputStream requestBodyStream = new ByteArrayInputStream(requestBody);

        final TelemetryContentType contentType = TelemetryContentType.APPLICATION_JSON;
        try (HttpResponseEntity httpResponseEntity = webClientService.post()
                .uri(uri)
                .body(requestBodyStream, OptionalLong.of(requestBody.length))
                .header(CONTENT_TYPE_HEADER, contentType.getContentType())
                .header(CONTENT_ENCODING_HEADER, GZIP_ENCODING)
                .retrieve()
        ) {
            assertResponseSuccess(contentType, httpResponseEntity);

            final byte[] responseBody = getResponseBody(httpResponseEntity.body());
            assertArrayEquals(JSON_OBJECT_SUCCESS_BYTES, responseBody);
        }

        runner.run(1, false, false);

        assertFlowFileFound(requestType);
    }

    @Test
    void testPostJsonPartialSuccess() throws Exception {
        final int queueCapacity = 1;
        runner.setProperty(ListenOTLP.QUEUE_CAPACITY, Integer.toString(queueCapacity));
        startServer();

        final TelemetryRequestType requestType = TelemetryRequestType.LOGS;
        final URI uri = getUri(requestType.getPath());

        final LogRecord logRecord = LogRecord.newBuilder().build();
        final ScopeLogs scopeLogs = ScopeLogs.newBuilder().addLogRecords(logRecord).build();
        final ResourceLogs resourceLogs = ResourceLogs.newBuilder().addScopeLogs(scopeLogs).build();

        final ExportLogsServiceRequest request = ExportLogsServiceRequest.newBuilder()
                .addResourceLogs(resourceLogs)
                .addResourceLogs(resourceLogs)
                .build();
        final byte[] requestSerialized = getRequestSerialized(request);

        final TelemetryContentType contentType = TelemetryContentType.APPLICATION_JSON;
        try (HttpResponseEntity httpResponseEntity = webClientService.post()
                .uri(uri)
                .body(new ByteArrayInputStream(requestSerialized), OptionalLong.of(requestSerialized.length))
                .header(CONTENT_TYPE_HEADER, contentType.getContentType())
                .retrieve()
        ) {
            assertResponseSuccess(contentType, httpResponseEntity);

            final RequestMapper requestMapper = new StandardRequestMapper();
            final ExportLogsServiceResponse serviceResponse = requestMapper.readValue(httpResponseEntity.body(), ExportLogsServiceResponse.class);

            final ExportLogsPartialSuccess partialSuccess = serviceResponse.getPartialSuccess();
            assertNotNull(partialSuccess);
            assertEquals(queueCapacity, partialSuccess.getRejectedLogRecords());
        }

        runner.run(1, false, false);

        assertFlowFileFound(requestType);
    }

    @Test
    void testPostProtobufUrlConnection() throws Exception {
        startServer();
        final TelemetryRequestType requestType = TelemetryRequestType.LOGS;
        final URI uri = getUri(requestType.getPath());

        final ResourceLogs resourceLogs = ResourceLogs.newBuilder().build();
        final ExportLogsServiceRequest request = ExportLogsServiceRequest.newBuilder()
                .addResourceLogs(resourceLogs)
                .build();
        final byte[] protobufRequest = request.toByteArray();

        final TelemetryContentType contentType = TelemetryContentType.APPLICATION_PROTOBUF;

        final HttpsURLConnection connection = (HttpsURLConnection) uri.toURL().openConnection();
        connection.setSSLSocketFactory(sslContext.getSocketFactory());
        connection.setRequestMethod(HttpMethod.POST.name());
        connection.setConnectTimeout(Math.toIntExact(CONNECT_TIMEOUT.toMillis()));
        connection.setReadTimeout(Math.toIntExact(READ_TIMEOUT.toMillis()));
        connection.setRequestProperty(CONTENT_TYPE_HEADER, contentType.getContentType());
        connection.setDoOutput(true);

        try (OutputStream connectionOutputStream = connection.getOutputStream()) {
            connectionOutputStream.write(protobufRequest);
        }

        final int responseCode = connection.getResponseCode();
        assertEquals(HttpURLConnection.HTTP_OK, responseCode);
        connection.disconnect();

        runner.run(1, false, false);

        assertFlowFileFound(requestType);
    }

    private byte[] getRequestSerialized(final Message message) throws IOException {
        final RequestMapper requestMapper = new StandardRequestMapper();
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        requestMapper.writeValue(outputStream, message);
        return outputStream.toByteArray();
    }

    private byte[] getRequestCompressed(final byte[] serialized) throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try (GZIPOutputStream gzipOutputStream = new GZIPOutputStream(outputStream)) {
            gzipOutputStream.write(serialized);
        }
        return outputStream.toByteArray();
    }

    private void assertFlowFileFound(final TelemetryRequestType requestType) {
        final Iterator<MockFlowFile> flowFiles = runner.getFlowFilesForRelationship(ListenOTLP.SUCCESS).iterator();
        assertTrue(flowFiles.hasNext());

        final MockFlowFile flowFile = flowFiles.next();
        flowFile.assertAttributeEquals(TelemetryAttributeName.MIME_TYPE, TelemetryContentType.APPLICATION_JSON.getContentType());
        flowFile.assertAttributeEquals(TelemetryAttributeName.RESOURCE_COUNT, Integer.toString(1));
        flowFile.assertAttributeEquals(TelemetryAttributeName.RESOURCE_TYPE, requestType.name());

        final String content = flowFile.getContent();
        assertTrue(content.contains(JSON_STRING_VALUE_LOCALHOST));
    }

    private void assertResponseSuccess(final TelemetryContentType expectedContentType, final HttpResponseEntity httpResponseEntity) {
        assertEquals(HttpResponseStatus.OK.getCode(), httpResponseEntity.statusCode());

        final HttpEntityHeaders headers = httpResponseEntity.headers();

        final Optional<String> firstContentType = headers.getFirstHeader(CONTENT_TYPE_HEADER);
        assertTrue(firstContentType.isPresent());
        assertEquals(expectedContentType.getContentType(), firstContentType.get());
    }

    private void startServer() throws InitializationException {
        runner.setProperty(ListenOTLP.ADDRESS, LOCALHOST);
        runner.setProperty(ListenOTLP.PORT, RANDOM_PORT);
        setSslContextService();
        when(sslContextProvider.createContext()).thenReturn(sslContext);

        runner.run(1, false, true);
    }

    private URI getUri(final String contextPath) {
        final int httpPort = processor.getPort();

        final String httpUrl = String.format(HTTP_URL_FORMAT, httpPort, contextPath);
        return URI.create(httpUrl);
    }

    private void setSslContextService() throws InitializationException {
        when(sslContextProvider.getIdentifier()).thenReturn(SERVICE_ID);

        runner.addControllerService(SERVICE_ID, sslContextProvider);
        runner.enableControllerService(sslContextProvider);

        runner.setProperty(ListenOTLP.SSL_CONTEXT_SERVICE, SERVICE_ID);
        runner.setProperty(ListenOTLP.CLIENT_AUTHENTICATION, ClientAuth.WANT.name());
    }

    private InputStream getRequestBody() {
        return new ByteArrayInputStream(new byte[]{});
    }

    private byte[] getResponseBody(final InputStream inputStream) throws IOException {
        final ByteArrayOutputStream outputStream = new ByteArrayOutputStream();

        int read = inputStream.read();
        while (read != -1) {
            outputStream.write(read);
            read = inputStream.read();
        }

        return outputStream.toByteArray();
    }
}
