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

package org.apache.nifi.snowflake.service;

import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import mockwebserver3.RecordedRequest;
import mockwebserver3.junit5.StartStop;
import okio.ByteString;
import org.apache.nifi.processors.snowflake.snowpipe.InsertFile;
import org.apache.nifi.processors.snowflake.snowpipe.InsertFileStatus;
import org.apache.nifi.processors.snowflake.snowpipe.InsertFiles;
import org.apache.nifi.processors.snowflake.snowpipe.InsertReport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.SocketAddress;
import java.net.URI;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.interfaces.RSAPrivateCrtKey;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(15)
class SnowpipeIngestClientTest {

    private static final String KEY_ALGORITHM = "RSA";

    private static final String ACCOUNT = "TEST-ACCOUNT";

    private static final String USER = "TEST-USER";

    private static final String PIPE_NAME = "DB.SCHEMA.PIPE";

    private static final String STAGED_FILE_PATH = "test-file.csv";

    private static final String BAD_FILE_PATH = "bad-file.csv";

    private static final String POST_METHOD = "POST";

    private static final String GET_METHOD = "GET";

    private static final String AUTHORIZATION_HEADER = "Authorization";

    private static final String CONTENT_TYPE_HEADER = "Content-Type";

    private static final String APPLICATION_JSON = "application/json";

    private static final String BEARER_PREFIX = "Bearer ";

    private static final String INSERT_FILES_PATH_PREFIX = "/v1/data/pipes/DB.SCHEMA.PIPE/insertFiles";

    private static final String INSERT_REPORT_PATH_PREFIX = "/v1/data/pipes/DB.SCHEMA.PIPE/insertReport";

    private static final String HTTP_URI_FORMAT = "http://%s:%d";

    private static final String REQUEST_ID_PARAMETER = "requestId=";

    private static final String FIRST_ERROR_MESSAGE = "Number of columns in file does not match";

    private static final String PIPE_NOT_FOUND_BODY = "Pipe not found";

    private static final String INTERNAL_ERROR_BODY = "Internal error";

    private static final String INSERT_FILES_SUCCESS_RESPONSE = """
            {"requestId":"test-id","status":"success"}""";

    private static final String INSERT_REPORT_RESPONSE = """
            {
                "pipe": "DB.SCHEMA.PIPE",
                "completeResult": true,
                "nextBeginMark": "1_1",
                "files": [
                    {
                        "path": "test-file.csv",
                        "stageLocation": "s3://bucket/",
                        "fileSize": 100,
                        "rowsInserted": 5,
                        "rowsParsed": 5,
                        "errorsSeen": 0,
                        "errorLimit": 1,
                        "complete": true,
                        "status": "LOADED"
                    }
                ]
            }""";

    private static final String INSERT_REPORT_WITH_ERROR_RESPONSE = """
            {
                "pipe": "DB.SCHEMA.PIPE",
                "files": [
                    {
                        "path": "bad-file.csv",
                        "errorsSeen": 3,
                        "firstError": "Number of columns in file does not match",
                        "complete": true,
                        "status": "LOAD_FAILED"
                    }
                ]
            }""";

    private static final String INSERT_REPORT_EMPTY_RESPONSE = """
            {"pipe": "DB.SCHEMA.PIPE", "files": []}""";

    @StartStop
    public final MockWebServer mockWebServer = new MockWebServer();

    private SnowpipeIngestClient client;

    @BeforeEach
    void setClient() throws NoSuchAlgorithmException {
        final URI baseUri = URI.create(HTTP_URI_FORMAT.formatted(mockWebServer.getHostName(), mockWebServer.getPort()));
        final RSAPrivateCrtKey privateKey = generatePrivateKey();
        final RSAKeyAuthorizationProvider authProvider = new RSAKeyAuthorizationProvider(ACCOUNT, USER, privateKey);
        client = new SnowpipeIngestClient(baseUri, PIPE_NAME, authProvider);
    }

    @AfterEach
    void closeClient() {
        client.close();
    }

    @Test
    void testInsertFiles() throws InterruptedException {
        mockWebServer.enqueue(new MockResponse.Builder()
                .code(HttpURLConnection.HTTP_OK)
                .addHeader(CONTENT_TYPE_HEADER, APPLICATION_JSON)
                .body(INSERT_FILES_SUCCESS_RESPONSE)
                .build());

        final InsertFiles insertFiles = new InsertFiles(List.of(new InsertFile(STAGED_FILE_PATH)));
        client.insertFiles(insertFiles);

        final RecordedRequest request = mockWebServer.takeRequest();
        assertEquals(POST_METHOD, request.getMethod());
        final String target = request.getTarget();
        assertNotNull(target);
        assertTrue(target.startsWith(INSERT_FILES_PATH_PREFIX));
        assertTrue(target.contains(REQUEST_ID_PARAMETER));

        final ByteString requestBodyEncoded = request.getBody();
        assertNotNull(requestBodyEncoded);
        final String requestBody = requestBodyEncoded.utf8();
        assertTrue(requestBody.contains(STAGED_FILE_PATH));

        final String authHeader = request.getHeaders().get(AUTHORIZATION_HEADER);
        assertNotNull(authHeader);
        assertTrue(authHeader.startsWith(BEARER_PREFIX));
    }

    @Test
    void testInsertFilesErrorResponse() {
        mockWebServer.enqueue(new MockResponse.Builder()
                .code(HttpURLConnection.HTTP_NOT_FOUND)
                .body(PIPE_NOT_FOUND_BODY)
                .build());

        final InsertFiles insertFiles = new InsertFiles(List.of(new InsertFile(STAGED_FILE_PATH)));
        final SnowpipeResponseException exception = assertThrows(
                SnowpipeResponseException.class,
                () -> client.insertFiles(insertFiles)
        );
        assertTrue(exception.getMessage().contains(String.valueOf(HttpURLConnection.HTTP_NOT_FOUND)));
    }

    @Test
    void testGetInsertReport() throws InterruptedException {
        mockWebServer.enqueue(new MockResponse.Builder()
                .code(HttpURLConnection.HTTP_OK)
                .addHeader(CONTENT_TYPE_HEADER, APPLICATION_JSON)
                .body(INSERT_REPORT_RESPONSE)
                .build());

        final InsertReport response = client.getInsertReport();

        assertNotNull(response);
        final List<InsertFileStatus> files = response.files();
        assertEquals(1, files.size());

        final InsertFileStatus entry = files.getFirst();
        assertEquals(STAGED_FILE_PATH, entry.path());
        assertTrue(entry.complete());
        assertEquals(0, entry.errorsSeen());

        final RecordedRequest request = mockWebServer.takeRequest();
        assertEquals(GET_METHOD, request.getMethod());
        final String target = request.getTarget();
        assertNotNull(target);
        assertTrue(target.startsWith(INSERT_REPORT_PATH_PREFIX));

        final String authHeader = request.getHeaders().get(AUTHORIZATION_HEADER);
        assertNotNull(authHeader);
        assertTrue(authHeader.startsWith(BEARER_PREFIX));
    }

    @Test
    void testGetInsertReportWithErrors() {
        mockWebServer.enqueue(new MockResponse.Builder()
                .code(HttpURLConnection.HTTP_OK)
                .addHeader(CONTENT_TYPE_HEADER, APPLICATION_JSON)
                .body(INSERT_REPORT_WITH_ERROR_RESPONSE)
                .build());

        final InsertReport response = client.getInsertReport();

        final List<InsertFileStatus> files = response.files();
        assertEquals(1, files.size());

        final InsertFileStatus entry = files.getFirst();
        assertEquals(BAD_FILE_PATH, entry.path());
        assertTrue(entry.complete());
        assertEquals(3, entry.errorsSeen());
        assertEquals(FIRST_ERROR_MESSAGE, entry.firstError());
    }

    @Test
    void testGetInsertReportEmpty() {
        mockWebServer.enqueue(new MockResponse.Builder()
                .code(HttpURLConnection.HTTP_OK)
                .addHeader(CONTENT_TYPE_HEADER, APPLICATION_JSON)
                .body(INSERT_REPORT_EMPTY_RESPONSE)
                .build());

        final InsertReport response = client.getInsertReport();

        assertNotNull(response);
        assertTrue(response.files().isEmpty());
    }

    @Test
    void testGetInsertReportErrorResponse() {
        mockWebServer.enqueue(new MockResponse.Builder()
                .code(HttpURLConnection.HTTP_INTERNAL_ERROR)
                .body(INTERNAL_ERROR_BODY)
                .build());

        final SnowpipeResponseException exception = assertThrows(
                SnowpipeResponseException.class,
                () -> client.getInsertReport()
        );
        assertTrue(exception.getMessage().contains(String.valueOf(HttpURLConnection.HTTP_INTERNAL_ERROR)));
    }

    @Test
    void testInsertFilesViaProxy() throws InterruptedException, NoSuchAlgorithmException {
        // MockWebServer acts as the HTTP proxy. The base URI points to a distinct fake host
        // so that the request only reaches MockWebServer if the proxy selector is honoured.
        // When Java's HttpClient routes a request through an HTTP proxy it sends the target
        // in absolute form (e.g. "http://fake-snowflake.example.com/v1/..."), which lets us
        // assert that the proxy path was actually exercised.
        mockWebServer.enqueue(new MockResponse.Builder()
                .code(HttpURLConnection.HTTP_OK)
                .addHeader(CONTENT_TYPE_HEADER, APPLICATION_JSON)
                .body(INSERT_FILES_SUCCESS_RESPONSE)
                .build());

        final InetSocketAddress proxyAddress = new InetSocketAddress(mockWebServer.getHostName(), mockWebServer.getPort());
        final Proxy proxy = new Proxy(Proxy.Type.HTTP, proxyAddress);
        final ProxySelector proxySelector = new ProxySelector() {
            @Override
            public List<Proxy> select(final URI uri) {
                return List.of(proxy);
            }

            @Override
            public void connectFailed(final URI uri, final SocketAddress sa, final IOException ioe) {
            }
        };

        // Intentionally different from the proxy address to prove routing goes via the proxy
        final URI targetBaseUri = URI.create("http://fake-snowflake.example.com");
        final RSAPrivateCrtKey privateKey = generatePrivateKey();
        final RSAKeyAuthorizationProvider authProvider = new RSAKeyAuthorizationProvider(ACCOUNT, USER, privateKey);

        try (final SnowpipeIngestClient proxyClient = new SnowpipeIngestClient(targetBaseUri, PIPE_NAME, authProvider, proxySelector, null)) {
            proxyClient.insertFiles(new InsertFiles(List.of(new InsertFile(STAGED_FILE_PATH))));
        }

        final RecordedRequest request = mockWebServer.takeRequest();
        // Absolute-form target confirms the request was routed through the configured proxy
        final String target = request.getTarget();
        assertNotNull(target);
        assertTrue(target.startsWith("http://fake-snowflake.example.com"), "Expected absolute-form proxy target but got: " + target);
        assertNotNull(request.getHeaders().get(AUTHORIZATION_HEADER));
    }

    private static RSAPrivateCrtKey generatePrivateKey() throws NoSuchAlgorithmException {
        final KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance(KEY_ALGORITHM);
        final KeyPair keyPair = keyPairGenerator.generateKeyPair();
        return (RSAPrivateCrtKey) keyPair.getPrivate();
    }
}
