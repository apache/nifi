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
package org.apache.nifi.remote.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import mockwebserver3.RecordedRequest;
import okhttp3.Headers;
import okhttp3.HttpUrl;
import org.apache.nifi.remote.Peer;
import org.apache.nifi.remote.PeerDescription;
import org.apache.nifi.remote.SiteToSiteEventReporter;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.io.http.HttpCommunicationsSession;
import org.apache.nifi.remote.protocol.CommunicationsInput;
import org.apache.nifi.remote.protocol.CommunicationsSession;
import org.apache.nifi.remote.protocol.ResponseCode;
import org.apache.nifi.remote.protocol.http.HttpHeaders;
import org.apache.nifi.remote.protocol.http.HttpProxy;
import org.apache.nifi.web.api.dto.ControllerDTO;
import org.apache.nifi.web.api.dto.remote.PeerDTO;
import org.apache.nifi.web.api.entity.ControllerEntity;
import org.apache.nifi.web.api.entity.PeersEntity;
import org.apache.nifi.web.api.entity.TransactionResultEntity;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.net.HttpURLConnection.HTTP_ACCEPTED;
import static java.net.HttpURLConnection.HTTP_CREATED;
import static java.net.HttpURLConnection.HTTP_INTERNAL_ERROR;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_PROXY_AUTH;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

@Timeout(5)
class SiteToSiteRestApiClientTest {

    private static final int TIMEOUT = 5;

    private static final String APPLICATION_JSON = "application/json";
    private static final String APPLICATION_OCTET_STREAM = "application/octet-stream";
    private static final String TEXT_PLAIN = "text/plain";

    private static final String ACCEPT_HEADER = "Accept";
    private static final String CONTENT_TYPE_HEADER = "Content-Type";
    private static final String PROXY_AUTHENTICATE_HEADER = "Proxy-Authenticate";
    private static final String PROXY_AUTHORIZATION_HEADER = "Proxy-Authorization";
    private static final String BASIC_REALM = "Basic realm=\"NiFi\"";

    private static final String NIFI_API_PATH = "/nifi-api";
    private static final String SITE_TO_SITE_PATH = "/site-to-site";
    private static final String SITE_TO_SITE_PEERS_PATH = "/site-to-site/peers";
    private static final String RECEIVE_TRANSACTION_PATH = "/data-transfer/output-ports/port-identifier/transactions";
    private static final String RECEIVE_TRANSACTION_FORMAT = "%s/data-transfer/output-ports/port-identifier/transactions";
    private static final String SEND_TRANSACTION_PATH = "/data-transfer/input-ports/port-identifier/transactions";
    private static final String SEND_TRANSACTION_FORMAT = "%s/data-transfer/input-ports/port-identifier/transactions";
    private static final String FLOW_FILES_PATH_FORMAT = "%s/flow-files";

    private static final String DELETE_METHOD = "DELETE";
    private static final String GET_METHOD = "GET";
    private static final String POST_METHOD = "POST";
    private static final String PUT_METHOD = "PUT";

    private static final String CHECKSUM_PARAMETER = "checksum";
    private static final String RESPONSE_CODE_PARAMETER = "responseCode";

    private static final int LATEST_PROTOCOL_VERSION = 5;
    private static final String FIRST_PROTOCOL_VERSION = "1";
    private static final String CHECKSUM = "CHECKSUM";
    private static final String PORT_ID = "port-identifier";
    private static final int TRANSACTION_TTL = 5000;
    private static final int READ_TIMEOUT = 5000;

    private static final String PROXY_USER = "user";
    private static final String PROXY_PASS = "granted";
    private static final String PROXY_CREDENTIALS = "Basic dXNlcjpncmFudGVk";

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final MockWebServer server = new MockWebServer();

    private SiteToSiteRestApiClient apiClient;

    @BeforeEach
    void setApiClient() throws IOException {
        server.start();
        apiClient = new SiteToSiteRestApiClient(null, null, SiteToSiteEventReporter.DISABLED);
        apiClient.setReadTimeoutMillis(READ_TIMEOUT);
        apiClient.setRequestExpirationMillis(READ_TIMEOUT);
        apiClient.setConnectTimeoutMillis(READ_TIMEOUT);
        apiClient.setCacheExpirationMillis(READ_TIMEOUT);

        final InetAddress localAddress = InetAddress.getByName("127.0.0.1");
        apiClient.setLocalAddress(localAddress);
    }

    @AfterEach
    void stopServer() throws IOException {
        server.close();
        apiClient.close();
    }

    @Test
    void testSetBaseUrl() {
        final HttpUrl url = server.url(NIFI_API_PATH);

        apiClient.setBaseUrl(url.scheme(), url.host(), url.port());

        final String baseUrl = apiClient.getBaseUrl();
        assertEquals(url.toString(), baseUrl);
    }

    @Test
    void testGetController() throws IOException, InterruptedException {
        enqueueController();

        final String serverUrl = getServerUrl();
        final ControllerDTO controller = apiClient.getController(serverUrl);

        assertNotNull(controller);
        assertEquals(ControllerDTO.class.getName(), controller.getId());

        assertRequestRecorded(SITE_TO_SITE_PATH);
        assertEquals(LATEST_PROTOCOL_VERSION, apiClient.getTransactionProtocolVersion());
    }

    @Test
    void testGetControllerProxy() throws IOException, InterruptedException {
        final Proxy proxyAddress = server.getProxyAddress();
        final InetSocketAddress proxyServerAddress = (InetSocketAddress) proxyAddress.address();
        final HttpProxy httpProxy = new HttpProxy(proxyServerAddress.getHostName(), proxyServerAddress.getPort(), PROXY_USER, PROXY_PASS);
        apiClient = new SiteToSiteRestApiClient(null, httpProxy, SiteToSiteEventReporter.DISABLED);
        final String serverUrl = getServerUrl();
        apiClient.setBaseUrl(serverUrl);
        apiClient.setReadTimeoutMillis(READ_TIMEOUT);

        server.enqueue(new MockResponse.Builder()
                .code(HTTP_PROXY_AUTH)
                .setHeader(PROXY_AUTHENTICATE_HEADER, BASIC_REALM)
                .build()
        );

        enqueueController();

        final ControllerDTO controller = apiClient.getController(serverUrl);
        assertEquals(ControllerDTO.class.getName(), controller.getId());

        assertRequestRecorded(SITE_TO_SITE_PATH);

        final RecordedRequest authenticatedRequest = assertRequestRecorded(SITE_TO_SITE_PATH);
        final Headers authenticatedHeaders = authenticatedRequest.getHeaders();
        final String proxyAuthorization = authenticatedHeaders.get(PROXY_AUTHORIZATION_HEADER);
        assertEquals(PROXY_CREDENTIALS, proxyAuthorization);
    }

    @Test
    void testGetPeers() throws IOException, InterruptedException {
        final PeersEntity entity = new PeersEntity();
        entity.setPeers(List.of());

        enqueueResponseBody(entity);

        final String serverUrl = getServerUrl();
        apiClient.setBaseUrl(serverUrl);

        final Collection<PeerDTO> peers = apiClient.getPeers();
        assertNotNull(peers);

        final RecordedRequest request = assertRequestRecorded(SITE_TO_SITE_PEERS_PATH);
        assertProtocolVersionFound(request);
    }

    @Test
    void testCommitTransferFlowFiles() throws IOException, InterruptedException {
        final String serverUrl = getServerUrl();
        apiClient.setBaseUrl(serverUrl);

        final TransactionResultEntity entity = new TransactionResultEntity();
        enqueueResponseBody(entity);

        final ResponseCode responseCode = ResponseCode.PROPERTIES_OK;
        final TransactionResultEntity resultEntity = apiClient.commitTransferFlowFiles(RECEIVE_TRANSACTION_PATH, responseCode);

        assertNotNull(resultEntity);
        final RecordedRequest request = assertRequestRecorded(RECEIVE_TRANSACTION_PATH);
        assertEquals(DELETE_METHOD, request.getMethod());

        assertResponseCodeEquals(request, responseCode);
        assertProtocolVersionFound(request);
    }

    @Test
    void testCommitReceivingFlowFiles() throws IOException, InterruptedException {
        final String serverUrl = getServerUrl();
        apiClient.setBaseUrl(serverUrl);

        final TransactionResultEntity entity = new TransactionResultEntity();
        enqueueResponseBody(entity);

        final ResponseCode responseCode = ResponseCode.CONFIRM_TRANSACTION;
        final TransactionResultEntity resultEntity = apiClient.commitReceivingFlowFiles(RECEIVE_TRANSACTION_PATH, responseCode, CHECKSUM);

        assertNotNull(resultEntity);
        final RecordedRequest request = assertRequestRecorded(RECEIVE_TRANSACTION_PATH);
        assertEquals(DELETE_METHOD, request.getMethod());

        final String checksumParameter = request.getUrl().queryParameter(CHECKSUM_PARAMETER);
        assertEquals(CHECKSUM, checksumParameter);

        assertResponseCodeEquals(request, responseCode);
        assertProtocolVersionFound(request);
    }

    @Test
    void testInitiateTransactionCreated() throws IOException, InterruptedException {
        final String serverUrl = getServerUrl();
        apiClient.setBaseUrl(serverUrl);

        initiateTransaction(serverUrl, TransferDirection.RECEIVE);
    }

    @Test
    void testInitiateTransactionSendCreated() throws IOException, InterruptedException {
        final String serverUrl = getServerUrl();
        apiClient.setBaseUrl(serverUrl);

        initiateTransaction(serverUrl, TransferDirection.SEND);
    }

    @Test
    void testInitiateTransactionSendProxyAuthentication() throws IOException, InterruptedException {
        final Proxy proxyAddress = server.getProxyAddress();
        final InetSocketAddress proxyServerAddress = (InetSocketAddress) proxyAddress.address();
        final HttpProxy httpProxy = new HttpProxy(proxyServerAddress.getHostName(), proxyServerAddress.getPort(), PROXY_USER, PROXY_PASS);
        apiClient = new SiteToSiteRestApiClient(null, httpProxy, SiteToSiteEventReporter.DISABLED);
        final String serverUrl = getServerUrl();
        apiClient.setBaseUrl(serverUrl);
        apiClient.setReadTimeoutMillis(READ_TIMEOUT);

        enqueueController();
        enqueueTransaction(serverUrl);

        final String transactionUri = apiClient.initiateTransaction(TransferDirection.SEND, PORT_ID);
        assertEquals(serverUrl, transactionUri);

        final RecordedRequest controllerRequest = assertRequestRecorded(SITE_TO_SITE_PATH);
        assertEquals(GET_METHOD, controllerRequest.getMethod());

        final RecordedRequest request = assertRequestRecorded(SEND_TRANSACTION_PATH);
        assertEquals(POST_METHOD, request.getMethod());
        assertProtocolVersionFound(request);
    }

    @Test
    void testExtendTransaction() throws IOException, InterruptedException {
        final String serverUrl = getServerUrl();
        apiClient.setBaseUrl(serverUrl);

        final TransactionResultEntity entity = new TransactionResultEntity();
        enqueueResponseBody(entity);

        final TransactionResultEntity resultEntity = apiClient.extendTransaction(RECEIVE_TRANSACTION_PATH);

        assertNotNull(resultEntity);
        final RecordedRequest request = assertRequestRecorded(RECEIVE_TRANSACTION_PATH);
        assertEquals(PUT_METHOD, request.getMethod());
        assertProtocolVersionFound(request);
    }

    @Test
    void testExtendTransactionServerError() throws InterruptedException {
        final String serverUrl = getServerUrl();
        apiClient.setBaseUrl(serverUrl);

        server.enqueue(new MockResponse.Builder().code(HTTP_INTERNAL_ERROR).build());

        assertThrows(IOException.class, () -> apiClient.extendTransaction(RECEIVE_TRANSACTION_PATH));

        final RecordedRequest request = assertRequestRecorded(RECEIVE_TRANSACTION_PATH);
        assertEquals(PUT_METHOD, request.getMethod());
        assertProtocolVersionFound(request);
    }

    @Test
    void testOpenConnectionForReceive() throws IOException, InterruptedException {
        final String serverUrl = getServerUrl();
        apiClient.setBaseUrl(serverUrl);
        final PeerDescription peerDescription = new PeerDescription(server.getHostName(), server.getPort(), false);
        final HttpCommunicationsSession session = new HttpCommunicationsSession();
        final Peer peer = new Peer(peerDescription, session, serverUrl, serverUrl);

        server.enqueue(new MockResponse.Builder().code(HTTP_OK).build());

        final boolean open = apiClient.openConnectionForReceive(RECEIVE_TRANSACTION_PATH, peer);
        assertFalse(open);

        final RecordedRequest request = server.takeRequest(TIMEOUT, TimeUnit.SECONDS);
        assertNotNull(request);
        assertEquals(GET_METHOD, request.getMethod());
        assertProtocolVersionFound(request);

        final Headers requestHeaders = request.getHeaders();
        final String acceptHeader = requestHeaders.get(ACCEPT_HEADER);
        assertNull(acceptHeader);
    }

    @Test
    void testOpenConnectionForReceiveAccepted() throws IOException, InterruptedException {
        final String serverUrl = getServerUrl();
        apiClient.setBaseUrl(serverUrl);
        final String receiveTransactionUri = RECEIVE_TRANSACTION_FORMAT.formatted(serverUrl);

        final String transactionUri = initiateTransaction(receiveTransactionUri, TransferDirection.RECEIVE);

        final PeerDescription peerDescription = new PeerDescription(server.getHostName(), server.getPort(), false);
        final HttpCommunicationsSession session = new HttpCommunicationsSession();
        final Peer peer = new Peer(peerDescription, session, serverUrl, serverUrl);

        server.enqueue(new MockResponse.Builder().code(HTTP_ACCEPTED).build());

        final boolean open = apiClient.openConnectionForReceive(transactionUri, peer);
        assertTrue(open);

        final CommunicationsInput input = session.getInput();
        try (InputStream inputStream = input.getInputStream()) {
            final int read = inputStream.read();
            assertEquals(-1, read);
        }

        final String transitUri = session.createTransitUri(null, null);
        final String expectedTransitUri = FLOW_FILES_PATH_FORMAT.formatted(receiveTransactionUri);
        assertEquals(expectedTransitUri, transitUri);

        final RecordedRequest request = server.takeRequest(TIMEOUT, TimeUnit.SECONDS);
        assertNotNull(request);
        assertEquals(GET_METHOD, request.getMethod());
        assertProtocolVersionFound(request);
    }

    @Test
    void testOpenConnectionForSend() throws IOException, InterruptedException {
        final String serverUrl = getServerUrl();
        apiClient.setBaseUrl(serverUrl);

        final String sendTransactionUri = SEND_TRANSACTION_FORMAT.formatted(serverUrl);
        final String transactionUri = initiateTransaction(sendTransactionUri, TransferDirection.SEND);

        final PeerDescription peerDescription = new PeerDescription(server.getHostName(), server.getPort(), false);
        final HttpCommunicationsSession session = new HttpCommunicationsSession();
        final Peer peer = new Peer(peerDescription, session, serverUrl, serverUrl);

        server.enqueue(new MockResponse.Builder().code(HTTP_ACCEPTED).body(CHECKSUM).build());

        apiClient.openConnectionForSend(transactionUri, peer);

        // Finish Transfer to complete Connection
        apiClient.finishTransferFlowFiles(session);

        assertEquals(CHECKSUM, session.getChecksum());
        final byte[] inputBytes = session.getInput().getInputStream().readAllBytes();
        assertEquals(CHECKSUM, new String(inputBytes, StandardCharsets.UTF_8));

        final RecordedRequest request = server.takeRequest(TIMEOUT, TimeUnit.SECONDS);
        assertNotNull(request);
        assertEquals(POST_METHOD, request.getMethod());
        assertProtocolVersionFound(request);

        final Headers headers = request.getHeaders();
        final String contentType = headers.get(CONTENT_TYPE_HEADER);
        assertEquals(APPLICATION_OCTET_STREAM, contentType);
        final String accept = headers.get(ACCEPT_HEADER);
        assertEquals(TEXT_PLAIN, accept);
    }

    @Test
    void testFinishTransferFlowFilesNotStarted() throws IOException {
        try (CommunicationsSession session = mock(CommunicationsSession.class)) {
            assertThrows(IllegalStateException.class, () -> apiClient.finishTransferFlowFiles(session));
        }
    }

    private String initiateTransaction(final String serverUrl, final TransferDirection transferDirection) throws IOException, InterruptedException {
        enqueueTransaction(serverUrl);

        final String transactionUri = apiClient.initiateTransaction(transferDirection, PORT_ID);

        assertEquals(serverUrl, transactionUri);

        final String expectedPath;
        if (TransferDirection.SEND == transferDirection) {
            expectedPath = SEND_TRANSACTION_PATH;
        } else {
            expectedPath = RECEIVE_TRANSACTION_PATH;
        }

        final RecordedRequest request = assertRequestRecorded(expectedPath);
        assertEquals(POST_METHOD, request.getMethod());
        assertProtocolVersionFound(request);

        return transactionUri;
    }

    private void enqueueTransaction(final String serverUrl) {
        server.enqueue(new MockResponse.Builder()
                .code(HTTP_CREATED)
                .setHeader(HttpHeaders.LOCATION_URI_INTENT_NAME, HttpHeaders.LOCATION_URI_INTENT_VALUE)
                .setHeader(HttpHeaders.LOCATION_HEADER_NAME, serverUrl)
                .setHeader(HttpHeaders.PROTOCOL_VERSION, FIRST_PROTOCOL_VERSION)
                .setHeader(HttpHeaders.SERVER_SIDE_TRANSACTION_TTL, Integer.toString(TRANSACTION_TTL))
                .build()
        );
    }

    private void enqueueController() throws JsonProcessingException {
        final ControllerDTO expected = new ControllerDTO();
        expected.setId(ControllerDTO.class.getName());
        final ControllerEntity entity = new ControllerEntity();
        entity.setController(expected);

        enqueueResponseBody(entity);
    }

    private void enqueueResponseBody(final Object responseBody) throws JsonProcessingException {
        final String body = objectMapper.writeValueAsString(responseBody);

        server.enqueue(new MockResponse.Builder().code(HTTP_OK).body(body).build());
    }

    private String getServerUrl() {
        final HttpUrl url = server.url(NIFI_API_PATH);
        return url.toString();
    }

    private void assertProtocolVersionFound(final RecordedRequest request) {
        final Headers headers = request.getHeaders();
        final String protocolVersion = headers.get(HttpHeaders.PROTOCOL_VERSION);
        assertEquals(FIRST_PROTOCOL_VERSION, protocolVersion);
    }

    private void assertResponseCodeEquals(final RecordedRequest request, final ResponseCode responseCode) {
        final String responseCodeParameter = request.getUrl().queryParameter(RESPONSE_CODE_PARAMETER);
        assertNotNull(responseCodeParameter);

        final int codeParameter = Integer.parseInt(responseCodeParameter);
        assertEquals(responseCode.getCode(), codeParameter);
    }

    private RecordedRequest assertRequestRecorded(final String expectedPath) throws InterruptedException {
        final RecordedRequest request = server.takeRequest(TIMEOUT, TimeUnit.SECONDS);
        assertNotNull(request);

        final Headers headers = request.getHeaders();
        final String accept = headers.get(ACCEPT_HEADER);

        final String method = request.getMethod();
        if (POST_METHOD.equals(method)) {
            assertEquals(APPLICATION_JSON, accept);
        }

        final HttpUrl url = request.getUrl();
        final String encodedPath = url.encodedPath();

        final String path = NIFI_API_PATH + expectedPath;
        assertEquals(path, encodedPath);

        return request;
    }
}
