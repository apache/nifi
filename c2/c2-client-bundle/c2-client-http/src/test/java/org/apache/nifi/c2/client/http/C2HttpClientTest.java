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
package org.apache.nifi.c2.client.http;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.nifi.c2.client.C2ClientConfig;
import org.apache.nifi.c2.protocol.api.C2Heartbeat;
import org.apache.nifi.c2.protocol.api.C2HeartbeatResponse;
import org.apache.nifi.c2.protocol.api.C2OperationAck;
import org.apache.nifi.c2.serializer.C2Serializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class C2HttpClientTest {

    private static final String HEARTBEAT_PATH = "c2/heartbeat";
    private static final String UPDATE_PATH = "c2/update";
    private static final String ACK_PATH = "c2/acknowledge";
    private static final int HTTP_STATUS_OK = 200;
    private static final int HTTP_STATUS_BAD_REQUEST = 400;
    private static final long KEEP_ALIVE_DURATION = 5000l;
    private static final int MAX_IDLE_CONNECTIONS = 5;

    @Mock
    private C2ClientConfig c2ClientConfig;

    @Mock
    private C2Serializer serializer;

    private C2HttpClient c2HttpClient;

    private MockWebServer mockWebServer;

    private String baseUrl;

    @BeforeEach
    public void startServer() {
        mockWebServer = new MockWebServer();
        baseUrl = mockWebServer.url("/").newBuilder().host("localhost").build().toString();
        when(c2ClientConfig.getKeepAliveDuration()).thenReturn(KEEP_ALIVE_DURATION);
        when(c2ClientConfig.getMaxIdleConnections()).thenReturn(MAX_IDLE_CONNECTIONS);
        c2HttpClient = new C2HttpClient(c2ClientConfig, serializer);
    }

    @AfterEach
    public void shutdownServer() throws IOException {
        mockWebServer.shutdown();
    }

    @Test
    void testPublishHeartbeatSuccess() throws InterruptedException {
        C2HeartbeatResponse hbResponse = new C2HeartbeatResponse();
        mockWebServer.enqueue(new MockResponse().setBody("responseBody"));

        when(serializer.serialize(any(C2Heartbeat.class))).thenReturn(Optional.of("Heartbeat"));
        when(serializer.deserialize(any(), any())).thenReturn(Optional.of(hbResponse));
        when(c2ClientConfig.getC2Url()).thenReturn(baseUrl + HEARTBEAT_PATH);

        Optional<C2HeartbeatResponse> response = c2HttpClient.publishHeartbeat(new C2Heartbeat());

        assertTrue(response.isPresent());
        assertEquals(response.get(), hbResponse);

        RecordedRequest request = mockWebServer.takeRequest();
        assertEquals("/" + HEARTBEAT_PATH, request.getPath());
    }

    @Test
    void testPublishHeartbeatReturnEmptyInCaseOfCommunicationIssue() {
        when(serializer.serialize(any(C2Heartbeat.class))).thenReturn(Optional.of("Heartbeat"));
        when(c2ClientConfig.getC2Url()).thenReturn("http://localhost/incorrectPath");

        Optional<C2HeartbeatResponse> response = c2HttpClient.publishHeartbeat(new C2Heartbeat());

        assertFalse(response.isPresent());
    }

    @Test
    void testConstructorThrowsExceptionForInvalidKeystoreFilenameAtInitialization() {
        when(c2ClientConfig.getKeystoreFilename()).thenReturn("incorrectKeystoreFilename");

        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> new C2HttpClient(c2ClientConfig, serializer));

        assertTrue(exception.getMessage().contains("TLS"));
    }

    @Test
    void testRetrieveUpdateContentReturnsEmptyWhenServerErrorResponse() throws InterruptedException {
        mockWebServer.enqueue(new MockResponse().setBody("updateContent").setResponseCode(HTTP_STATUS_BAD_REQUEST));

        Optional<byte[]> response = c2HttpClient.retrieveUpdateContent(baseUrl + UPDATE_PATH);

        assertFalse(response.isPresent());

        RecordedRequest request = mockWebServer.takeRequest();
        assertEquals("/" + UPDATE_PATH, request.getPath());
    }

    @Test
    void testRetrieveUpdateContentReturnsResponseWithBody() throws InterruptedException {
        String content = "updateContent";
        mockWebServer.enqueue(new MockResponse().setBody(content).setResponseCode(HTTP_STATUS_OK));

        Optional<byte[]> response = c2HttpClient.retrieveUpdateContent(baseUrl + UPDATE_PATH);

        assertTrue(response.isPresent());
        assertArrayEquals(content.getBytes(StandardCharsets.UTF_8), response.get());

        RecordedRequest request = mockWebServer.takeRequest();
        assertEquals("/" + UPDATE_PATH, request.getPath());
    }

    @Test
    void testAcknowledgeOperation() throws InterruptedException {
        String ackContent = "ack";
        when(c2ClientConfig.getC2AckUrl()).thenReturn(baseUrl + ACK_PATH);
        when(serializer.serialize(any(C2OperationAck.class))).thenReturn(Optional.of(ackContent));
        mockWebServer.enqueue(new MockResponse().setResponseCode(HTTP_STATUS_OK));

        c2HttpClient.acknowledgeOperation(new C2OperationAck());

        RecordedRequest request = mockWebServer.takeRequest();
        assertEquals("/" + ACK_PATH, request.getPath());
        assertTrue(request.getHeader("Content-Type").contains("application/json"));
        assertArrayEquals(ackContent.getBytes(StandardCharsets.UTF_8), request.getBody().readByteArray());
    }
}
