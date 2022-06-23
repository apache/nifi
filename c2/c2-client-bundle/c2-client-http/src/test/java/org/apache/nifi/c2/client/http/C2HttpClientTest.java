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

import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.matching;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.stubFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.apache.nifi.c2.client.http.C2HttpClient.HTTP_STATUS_BAD_REQUEST;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.given;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.junit5.WireMockRuntimeInfo;
import com.github.tomakehurst.wiremock.junit5.WireMockTest;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import org.apache.nifi.c2.client.C2ClientConfig;
import org.apache.nifi.c2.client.api.C2Serializer;
import org.apache.nifi.c2.protocol.api.C2Heartbeat;
import org.apache.nifi.c2.protocol.api.C2HeartbeatResponse;
import org.apache.nifi.c2.protocol.api.C2OperationAck;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@WireMockTest
@ExtendWith(MockitoExtension.class)
public class C2HttpClientTest {

    private static final String HEARTBEAT_PATH = "/c2/heartbeat";
    private static final String UPDATE_PATH = "/c2/update";
    private static final String ACK_PATH = "/c2/acknowledge";
    private static final int HTTP_STATUS_OK = 200;

    @Mock
    private C2ClientConfig c2ClientConfig;

    @Mock
    private C2Serializer serializer;

    @InjectMocks
    private C2HttpClient c2HttpClient;

    @Test
    void shouldReturnC2HeartbeatResponse(WireMockRuntimeInfo wmRuntimeInfo) {
        // given
        C2HeartbeatResponse hbResponse = new C2HeartbeatResponse();
        stubFor(WireMock.post(HEARTBEAT_PATH)
            .willReturn(aResponse().withBody("dummyResponseBody")));

        given(serializer.serialize(any(C2Heartbeat.class))).willReturn(Optional.of("Heartbeat"));
        given(serializer.deserialize(any(), any())).willReturn(Optional.of(hbResponse));
        given(c2ClientConfig.getC2Url()).willReturn(wmRuntimeInfo.getHttpBaseUrl() + HEARTBEAT_PATH);

        // when
        Optional<C2HeartbeatResponse> response = c2HttpClient.publishHeartbeat(new C2Heartbeat());

        // then
        assertTrue(response.isPresent());
        assertEquals(response.get(), hbResponse);
    }

    @Test
    void shouldReturnEmptyInCaseOfCommunicationIssue() {
        // given
        given(serializer.serialize(any(C2Heartbeat.class))).willReturn(Optional.of("Heartbeat"));
        given(c2ClientConfig.getC2Url()).willReturn("http://localhost/incorrectPath");

        // when
        Optional<C2HeartbeatResponse> response = c2HttpClient.publishHeartbeat(new C2Heartbeat());

        // then
        assertFalse(response.isPresent());
    }

    @Test
    void shouldThrowExceptionForInvalidKeystoreFilenameAtInitialization() {
        // given
        given(c2ClientConfig.getKeystoreFilename()).willReturn("incorrectKeystoreFilename");

        // when
        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> new C2HttpClient(c2ClientConfig, serializer));

        // then
        assertEquals("OkHttp TLS configuration failed", exception.getMessage());
    }

    @Test
    void shouldReturnEmptyWhenServerErrorResponse(WireMockRuntimeInfo wmRuntimeInfo) {
        // given
        stubFor(WireMock.get(UPDATE_PATH)
            .willReturn(aResponse().withStatus(HTTP_STATUS_BAD_REQUEST).withBody("updateContent")));

        // when
        Optional<byte[]> response = c2HttpClient.retrieveUpdateContent(wmRuntimeInfo.getHttpBaseUrl() + UPDATE_PATH);

        // then
        assertFalse(response.isPresent());
    }

    @Test
    void shouldReturnResponseWithBody(WireMockRuntimeInfo wmRuntimeInfo) {
        // given
        String content = "updateContent";
        stubFor(WireMock.get(UPDATE_PATH)
            .willReturn(aResponse().withStatus(HTTP_STATUS_OK).withBody(content)));

        // when
        Optional<byte[]> response = c2HttpClient.retrieveUpdateContent(wmRuntimeInfo.getHttpBaseUrl() + UPDATE_PATH);

        // then
        assertTrue(response.isPresent());
        assertArrayEquals(content.getBytes(StandardCharsets.UTF_8), response.get());
    }

    @Test
    void shouldSendAck(WireMockRuntimeInfo wmRuntimeInfo) {
        // given
        String ackContent = "ack";
        given(c2ClientConfig.getC2AckUrl()).willReturn(wmRuntimeInfo.getHttpBaseUrl() + ACK_PATH);
        given(serializer.serialize(any(C2OperationAck.class))).willReturn(Optional.of(ackContent));
        stubFor(WireMock.post(ACK_PATH)
            .willReturn(aResponse().withStatus(HTTP_STATUS_OK)));

        // when
       c2HttpClient.acknowledgeOperation(new C2OperationAck());

        // then
        verify(postRequestedFor(urlEqualTo(ACK_PATH))
            .withHeader("Content-Type", containing("application/json"))
            .withRequestBody(matching(ackContent)));
    }
}
