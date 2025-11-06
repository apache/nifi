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
package org.apache.nifi.stateless.core;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import mockwebserver3.MockResponse;
import mockwebserver3.MockWebServer;
import mockwebserver3.RecordedRequest;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.registry.flow.VersionedFlow;
import org.apache.nifi.registry.flow.VersionedFlowSnapshot;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@Timeout(value = 10, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class RegistryFlowSnapshotProviderTest {
    private static final int REQUEST_TIMEOUT = 2;

    private static final String API_PATH = "/nifi-registry-api";

    private static final String BUCKET_ID = "testing";

    private static final String FLOW_ID = "primary";

    private static final int VERSION = 100;

    private static final int LATEST_VERSION = -1;

    private static final String FLOW_PATH = "/nifi-registry-api/buckets/testing/flows/primary";

    private static final String FLOW_VERSION_PATH = "/nifi-registry-api/buckets/testing/flows/primary/versions/100";

    private static final String CONTENT_TYPE_HEADER = "Content-Type";

    private static final String APPLICATION_JSON = "application/json";

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private MockWebServer mockWebServer;

    private RegistryFlowSnapshotProvider provider;

    @BeforeEach
    void startServer() throws IOException {
        mockWebServer = new MockWebServer();
        mockWebServer.start();

        final String url = mockWebServer.url(API_PATH).toString();

        provider = new RegistryFlowSnapshotProvider(url, null);
    }

    @AfterEach
    void shutdownServer() {
        mockWebServer.close();
    }

    @Test
    void testGetFlowSnapshot() throws Exception {
        enqueueSnapshotResponse();

        final VersionedFlowSnapshot snapshot = provider.getFlowSnapshot(BUCKET_ID, FLOW_ID, VERSION);

        assertNotNull(snapshot);
        assertFlowVersionRequestRecorded();
    }

    @Test
    void testGetFlowSnapshotLatestVersion() throws Exception {
        enqueueFlowResponse();
        enqueueSnapshotResponse();

        final VersionedFlowSnapshot snapshot = provider.getFlowSnapshot(BUCKET_ID, FLOW_ID, LATEST_VERSION);

        assertNotNull(snapshot);
        assertFlowRequestRecorded();
        assertFlowVersionRequestRecorded();
    }

    private void enqueueFlowResponse() throws JsonProcessingException {
        final VersionedFlow versionedFlow = new VersionedFlow();
        versionedFlow.setVersionCount(VERSION);

        final String responseBody = objectMapper.writeValueAsString(versionedFlow);

        final MockResponse response = new MockResponse.Builder()
                .code(HttpURLConnection.HTTP_OK)
                .setHeader(CONTENT_TYPE_HEADER, APPLICATION_JSON)
                .body(responseBody)
                .build();
        mockWebServer.enqueue(response);
    }

    private void enqueueSnapshotResponse() throws JsonProcessingException {
        final VersionedFlowSnapshot snapshotExpected = new VersionedFlowSnapshot();
        final VersionedProcessGroup flowContents = new VersionedProcessGroup();
        snapshotExpected.setFlowContents(flowContents);

        final String responseBody = objectMapper.writeValueAsString(snapshotExpected);

        final MockResponse response = new MockResponse.Builder()
                .code(HttpURLConnection.HTTP_OK)
                .setHeader(CONTENT_TYPE_HEADER, APPLICATION_JSON)
                .body(responseBody)
                .build();
        mockWebServer.enqueue(response);
    }

    private void assertFlowRequestRecorded() throws InterruptedException {
        final RecordedRequest request = mockWebServer.takeRequest(REQUEST_TIMEOUT, TimeUnit.SECONDS);
        assertNotNull(request);
        final String encodedPath = request.getUrl().encodedPath();
        assertEquals(FLOW_PATH, encodedPath);
    }

    private void assertFlowVersionRequestRecorded() throws InterruptedException {
        final RecordedRequest request = mockWebServer.takeRequest(REQUEST_TIMEOUT, TimeUnit.SECONDS);
        assertNotNull(request);
        final String encodedPath = request.getUrl().encodedPath();
        assertEquals(FLOW_VERSION_PATH, encodedPath);
    }
}
