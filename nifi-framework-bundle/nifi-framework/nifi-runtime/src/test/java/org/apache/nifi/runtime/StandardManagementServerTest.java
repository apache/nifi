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
package org.apache.nifi.runtime;

import org.apache.nifi.NiFiServer;
import org.apache.nifi.cluster.ClusterDetailsFactory;
import org.apache.nifi.cluster.ConnectionState;
import org.apache.nifi.controller.DecommissionTask;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StandardManagementServerTest {
    private static final String LOCALHOST = "127.0.0.1";

    private static final String HEALTH_URI = "http://%s:%d/health";

    private static final String HEALTH_CLUSTER_URI = "http://%s:%d/health/cluster";

    private static final String GET_METHOD = "GET";

    private static final String DELETE_METHOD = "DELETE";

    private static final Duration TIMEOUT = Duration.ofSeconds(15);

    private static final String DECOMMISSION_FAILURE = "Cannot decommission Node";

    @Mock
    private NiFiServer server;

    @Mock
    private ClusterDetailsFactory clusterDetailsFactory;

    @Mock
    private DecommissionTask decommissionTask;

    @Test
    void testStartStop() {
        final InetSocketAddress initialBindAddress = new InetSocketAddress(LOCALHOST, 0);
        final StandardManagementServer standardManagementServer = new StandardManagementServer(initialBindAddress, server);

        try {
            standardManagementServer.start();

            final InetSocketAddress bindAddress = standardManagementServer.getServerAddress();
            assertNotSame(initialBindAddress.getPort(), bindAddress.getPort());
        } finally {
            standardManagementServer.stop();
        }
    }

    @Test
    void testGetHealth() throws Exception {
        final InetSocketAddress initialBindAddress = new InetSocketAddress(LOCALHOST, 0);
        final StandardManagementServer standardManagementServer = new StandardManagementServer(initialBindAddress, server);

        try {
            standardManagementServer.start();

            final InetSocketAddress serverAddress = standardManagementServer.getServerAddress();
            assertNotSame(initialBindAddress.getPort(), serverAddress.getPort());

            assertResponseStatusCode(HEALTH_URI, serverAddress, GET_METHOD, HttpURLConnection.HTTP_OK);
        } finally {
            standardManagementServer.stop();
        }
    }

    @Test
    void testGetHealthClusterDisconnected() throws Exception {
        final InetSocketAddress initialBindAddress = new InetSocketAddress(LOCALHOST, 0);
        final StandardManagementServer standardManagementServer = new StandardManagementServer(initialBindAddress, server);

        when(server.getClusterDetailsFactory()).thenReturn(clusterDetailsFactory);
        when(clusterDetailsFactory.getConnectionState()).thenReturn(ConnectionState.DISCONNECTED);

        try {
            standardManagementServer.start();

            final InetSocketAddress serverAddress = standardManagementServer.getServerAddress();
            assertNotSame(initialBindAddress.getPort(), serverAddress.getPort());

            assertResponseStatusCode(HEALTH_CLUSTER_URI, serverAddress, GET_METHOD, HttpURLConnection.HTTP_UNAVAILABLE);
        } finally {
            standardManagementServer.stop();
        }
    }

    @Test
    void testGetHealthClusterConnected() throws Exception {
        final InetSocketAddress initialBindAddress = new InetSocketAddress(LOCALHOST, 0);
        final StandardManagementServer standardManagementServer = new StandardManagementServer(initialBindAddress, server);

        when(server.getClusterDetailsFactory()).thenReturn(clusterDetailsFactory);
        when(clusterDetailsFactory.getConnectionState()).thenReturn(ConnectionState.CONNECTED);

        try {
            standardManagementServer.start();

            final InetSocketAddress serverAddress = standardManagementServer.getServerAddress();
            assertNotSame(initialBindAddress.getPort(), serverAddress.getPort());

            final String responseBody = assertResponseStatusCode(HEALTH_CLUSTER_URI, serverAddress, GET_METHOD, HttpURLConnection.HTTP_OK);
            assertEquals("Cluster Status: CONNECTED\n", responseBody);
        } finally {
            standardManagementServer.stop();
        }
    }

    @Test
    void testDeleteHealthCluster() throws Exception {
        final InetSocketAddress initialBindAddress = new InetSocketAddress(LOCALHOST, 0);
        final StandardManagementServer standardManagementServer = new StandardManagementServer(initialBindAddress, server);

        when(server.getClusterDetailsFactory()).thenReturn(clusterDetailsFactory);
        when(clusterDetailsFactory.getConnectionState()).thenReturn(ConnectionState.CONNECTED);
        when(server.getDecommissionTask()).thenReturn(decommissionTask);

        final CountDownLatch decommissionLatch = new CountDownLatch(1);
        doAnswer(invocation -> {
            decommissionLatch.await();
            throw new IllegalStateException(DECOMMISSION_FAILURE);
        }).when(decommissionTask).decommission();

        try {
            standardManagementServer.start();

            final InetSocketAddress serverAddress = standardManagementServer.getServerAddress();

            final String deleteResponseBody = assertResponseStatusCode(HEALTH_CLUSTER_URI, serverAddress, DELETE_METHOD, HttpURLConnection.HTTP_ACCEPTED);
            assertEquals("Cluster Status: OFFLOADING\nDecommission Status: IN_PROGRESS\n", deleteResponseBody);
            verify(decommissionTask, timeout(TIMEOUT.toMillis())).decommission();

            final String inProgressResponseBody = assertResponseStatusCode(HEALTH_CLUSTER_URI, serverAddress, GET_METHOD, HttpURLConnection.HTTP_OK);
            assertEquals("Cluster Status: CONNECTED\nDecommission Status: IN_PROGRESS\n", inProgressResponseBody);

            // A request while a decommission is in progress is accepted without starting another decommission
            final String duplicateDeleteResponseBody = assertResponseStatusCode(HEALTH_CLUSTER_URI, serverAddress, DELETE_METHOD, HttpURLConnection.HTTP_ACCEPTED);
            assertEquals("Cluster Status: OFFLOADING\nDecommission Status: IN_PROGRESS\n", duplicateDeleteResponseBody);

            decommissionLatch.countDown();

            final String failedResponseBody = "Cluster Status: CONNECTED\nDecommission Status: FAILED: " + DECOMMISSION_FAILURE + "\n";
            final long timeoutNanos = System.nanoTime() + TIMEOUT.toNanos();
            String responseBody = assertResponseStatusCode(HEALTH_CLUSTER_URI, serverAddress, GET_METHOD, HttpURLConnection.HTTP_OK);
            while (!failedResponseBody.equals(responseBody) && System.nanoTime() < timeoutNanos) {
                TimeUnit.MILLISECONDS.sleep(100);
                responseBody = assertResponseStatusCode(HEALTH_CLUSTER_URI, serverAddress, GET_METHOD, HttpURLConnection.HTTP_OK);
            }

            assertEquals(failedResponseBody, responseBody);
            verify(decommissionTask, times(1)).decommission();
        } finally {
            decommissionLatch.countDown();
            standardManagementServer.stop();
        }
    }

    @Test
    void testDeleteHealth() throws Exception {
        final InetSocketAddress initialBindAddress = new InetSocketAddress(LOCALHOST, 0);

        final StandardManagementServer standardManagementServer = new StandardManagementServer(initialBindAddress, server);

        try {
            standardManagementServer.start();

            final InetSocketAddress serverAddress = standardManagementServer.getServerAddress();
            assertNotSame(initialBindAddress.getPort(), serverAddress.getPort());

            assertResponseStatusCode(HEALTH_URI, serverAddress, DELETE_METHOD, HttpURLConnection.HTTP_BAD_METHOD);
        } finally {
            standardManagementServer.stop();
        }
    }

    private String assertResponseStatusCode(final String uri, final InetSocketAddress serverAddress, final String method, final int responseStatusCode) throws IOException, InterruptedException {
        final URI healthUri = URI.create(uri.formatted(serverAddress.getHostString(), serverAddress.getPort()));

        try (HttpClient httpClient = HttpClient.newBuilder().connectTimeout(TIMEOUT).build()) {
            final HttpRequest request = HttpRequest.newBuilder(healthUri)
                    .method(method, HttpRequest.BodyPublishers.noBody())
                    .timeout(TIMEOUT)
                    .build();

            final HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            final int statusCode = response.statusCode();

            assertEquals(responseStatusCode, statusCode);

            final String responseBody = response.body();
            assertNotNull(responseBody);
            return responseBody;
        }
    }
}
