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
package org.apache.nifi.cluster.coordination.http.replication;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.ws.rs.HttpMethod;
import javax.ws.rs.ProcessingException;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.nifi.authorization.user.NiFiUser;
import org.apache.nifi.authorization.user.NiFiUserDetails;
import org.apache.nifi.authorization.user.NiFiUserUtils;
import org.apache.nifi.authorization.user.StandardNiFiUser;
import org.apache.nifi.authorization.user.StandardNiFiUser.Builder;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.http.replication.util.MockReplicationClient;
import org.apache.nifi.cluster.coordination.node.NodeConnectionState;
import org.apache.nifi.cluster.coordination.node.NodeConnectionStatus;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.manager.exception.ConnectingNodeMutableRequestException;
import org.apache.nifi.cluster.manager.exception.DisconnectedNodeMutableRequestException;
import org.apache.nifi.cluster.manager.exception.IllegalClusterStateException;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.api.entity.Entity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.security.ProxiedEntitiesUtils;
import org.apache.nifi.web.security.token.NiFiAuthenticationToken;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

public class TestThreadPoolRequestReplicator {

    @BeforeClass
    public static void setupClass() {
        System.setProperty(NiFiProperties.PROPERTIES_FILE_PATH, "src/test/resources/conf/nifi.properties");
    }

    @Test
    public void testFailedRequestsAreCleanedUp() {
        withReplicator(replicator -> {
            final Set<NodeIdentifier> nodeIds = new HashSet<>();
            nodeIds.add(new NodeIdentifier("1", "localhost", 8000, "localhost", 8001, "localhost", 8002, 8003, false));
            final URI uri = new URI("http://localhost:8080/processors/1");
            final Entity entity = new ProcessorEntity();

            // set the user
            final Authentication authentication = new NiFiAuthenticationToken(new NiFiUserDetails(StandardNiFiUser.ANONYMOUS));
            SecurityContextHolder.getContext().setAuthentication(authentication);

            final AsyncClusterResponse response = replicator.replicate(nodeIds, HttpMethod.GET, uri, entity, new HashMap<>(), true, true);

            // We should get back the same response object
            assertTrue(response == replicator.getClusterResponse(response.getRequestIdentifier()));

            assertEquals(HttpMethod.GET, response.getMethod());
            assertEquals(nodeIds, response.getNodesInvolved());

            assertTrue(response == replicator.getClusterResponse(response.getRequestIdentifier()));

            final NodeResponse nodeResponse = response.awaitMergedResponse(3, TimeUnit.SECONDS);
            assertEquals(8000, nodeResponse.getNodeId().getApiPort());
            assertEquals(Response.Status.FORBIDDEN.getStatusCode(), nodeResponse.getStatus());

            assertNull(replicator.getClusterResponse(response.getRequestIdentifier()));
        }, Status.FORBIDDEN, 0L, null);
    }

    /**
     * If we replicate a request, whenever we obtain the merged response from
     * the AsyncClusterResponse object, the response should no longer be
     * available and should be cleared from internal state. This test is to
     * verify that this behavior occurs.
     */
    @Test
    public void testResponseRemovedWhenCompletedAndFetched() {
        withReplicator(replicator -> {
            final Set<NodeIdentifier> nodeIds = new HashSet<>();
            nodeIds.add(new NodeIdentifier("1", "localhost", 8000, "localhost", 8001, "localhost", 8002, 8003, false));
            final URI uri = new URI("http://localhost:8080/processors/1");
            final Entity entity = new ProcessorEntity();

            // set the user
            final Authentication authentication = new NiFiAuthenticationToken(new NiFiUserDetails(StandardNiFiUser.ANONYMOUS));
            SecurityContextHolder.getContext().setAuthentication(authentication);

            final AsyncClusterResponse response = replicator.replicate(nodeIds, HttpMethod.GET, uri, entity, new HashMap<>(), true, true);

            // We should get back the same response object
            assertTrue(response == replicator.getClusterResponse(response.getRequestIdentifier()));

            assertEquals(HttpMethod.GET, response.getMethod());
            assertEquals(nodeIds, response.getNodesInvolved());

            assertTrue(response == replicator.getClusterResponse(response.getRequestIdentifier()));

            final NodeResponse nodeResponse = response.awaitMergedResponse(3, TimeUnit.SECONDS);
            assertEquals(8000, nodeResponse.getNodeId().getApiPort());
            assertEquals(Response.Status.OK.getStatusCode(), nodeResponse.getStatus());

            assertNull(replicator.getClusterResponse(response.getRequestIdentifier()));
        });
    }

    @Test
    public void testRequestChain() {
        final String proxyIdentity2 = "proxy-2";
        final String proxyIdentity1 = "proxy-1";
        final String userIdentity = "user";

        withReplicator(replicator -> {
            final Set<NodeIdentifier> nodeIds = new HashSet<>();
            nodeIds.add(new NodeIdentifier("1", "localhost", 8000, "localhost", 8001, "localhost", 8002, 8003, false));
            final URI uri = new URI("http://localhost:8080/processors/1");
            final Entity entity = new ProcessorEntity();

            // set the user
            final NiFiUser proxy2 = new Builder().identity(proxyIdentity2).build();
            final NiFiUser proxy1 = new Builder().identity(proxyIdentity1).chain(proxy2).build();
            final NiFiUser user = new Builder().identity(userIdentity).chain(proxy1).build();
            final Authentication authentication = new NiFiAuthenticationToken(new NiFiUserDetails(user));
            SecurityContextHolder.getContext().setAuthentication(authentication);

            replicator.replicate(nodeIds, HttpMethod.GET, uri, entity, new HashMap<>(), true, true);
        }, Response.Status.OK, 0L, null, "<" + userIdentity + "><" + proxyIdentity1 + "><" + proxyIdentity2 +">");
    }

    @Test(timeout = 15000)
    public void testLongWaitForResponse() {
        withReplicator(replicator -> {
            final Set<NodeIdentifier> nodeIds = new HashSet<>();
            final NodeIdentifier nodeId = new NodeIdentifier("1", "localhost", 8000, "localhost", 8001, "localhost", 8002, 8003, false);
            nodeIds.add(nodeId);
            final URI uri = new URI("http://localhost:8080/processors/1");
            final Entity entity = new ProcessorEntity();

            // set the user
            final Authentication authentication = new NiFiAuthenticationToken(new NiFiUserDetails(StandardNiFiUser.ANONYMOUS));
            SecurityContextHolder.getContext().setAuthentication(authentication);

            final AsyncClusterResponse response = replicator.replicate(nodeIds, HttpMethod.GET, uri, entity, new HashMap<>(), true, true);

            // We should get back the same response object
            assertTrue(response == replicator.getClusterResponse(response.getRequestIdentifier()));

            final NodeResponse completedNodeResponse = response.awaitMergedResponse(2, TimeUnit.SECONDS);
            assertNotNull(completedNodeResponse);
            assertNotNull(completedNodeResponse.getThrowable());
            assertEquals(500, completedNodeResponse.getStatus());

            assertTrue(response.isComplete());
            assertNotNull(response.getMergedResponse());
            assertNull(replicator.getClusterResponse(response.getRequestIdentifier()));
        }, Status.OK, 1000, new ProcessingException(new SocketTimeoutException()));
    }

    @Test(timeout = 15000)
    public void testCompleteOnError() {
        withReplicator(replicator -> {
            final Set<NodeIdentifier> nodeIds = new HashSet<>();
            final NodeIdentifier id1 = new NodeIdentifier("1", "localhost", 8100, "localhost", 8101, "localhost", 8102, 8103, false);
            final NodeIdentifier id2 = new NodeIdentifier("2", "localhost", 8200, "localhost", 8201, "localhost", 8202, 8203, false);
            final NodeIdentifier id3 = new NodeIdentifier("3", "localhost", 8300, "localhost", 8301, "localhost", 8302, 8303, false);
            final NodeIdentifier id4 = new NodeIdentifier("4", "localhost", 8400, "localhost", 8401, "localhost", 8402, 8403, false);
            nodeIds.add(id1);
            nodeIds.add(id2);
            nodeIds.add(id3);
            nodeIds.add(id4);

            final URI uri = new URI("http://localhost:8080/processors/1");
            final Entity entity = new ProcessorEntity();

            // set the user
            final Authentication authentication = new NiFiAuthenticationToken(new NiFiUserDetails(StandardNiFiUser.ANONYMOUS));
            SecurityContextHolder.getContext().setAuthentication(authentication);

            final AsyncClusterResponse response = replicator.replicate(nodeIds, HttpMethod.GET, uri, entity, new HashMap<>(), true, true);
            assertNotNull(response.awaitMergedResponse(1, TimeUnit.SECONDS));
        }, null, 0L, new IllegalArgumentException("Exception created for unit test"));
    }

    @Test(timeout = 15000)
    public void testMultipleRequestWithTwoPhaseCommit() {
        final Set<NodeIdentifier> nodeIds = new HashSet<>();
        final NodeIdentifier nodeId = new NodeIdentifier("1", "localhost", 8100, "localhost", 8101, "localhost", 8102, 8103, false);
        nodeIds.add(nodeId);

        final ClusterCoordinator coordinator = mock(ClusterCoordinator.class);
        when(coordinator.getConnectionStatus(Mockito.any(NodeIdentifier.class))).thenReturn(new NodeConnectionStatus(nodeId, NodeConnectionState.CONNECTED));

        final AtomicInteger requestCount = new AtomicInteger(0);
        final NiFiProperties props = NiFiProperties.createBasicNiFiProperties(null, null);

        final MockReplicationClient client = new MockReplicationClient();
        final RequestCompletionCallback requestCompletionCallback = (uri, method, responses) -> {
        };

        final ThreadPoolRequestReplicator replicator = new ThreadPoolRequestReplicator(2, 5, 100, client, coordinator, requestCompletionCallback, EventReporter.NO_OP, props) {
            @Override
            protected NodeResponse replicateRequest(final PreparedRequest request, final NodeIdentifier nodeId,
                final URI uri, final String requestId, final StandardAsyncClusterResponse response) {
                // the resource builder will not expose its headers to us, so we are using Mockito's Whitebox class to extract them.
                final Object expectsHeader = request.getHeaders().get(ThreadPoolRequestReplicator.REQUEST_VALIDATION_HTTP_HEADER);

                final int statusCode;
                if (requestCount.incrementAndGet() == 1) {
                    assertEquals(ThreadPoolRequestReplicator.NODE_CONTINUE, expectsHeader);
                    statusCode = Status.ACCEPTED.getStatusCode();
                } else {
                    assertNull(expectsHeader);
                    statusCode = Status.OK.getStatusCode();
                }

                // Return given response from all nodes.
                final Response clientResponse = mock(Response.class);
                when(clientResponse.getStatus()).thenReturn(statusCode);
                return new NodeResponse(nodeId, request.getMethod(), uri, clientResponse, -1L, requestId);
            }
        };

        try {
            // set the user
            final Authentication authentication = new NiFiAuthenticationToken(new NiFiUserDetails(StandardNiFiUser.ANONYMOUS));
            SecurityContextHolder.getContext().setAuthentication(authentication);

            final AsyncClusterResponse clusterResponse = replicator.replicate(nodeIds, HttpMethod.POST,
                    new URI("http://localhost:80/processors/1"), new ProcessorEntity(), new HashMap<>(), true, true);
            clusterResponse.awaitMergedResponse();

            // Ensure that we received two requests - the first should contain the X-NcmExpects header; the second should not.
            // These assertions are validated above, in the overridden replicateRequest method.
            assertEquals(2, requestCount.get());
        } catch (final Exception e) {
            e.printStackTrace();
            Assert.fail(e.toString());
        } finally {
            replicator.shutdown();
        }
    }

    private ClusterCoordinator createClusterCoordinator() {
        final ClusterCoordinator coordinator = mock(ClusterCoordinator.class);
        when(coordinator.getConnectionStatus(Mockito.any(NodeIdentifier.class))).thenAnswer(new Answer<NodeConnectionStatus>() {
            @Override
            public NodeConnectionStatus answer(InvocationOnMock invocation) throws Throwable {
                return new NodeConnectionStatus(invocation.getArgumentAt(0, NodeIdentifier.class), NodeConnectionState.CONNECTED);
            }
        });

        return coordinator;
    }

    @Test
    public void testMutableRequestRequiresAllNodesConnected() throws URISyntaxException {
        final ClusterCoordinator coordinator = createClusterCoordinator();

        // build a map of connection state to node ids
        final Map<NodeConnectionState, List<NodeIdentifier>> nodeMap = new HashMap<>();
        final List<NodeIdentifier> connectedNodes = new ArrayList<>();
        connectedNodes.add(new NodeIdentifier("1", "localhost", 8100, "localhost", 8101, "localhost", 8102, 8103, false));
        connectedNodes.add(new NodeIdentifier("2", "localhost", 8200, "localhost", 8201, "localhost", 8202, 8203, false));
        nodeMap.put(NodeConnectionState.CONNECTED, connectedNodes);

        final List<NodeIdentifier> otherState = new ArrayList<>();
        otherState.add(new NodeIdentifier("3", "localhost", 8300, "localhost", 8301, "localhost", 8302, 8303, false));
        nodeMap.put(NodeConnectionState.CONNECTING, otherState);

        when(coordinator.getConnectionStates()).thenReturn(nodeMap);
        final NiFiProperties props = NiFiProperties.createBasicNiFiProperties(null, null);

        final MockReplicationClient client = new MockReplicationClient();
        final RequestCompletionCallback requestCompletionCallback = (uri, method, responses) -> {
        };

        final ThreadPoolRequestReplicator replicator = new ThreadPoolRequestReplicator(2, 5, 100, client, coordinator, requestCompletionCallback, EventReporter.NO_OP, props) {
            @Override
            public AsyncClusterResponse replicate(Set<NodeIdentifier> nodeIds, String method, URI uri, Object entity, Map<String, String> headers,
                                                  boolean indicateReplicated, boolean verify) {
                return null;
            }
        };

        try {
            // set the user
            final Authentication authentication = new NiFiAuthenticationToken(new NiFiUserDetails(StandardNiFiUser.ANONYMOUS));
            SecurityContextHolder.getContext().setAuthentication(authentication);

            try {
                replicator.replicate(HttpMethod.POST, new URI("http://localhost:80/processors/1"), new ProcessorEntity(), new HashMap<>());
                Assert.fail("Expected ConnectingNodeMutableRequestException");
            } catch (final ConnectingNodeMutableRequestException e) {
                // expected behavior
            }

            nodeMap.remove(NodeConnectionState.CONNECTING);
            nodeMap.put(NodeConnectionState.DISCONNECTED, otherState);
            try {
                replicator.replicate(HttpMethod.POST, new URI("http://localhost:80/processors/1"), new ProcessorEntity(), new HashMap<>());
                Assert.fail("Expected DisconnectedNodeMutableRequestException");
            } catch (final DisconnectedNodeMutableRequestException e) {
                // expected behavior
            }

            nodeMap.remove(NodeConnectionState.DISCONNECTED);
            nodeMap.put(NodeConnectionState.DISCONNECTING, otherState);
            try {
                replicator.replicate(HttpMethod.POST, new URI("http://localhost:80/processors/1"), new ProcessorEntity(), new HashMap<>());
                Assert.fail("Expected DisconnectedNodeMutableRequestException");
            } catch (final DisconnectedNodeMutableRequestException e) {
                // expected behavior
            }

            // should not throw an Exception because it's a GET
            replicator.replicate(HttpMethod.GET, new URI("http://localhost:80/processors/1"), new MultivaluedHashMap<>(), new HashMap<>());

            // should not throw an Exception because all nodes are now connected
            nodeMap.remove(NodeConnectionState.DISCONNECTING);
            replicator.replicate(HttpMethod.POST, new URI("http://localhost:80/processors/1"), new ProcessorEntity(), new HashMap<>());
        } finally {
            replicator.shutdown();
        }
    }

    @Test(timeout = 15000)
    public void testOneNodeRejectsTwoPhaseCommit() {
        final Set<NodeIdentifier> nodeIds = new HashSet<>();
        nodeIds.add(new NodeIdentifier("1", "localhost", 8100, "localhost", 8101, "localhost", 8102, 8103, false));
        nodeIds.add(new NodeIdentifier("2", "localhost", 8200, "localhost", 8201, "localhost", 8202, 8203, false));

        final ClusterCoordinator coordinator = createClusterCoordinator();
        final AtomicInteger requestCount = new AtomicInteger(0);
        final NiFiProperties props = NiFiProperties.createBasicNiFiProperties(null, null);

        final MockReplicationClient client = new MockReplicationClient();
        final RequestCompletionCallback requestCompletionCallback = (uri, method, responses) -> {
        };

        final ThreadPoolRequestReplicator replicator = new ThreadPoolRequestReplicator(2, 5, 100, client, coordinator, requestCompletionCallback, EventReporter.NO_OP, props) {
            @Override
            protected NodeResponse replicateRequest(final PreparedRequest request, final NodeIdentifier nodeId,
                final URI uri, final String requestId, final StandardAsyncClusterResponse response) {
                // the resource builder will not expose its headers to us, so we are using Mockito's Whitebox class to extract them.
                final Object expectsHeader = request.getHeaders().get(ThreadPoolRequestReplicator.REQUEST_VALIDATION_HTTP_HEADER);

                final int requestIndex = requestCount.incrementAndGet();
                assertEquals(ThreadPoolRequestReplicator.NODE_CONTINUE, expectsHeader);

                if (requestIndex == 1) {
                    final Response clientResponse = mock(Response.class);
                    when(clientResponse.getStatus()).thenReturn(202);
                    return new NodeResponse(nodeId, request.getMethod(), uri, clientResponse, -1L, requestId);
                } else {
                    final IllegalClusterStateException explanation = new IllegalClusterStateException("Intentional Exception for Unit Testing");
                    return new NodeResponse(nodeId, request.getMethod(), uri, explanation);
                }
            }
        };

        try {
            // set the user
            final Authentication authentication = new NiFiAuthenticationToken(new NiFiUserDetails(StandardNiFiUser.ANONYMOUS));
            SecurityContextHolder.getContext().setAuthentication(authentication);

            final AsyncClusterResponse clusterResponse = replicator.replicate(nodeIds, HttpMethod.POST,
                    new URI("http://localhost:80/processors/1"), new ProcessorEntity(), new HashMap<>(), true, true);
            clusterResponse.awaitMergedResponse();

            Assert.fail("Expected to get an IllegalClusterStateException but did not");
        } catch (final IllegalClusterStateException e) {
            // Expected
        } catch (final Exception e) {
            Assert.fail(e.toString());
        } finally {
            replicator.shutdown();
        }
    }

    @Test(timeout = 5000)
    public void testMonitorNotifiedOnException() {
        withReplicator(replicator -> {
            final Object monitor = new Object();

            final CountDownLatch preNotifyLatch = new CountDownLatch(1);
            final CountDownLatch postNotifyLatch = new CountDownLatch(1);

            new Thread(new Runnable() {
                @Override
                public void run() {
                    synchronized (monitor) {
                        while (true) {
                            // If monitor is not notified, this will block indefinitely, and the test will timeout
                            try {
                                preNotifyLatch.countDown();
                                monitor.wait();
                                break;
                            } catch (InterruptedException e) {
                                continue;
                            }
                        }

                        postNotifyLatch.countDown();
                    }
                }
            }).start();

            // wait for the background thread to notify that it is synchronized on monitor.
            preNotifyLatch.await();

            try {
                // set the user
                final Authentication authentication = new NiFiAuthenticationToken(new NiFiUserDetails(StandardNiFiUser.ANONYMOUS));
                SecurityContextHolder.getContext().setAuthentication(authentication);

                // ensure the proxied entities header is set
                final Map<String, String> updatedHeaders = new HashMap<>();
                replicator.updateRequestHeaders(updatedHeaders, NiFiUserUtils.getNiFiUser());

                // Pass in Collections.emptySet() for the node ID's so that an Exception is thrown
                replicator.replicate(Collections.emptySet(), "GET", new URI("localhost:8080/nifi"), Collections.emptyMap(),
                    updatedHeaders, true, null, true, true, monitor);
                Assert.fail("replicate did not throw IllegalArgumentException");
            } catch (final IllegalArgumentException iae) {
                // expected
            }

            // wait for monitor to be notified.
            postNotifyLatch.await();
        });
    }

    @Test(timeout = 5000)
    public void testMonitorNotifiedOnSuccessfulCompletion() {
        withReplicator(replicator -> {
            final Object monitor = new Object();

            final CountDownLatch preNotifyLatch = new CountDownLatch(1);
            final CountDownLatch postNotifyLatch = new CountDownLatch(1);

            new Thread(new Runnable() {
                @Override
                public void run() {
                    synchronized (monitor) {
                        while (true) {
                            // If monitor is not notified, this will block indefinitely, and the test will timeout
                            try {
                                preNotifyLatch.countDown();
                                monitor.wait();
                                break;
                            } catch (InterruptedException e) {
                                continue;
                            }
                        }

                        postNotifyLatch.countDown();
                    }
                }
            }).start();

            // wait for the background thread to notify that it is synchronized on monitor.
            preNotifyLatch.await();

            final Set<NodeIdentifier> nodeIds = new HashSet<>();
            final NodeIdentifier nodeId = new NodeIdentifier("1", "localhost", 8000, "localhost", 8001, "localhost", 8002, 8003, false);
            nodeIds.add(nodeId);
            final URI uri = new URI("http://localhost:8080/processors/1");
            final Entity entity = new ProcessorEntity();

            // set the user
            final Authentication authentication = new NiFiAuthenticationToken(new NiFiUserDetails(StandardNiFiUser.ANONYMOUS));
            SecurityContextHolder.getContext().setAuthentication(authentication);

            // ensure the proxied entities header is set
            final Map<String, String> updatedHeaders = new HashMap<>();
            replicator.updateRequestHeaders(updatedHeaders, NiFiUserUtils.getNiFiUser());

            replicator.replicate(nodeIds, HttpMethod.GET, uri, entity, updatedHeaders, true, null, true, true, monitor);

            // wait for monitor to be notified.
            postNotifyLatch.await();
        });
    }


    @Test(timeout = 5000)
    public void testMonitorNotifiedOnFailureResponse() {
        withReplicator(replicator -> {
            final Object monitor = new Object();

            final CountDownLatch preNotifyLatch = new CountDownLatch(1);
            final CountDownLatch postNotifyLatch = new CountDownLatch(1);

            new Thread(new Runnable() {
                @Override
                public void run() {
                    synchronized (monitor) {
                        while (true) {
                            // If monitor is not notified, this will block indefinitely, and the test will timeout
                            try {
                                preNotifyLatch.countDown();
                                monitor.wait();
                                break;
                            } catch (InterruptedException e) {
                                continue;
                            }
                        }

                        postNotifyLatch.countDown();
                    }
                }
            }).start();

            // wait for the background thread to notify that it is synchronized on monitor.
            preNotifyLatch.await();

            final Set<NodeIdentifier> nodeIds = new HashSet<>();
            final NodeIdentifier nodeId = new NodeIdentifier("1", "localhost", 8000, "localhost", 8001, "localhost", 8002, 8003, false);
            nodeIds.add(nodeId);
            final URI uri = new URI("http://localhost:8080/processors/1");
            final Entity entity = new ProcessorEntity();

            // set the user
            final Authentication authentication = new NiFiAuthenticationToken(new NiFiUserDetails(StandardNiFiUser.ANONYMOUS));
            SecurityContextHolder.getContext().setAuthentication(authentication);

            // ensure the proxied entities header is set
            final Map<String, String> updatedHeaders = new HashMap<>();
            replicator.updateRequestHeaders(updatedHeaders, NiFiUserUtils.getNiFiUser());

            replicator.replicate(nodeIds, HttpMethod.GET, uri, entity, updatedHeaders, true, null, true, true, monitor);

            // wait for monitor to be notified.
            postNotifyLatch.await();
        }, Status.INTERNAL_SERVER_ERROR, 0L, null);
    }


    private void withReplicator(final WithReplicator function) {
        withReplicator(function, Response.Status.OK, 0L, null);
    }

    private void withReplicator(final WithReplicator function, final Status status, final long delayMillis, final RuntimeException failure) {
        withReplicator(function, status, delayMillis, failure, "<>");
    }

    private void withReplicator(final WithReplicator function, final Status status, final long delayMillis, final RuntimeException failure, final String expectedRequestChain) {
        final ClusterCoordinator coordinator = createClusterCoordinator();
        final NiFiProperties nifiProps = NiFiProperties.createBasicNiFiProperties(null, null);
        final MockReplicationClient client = new MockReplicationClient();
        final RequestCompletionCallback requestCompletionCallback = (uri, method, responses) -> {
        };

        final ThreadPoolRequestReplicator replicator = new ThreadPoolRequestReplicator(2, 5, 100, client, coordinator, requestCompletionCallback, EventReporter.NO_OP, nifiProps) {
            @Override
            protected NodeResponse replicateRequest(final PreparedRequest request, final NodeIdentifier nodeId, final URI uri, final String requestId,
                    final StandardAsyncClusterResponse response) {

                if (delayMillis > 0L) {
                    try {
                        Thread.sleep(delayMillis);
                    } catch (InterruptedException e) {
                        Assert.fail("Thread Interrupted during test");
                    }
                }

                if (failure != null) {
                    throw failure;
                }

                final Object proxiedEntities = request.getHeaders().get(ProxiedEntitiesUtils.PROXY_ENTITIES_CHAIN);

                // ensure the request chain is in the request
                Assert.assertEquals(expectedRequestChain, proxiedEntities);

                // Return given response from all nodes.
                final Response clientResponse = mock(Response.class);
                when(clientResponse.getStatus()).thenReturn(status.getStatusCode());
                return new NodeResponse(nodeId, request.getMethod(), uri, clientResponse, -1L, requestId);
            }
        };

        try {
            function.withReplicator(replicator);
        } catch (final Exception e) {
            e.printStackTrace();
            Assert.fail(e.toString());
        } finally {
            replicator.shutdown();
        }
    }

    private interface WithReplicator {

        void withReplicator(ThreadPoolRequestReplicator replicator) throws Exception;
    }
}
