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

package org.apache.nifi.controller.queue.clustered;

import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.ClusterTopologyEventListener;
import org.apache.nifi.cluster.coordination.node.NodeConnectionState;
import org.apache.nifi.cluster.coordination.node.NodeConnectionStatus;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.MockFlowFileRecord;
import org.apache.nifi.controller.MockSwapManager;
import org.apache.nifi.controller.ProcessScheduler;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.queue.LoadBalanceCompression;
import org.apache.nifi.controller.queue.LoadBalancedFlowFileQueue;
import org.apache.nifi.controller.queue.NopConnectionEventListener;
import org.apache.nifi.controller.queue.clustered.client.StandardLoadBalanceFlowFileCodec;
import org.apache.nifi.controller.queue.clustered.client.async.AsyncLoadBalanceClient;
import org.apache.nifi.controller.queue.clustered.client.async.nio.NioAsyncLoadBalanceClientFactory;
import org.apache.nifi.controller.queue.clustered.client.async.nio.NioAsyncLoadBalanceClientRegistry;
import org.apache.nifi.controller.queue.clustered.client.async.nio.NioAsyncLoadBalanceClientTask;
import org.apache.nifi.controller.queue.clustered.partition.FlowFilePartitioner;
import org.apache.nifi.controller.queue.clustered.partition.QueuePartition;
import org.apache.nifi.controller.queue.clustered.partition.RoundRobinPartitioner;
import org.apache.nifi.controller.queue.clustered.server.ConnectionLoadBalanceServer;
import org.apache.nifi.controller.queue.clustered.server.LoadBalanceAuthorizer;
import org.apache.nifi.controller.queue.clustered.server.LoadBalanceProtocol;
import org.apache.nifi.controller.queue.clustered.server.NotAuthorizedException;
import org.apache.nifi.controller.queue.clustered.server.StandardLoadBalanceProtocol;
import org.apache.nifi.controller.repository.ContentNotFoundException;
import org.apache.nifi.controller.repository.ContentRepository;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.repository.RepositoryRecord;
import org.apache.nifi.controller.repository.RepositoryRecordType;
import org.apache.nifi.controller.repository.claim.ContentClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaim;
import org.apache.nifi.controller.repository.claim.ResourceClaimManager;
import org.apache.nifi.controller.repository.claim.StandardResourceClaimManager;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.provenance.ProvenanceRepository;
import org.apache.nifi.security.util.SslContextFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.net.ssl.SSLContext;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LoadBalancedQueueIT {
    private final LoadBalanceAuthorizer ALWAYS_AUTHORIZED = (sslSocket) -> sslSocket == null ? null : "authorized.mydomain.com";
    private final LoadBalanceAuthorizer NEVER_AUTHORIZED = (sslSocket) -> {
        throw new NotAuthorizedException("Intentional Unit Test Failure - Not Authorized");
    };

    private final MockSwapManager flowFileSwapManager = new MockSwapManager();
    private final String queueId = "unit-test";
    private final EventReporter eventReporter = EventReporter.NO_OP;
    private final int swapThreshold = 10_000;

    private Set<NodeIdentifier> nodeIdentifiers;
    private ClusterCoordinator clusterCoordinator;
    private NodeIdentifier localNodeId;
    private ProcessScheduler processScheduler;
    private ResourceClaimManager resourceClaimManager;
    private LoadBalancedFlowFileQueue serverQueue;
    private FlowController flowController;

    private ProvenanceRepository clientProvRepo;
    private ContentRepository clientContentRepo;
    private List<RepositoryRecord> clientRepoRecords;
    private FlowFileRepository clientFlowFileRepo;
    private ConcurrentMap<ContentClaim, byte[]> clientClaimContents;

    private ProvenanceRepository serverProvRepo;
    private List<RepositoryRecord> serverRepoRecords;
    private FlowFileRepository serverFlowFileRepo;
    private ConcurrentMap<ContentClaim, byte[]> serverClaimContents;
    private ContentRepository serverContentRepo;

    private SSLContext sslContext;

    private final Set<ClusterTopologyEventListener> clusterEventListeners = Collections.synchronizedSet(new HashSet<>());
    private final AtomicReference<LoadBalanceCompression> compressionReference = new AtomicReference<>();

    @Before
    public void setup() throws IOException, UnrecoverableKeyException, CertificateException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
        compressionReference.set(LoadBalanceCompression.DO_NOT_COMPRESS);

        nodeIdentifiers = new HashSet<>();

        clusterCoordinator = mock(ClusterCoordinator.class);
        when(clusterCoordinator.getNodeIdentifiers()).thenAnswer(invocation -> new HashSet<>(nodeIdentifiers));
        when(clusterCoordinator.getLocalNodeIdentifier()).thenAnswer(invocation -> localNodeId);

        clusterEventListeners.clear();
        doAnswer(new Answer() {
            @Override
            public Object answer(final InvocationOnMock invocation) {
                clusterEventListeners.add(invocation.getArgumentAt(0, ClusterTopologyEventListener.class));
                return null;
            }
        }).when(clusterCoordinator).registerEventListener(any(ClusterTopologyEventListener.class));

        processScheduler = mock(ProcessScheduler.class);
        clientProvRepo = mock(ProvenanceRepository.class);
        resourceClaimManager = new StandardResourceClaimManager();
        final Connection connection = mock(Connection.class);
        when(connection.getIdentifier()).thenReturn(queueId);

        serverQueue = mock(LoadBalancedFlowFileQueue.class);
        when(serverQueue.isFull()).thenReturn(false);
        when(connection.getFlowFileQueue()).thenReturn(serverQueue);
        doAnswer(invocation -> compressionReference.get()).when(serverQueue).getLoadBalanceCompression();

        flowController = mock(FlowController.class);
        final FlowManager flowManager = Mockito.mock(FlowManager.class);
        when(flowManager.getConnection(Mockito.anyString())).thenReturn(connection);
        when(flowController.getFlowManager()).thenReturn(flowManager);

        // Create repos for the server
        serverRepoRecords = Collections.synchronizedList(new ArrayList<>());
        serverFlowFileRepo = createFlowFileRepository(serverRepoRecords);

        serverClaimContents = new ConcurrentHashMap<>();
        serverContentRepo = createContentRepository(serverClaimContents);
        serverProvRepo = mock(ProvenanceRepository.class);

        clientClaimContents = new ConcurrentHashMap<>();
        clientContentRepo = createContentRepository(clientClaimContents);
        clientRepoRecords = Collections.synchronizedList(new ArrayList<>());
        clientFlowFileRepo = createFlowFileRepository(clientRepoRecords);

        final String keystore = "src/test/resources/localhost-ks.jks";
        final String keystorePass = "OI7kMpWzzVNVx/JGhTL/0uO4+PWpGJ46uZ/pfepbkwI";
        final String keyPass = keystorePass;
        final String truststore = "src/test/resources/localhost-ts.jks";
        final String truststorePass = "wAOR0nQJ2EXvOP0JZ2EaqA/n7W69ILS4sWAHghmIWCc";
        sslContext = SslContextFactory.createSslContext(keystore, keystorePass.toCharArray(), keyPass.toCharArray(), "JKS",
                truststore, truststorePass.toCharArray(), "JKS",
                SslContextFactory.ClientAuth.REQUIRED, "TLS");
    }


    private ContentClaim createContentClaim(final byte[] bytes) {
        final ResourceClaim resourceClaim = mock(ResourceClaim.class);
        when(resourceClaim.getContainer()).thenReturn("container");
        when(resourceClaim.getSection()).thenReturn("section");
        when(resourceClaim.getId()).thenReturn("identifier");

        final ContentClaim contentClaim = mock(ContentClaim.class);
        when(contentClaim.getResourceClaim()).thenReturn(resourceClaim);

        if (bytes != null) {
            clientClaimContents.put(contentClaim, bytes);
        }

        return contentClaim;
    }


    private NioAsyncLoadBalanceClientFactory createClientFactory(final SSLContext sslContext) {
        final FlowFileContentAccess flowFileContentAccess = flowFile -> clientContentRepo.read(flowFile.getContentClaim());
        return new NioAsyncLoadBalanceClientFactory(sslContext, 30000, flowFileContentAccess, eventReporter, new StandardLoadBalanceFlowFileCodec());
    }

    @Test(timeout = 20_000)
    public void testNewNodeAdded() throws IOException, InterruptedException {
        localNodeId = new NodeIdentifier("unit-test-local", "localhost", 7090, "localhost", 7090, "localhost", 7090, null, null, null, false, null);
        nodeIdentifiers.add(localNodeId);

        // Create the server
        final int timeoutMillis = 1000;
        final LoadBalanceProtocol loadBalanceProtocol = new StandardLoadBalanceProtocol(serverFlowFileRepo, serverContentRepo, serverProvRepo, flowController, ALWAYS_AUTHORIZED);
        final SSLContext sslContext = null;

        final NioAsyncLoadBalanceClientRegistry clientRegistry = new NioAsyncLoadBalanceClientRegistry(createClientFactory(sslContext), 1);
        clientRegistry.start();

        final NodeConnectionStatus connectionStatus = mock(NodeConnectionStatus.class);
        when(connectionStatus.getState()).thenReturn(NodeConnectionState.CONNECTED);
        when(clusterCoordinator.getConnectionStatus(any(NodeIdentifier.class))).thenReturn(connectionStatus);
        final NioAsyncLoadBalanceClientTask clientTask = new NioAsyncLoadBalanceClientTask(clientRegistry, clusterCoordinator, eventReporter);
        final Thread clientThread = new Thread(clientTask);

        final SocketLoadBalancedFlowFileQueue flowFileQueue = new SocketLoadBalancedFlowFileQueue(queueId, new NopConnectionEventListener(), processScheduler, clientFlowFileRepo, clientProvRepo,
                clientContentRepo, resourceClaimManager, clusterCoordinator, clientRegistry, flowFileSwapManager, swapThreshold, eventReporter);

        flowFileQueue.setFlowFilePartitioner(new RoundRobinPartitioner());

        final int serverCount = 5;
        final ConnectionLoadBalanceServer[] servers = new ConnectionLoadBalanceServer[serverCount];

        try {
            flowFileQueue.startLoadBalancing();
            clientThread.start();

            for (int i = 0; i < serverCount; i++) {
                final ConnectionLoadBalanceServer server = new ConnectionLoadBalanceServer("localhost", 0, sslContext, 8, loadBalanceProtocol, eventReporter, timeoutMillis);
                servers[i] = server;
                server.start();

                final int loadBalancePort = server.getPort();

                // Create the Load Balanced FlowFile Queue
                final NodeIdentifier nodeId = new NodeIdentifier("unit-test-" + i, "localhost", 8090 + i, "localhost", 8090, "localhost", loadBalancePort, null, null, null, false, null);
                nodeIdentifiers.add(nodeId);


                clusterEventListeners.forEach(listener -> listener.onNodeAdded(nodeId));

                for (int j=0; j < 2; j++) {
                    final Map<String, String> attributes = new HashMap<>();
                    attributes.put("greeting", "hello");

                    final MockFlowFileRecord flowFile = new MockFlowFileRecord(attributes, 0L);
                    flowFileQueue.put(flowFile);
                }
            }

            final int totalFlowFileCount = 7;

            // Wait up to 10 seconds for the server's FlowFile Repository to be updated
            final long endTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10L);
            while (serverRepoRecords.size() < totalFlowFileCount && System.currentTimeMillis() < endTime) {
                Thread.sleep(10L);
            }

            assertFalse("Server's FlowFile Repo was never fully updated", serverRepoRecords.isEmpty());

            assertEquals(totalFlowFileCount, serverRepoRecords.size());

            for (final RepositoryRecord serverRecord : serverRepoRecords) {
                final FlowFileRecord serverFlowFile = serverRecord.getCurrent();
                assertEquals("hello", serverFlowFile.getAttribute("greeting"));
            }

            while (clientRepoRecords.size() < totalFlowFileCount) {
                Thread.sleep(10L);
            }

            assertEquals(totalFlowFileCount, clientRepoRecords.size());

            for (final RepositoryRecord clientRecord : clientRepoRecords) {
                assertEquals(RepositoryRecordType.DELETE, clientRecord.getType());
            }
        } finally {
            clientTask.stop();

            flowFileQueue.stopLoadBalancing();

            clientRegistry.getAllClients().forEach(AsyncLoadBalanceClient::stop);
            Arrays.stream(servers).filter(Objects::nonNull).forEach(ConnectionLoadBalanceServer::stop);
        }
    }

    @Test(timeout = 60_000)
    public void testFailover() throws IOException, InterruptedException {
        localNodeId = new NodeIdentifier("unit-test-local", "localhost", 7090, "localhost", 7090, "localhost", 7090, null, null, null, false, null);
        nodeIdentifiers.add(localNodeId);

        // Create the server
        final int timeoutMillis = 1000;
        final LoadBalanceProtocol loadBalanceProtocol = new StandardLoadBalanceProtocol(serverFlowFileRepo, serverContentRepo, serverProvRepo, flowController, ALWAYS_AUTHORIZED);
        final SSLContext sslContext = null;

        final ConnectionLoadBalanceServer server = new ConnectionLoadBalanceServer("localhost", 0, sslContext, 2, loadBalanceProtocol, eventReporter, timeoutMillis);
        server.start();

        try {
            final int loadBalancePort = server.getPort();

            // Create the Load Balanced FlowFile Queue
            final NodeIdentifier availableNodeId = new NodeIdentifier("unit-test", "localhost", 8090, "localhost", 8090, "localhost", loadBalancePort, null, null, null, false, null);
            nodeIdentifiers.add(availableNodeId);

            // Add a Node Identifier pointing to a non-existent server
            final NodeIdentifier inaccessibleNodeId = new NodeIdentifier("unit-test-invalid-host-does-not-exist", "invalid-host-does-not-exist", 8090, "invalid-host-does-not-exist", 8090,
                    "invalid-host-does-not-exist", loadBalancePort, null, null, null, false, null);
            nodeIdentifiers.add(inaccessibleNodeId);


            final NioAsyncLoadBalanceClientRegistry clientRegistry = new NioAsyncLoadBalanceClientRegistry(createClientFactory(sslContext), 1);
            clientRegistry.start();

            final NodeConnectionStatus connectionStatus = mock(NodeConnectionStatus.class);
            when(connectionStatus.getState()).thenReturn(NodeConnectionState.CONNECTED);
            when(clusterCoordinator.getConnectionStatus(any(NodeIdentifier.class))).thenReturn(connectionStatus);
            final NioAsyncLoadBalanceClientTask clientTask = new NioAsyncLoadBalanceClientTask(clientRegistry, clusterCoordinator, eventReporter);

            final Thread clientThread = new Thread(clientTask);
            clientThread.setDaemon(true);

            final SocketLoadBalancedFlowFileQueue flowFileQueue = new SocketLoadBalancedFlowFileQueue(queueId, new NopConnectionEventListener(), processScheduler, clientFlowFileRepo, clientProvRepo,
                    clientContentRepo, resourceClaimManager, clusterCoordinator, clientRegistry, flowFileSwapManager, swapThreshold, eventReporter);
            flowFileQueue.setFlowFilePartitioner(new RoundRobinPartitioner());

            try {
                final int numFlowFiles = 1200;
                for (int i = 0; i < numFlowFiles; i++) {
                    final ContentClaim contentClaim = createContentClaim("hello".getBytes());

                    final Map<String, String> attributes = new HashMap<>();
                    attributes.put("uuid", UUID.randomUUID().toString());
                    attributes.put("greeting", "hello");

                    final MockFlowFileRecord flowFile = new MockFlowFileRecord(attributes, 5L, contentClaim);
                    flowFileQueue.put(flowFile);
                }

                flowFileQueue.startLoadBalancing();

                clientThread.start();

                // Sending to one partition should fail. When that happens, half of the FlowFiles should go to the local partition,
                // the other half to the other node. So the total number of FlowFiles expected is ((numFlowFiles per node) / 3 * 1.5)
                final int flowFilesPerNode = numFlowFiles / 3;
                final int expectedFlowFileReceiveCount = flowFilesPerNode + flowFilesPerNode / 2;

                // Wait up to 10 seconds for the server's FlowFile Repository to be updated
                final long endTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30L);
                while (serverRepoRecords.size() < expectedFlowFileReceiveCount && System.currentTimeMillis() < endTime) {
                    Thread.sleep(10L);
                }

                assertFalse("Server's FlowFile Repo was never fully updated", serverRepoRecords.isEmpty());

                assertEquals(expectedFlowFileReceiveCount, serverRepoRecords.size());

                for (final RepositoryRecord serverRecord : serverRepoRecords) {
                    final FlowFileRecord serverFlowFile = serverRecord.getCurrent();
                    assertEquals("hello", serverFlowFile.getAttribute("greeting"));

                    final ContentClaim serverContentClaim = serverFlowFile.getContentClaim();
                    final byte[] serverFlowFileContent = serverClaimContents.get(serverContentClaim);
                    assertArrayEquals("hello".getBytes(), Arrays.copyOfRange(serverFlowFileContent, serverFlowFileContent.length - 5, serverFlowFileContent.length));
                }

                // We expect the client records to be numFlowFiles / 2 because half of the FlowFile will have gone to the other node
                // in the cluster and half would still be in the local partition.
                while (clientRepoRecords.size() < numFlowFiles / 2) {
                    Thread.sleep(10L);
                }

                assertEquals(numFlowFiles / 2, clientRepoRecords.size());

                for (final RepositoryRecord clientRecord : clientRepoRecords) {
                    assertEquals(RepositoryRecordType.DELETE, clientRecord.getType());
                }
            } finally {
                flowFileQueue.stopLoadBalancing();
                clientRegistry.getAllClients().forEach(AsyncLoadBalanceClient::stop);
            }
        } finally {
            server.stop();
        }
    }


    @Test(timeout = 20_000)
    public void testTransferToRemoteNode() throws IOException, InterruptedException {
        localNodeId = new NodeIdentifier("unit-test-local", "localhost", 7090, "localhost", 7090, "localhost", 7090, null, null, null, false, null);
        nodeIdentifiers.add(localNodeId);

        // Create the server
        final int timeoutMillis = 30000;
        final LoadBalanceProtocol loadBalanceProtocol = new StandardLoadBalanceProtocol(serverFlowFileRepo, serverContentRepo, serverProvRepo, flowController, ALWAYS_AUTHORIZED);
        final SSLContext sslContext = null;

        final ConnectionLoadBalanceServer server = new ConnectionLoadBalanceServer("localhost", 0, sslContext, 2, loadBalanceProtocol, eventReporter, timeoutMillis);
        server.start();

        try {
            final int loadBalancePort = server.getPort();

            // Create the Load Balanced FlowFile Queue
            final NodeIdentifier remoteNodeId = new NodeIdentifier("unit-test", "localhost", 8090, "localhost", 8090, "localhost", loadBalancePort, null, null, null, false, null);
            nodeIdentifiers.add(remoteNodeId);

            final NioAsyncLoadBalanceClientRegistry clientRegistry = new NioAsyncLoadBalanceClientRegistry(createClientFactory(sslContext), 1);
            clientRegistry.start();

            final NodeConnectionStatus connectionStatus = mock(NodeConnectionStatus.class);
            when(connectionStatus.getState()).thenReturn(NodeConnectionState.CONNECTED);
            when(clusterCoordinator.getConnectionStatus(any(NodeIdentifier.class))).thenReturn(connectionStatus);
            final NioAsyncLoadBalanceClientTask clientTask = new NioAsyncLoadBalanceClientTask(clientRegistry, clusterCoordinator, eventReporter);

            final Thread clientThread = new Thread(clientTask);
            clientThread.setDaemon(true);
            clientThread.start();

            final SocketLoadBalancedFlowFileQueue flowFileQueue = new SocketLoadBalancedFlowFileQueue(queueId, new NopConnectionEventListener(), processScheduler, clientFlowFileRepo, clientProvRepo,
                    clientContentRepo, resourceClaimManager, clusterCoordinator, clientRegistry, flowFileSwapManager, swapThreshold, eventReporter);
            flowFileQueue.setFlowFilePartitioner(new RoundRobinPartitioner());

            try {
                final MockFlowFileRecord firstFlowFile = new MockFlowFileRecord(0L);
                flowFileQueue.put(firstFlowFile);

                final Map<String, String> attributes = new HashMap<>();
                attributes.put("integration", "test");
                attributes.put("unit-test", "false");
                attributes.put("integration-test", "true");

                final ContentClaim contentClaim = createContentClaim("hello".getBytes());
                final MockFlowFileRecord secondFlowFile = new MockFlowFileRecord(attributes, 5L, contentClaim);
                flowFileQueue.put(secondFlowFile);

                flowFileQueue.startLoadBalancing();

                // Wait up to 10 seconds for the server's FlowFile Repository to be updated
                final long endTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10L);
                while (serverRepoRecords.isEmpty() && System.currentTimeMillis() < endTime) {
                    Thread.sleep(10L);
                }

                assertFalse("Server's FlowFile Repo was never updated", serverRepoRecords.isEmpty());

                assertEquals(1, serverRepoRecords.size());

                final RepositoryRecord serverRecord = serverRepoRecords.iterator().next();
                final FlowFileRecord serverFlowFile = serverRecord.getCurrent();
                assertEquals("test", serverFlowFile.getAttribute("integration"));
                assertEquals("false", serverFlowFile.getAttribute("unit-test"));
                assertEquals("true", serverFlowFile.getAttribute("integration-test"));

                final ContentClaim serverContentClaim = serverFlowFile.getContentClaim();
                final byte[] serverFlowFileContent = serverClaimContents.get(serverContentClaim);
                assertArrayEquals("hello".getBytes(), serverFlowFileContent);

                while (clientRepoRecords.size() == 0) {
                    Thread.sleep(10L);
                }

                assertEquals(1, clientRepoRecords.size());
                final RepositoryRecord clientRecord = clientRepoRecords.iterator().next();
                assertEquals(RepositoryRecordType.DELETE, clientRecord.getType());
            } finally {
                flowFileQueue.stopLoadBalancing();
                clientRegistry.getAllClients().forEach(AsyncLoadBalanceClient::stop);
            }
        } finally {
            server.stop();
        }
    }


    @Test(timeout = 20_000)
    public void testContentNotFound() throws IOException, InterruptedException {
        localNodeId = new NodeIdentifier("unit-test-local", "localhost", 7090, "localhost", 7090, "localhost", 7090, null, null, null, false, null);
        nodeIdentifiers.add(localNodeId);

        // Create the server
        final int timeoutMillis = 30000;
        final LoadBalanceProtocol loadBalanceProtocol = new StandardLoadBalanceProtocol(serverFlowFileRepo, serverContentRepo, serverProvRepo, flowController, ALWAYS_AUTHORIZED);
        final SSLContext sslContext = null;

        final ConnectionLoadBalanceServer server = new ConnectionLoadBalanceServer("localhost", 0, sslContext, 2, loadBalanceProtocol, eventReporter, timeoutMillis);
        server.start();

        try {
            final int loadBalancePort = server.getPort();

            // Create the Load Balanced FlowFile Queue
            final NodeIdentifier remoteNodeId = new NodeIdentifier("unit-test", "localhost", 8090, "localhost", 8090, "localhost", loadBalancePort, null, null, null, false, null);
            nodeIdentifiers.add(remoteNodeId);

            final NioAsyncLoadBalanceClientRegistry clientRegistry = new NioAsyncLoadBalanceClientRegistry(createClientFactory(sslContext), 1);
            clientRegistry.start();

            final NodeConnectionStatus connectionStatus = mock(NodeConnectionStatus.class);
            when(connectionStatus.getState()).thenReturn(NodeConnectionState.CONNECTED);
            when(clusterCoordinator.getConnectionStatus(any(NodeIdentifier.class))).thenReturn(connectionStatus);
            final NioAsyncLoadBalanceClientTask clientTask = new NioAsyncLoadBalanceClientTask(clientRegistry, clusterCoordinator, eventReporter);

            final Thread clientThread = new Thread(clientTask);
            clientThread.setDaemon(true);
            clientThread.start();

            final SocketLoadBalancedFlowFileQueue flowFileQueue = new SocketLoadBalancedFlowFileQueue(queueId, new NopConnectionEventListener(), processScheduler, clientFlowFileRepo, clientProvRepo,
                clientContentRepo, resourceClaimManager, clusterCoordinator, clientRegistry, flowFileSwapManager, swapThreshold, eventReporter);
            flowFileQueue.setFlowFilePartitioner(new RoundRobinPartitioner());

            try {
                final MockFlowFileRecord firstFlowFile = new MockFlowFileRecord(0L);
                flowFileQueue.put(firstFlowFile);

                final Map<String, String> attributes = new HashMap<>();
                attributes.put("integration", "test");
                attributes.put("unit-test", "false");
                attributes.put("integration-test", "true");

                final ContentClaim contentClaim = createContentClaim("hello".getBytes());
                this.clientClaimContents.remove(contentClaim); // cause ContentNotFoundException

                final MockFlowFileRecord secondFlowFile = new MockFlowFileRecord(attributes, 5L, contentClaim);
                flowFileQueue.put(secondFlowFile);

                flowFileQueue.startLoadBalancing();

                while (clientRepoRecords.size() == 0) {
                    Thread.sleep(10L);
                }

                assertEquals(1, clientRepoRecords.size());
                final RepositoryRecord clientRecord = clientRepoRecords.iterator().next();
                assertEquals(RepositoryRecordType.CONTENTMISSING, clientRecord.getType());
            } finally {
                flowFileQueue.stopLoadBalancing();
                clientRegistry.getAllClients().forEach(AsyncLoadBalanceClient::stop);
            }
        } finally {
            server.stop();
        }
    }


    @Test(timeout = 20_000)
    public void testTransferToRemoteNodeAttributeCompression() throws IOException, InterruptedException {
        localNodeId = new NodeIdentifier("unit-test-local", "localhost", 7090, "localhost", 7090, "localhost", 7090, null, null, null, false, null);
        nodeIdentifiers.add(localNodeId);
        compressionReference.set(LoadBalanceCompression.COMPRESS_ATTRIBUTES_ONLY);

        // Create the server
        final int timeoutMillis = 30000;
        final LoadBalanceProtocol loadBalanceProtocol = new StandardLoadBalanceProtocol(serverFlowFileRepo, serverContentRepo, serverProvRepo, flowController, ALWAYS_AUTHORIZED);
        final SSLContext sslContext = null;

        final ConnectionLoadBalanceServer server = new ConnectionLoadBalanceServer("localhost", 0, sslContext, 2, loadBalanceProtocol, eventReporter, timeoutMillis);
        server.start();

        try {
            final int loadBalancePort = server.getPort();

            // Create the Load Balanced FlowFile Queue
            final NodeIdentifier remoteNodeId = new NodeIdentifier("unit-test", "localhost", 8090, "localhost", 8090, "localhost", loadBalancePort, null, null, null, false, null);
            nodeIdentifiers.add(remoteNodeId);

            final NioAsyncLoadBalanceClientRegistry clientRegistry = new NioAsyncLoadBalanceClientRegistry(createClientFactory(sslContext), 1);
            clientRegistry.start();

            final NodeConnectionStatus connectionStatus = mock(NodeConnectionStatus.class);
            when(connectionStatus.getState()).thenReturn(NodeConnectionState.CONNECTED);
            when(clusterCoordinator.getConnectionStatus(any(NodeIdentifier.class))).thenReturn(connectionStatus);
            final NioAsyncLoadBalanceClientTask clientTask = new NioAsyncLoadBalanceClientTask(clientRegistry, clusterCoordinator, eventReporter);

            final Thread clientThread = new Thread(clientTask);
            clientThread.setDaemon(true);
            clientThread.start();

            final SocketLoadBalancedFlowFileQueue flowFileQueue = new SocketLoadBalancedFlowFileQueue(queueId, new NopConnectionEventListener(), processScheduler, clientFlowFileRepo, clientProvRepo,
                clientContentRepo, resourceClaimManager, clusterCoordinator, clientRegistry, flowFileSwapManager, swapThreshold, eventReporter);
            flowFileQueue.setFlowFilePartitioner(new RoundRobinPartitioner());
            flowFileQueue.setLoadBalanceCompression(LoadBalanceCompression.COMPRESS_ATTRIBUTES_ONLY);

            try {
                final MockFlowFileRecord firstFlowFile = new MockFlowFileRecord(0L);
                flowFileQueue.put(firstFlowFile);

                final Map<String, String> attributes = new HashMap<>();
                attributes.put("integration", "test");
                attributes.put("unit-test", "false");
                attributes.put("integration-test", "true");

                final ContentClaim contentClaim = createContentClaim("hello".getBytes());
                final MockFlowFileRecord secondFlowFile = new MockFlowFileRecord(attributes, 5L, contentClaim);
                flowFileQueue.put(secondFlowFile);

                flowFileQueue.startLoadBalancing();

                // Wait up to 10 seconds for the server's FlowFile Repository to be updated
                final long endTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10L);
                while (serverRepoRecords.isEmpty() && System.currentTimeMillis() < endTime) {
                    Thread.sleep(10L);
                }

                assertFalse("Server's FlowFile Repo was never updated", serverRepoRecords.isEmpty());

                assertEquals(1, serverRepoRecords.size());

                final RepositoryRecord serverRecord = serverRepoRecords.iterator().next();
                final FlowFileRecord serverFlowFile = serverRecord.getCurrent();
                assertEquals("test", serverFlowFile.getAttribute("integration"));
                assertEquals("false", serverFlowFile.getAttribute("unit-test"));
                assertEquals("true", serverFlowFile.getAttribute("integration-test"));

                final ContentClaim serverContentClaim = serverFlowFile.getContentClaim();
                final byte[] serverFlowFileContent = serverClaimContents.get(serverContentClaim);
                assertArrayEquals("hello".getBytes(), serverFlowFileContent);

                while (clientRepoRecords.size() == 0) {
                    Thread.sleep(10L);
                }

                assertEquals(1, clientRepoRecords.size());
                final RepositoryRecord clientRecord = clientRepoRecords.iterator().next();
                assertEquals(RepositoryRecordType.DELETE, clientRecord.getType());
            } finally {
                flowFileQueue.stopLoadBalancing();
                clientRegistry.getAllClients().forEach(AsyncLoadBalanceClient::stop);
            }
        } finally {
            server.stop();
        }
    }


    @Test(timeout = 20_000)
    public void testTransferToRemoteNodeContentCompression() throws IOException, InterruptedException {
        localNodeId = new NodeIdentifier("unit-test-local", "localhost", 7090, "localhost", 7090, "localhost", 7090, null, null, null, false, null);
        nodeIdentifiers.add(localNodeId);
        compressionReference.set(LoadBalanceCompression.COMPRESS_ATTRIBUTES_AND_CONTENT);

        // Create the server
        final int timeoutMillis = 30000;
        final LoadBalanceProtocol loadBalanceProtocol = new StandardLoadBalanceProtocol(serverFlowFileRepo, serverContentRepo, serverProvRepo, flowController, ALWAYS_AUTHORIZED);
        final SSLContext sslContext = null;

        final ConnectionLoadBalanceServer server = new ConnectionLoadBalanceServer("localhost", 0, sslContext, 2, loadBalanceProtocol, eventReporter, timeoutMillis);
        server.start();

        try {
            final int loadBalancePort = server.getPort();

            // Create the Load Balanced FlowFile Queue
            final NodeIdentifier remoteNodeId = new NodeIdentifier("unit-test", "localhost", 8090, "localhost", 8090, "localhost", loadBalancePort, null, null, null, false, null);
            nodeIdentifiers.add(remoteNodeId);

            final NioAsyncLoadBalanceClientRegistry clientRegistry = new NioAsyncLoadBalanceClientRegistry(createClientFactory(sslContext), 1);
            clientRegistry.start();

            final NodeConnectionStatus connectionStatus = mock(NodeConnectionStatus.class);
            when(connectionStatus.getState()).thenReturn(NodeConnectionState.CONNECTED);
            when(clusterCoordinator.getConnectionStatus(any(NodeIdentifier.class))).thenReturn(connectionStatus);
            final NioAsyncLoadBalanceClientTask clientTask = new NioAsyncLoadBalanceClientTask(clientRegistry, clusterCoordinator, eventReporter);

            final Thread clientThread = new Thread(clientTask);
            clientThread.setDaemon(true);
            clientThread.start();

            final SocketLoadBalancedFlowFileQueue flowFileQueue = new SocketLoadBalancedFlowFileQueue(queueId, new NopConnectionEventListener(), processScheduler, clientFlowFileRepo, clientProvRepo,
                clientContentRepo, resourceClaimManager, clusterCoordinator, clientRegistry, flowFileSwapManager, swapThreshold, eventReporter);
            flowFileQueue.setFlowFilePartitioner(new RoundRobinPartitioner());
            flowFileQueue.setLoadBalanceCompression(LoadBalanceCompression.COMPRESS_ATTRIBUTES_AND_CONTENT);

            try {
                final MockFlowFileRecord firstFlowFile = new MockFlowFileRecord(0L);
                flowFileQueue.put(firstFlowFile);

                final Map<String, String> attributes = new HashMap<>();
                attributes.put("integration", "test");
                attributes.put("unit-test", "false");
                attributes.put("integration-test", "true");

                final ContentClaim contentClaim = createContentClaim("hello".getBytes());
                final MockFlowFileRecord secondFlowFile = new MockFlowFileRecord(attributes, 5L, contentClaim);
                flowFileQueue.put(secondFlowFile);

                flowFileQueue.startLoadBalancing();

                // Wait up to 10 seconds for the server's FlowFile Repository to be updated
                final long endTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10L);
                while (serverRepoRecords.isEmpty() && System.currentTimeMillis() < endTime) {
                    Thread.sleep(10L);
                }

                assertFalse("Server's FlowFile Repo was never updated", serverRepoRecords.isEmpty());

                assertEquals(1, serverRepoRecords.size());

                final RepositoryRecord serverRecord = serverRepoRecords.iterator().next();
                final FlowFileRecord serverFlowFile = serverRecord.getCurrent();
                assertEquals("test", serverFlowFile.getAttribute("integration"));
                assertEquals("false", serverFlowFile.getAttribute("unit-test"));
                assertEquals("true", serverFlowFile.getAttribute("integration-test"));

                final ContentClaim serverContentClaim = serverFlowFile.getContentClaim();
                final byte[] serverFlowFileContent = serverClaimContents.get(serverContentClaim);
                assertArrayEquals("hello".getBytes(), serverFlowFileContent);

                while (clientRepoRecords.size() == 0) {
                    Thread.sleep(10L);
                }

                assertEquals(1, clientRepoRecords.size());
                final RepositoryRecord clientRecord = clientRepoRecords.iterator().next();
                assertEquals(RepositoryRecordType.DELETE, clientRecord.getType());
            } finally {
                flowFileQueue.stopLoadBalancing();
                clientRegistry.getAllClients().forEach(AsyncLoadBalanceClient::stop);
            }
        } finally {
            server.stop();
        }
    }

    @Test(timeout = 20_000)
    public void testWithSSLContext() throws IOException, InterruptedException, UnrecoverableKeyException, CertificateException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
        localNodeId = new NodeIdentifier("unit-test-local", "localhost", 7090, "localhost", 7090, "localhost", 7090, null, null, null, false, null);
        nodeIdentifiers.add(localNodeId);

        // Create the server
        final int timeoutMillis = 30000;
        final LoadBalanceProtocol loadBalanceProtocol = new StandardLoadBalanceProtocol(serverFlowFileRepo, serverContentRepo, serverProvRepo, flowController, ALWAYS_AUTHORIZED);


        final ConnectionLoadBalanceServer server = new ConnectionLoadBalanceServer("localhost", 0, sslContext, 2, loadBalanceProtocol, eventReporter, timeoutMillis);
        server.start();

        try {
            final int loadBalancePort = server.getPort();

            // Create the Load Balanced FlowFile Queue
            final NodeIdentifier remoteNodeId = new NodeIdentifier("unit-test", "localhost", 8090, "localhost", 8090, "localhost", loadBalancePort, null, null, null, false, null);
            nodeIdentifiers.add(remoteNodeId);

            final NioAsyncLoadBalanceClientRegistry clientRegistry = new NioAsyncLoadBalanceClientRegistry(createClientFactory(sslContext), 1);
            clientRegistry.start();

            final NodeConnectionStatus connectionStatus = mock(NodeConnectionStatus.class);
            when(connectionStatus.getState()).thenReturn(NodeConnectionState.CONNECTED);
            when(clusterCoordinator.getConnectionStatus(any(NodeIdentifier.class))).thenReturn(connectionStatus);
            final NioAsyncLoadBalanceClientTask clientTask = new NioAsyncLoadBalanceClientTask(clientRegistry, clusterCoordinator, eventReporter);

            final Thread clientThread = new Thread(clientTask);
            clientThread.setDaemon(true);
            clientThread.start();

            final SocketLoadBalancedFlowFileQueue flowFileQueue = new SocketLoadBalancedFlowFileQueue(queueId, new NopConnectionEventListener(), processScheduler, clientFlowFileRepo, clientProvRepo,
                    clientContentRepo, resourceClaimManager, clusterCoordinator, clientRegistry, flowFileSwapManager, swapThreshold, eventReporter);
            flowFileQueue.setFlowFilePartitioner(new RoundRobinPartitioner());

            try {
                final MockFlowFileRecord firstFlowFile = new MockFlowFileRecord(0L);
                flowFileQueue.put(firstFlowFile);

                final Map<String, String> attributes = new HashMap<>();
                attributes.put("integration", "test");
                attributes.put("unit-test", "false");
                attributes.put("integration-test", "true");

                final ContentClaim contentClaim = createContentClaim("hello".getBytes());
                final MockFlowFileRecord secondFlowFile = new MockFlowFileRecord(attributes, 5L, contentClaim);
                flowFileQueue.put(secondFlowFile);

                flowFileQueue.startLoadBalancing();

                // Wait up to 10 seconds for the server's FlowFile Repository to be updated
                final long endTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10L);
                while (serverRepoRecords.isEmpty() && System.currentTimeMillis() < endTime) {
                    Thread.sleep(10L);
                }

                assertFalse("Server's FlowFile Repo was never updated", serverRepoRecords.isEmpty());

                assertEquals(1, serverRepoRecords.size());

                final RepositoryRecord serverRecord = serverRepoRecords.iterator().next();
                final FlowFileRecord serverFlowFile = serverRecord.getCurrent();
                assertEquals("test", serverFlowFile.getAttribute("integration"));
                assertEquals("false", serverFlowFile.getAttribute("unit-test"));
                assertEquals("true", serverFlowFile.getAttribute("integration-test"));

                final ContentClaim serverContentClaim = serverFlowFile.getContentClaim();
                final byte[] serverFlowFileContent = serverClaimContents.get(serverContentClaim);
                assertArrayEquals("hello".getBytes(), serverFlowFileContent);

                while (clientRepoRecords.size() == 0) {
                    Thread.sleep(10L);
                }

                assertEquals(1, clientRepoRecords.size());
                final RepositoryRecord clientRecord = clientRepoRecords.iterator().next();
                assertEquals(RepositoryRecordType.DELETE, clientRecord.getType());
            } finally {
                flowFileQueue.stopLoadBalancing();
                clientRegistry.getAllClients().forEach(AsyncLoadBalanceClient::stop);
            }
        } finally {
            server.stop();
        }
    }


    @Test(timeout = 60_000)
    public void testReusingClient() throws IOException, InterruptedException, UnrecoverableKeyException, CertificateException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
        localNodeId = new NodeIdentifier("unit-test-local", "localhost", 7090, "localhost", 7090, "localhost", 7090, null, null, null, false, null);
        nodeIdentifiers.add(localNodeId);

        // Create the server
        final int timeoutMillis = 30000;
        final LoadBalanceProtocol loadBalanceProtocol = new StandardLoadBalanceProtocol(serverFlowFileRepo, serverContentRepo, serverProvRepo, flowController, ALWAYS_AUTHORIZED);

        final ConnectionLoadBalanceServer server = new ConnectionLoadBalanceServer("localhost", 0, sslContext, 2, loadBalanceProtocol, eventReporter, timeoutMillis);
        server.start();

        try {
            final int loadBalancePort = server.getPort();

            // Create the Load Balanced FlowFile Queue
            final NodeIdentifier remoteNodeId = new NodeIdentifier("unit-test", "localhost", 8090, "localhost", 8090, "localhost", loadBalancePort, null, null, null, false, null);
            nodeIdentifiers.add(remoteNodeId);

            final NioAsyncLoadBalanceClientRegistry clientRegistry = new NioAsyncLoadBalanceClientRegistry(createClientFactory(sslContext), 1);
            clientRegistry.start();

            final NodeConnectionStatus connectionStatus = mock(NodeConnectionStatus.class);
            when(connectionStatus.getState()).thenReturn(NodeConnectionState.CONNECTED);
            when(clusterCoordinator.getConnectionStatus(any(NodeIdentifier.class))).thenReturn(connectionStatus);
            final NioAsyncLoadBalanceClientTask clientTask = new NioAsyncLoadBalanceClientTask(clientRegistry, clusterCoordinator, eventReporter);

            final Thread clientThread = new Thread(clientTask);
            clientThread.setDaemon(true);
            clientThread.start();

            final SocketLoadBalancedFlowFileQueue flowFileQueue = new SocketLoadBalancedFlowFileQueue(queueId, new NopConnectionEventListener(), processScheduler, clientFlowFileRepo, clientProvRepo,
                    clientContentRepo, resourceClaimManager, clusterCoordinator, clientRegistry, flowFileSwapManager, swapThreshold, eventReporter);
            flowFileQueue.setFlowFilePartitioner(new RoundRobinPartitioner());

            try {
                for (int i = 1; i <= 10; i++) {
                    final MockFlowFileRecord firstFlowFile = new MockFlowFileRecord(0L);
                    flowFileQueue.put(firstFlowFile);

                    final Map<String, String> attributes = new HashMap<>();
                    attributes.put("integration", "test");
                    attributes.put("unit-test", "false");
                    attributes.put("integration-test", "true");

                    final ContentClaim contentClaim = createContentClaim("hello".getBytes());
                    final MockFlowFileRecord secondFlowFile = new MockFlowFileRecord(attributes, 5L, contentClaim);
                    flowFileQueue.put(secondFlowFile);

                    flowFileQueue.startLoadBalancing();

                    // Wait up to 10 seconds for the server's FlowFile Repository to be updated
                    final long endTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10L);
                    while (serverRepoRecords.size() < i && System.currentTimeMillis() < endTime) {
                        Thread.sleep(10L);
                    }

                    assertEquals(i, serverRepoRecords.size());

                    final RepositoryRecord serverRecord = serverRepoRecords.iterator().next();
                    final FlowFileRecord serverFlowFile = serverRecord.getCurrent();
                    assertEquals("test", serverFlowFile.getAttribute("integration"));
                    assertEquals("false", serverFlowFile.getAttribute("unit-test"));
                    assertEquals("true", serverFlowFile.getAttribute("integration-test"));

                    final ContentClaim serverContentClaim = serverFlowFile.getContentClaim();
                    final byte[] serverFlowFileContent = serverClaimContents.get(serverContentClaim);
                    assertArrayEquals("hello".getBytes(), serverFlowFileContent);

                    while (clientRepoRecords.size() < i) {
                        Thread.sleep(10L);
                    }

                    assertEquals(i, clientRepoRecords.size());
                    final RepositoryRecord clientRecord = clientRepoRecords.iterator().next();
                    assertEquals(RepositoryRecordType.DELETE, clientRecord.getType());
                }
            } finally {
                flowFileQueue.stopLoadBalancing();
                clientRegistry.getAllClients().forEach(AsyncLoadBalanceClient::stop);
            }
        } finally {
            server.stop();
        }
    }


    @Test(timeout = 20_000)
    public void testLargePayload() throws IOException, InterruptedException, UnrecoverableKeyException, CertificateException, NoSuchAlgorithmException, KeyStoreException, KeyManagementException {
        localNodeId = new NodeIdentifier("unit-test-local", "localhost", 7090, "localhost", 7090, "localhost", 7090, null, null, null, false, null);
        nodeIdentifiers.add(localNodeId);

        // Create the server
        final int timeoutMillis = 30000;
        final LoadBalanceProtocol loadBalanceProtocol = new StandardLoadBalanceProtocol(serverFlowFileRepo, serverContentRepo, serverProvRepo, flowController, ALWAYS_AUTHORIZED);

        final ConnectionLoadBalanceServer server = new ConnectionLoadBalanceServer("localhost", 0, sslContext, 2, loadBalanceProtocol, eventReporter, timeoutMillis);
        server.start();

        try {
            final int loadBalancePort = server.getPort();

            // Create the Load Balanced FlowFile Queue
            final NodeIdentifier remoteNodeId = new NodeIdentifier("unit-test", "localhost", 8090, "localhost", 8090, "localhost", loadBalancePort, null, null, null, false, null);
            nodeIdentifiers.add(remoteNodeId);

            final NioAsyncLoadBalanceClientRegistry clientRegistry = new NioAsyncLoadBalanceClientRegistry(createClientFactory(sslContext), 1);
            clientRegistry.start();

            final NodeConnectionStatus connectionStatus = mock(NodeConnectionStatus.class);
            when(connectionStatus.getState()).thenReturn(NodeConnectionState.CONNECTED);
            when(clusterCoordinator.getConnectionStatus(any(NodeIdentifier.class))).thenReturn(connectionStatus);
            final NioAsyncLoadBalanceClientTask clientTask = new NioAsyncLoadBalanceClientTask(clientRegistry, clusterCoordinator, eventReporter);

            final Thread clientThread = new Thread(clientTask);
            clientThread.setDaemon(true);
            clientThread.start();

            final SocketLoadBalancedFlowFileQueue flowFileQueue = new SocketLoadBalancedFlowFileQueue(queueId, new NopConnectionEventListener(), processScheduler, clientFlowFileRepo, clientProvRepo,
                    clientContentRepo, resourceClaimManager, clusterCoordinator, clientRegistry, flowFileSwapManager, swapThreshold, eventReporter);
            flowFileQueue.setFlowFilePartitioner(new RoundRobinPartitioner());

            final byte[] payload = new byte[1024 * 1024];
            Arrays.fill(payload, (byte) 'A');

            try {
                final MockFlowFileRecord firstFlowFile = new MockFlowFileRecord(0L);
                flowFileQueue.put(firstFlowFile);

                final Map<String, String> attributes = new HashMap<>();
                attributes.put("integration", "test");
                attributes.put("unit-test", "false");
                attributes.put("integration-test", "true");

                final ContentClaim contentClaim = createContentClaim(payload);
                final MockFlowFileRecord secondFlowFile = new MockFlowFileRecord(attributes, payload.length, contentClaim);
                flowFileQueue.put(secondFlowFile);

                flowFileQueue.startLoadBalancing();

                // Wait up to 10 seconds for the server's FlowFile Repository to be updated
                final long endTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10L);
                while (serverRepoRecords.isEmpty() && System.currentTimeMillis() < endTime) {
                    Thread.sleep(10L);
                }

                assertFalse("Server's FlowFile Repo was never updated", serverRepoRecords.isEmpty());

                assertEquals(1, serverRepoRecords.size());

                final RepositoryRecord serverRecord = serverRepoRecords.iterator().next();
                final FlowFileRecord serverFlowFile = serverRecord.getCurrent();
                assertEquals("test", serverFlowFile.getAttribute("integration"));
                assertEquals("false", serverFlowFile.getAttribute("unit-test"));
                assertEquals("true", serverFlowFile.getAttribute("integration-test"));

                final ContentClaim serverContentClaim = serverFlowFile.getContentClaim();
                final byte[] serverFlowFileContent = serverClaimContents.get(serverContentClaim);
                assertArrayEquals(payload, serverFlowFileContent);

                while (clientRepoRecords.size() == 0) {
                    Thread.sleep(10L);
                }

                assertEquals(1, clientRepoRecords.size());
                final RepositoryRecord clientRecord = clientRepoRecords.iterator().next();
                assertEquals(RepositoryRecordType.DELETE, clientRecord.getType());
            } finally {
                flowFileQueue.stopLoadBalancing();
                clientRegistry.getAllClients().forEach(AsyncLoadBalanceClient::stop);
            }
        } finally {
            server.stop();
        }
    }


    @Test(timeout = 60_000)
    public void testServerClosesUnexpectedly() throws IOException, InterruptedException {

        doAnswer(new Answer<OutputStream>() {
            int iterations = 0;

            @Override
            public OutputStream answer(final InvocationOnMock invocation) {
                if (iterations++ < 5) {
                    return new OutputStream() {
                        @Override
                        public void write(final int b) throws IOException {
                            throw new IOException("Intentional unit test failure");
                        }
                    };
                }

                final ContentClaim contentClaim = invocation.getArgumentAt(0, ContentClaim.class);
                final ByteArrayOutputStream baos = new ByteArrayOutputStream() {
                    @Override
                    public void close() throws IOException {
                        super.close();
                        serverClaimContents.put(contentClaim, toByteArray());
                    }
                };

                return baos;
            }
        }).when(serverContentRepo).write(any(ContentClaim.class));

        // Create the server
        final int timeoutMillis = 30000;
        final LoadBalanceProtocol loadBalanceProtocol = new StandardLoadBalanceProtocol(serverFlowFileRepo, serverContentRepo, serverProvRepo, flowController, ALWAYS_AUTHORIZED);
        final SSLContext sslContext = null;

        final ConnectionLoadBalanceServer server = new ConnectionLoadBalanceServer("localhost", 0, sslContext, 2, loadBalanceProtocol, eventReporter, timeoutMillis);
        server.start();

        try {
            final int loadBalancePort = server.getPort();

            // Create the Load Balanced FlowFile Queue
            final NodeIdentifier remoteNodeId = new NodeIdentifier("unit-test", "localhost", 8090, "localhost", 8090, "localhost", loadBalancePort, null, null, null, false, null);
            nodeIdentifiers.add(remoteNodeId);

            final NioAsyncLoadBalanceClientRegistry clientRegistry = new NioAsyncLoadBalanceClientRegistry(createClientFactory(sslContext), 1);
            clientRegistry.start();

            final NodeConnectionStatus connectionStatus = mock(NodeConnectionStatus.class);
            when(connectionStatus.getState()).thenReturn(NodeConnectionState.CONNECTED);
            when(clusterCoordinator.getConnectionStatus(any(NodeIdentifier.class))).thenReturn(connectionStatus);
            final NioAsyncLoadBalanceClientTask clientTask = new NioAsyncLoadBalanceClientTask(clientRegistry, clusterCoordinator, eventReporter);

            final Thread clientThread = new Thread(clientTask);
            clientThread.setDaemon(true);
            clientThread.start();

            final SocketLoadBalancedFlowFileQueue flowFileQueue = new SocketLoadBalancedFlowFileQueue(queueId, new NopConnectionEventListener(), processScheduler, clientFlowFileRepo, clientProvRepo,
                    clientContentRepo, resourceClaimManager, clusterCoordinator, clientRegistry, flowFileSwapManager, swapThreshold, eventReporter);
            flowFileQueue.setFlowFilePartitioner(new FlowFilePartitioner() {
                @Override
                public QueuePartition getPartition(final FlowFileRecord flowFile, final QueuePartition[] partitions, final QueuePartition localPartition) {
                    for (final QueuePartition partition : partitions) {
                        if (partition != localPartition) {
                            return partition;
                        }
                    }

                    return null;
                }

                @Override
                public boolean isRebalanceOnClusterResize() {
                    return true;
                }
                @Override
                public boolean isRebalanceOnFailure() {
                    return true;
                }
            });

            try {
                final Map<String, String> attributes = new HashMap<>();
                attributes.put("integration", "test");
                attributes.put("unit-test", "false");
                attributes.put("integration-test", "true");

                final ContentClaim contentClaim = createContentClaim("hello".getBytes());
                final MockFlowFileRecord secondFlowFile = new MockFlowFileRecord(attributes, 5L, contentClaim);
                flowFileQueue.put(secondFlowFile);

                flowFileQueue.startLoadBalancing();

                // Wait up to 10 seconds for the server's FlowFile Repository to be updated
                final long endTime = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(10L);
                while (serverRepoRecords.isEmpty() && System.currentTimeMillis() < endTime) {
                    Thread.sleep(10L);
                }

                assertFalse("Server's FlowFile Repo was never updated", serverRepoRecords.isEmpty());

                assertEquals(1, serverRepoRecords.size());

                final RepositoryRecord serverRecord = serverRepoRecords.iterator().next();
                final FlowFileRecord serverFlowFile = serverRecord.getCurrent();
                assertEquals("test", serverFlowFile.getAttribute("integration"));
                assertEquals("false", serverFlowFile.getAttribute("unit-test"));
                assertEquals("true", serverFlowFile.getAttribute("integration-test"));

                final ContentClaim serverContentClaim = serverFlowFile.getContentClaim();
                final byte[] serverFlowFileContent = serverClaimContents.get(serverContentClaim);
                assertArrayEquals("hello".getBytes(), serverFlowFileContent);

                while (clientRepoRecords.size() == 0) {
                    Thread.sleep(10L);
                }

                assertEquals(1, clientRepoRecords.size());
                final RepositoryRecord clientRecord = clientRepoRecords.iterator().next();
                assertEquals(RepositoryRecordType.DELETE, clientRecord.getType());
            } finally {
                flowFileQueue.stopLoadBalancing();
                clientRegistry.getAllClients().forEach(AsyncLoadBalanceClient::stop);
            }
        } finally {
            server.stop();
        }
    }


    @Test(timeout = 20_000)
    public void testNotAuthorized() throws IOException, InterruptedException {
        localNodeId = new NodeIdentifier("unit-test-local", "localhost", 7090, "localhost", 7090, "localhost", 7090, null, null, null, false, null);
        nodeIdentifiers.add(localNodeId);

        // Create the server
        final int timeoutMillis = 30000;
        final LoadBalanceProtocol loadBalanceProtocol = new StandardLoadBalanceProtocol(serverFlowFileRepo, serverContentRepo, serverProvRepo, flowController, NEVER_AUTHORIZED);

        final ConnectionLoadBalanceServer server = new ConnectionLoadBalanceServer("localhost", 0, sslContext, 2, loadBalanceProtocol, eventReporter, timeoutMillis);
        server.start();

        try {
            final int loadBalancePort = server.getPort();

            // Create the Load Balanced FlowFile Queue
            final NodeIdentifier remoteNodeId = new NodeIdentifier("unit-test", "localhost", 8090, "localhost", 8090, "localhost", loadBalancePort, null, null, null, false, null);
            nodeIdentifiers.add(remoteNodeId);

            final NioAsyncLoadBalanceClientRegistry clientRegistry = new NioAsyncLoadBalanceClientRegistry(createClientFactory(sslContext), 1);
            clientRegistry.start();

            final NodeConnectionStatus connectionStatus = mock(NodeConnectionStatus.class);
            when(connectionStatus.getState()).thenReturn(NodeConnectionState.CONNECTED);
            when(clusterCoordinator.getConnectionStatus(any(NodeIdentifier.class))).thenReturn(connectionStatus);
            final NioAsyncLoadBalanceClientTask clientTask = new NioAsyncLoadBalanceClientTask(clientRegistry, clusterCoordinator, eventReporter);

            final Thread clientThread = new Thread(clientTask);
            clientThread.setDaemon(true);
            clientThread.start();

            final SocketLoadBalancedFlowFileQueue flowFileQueue = new SocketLoadBalancedFlowFileQueue(queueId, new NopConnectionEventListener(), processScheduler, clientFlowFileRepo, clientProvRepo,
                    clientContentRepo, resourceClaimManager, clusterCoordinator, clientRegistry, flowFileSwapManager, swapThreshold, eventReporter);
            flowFileQueue.setFlowFilePartitioner(new RoundRobinPartitioner());

            try {
                final MockFlowFileRecord firstFlowFile = new MockFlowFileRecord(0L);
                flowFileQueue.put(firstFlowFile);

                final Map<String, String> attributes = new HashMap<>();
                attributes.put("integration", "test");
                attributes.put("unit-test", "false");
                attributes.put("integration-test", "true");

                final ContentClaim contentClaim = createContentClaim("hello".getBytes());
                final MockFlowFileRecord secondFlowFile = new MockFlowFileRecord(attributes, 5L, contentClaim);
                flowFileQueue.put(secondFlowFile);

                flowFileQueue.startLoadBalancing();

                Thread.sleep(5000L);

                assertTrue("Server's FlowFile Repo was updated", serverRepoRecords.isEmpty());
                assertTrue(clientRepoRecords.isEmpty());

                assertEquals(2, flowFileQueue.size().getObjectCount());
            } finally {
                flowFileQueue.stopLoadBalancing();
                clientRegistry.getAllClients().forEach(AsyncLoadBalanceClient::stop);
            }
        } finally {
            server.stop();
        }
    }


    @Test(timeout = 35_000)
    public void testDestinationNodeQueueFull() throws IOException, InterruptedException {
        localNodeId = new NodeIdentifier("unit-test-local", "localhost", 7090, "localhost", 7090, "localhost", 7090, null, null, null, false, null);
        nodeIdentifiers.add(localNodeId);

        when(serverQueue.isLocalPartitionFull()).thenReturn(true);

        // Create the server
        final int timeoutMillis = 30000;
        final LoadBalanceProtocol loadBalanceProtocol = new StandardLoadBalanceProtocol(serverFlowFileRepo, serverContentRepo, serverProvRepo, flowController, ALWAYS_AUTHORIZED);

        final ConnectionLoadBalanceServer server = new ConnectionLoadBalanceServer("localhost", 0, sslContext, 2, loadBalanceProtocol, eventReporter, timeoutMillis);
        server.start();

        try {
            final int loadBalancePort = server.getPort();

            // Create the Load Balanced FlowFile Queue
            final NodeIdentifier remoteNodeId = new NodeIdentifier("unit-test", "localhost", 8090, "localhost", 8090, "localhost", loadBalancePort, null, null, null, false, null);
            nodeIdentifiers.add(remoteNodeId);

            final NioAsyncLoadBalanceClientRegistry clientRegistry = new NioAsyncLoadBalanceClientRegistry(createClientFactory(sslContext), 1);
            clientRegistry.start();

            final NodeConnectionStatus connectionStatus = mock(NodeConnectionStatus.class);
            when(connectionStatus.getState()).thenReturn(NodeConnectionState.CONNECTED);
            when(clusterCoordinator.getConnectionStatus(any(NodeIdentifier.class))).thenReturn(connectionStatus);
            final NioAsyncLoadBalanceClientTask clientTask = new NioAsyncLoadBalanceClientTask(clientRegistry, clusterCoordinator, eventReporter);

            final Thread clientThread = new Thread(clientTask);
            clientThread.setDaemon(true);
            clientThread.start();

            final SocketLoadBalancedFlowFileQueue flowFileQueue = new SocketLoadBalancedFlowFileQueue(queueId, new NopConnectionEventListener(), processScheduler, clientFlowFileRepo, clientProvRepo,
                clientContentRepo, resourceClaimManager, clusterCoordinator, clientRegistry, flowFileSwapManager, swapThreshold, eventReporter);
            flowFileQueue.setFlowFilePartitioner(new RoundRobinPartitioner());

            try {
                final MockFlowFileRecord firstFlowFile = new MockFlowFileRecord(0L);
                flowFileQueue.put(firstFlowFile);

                final Map<String, String> attributes = new HashMap<>();
                attributes.put("integration", "test");
                attributes.put("unit-test", "false");
                attributes.put("integration-test", "true");

                final ContentClaim contentClaim = createContentClaim("hello".getBytes());
                final MockFlowFileRecord secondFlowFile = new MockFlowFileRecord(attributes, 5L, contentClaim);
                flowFileQueue.put(secondFlowFile);

                flowFileQueue.startLoadBalancing();

                Thread.sleep(5000L);

                assertTrue("Server's FlowFile Repo was updated", serverRepoRecords.isEmpty());
                assertTrue(clientRepoRecords.isEmpty());

                assertEquals(2, flowFileQueue.size().getObjectCount());

                // Enable data to be transferred
                when(serverQueue.isLocalPartitionFull()).thenReturn(false);

                while (clientRepoRecords.size() != 1) {
                    Thread.sleep(10L);
                }

                assertEquals(1, serverRepoRecords.size());
            } finally {
                flowFileQueue.stopLoadBalancing();
                clientRegistry.getAllClients().forEach(AsyncLoadBalanceClient::stop);
            }
        } finally {
            server.stop();
        }
    }

    private FlowFileRepository createFlowFileRepository(final List<RepositoryRecord> repoRecords) throws IOException {
        final FlowFileRepository flowFileRepo = mock(FlowFileRepository.class);
        doAnswer(invocation -> {
            final Collection records = invocation.getArgumentAt(0, Collection.class);
            repoRecords.addAll(records);
            return null;
        }).when(flowFileRepo).updateRepository(anyCollection());

        return flowFileRepo;
    }


    private ContentRepository createContentRepository(final ConcurrentMap<ContentClaim, byte[]> claimContents) throws IOException {
        final ContentRepository contentRepo = mock(ContentRepository.class);

        Mockito.doAnswer(new Answer<ContentClaim>() {
            @Override
            public ContentClaim answer(final InvocationOnMock invocation) {
                return createContentClaim(null);
            }
        }).when(contentRepo).create(Mockito.anyBoolean());


        Mockito.doAnswer(new Answer<OutputStream>() {
            @Override
            public OutputStream answer(final InvocationOnMock invocation) {
                final ContentClaim contentClaim = invocation.getArgumentAt(0, ContentClaim.class);

                final ByteArrayOutputStream baos = new ByteArrayOutputStream() {
                    @Override
                    public void close() throws IOException {
                        super.close();
                        claimContents.put(contentClaim, toByteArray());
                    }
                };

                return baos;
            }
        }).when(contentRepo).write(any(ContentClaim.class));


        Mockito.doAnswer(new Answer<InputStream>() {
            @Override
            public InputStream answer(final InvocationOnMock invocation) {
                final ContentClaim contentClaim = invocation.getArgumentAt(0, ContentClaim.class);
                if (contentClaim == null) {
                    return new ByteArrayInputStream(new byte[0]);
                }

                final byte[] bytes = claimContents.get(contentClaim);
                if (bytes == null) {
                    throw new ContentNotFoundException(contentClaim);
                }

                return new ByteArrayInputStream(bytes);
            }
        }).when(contentRepo).read(any(ContentClaim.class));

        return contentRepo;
    }
}
