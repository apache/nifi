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
package org.apache.nifi.controller;

import org.apache.nifi.admin.service.AuditService;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.heartbeat.HeartbeatMonitor;
import org.apache.nifi.cluster.coordination.node.NodeConnectionStatus;
import org.apache.nifi.cluster.protocol.DataFlow;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.cluster.protocol.NodeProtocolSender;
import org.apache.nifi.components.connector.ConnectorRequestReplicator;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.controller.leader.election.LeaderElectionManager;
import org.apache.nifi.controller.metrics.ComponentMetricReporter;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.repository.metrics.RingBufferEventRepository;
import org.apache.nifi.controller.serialization.FlowSynchronizer;
import org.apache.nifi.controller.status.history.StatusHistoryRepository;
import org.apache.nifi.events.VolatileBulletinRepository;
import org.apache.nifi.groups.BundleUpdateStrategy;
import org.apache.nifi.nar.ExtensionDiscoveringManager;
import org.apache.nifi.nar.StandardExtensionDiscoveringManager;
import org.apache.nifi.nar.SystemBundle;
import org.apache.nifi.provenance.MockProvenanceRepository;
import org.apache.nifi.security.encryption.InternalPassThroughPropertyEncryptionProvider;
import org.apache.nifi.security.encryption.PropertyEncryptionProvider;
import org.apache.nifi.services.FlowService;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.revision.RevisionManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.opentest4j.AssertionFailedError;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestFlowController {

    private final List<FlowController> flowControllers = new ArrayList<>();
    private final AtomicInteger controllerCounter = new AtomicInteger();

    @TempDir
    private Path tempDir;

    @Test
    void shouldRemainStandaloneWhenConnectionStatusIsAssigned() throws IOException {
        final FlowController flowController = createStandaloneFlowController();

        assertEquals(NodeConnectionState.STANDALONE, flowController.getNodeConnectionState());

        flowController.setConnectionStatus(new NodeConnectionStatus(createNodeIdentifier("standalone-node"), org.apache.nifi.cluster.coordination.node.NodeConnectionState.CONNECTED));

        assertEquals(NodeConnectionState.STANDALONE, flowController.getNodeConnectionState());
    }

    @Test
    void shouldReportDisconnectedForNewClusteredController() throws IOException {
        final FlowController flowController = createClusteredFlowController();

        assertEquals(NodeConnectionState.DISCONNECTED, flowController.getNodeConnectionState());
    }

    @ParameterizedTest
    @MethodSource("supportedClusterProtocolStates")
    void shouldMapClusterProtocolNodeStatesToApiNodeStates(
            final org.apache.nifi.cluster.coordination.node.NodeConnectionState protocolState,
            final NodeConnectionState apiState) throws IOException {
        final FlowController flowController = createClusteredFlowController();

        flowController.setConnectionStatus(new NodeConnectionStatus(createNodeIdentifier("clustered-node"), protocolState));

        assertEquals(apiState, flowController.getNodeConnectionState());
    }

    @Test
    void shouldProvideNodeConnectionStateDuringFlowSynchronization() throws Exception {
        final FlowController flowController = createClusteredFlowController();
        flowController.setConnectionStatus(new NodeConnectionStatus(
                createNodeIdentifier("clustered-node"),
                org.apache.nifi.cluster.coordination.node.NodeConnectionState.CONNECTING
        ));

        final ExecutorService lifecycleExecutor = Executors.newSingleThreadExecutor(runnable -> {
            final Thread thread = new Thread(runnable);
            thread.setName("node-connection-state-lifecycle");
            thread.setDaemon(true);
            return thread;
        });
        final AtomicReference<Future<NodeConnectionState>> nodeStateFutureReference = new AtomicReference<>();

        try {
            final FlowSynchronizer synchronizer = (controller, dataFlow, flowService, bundleUpdateStrategy) -> {
                final Future<NodeConnectionState> nodeStateFuture = lifecycleExecutor.submit(controller::getNodeConnectionState);
                nodeStateFutureReference.set(nodeStateFuture);

                try {
                    assertEquals(NodeConnectionState.CONNECTING, nodeStateFuture.get(5, TimeUnit.SECONDS));
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new AssertionFailedError("Interrupted while waiting for node connection state", e);
                } catch (final ExecutionException | TimeoutException e) {
                    throw new AssertionFailedError("Failed to obtain node connection state", e);
                }
            };

            flowController.synchronize(
                    synchronizer,
                    mock(DataFlow.class),
                    mock(FlowService.class),
                    BundleUpdateStrategy.USE_SPECIFIED_OR_GHOST
            );
        } finally {
            final Future<NodeConnectionState> nodeStateFuture = nodeStateFutureReference.get();
            if (nodeStateFuture != null) {
                nodeStateFuture.cancel(true);
            }

            lifecycleExecutor.shutdownNow();
            lifecycleExecutor.awaitTermination(5, TimeUnit.SECONDS);
        }
    }

    FlowController createStandaloneFlowController() throws IOException {
        final NiFiProperties nifiProperties = createNiFiProperties(false);
        final FlowFileEventRepository flowFileEventRepository = new RingBufferEventRepository(5);
        final ExtensionDiscoveringManager extensionManager = createExtensionManager(nifiProperties);
        final StateManagerProvider stateManagerProvider = new MockStateManagerProvider();
        final PropertyEncryptionProvider propertyEncryptionProvider = new InternalPassThroughPropertyEncryptionProvider();

        final FlowController flowController = FlowController.createStandaloneInstance(
                flowFileEventRepository,
                null,
                nifiProperties,
                mock(Authorizer.class),
                mock(AuditService.class),
                mock(ComponentMetricReporter.class),
                propertyEncryptionProvider,
                new VolatileBulletinRepository(),
                extensionManager,
                mock(StatusHistoryRepository.class),
                null,
                stateManagerProvider,
                mock(ConnectorRequestReplicator.class)
        );

        flowControllers.add(flowController);
        return flowController;
    }

    FlowController createClusteredFlowController() throws IOException {
        final NiFiProperties nifiProperties = createNiFiProperties(true);
        final FlowFileEventRepository flowFileEventRepository = new RingBufferEventRepository(5);
        final ExtensionDiscoveringManager extensionManager = createExtensionManager(nifiProperties);
        final StateManagerProvider stateManagerProvider = new MockStateManagerProvider();
        final PropertyEncryptionProvider propertyEncryptionProvider = new InternalPassThroughPropertyEncryptionProvider();
        final ClusterCoordinator clusterCoordinator = mock(ClusterCoordinator.class);
        final HeartbeatMonitor heartbeatMonitor = mock(HeartbeatMonitor.class);
        final LeaderElectionManager leaderElectionManager = mock(LeaderElectionManager.class);
        final ConnectorRequestReplicator connectorRequestReplicator = mock(ConnectorRequestReplicator.class);

        when(clusterCoordinator.getConnectionStatus(any())).thenReturn(null);
        when(heartbeatMonitor.getHeartbeatAddress()).thenReturn("localhost:9090");
        when(leaderElectionManager.getLeader(anyString())).thenReturn(Optional.of("cluster-coordinator"));
        when(leaderElectionManager.isLeader(anyString())).thenReturn(false);

        final FlowController flowController = FlowController.createClusteredInstance(
                flowFileEventRepository,
                null,
                nifiProperties,
                mock(Authorizer.class),
                mock(AuditService.class),
                mock(ComponentMetricReporter.class),
                propertyEncryptionProvider,
                mock(NodeProtocolSender.class),
                new VolatileBulletinRepository(),
                clusterCoordinator,
                heartbeatMonitor,
                leaderElectionManager,
                extensionManager,
                mock(RevisionManager.class),
                mock(StatusHistoryRepository.class),
                null,
                stateManagerProvider,
                connectorRequestReplicator
        );

        flowControllers.add(flowController);
        return flowController;
    }

    @AfterEach
    void tearDownFlowControllers() {
        for (int index = flowControllers.size() - 1; index >= 0; index--) {
            final FlowController flowController = flowControllers.get(index);
            if (!flowController.isTerminated()) {
                flowController.shutdown(true);
            }
        }

        flowControllers.clear();
    }

    private static Stream<Arguments> supportedClusterProtocolStates() {
        return Stream.of(
                arguments(org.apache.nifi.cluster.coordination.node.NodeConnectionState.CONNECTING, NodeConnectionState.CONNECTING),
                arguments(org.apache.nifi.cluster.coordination.node.NodeConnectionState.CONNECTED, NodeConnectionState.CONNECTED),
                arguments(org.apache.nifi.cluster.coordination.node.NodeConnectionState.OFFLOADING, NodeConnectionState.OFFLOADING),
                arguments(org.apache.nifi.cluster.coordination.node.NodeConnectionState.DISCONNECTING, NodeConnectionState.DISCONNECTING),
                arguments(org.apache.nifi.cluster.coordination.node.NodeConnectionState.OFFLOADED, NodeConnectionState.OFFLOADED),
                arguments(org.apache.nifi.cluster.coordination.node.NodeConnectionState.DISCONNECTED, NodeConnectionState.DISCONNECTED),
                arguments(org.apache.nifi.cluster.coordination.node.NodeConnectionState.REMOVED, NodeConnectionState.REMOVED)
        );
    }

    private ExtensionDiscoveringManager createExtensionManager(final NiFiProperties nifiProperties) {
        final StandardExtensionDiscoveringManager extensionManager = new StandardExtensionDiscoveringManager();
        extensionManager.discoverExtensions(SystemBundle.create(nifiProperties), Set.of());
        return extensionManager;
    }

    private NodeIdentifier createNodeIdentifier(final String id) {
        return new NodeIdentifier(id, "localhost", 8443, "localhost", 9090, "localhost", 10000, 10001, true);
    }

    private NiFiProperties createNiFiProperties(final boolean clustered) throws IOException {
        final Path controllerDirectory = tempDir.resolve("controller-" + controllerCounter.incrementAndGet());
        final Path narLibraryDirectory = Files.createDirectories(controllerDirectory.resolve("lib"));
        final Path narWorkingDirectory = Files.createDirectories(controllerDirectory.resolve("work").resolve("nar"));
        final Path databaseDirectory = Files.createDirectories(controllerDirectory.resolve("database_repository"));
        final Path flowFileRepositoryDirectory = Files.createDirectories(controllerDirectory.resolve("flowfile_repository"));
        final Path contentRepositoryDirectory = Files.createDirectories(controllerDirectory.resolve("content_repository"));

        final Map<String, String> properties = clustered
                ? Map.ofEntries(
                        Map.entry(NiFiProperties.CLUSTER_NODE_ADDRESS, "localhost"),
                        Map.entry(NiFiProperties.CLUSTER_NODE_PROTOCOL_PORT, "9090"),
                        Map.entry(NiFiProperties.CONTENT_ARCHIVE_ENABLED, "false"),
                        Map.entry(NiFiProperties.FLOW_CONFIGURATION_ARCHIVE_ENABLED, "false"),
                        Map.entry(NiFiProperties.FLOW_CONFIGURATION_FILE, controllerDirectory.resolve("flow.json.gz").toString()),
                        Map.entry(NiFiProperties.FLOW_CONTROLLER_GRACEFUL_SHUTDOWN_PERIOD, "10 secs"),
                        Map.entry(NiFiProperties.FLOWFILE_REPOSITORY_DIRECTORY, flowFileRepositoryDirectory.toString()),
                        Map.entry(NiFiProperties.LOAD_BALANCE_HOST, "localhost"),
                        Map.entry(NiFiProperties.LOAD_BALANCE_PORT, "6342"),
                        Map.entry(NiFiProperties.NAR_LIBRARY_DIRECTORY, narLibraryDirectory.toString()),
                        Map.entry(NiFiProperties.NAR_WORKING_DIRECTORY, narWorkingDirectory.toString()),
                        Map.entry(NiFiProperties.PROVENANCE_REPO_IMPLEMENTATION_CLASS, MockProvenanceRepository.class.getName()),
                        Map.entry(NiFiProperties.PROVENANCE_REPO_DIRECTORY_PREFIX + "default", controllerDirectory.resolve("provenance_repository").toString()),
                        Map.entry(NiFiProperties.QUEUE_SWAP_THRESHOLD, String.valueOf(NiFiProperties.DEFAULT_QUEUE_SWAP_THRESHOLD)),
                        Map.entry(NiFiProperties.REPOSITORY_CONTENT_PREFIX + "default", contentRepositoryDirectory.toString()),
                        Map.entry(NiFiProperties.REPOSITORY_DATABASE_DIRECTORY, databaseDirectory.toString()),
                        Map.entry(NiFiProperties.WEB_HTTPS_HOST, "localhost"),
                        Map.entry(NiFiProperties.WEB_HTTPS_PORT, "8443")
                )
                : Map.ofEntries(
                        Map.entry(NiFiProperties.CONTENT_ARCHIVE_ENABLED, "false"),
                        Map.entry(NiFiProperties.FLOW_CONFIGURATION_ARCHIVE_ENABLED, "false"),
                        Map.entry(NiFiProperties.FLOW_CONFIGURATION_FILE, controllerDirectory.resolve("flow.json.gz").toString()),
                        Map.entry(NiFiProperties.FLOW_CONTROLLER_GRACEFUL_SHUTDOWN_PERIOD, "10 secs"),
                        Map.entry(NiFiProperties.FLOWFILE_REPOSITORY_DIRECTORY, flowFileRepositoryDirectory.toString()),
                        Map.entry(NiFiProperties.NAR_LIBRARY_DIRECTORY, narLibraryDirectory.toString()),
                        Map.entry(NiFiProperties.NAR_WORKING_DIRECTORY, narWorkingDirectory.toString()),
                        Map.entry(NiFiProperties.PROVENANCE_REPO_IMPLEMENTATION_CLASS, MockProvenanceRepository.class.getName()),
                        Map.entry(NiFiProperties.PROVENANCE_REPO_DIRECTORY_PREFIX + "default", controllerDirectory.resolve("provenance_repository").toString()),
                        Map.entry(NiFiProperties.QUEUE_SWAP_THRESHOLD, String.valueOf(NiFiProperties.DEFAULT_QUEUE_SWAP_THRESHOLD)),
                        Map.entry(NiFiProperties.REPOSITORY_CONTENT_PREFIX + "default", contentRepositoryDirectory.toString()),
                        Map.entry(NiFiProperties.REPOSITORY_DATABASE_DIRECTORY, databaseDirectory.toString()),
                        Map.entry(NiFiProperties.WEB_HTTPS_HOST, "localhost"),
                        Map.entry(NiFiProperties.WEB_HTTPS_PORT, "8443")
                );

        return NiFiProperties.createBasicNiFiProperties(null, properties);
    }
}
