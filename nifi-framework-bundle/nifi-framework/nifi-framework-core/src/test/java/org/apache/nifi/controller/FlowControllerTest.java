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
import org.apache.nifi.controller.scheduling.auto.AutoSchedulingDiagnostics;
import org.apache.nifi.controller.serialization.FlowSynchronizer;
import org.apache.nifi.controller.status.history.StatusHistoryRepository;
import org.apache.nifi.events.VolatileBulletinRepository;
import org.apache.nifi.groups.BundleUpdateStrategy;
import org.apache.nifi.nar.ExtensionDiscoveringManager;
import org.apache.nifi.nar.StandardExtensionDiscoveringManager;
import org.apache.nifi.nar.SystemBundle;
import org.apache.nifi.provenance.MockProvenanceRepository;
import org.apache.nifi.scheduling.SchedulingStrategy;
import org.apache.nifi.security.encryption.InternalPassThroughPropertyEncryptionProvider;
import org.apache.nifi.security.encryption.PropertyEncryptionProvider;
import org.apache.nifi.services.FlowService;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.revision.RevisionManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opentest4j.AssertionFailedError;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FlowControllerTest {

    private static final String CLUSTERED_NODE = "clustered-node";
    private static final String STANDALONE_NODE = "standalone-node";
    private static final String LOCALHOST = "localhost";

    private final List<FlowController> flowControllers = new ArrayList<>();
    private final AtomicInteger controllerCounter = new AtomicInteger();

    @Mock
    private Authorizer authorizer;

    @Mock
    private AuditService auditService;

    @Mock
    private ComponentMetricReporter componentMetricReporter;

    @Mock
    private StatusHistoryRepository statusHistoryRepository;

    @Mock
    private ClusterCoordinator clusterCoordinator;

    @Mock
    private HeartbeatMonitor heartbeatMonitor;

    @Mock
    private LeaderElectionManager leaderElectionManager;

    @Mock
    private ConnectorRequestReplicator connectorRequestReplicator;

    @TempDir
    private Path tempDir;

    @Test
    void testRemainStandaloneWhenConnectionStatusIsAssigned() throws IOException {
        final FlowController flowController = createStandaloneFlowController();

        assertEquals(NodeConnectionState.STANDALONE, flowController.getNodeConnectionState());

        flowController.setConnectionStatus(new NodeConnectionStatus(createNodeIdentifier(STANDALONE_NODE), org.apache.nifi.cluster.coordination.node.NodeConnectionState.CONNECTED));

        assertEquals(NodeConnectionState.STANDALONE, flowController.getNodeConnectionState());
    }

    @Test
    void testReportDisconnectedForNewClusteredController() throws IOException {
        final FlowController flowController = createClusteredFlowController();

        assertEquals(NodeConnectionState.DISCONNECTED, flowController.getNodeConnectionState());

        flowController.setConnectionStatus(null);
        assertEquals(NodeConnectionState.DISCONNECTED, flowController.getNodeConnectionState());
    }

    @ParameterizedTest
    @MethodSource("supportedClusterProtocolStates")
    void testMapClusterProtocolNodeStatesToApiNodeStates(
            final org.apache.nifi.cluster.coordination.node.NodeConnectionState protocolState,
            final NodeConnectionState apiState) throws IOException {
        final FlowController flowController = createClusteredFlowController();

        flowController.setConnectionStatus(new NodeConnectionStatus(createNodeIdentifier(CLUSTERED_NODE), protocolState));

        assertEquals(apiState, flowController.getNodeConnectionState());
    }

    @Test
    void testProvideNodeConnectionStateDuringFlowSynchronization() throws Exception {
        final FlowController flowController = createClusteredFlowController();
        flowController.setConnectionStatus(new NodeConnectionStatus(
                createNodeIdentifier(CLUSTERED_NODE),
                org.apache.nifi.cluster.coordination.node.NodeConnectionState.CONNECTING
        ));

        final ExecutorService lifecycleExecutor = Executors.newSingleThreadExecutor();
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

    @Test
    void testPlatformAutomaticDiagnosticsUseProcessorActiveCount() {
        final FlowController flowController = mock(FlowController.class, CALLS_REAL_METHODS);
        final ProcessorNode processorNode = mock(ProcessorNode.class);
        when(processorNode.getSchedulingStrategy()).thenReturn(SchedulingStrategy.AUTO);
        when(processorNode.getActiveThreadCount()).thenReturn(7);

        final AutoSchedulingDiagnostics diagnostics = flowController.getAutoSchedulingDiagnostics(processorNode);
        assertEquals(7, diagnostics.activeProcessorInvocations());
    }

    private FlowController createStandaloneFlowController() throws IOException {
        final NiFiProperties nifiProperties = createNiFiProperties(false);
        final FlowFileEventRepository flowFileEventRepository = new RingBufferEventRepository(5);
        final ExtensionDiscoveringManager extensionManager = createExtensionManager(nifiProperties);
        final StateManagerProvider stateManagerProvider = new MockStateManagerProvider();
        final PropertyEncryptionProvider propertyEncryptionProvider = new InternalPassThroughPropertyEncryptionProvider();

        final FlowController flowController = FlowController.createStandaloneInstance(
                flowFileEventRepository,
                null,
                nifiProperties,
                authorizer,
                auditService,
                componentMetricReporter,
                propertyEncryptionProvider,
                new VolatileBulletinRepository(),
                extensionManager,
                statusHistoryRepository,
                null,
                stateManagerProvider,
                connectorRequestReplicator
        );

        flowControllers.add(flowController);
        return flowController;
    }

    private FlowController createClusteredFlowController() throws IOException {
        final NiFiProperties nifiProperties = createNiFiProperties(true);
        final FlowFileEventRepository flowFileEventRepository = new RingBufferEventRepository(5);
        final ExtensionDiscoveringManager extensionManager = createExtensionManager(nifiProperties);
        final StateManagerProvider stateManagerProvider = new MockStateManagerProvider();
        final PropertyEncryptionProvider propertyEncryptionProvider = new InternalPassThroughPropertyEncryptionProvider();
        final FlowController flowController = FlowController.createClusteredInstance(
                flowFileEventRepository,
                null,
                nifiProperties,
                authorizer,
                auditService,
                componentMetricReporter,
                propertyEncryptionProvider,
                mock(NodeProtocolSender.class),
                new VolatileBulletinRepository(),
                clusterCoordinator,
                heartbeatMonitor,
                leaderElectionManager,
                extensionManager,
                mock(RevisionManager.class),
                statusHistoryRepository,
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
        return new NodeIdentifier(id, LOCALHOST, 8443, LOCALHOST, 9090, LOCALHOST, 10000, 10001, true);
    }

    private NiFiProperties createNiFiProperties(final boolean clustered) {
        final Path controllerDirectory = tempDir.resolve("controller-" + controllerCounter.incrementAndGet());
        final Map<String, String> properties = new HashMap<>(Map.ofEntries(
                Map.entry(NiFiProperties.CONTENT_ARCHIVE_ENABLED, "false"),
                Map.entry(NiFiProperties.FLOW_CONFIGURATION_ARCHIVE_ENABLED, "false"),
                Map.entry(NiFiProperties.FLOW_CONFIGURATION_FILE, controllerDirectory.resolve("flow.json.gz").toString()),
                Map.entry(NiFiProperties.FLOW_CONTROLLER_GRACEFUL_SHUTDOWN_PERIOD, "10 secs"),
                Map.entry(NiFiProperties.FLOWFILE_REPOSITORY_DIRECTORY, controllerDirectory.resolve("flowfile_repository").toString()),
                Map.entry(NiFiProperties.NAR_LIBRARY_DIRECTORY, controllerDirectory.resolve("lib").toString()),
                Map.entry(NiFiProperties.NAR_WORKING_DIRECTORY, controllerDirectory.resolve("work").resolve("nar").toString()),
                Map.entry(NiFiProperties.PROVENANCE_REPO_IMPLEMENTATION_CLASS, MockProvenanceRepository.class.getName()),
                Map.entry(NiFiProperties.PROVENANCE_REPO_DIRECTORY_PREFIX + "default", controllerDirectory.resolve("provenance_repository").toString()),
                Map.entry(NiFiProperties.QUEUE_SWAP_THRESHOLD, String.valueOf(NiFiProperties.DEFAULT_QUEUE_SWAP_THRESHOLD)),
                Map.entry(NiFiProperties.REPOSITORY_CONTENT_PREFIX + "default", controllerDirectory.resolve("content_repository").toString()),
                Map.entry(NiFiProperties.REPOSITORY_DATABASE_DIRECTORY, controllerDirectory.resolve("database_repository").toString()),
                Map.entry(NiFiProperties.WEB_HTTPS_HOST, LOCALHOST),
                Map.entry(NiFiProperties.WEB_HTTPS_PORT, "8443")
        ));

        if (clustered) {
            properties.put(NiFiProperties.CLUSTER_NODE_ADDRESS, LOCALHOST);
            properties.put(NiFiProperties.CLUSTER_NODE_PROTOCOL_PORT, "9090");
            properties.put(NiFiProperties.LOAD_BALANCE_HOST, LOCALHOST);
            properties.put(NiFiProperties.LOAD_BALANCE_PORT, "6342");
        }

        return NiFiProperties.createBasicNiFiProperties(null, properties);
    }
}
