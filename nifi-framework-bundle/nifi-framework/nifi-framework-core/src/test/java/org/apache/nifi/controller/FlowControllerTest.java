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
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.connectable.Funnel;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.leader.election.LeaderElectionManager;
import org.apache.nifi.controller.metrics.ComponentMetricReporter;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.repository.FlowFileEventRepository;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.controller.repository.FlowFileRepository;
import org.apache.nifi.controller.repository.StandardFlowFileRecord;
import org.apache.nifi.controller.repository.StandardRepositoryRecord;
import org.apache.nifi.controller.repository.metrics.RingBufferEventRepository;
import org.apache.nifi.controller.serialization.FlowSynchronizer;
import org.apache.nifi.controller.status.history.StatusHistoryRepository;
import org.apache.nifi.events.VolatileBulletinRepository;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.FlowFilePrioritizer;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.groups.BundleUpdateStrategy;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.nar.ExtensionDiscoveringManager;
import org.apache.nifi.nar.StandardExtensionDiscoveringManager;
import org.apache.nifi.nar.SystemBundle;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.provenance.MockProvenanceRepository;
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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class FlowControllerTest {
    private static final String INDEX_ATTRIBUTE = "index";


    private static final String CLUSTERED_NODE = "clustered-node";
    private static final String STANDALONE_NODE = "standalone-node";
    private static final String LOCALHOST = "localhost";
    private static final String JOINING_NODE = "joining-node";
    private static final String JOINING_HOST = "joining-host";

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
    void testRemainStandaloneWhenConnectionStatusIsAssigned() {
        final FlowController flowController = createStandaloneFlowController();

        assertEquals(NodeConnectionState.STANDALONE, flowController.getNodeConnectionState());

        flowController.setConnectionStatus(new NodeConnectionStatus(createNodeIdentifier(STANDALONE_NODE), org.apache.nifi.cluster.coordination.node.NodeConnectionState.CONNECTED));

        assertEquals(NodeConnectionState.STANDALONE, flowController.getNodeConnectionState());
        assertEquals(Set.of(), flowController.getClusterMembers());
    }

    @Test
    void testReportDisconnectedForNewClusteredController() {
        final FlowController flowController = createClusteredFlowController();

        assertEquals(NodeConnectionState.DISCONNECTED, flowController.getNodeConnectionState());

        flowController.setConnectionStatus(null);
        assertEquals(NodeConnectionState.DISCONNECTED, flowController.getNodeConnectionState());
    }

    @ParameterizedTest
    @MethodSource("supportedClusterProtocolStates")
    void testMapClusterProtocolNodeStatesToApiNodeStates(
            final org.apache.nifi.cluster.coordination.node.NodeConnectionState protocolState,
            final NodeConnectionState apiState) {
        final FlowController flowController = createClusteredFlowController();

        flowController.setConnectionStatus(new NodeConnectionStatus(createNodeIdentifier(CLUSTERED_NODE), protocolState));

        assertEquals(apiState, flowController.getNodeConnectionState());
    }

    @Test
    void testProvideNodeConnectionStateDuringFlowSynchronization() throws Exception {
        final FlowController flowController = createClusteredFlowController();
        final NodeConnectionStatus localNodeStatus = new NodeConnectionStatus(
                createNodeIdentifier(CLUSTERED_NODE),
                org.apache.nifi.cluster.coordination.node.NodeConnectionState.CONNECTING
        );
        final NodeConnectionStatus joiningNodeStatus = new NodeConnectionStatus(
                new NodeIdentifier(JOINING_NODE, JOINING_HOST, 8443, JOINING_HOST, 9090, JOINING_HOST, 10000, 10001, true),
                org.apache.nifi.cluster.coordination.node.NodeConnectionState.CONNECTING
        );
        when(clusterCoordinator.getConnectionStatuses()).thenReturn(List.of(localNodeStatus, joiningNodeStatus));
        flowController.setConnectionStatus(localNodeStatus);

        final ExecutorService lifecycleExecutor = Executors.newSingleThreadExecutor();
        final AtomicReference<Future<NodeConnectionState>> nodeStateFutureReference = new AtomicReference<>();

        try {
            final FlowSynchronizer synchronizer = (controller, dataFlow, flowService, bundleUpdateStrategy) -> {
                final Future<NodeConnectionState> nodeStateFuture = lifecycleExecutor.submit(controller::getNodeConnectionState);
                nodeStateFutureReference.set(nodeStateFuture);

                try {
                    assertEquals(NodeConnectionState.CONNECTING, nodeStateFuture.get(5, TimeUnit.SECONDS));
                    assertEquals(Set.of(LOCALHOST, JOINING_HOST), controller.getClusterMembers());
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

    /**
     * Reproduces FIFO order loss after restarting a FlowController whose connection queue is in swap mode. Before shutdown the queue holds
     * FlowFiles in its active queue, in a swap file and in its in-memory swap queue. On restart {@link FlowController#initializeFlow()} loads the
     * FlowFiles from the write-ahead log, in hash order of their ids, before recovering the older swap file, so newer FlowFiles from the
     * in-memory swap queue fill the active queue ahead of older swapped-out ones.
     */
    @Test
    void testFifoOrderPreservedAcrossRestartWithSwappedFlowFiles() throws IOException {
        final int swapThreshold = 100;
        final int activeCount = 100;
        final int swappedCount = 100;
        final int totalCount = activeCount + swappedCount + 50;
        // Ids crossing 2^20 make the HashMap iteration order of the recovered records differ from id order
        final long firstId = (1L << 20) - 220;
        final NiFiProperties nifiProperties = createNiFiProperties(false, Map.of(NiFiProperties.QUEUE_SWAP_THRESHOLD, String.valueOf(swapThreshold)));

        final FlowController flowController = createStandaloneFlowController(nifiProperties);
        final FlowFileQueue queue = createFifoConnection(flowController).getFlowFileQueue();
        flowController.initializeFlow();
        final FlowFileRepository flowFileRepository = flowController.getRepositoryContextFactory().getFlowFileRepository();
        for (int index = 0; index < totalCount; index++) {
            final FlowFileRecord flowFile = new StandardFlowFileRecord.Builder()
                    .id(firstId + index)
                    .addAttribute(CoreAttributes.UUID.key(), UUID.randomUUID().toString())
                    .addAttribute(INDEX_ATTRIBUTE, String.valueOf(index))
                    .lastQueued(1_000_000L + index, index)
                    .build();
            final StandardRepositoryRecord record = new StandardRepositoryRecord(queue);
            record.setWorking(flowFile, false);
            record.setDestination(queue);
            flowFileRepository.updateRepository(List.of(record));
            queue.put(flowFile);
        }
        final String[] swapFiles = nifiProperties.getFlowFileRepositoryPath().resolve("swap").toFile().list();
        assertEquals(1, swapFiles == null ? 0 : swapFiles.length, "Expected exactly one swap file on disk before restart");
        flowController.shutdown(true);

        final FlowController restartedFlowController = createStandaloneFlowController(nifiProperties);
        final FlowFileQueue recoveredQueue = createFifoConnection(restartedFlowController).getFlowFileQueue();
        restartedFlowController.initializeFlow();

        final List<Integer> polledIndexes = new ArrayList<>();
        FlowFileRecord polled;
        while ((polled = recoveredQueue.poll(new HashSet<>())) != null) {
            polledIndexes.add(Integer.parseInt(polled.getAttribute(INDEX_ATTRIBUTE)));
        }
        assertEquals(totalCount, polledIndexes.size());
        final int firstSwapFilePosition = polledIndexes.indexOf(activeCount);
        int firstSwapQueuePosition = 0;
        while (polledIndexes.get(firstSwapQueuePosition) < activeCount + swappedCount) {
            firstSwapQueuePosition++;
        }
        assertTrue(firstSwapFilePosition < firstSwapQueuePosition, "Newer FlowFile index " + polledIndexes.get(firstSwapQueuePosition)
                + " from the in-memory swap queue was polled at position " + firstSwapQueuePosition + ", before older FlowFile index "
                + activeCount + " from the swap file at position " + firstSwapFilePosition);
        for (int position = 0; position < totalCount; position++) {
            final int index = polledIndexes.get(position);
            final String origin = index < activeCount ? "active queue" : index < activeCount + swappedCount ? "swap file" : "in-memory swap queue";
            assertEquals(position, index, "FlowFile index " + index + " (before restart in the " + origin + ") was polled at position " + position
                    + "; polled order: " + polledIndexes);
        }
    }

    /**
     * Creates a connection between two funnels in the root group, with fixed identifiers so the same connection can be re-created after a
     * restart, ordered first-in first-out. The prioritizer orders by queue date like {@code FirstInFirstOutPrioritizer}, which is not on this
     * module's test classpath.
     */
    private static Connection createFifoConnection(final FlowController flowController) {
        final FlowManager flowManager = flowController.getFlowManager();
        final ProcessGroup rootGroup = flowManager.getRootGroup();
        final Funnel source = flowManager.createFunnel("5f1c7a3e-0000-4000-8000-000000000001");
        final Funnel destination = flowManager.createFunnel("5f1c7a3e-0000-4000-8000-000000000002");
        rootGroup.addFunnel(source);
        rootGroup.addFunnel(destination);

        final Connection connection = flowManager.createConnection("5f1c7a3e-0000-4000-8000-000000000003", null, source, destination, List.of(Relationship.ANONYMOUS.getName()));
        rootGroup.addConnection(connection);
        final FlowFilePrioritizer fifo = (first, second) -> Comparator.comparingLong(FlowFile::getLastQueueDate)
                .thenComparingLong(FlowFile::getQueueDateIndex)
                .compare(first, second);
        connection.getFlowFileQueue().setPriorities(List.of(fifo));
        return connection;
    }

    private FlowController createStandaloneFlowController() {
        return createStandaloneFlowController(createNiFiProperties(false));
    }

    private FlowController createStandaloneFlowController(final NiFiProperties nifiProperties) {
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

    private FlowController createClusteredFlowController() {
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
        return createNiFiProperties(clustered, Map.of());
    }

    private NiFiProperties createNiFiProperties(final boolean clustered, final Map<String, String> overrides) {
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

        properties.putAll(overrides);
        return NiFiProperties.createBasicNiFiProperties(null, properties);
    }
}
