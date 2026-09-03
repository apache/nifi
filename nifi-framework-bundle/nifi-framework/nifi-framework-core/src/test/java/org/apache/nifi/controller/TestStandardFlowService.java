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

import org.apache.nifi.asset.AssetSynchronizer;
import org.apache.nifi.authorization.Authorizer;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.node.NodeConnectionState;
import org.apache.nifi.cluster.coordination.node.NodeConnectionStatus;
import org.apache.nifi.cluster.protocol.ComponentRevisionSnapshot;
import org.apache.nifi.cluster.protocol.ConnectionResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.cluster.protocol.StandardDataFlow;
import org.apache.nifi.cluster.protocol.impl.NodeProtocolSenderListener;
import org.apache.nifi.cluster.protocol.message.DisconnectMessage;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.components.state.StateManagerProvider;
import org.apache.nifi.connectable.Connection;
import org.apache.nifi.controller.flow.FlowManager;
import org.apache.nifi.controller.queue.FlowFileQueue;
import org.apache.nifi.controller.serialization.FlowSynchronizationException;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.groups.BundleUpdateStrategy;
import org.apache.nifi.groups.ProcessGroup;
import org.apache.nifi.groups.RemoteProcessGroup;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarManager;
import org.apache.nifi.reporting.UserAwareEventAccess;
import org.apache.nifi.state.MockStateMap;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.revision.RevisionManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TestStandardFlowService {

    private FlowController controller;
    private ClusterCoordinator clusterCoordinator;
    private StandardFlowService flowService;
    private NiFiProperties nifiProperties;

    @TempDir
    private Path tempDir;

    @BeforeEach
    public void setup() throws IOException {
        controller = mock(FlowController.class);
        clusterCoordinator = mock(ClusterCoordinator.class);
        final NodeProtocolSenderListener senderListener = mock(NodeProtocolSenderListener.class);
        final RevisionManager revisionManager = mock(RevisionManager.class);
        final NarManager narManager = mock(NarManager.class);
        final AssetSynchronizer parameterContextAssetSynchronizer = mock(AssetSynchronizer.class);
        final AssetSynchronizer connectorAssetSynchronizer = mock(AssetSynchronizer.class);
        final Authorizer authorizer = mock(Authorizer.class);

        final StateManagerProvider stateManagerProvider = mock(StateManagerProvider.class);
        final StateManager stateManager = mock(StateManager.class);
        when(stateManager.getState(any(Scope.class))).thenReturn(new MockStateMap(Collections.emptyMap(), 1));
        when(stateManagerProvider.getStateManager(anyString())).thenReturn(stateManager);
        when(controller.getStateManagerProvider()).thenReturn(stateManagerProvider);
        when(controller.getExtensionManager()).thenReturn(mock(ExtensionManager.class));

        final Path flowConfigFile = tempDir.resolve("flow.json.gz");
        nifiProperties = NiFiProperties.createBasicNiFiProperties(null, Map.of(
                NiFiProperties.FLOW_CONFIGURATION_FILE, flowConfigFile.toString(),
                NiFiProperties.FLOW_CONTROLLER_GRACEFUL_SHUTDOWN_PERIOD, "10 secs",
                NiFiProperties.WEB_HTTPS_HOST, "localhost",
                NiFiProperties.WEB_HTTPS_PORT, "8443",
                NiFiProperties.CLUSTER_NODE_ADDRESS, "localhost",
                NiFiProperties.CLUSTER_NODE_PROTOCOL_PORT, "9090",
                NiFiProperties.LOAD_BALANCE_HOST, "localhost",
                NiFiProperties.LOAD_BALANCE_PORT, "6342",
                NiFiProperties.FLOW_CONFIGURATION_ARCHIVE_ENABLED, "false"
        ));

        flowService = StandardFlowService.createClusteredInstance(controller, nifiProperties, senderListener,
                clusterCoordinator, revisionManager, narManager, parameterContextAssetSynchronizer,
                connectorAssetSynchronizer, authorizer);
    }

    @Test
    public void testDisconnectionRequestWithMatchingGeneration() {
        assertEquals(0, flowService.getConnectionGeneration());

        final DisconnectMessage disconnectMessage = createDisconnectMessage("Test disconnect");
        flowService.handleDisconnectionRequest(disconnectMessage, 0);

        verify(controller).setConnectionStatus(any(NodeConnectionStatus.class));
        verify(controller).setClustered(false, null);
        verify(clusterCoordinator).setConnected(false);
    }

    @Test
    public void testDisconnectionRequestWithStaleGeneration() {
        assertEquals(0, flowService.getConnectionGeneration());

        final DisconnectMessage disconnectMessage = createDisconnectMessage("Test disconnect");
        flowService.handleDisconnectionRequest(disconnectMessage, 0);

        verify(controller).setClustered(false, null);

        reset(controller, clusterCoordinator);

        final DisconnectMessage staleDisconnectMessage = createDisconnectMessage("Stale disconnect from before reconnect");
        flowService.handleDisconnectionRequest(staleDisconnectMessage, 5);

        verify(controller, never()).setClustered(false, null);
        verify(clusterCoordinator, never()).setConnected(false);
    }

    @Test
    public void testNodeConnectionStateConnectingBeforeFlowSynchronization() throws Exception {
        final FlowController clusteredController = mock(FlowController.class);
        final StateManagerProvider stateManagerProvider = controller.getStateManagerProvider();
        final ExtensionManager extensionManager = controller.getExtensionManager();
        final NarManager narManager = mock(NarManager.class);
        when(clusteredController.getStateManagerProvider()).thenReturn(stateManagerProvider);
        when(clusteredController.getExtensionManager()).thenReturn(extensionManager);

        final StandardFlowService clusteredFlowService = spy(StandardFlowService.createClusteredInstance(clusteredController, nifiProperties,
                mock(NodeProtocolSenderListener.class), clusterCoordinator, mock(RevisionManager.class), narManager,
                mock(AssetSynchronizer.class), mock(AssetSynchronizer.class), mock(Authorizer.class)));
        final NodeIdentifier nodeIdentifier = createNodeIdentifier();
        final NodeConnectionStatus connectingStatus = new NodeConnectionStatus(nodeIdentifier, NodeConnectionState.CONNECTING);
        final ConnectionResponse response = new ConnectionResponse(nodeIdentifier,
                new StandardDataFlow(new byte[0], new byte[0], new byte[0], Collections.emptySet()), "instance-1",
                List.of(connectingStatus), mock(ComponentRevisionSnapshot.class));
        final ArgumentCaptor<NodeConnectionStatus> statusCaptor = ArgumentCaptor.forClass(NodeConnectionStatus.class);
        doAnswer(invocation -> {
            verify(clusteredController, never()).setClustered(eq(true), any());
            throw new FlowSynchronizationException("Stop after observing the flow synchronization boundary");
        }).when(clusteredFlowService).loadFromBytes(any(), eq(true), eq(BundleUpdateStrategy.USE_SPECIFIED_OR_COMPATIBLE_OR_GHOST));

        assertThrows(FlowSynchronizationException.class, () -> clusteredFlowService.loadFromConnectionResponse(response));

        final InOrder inOrder = inOrder(clusterCoordinator, clusteredController, narManager, clusteredFlowService);
        inOrder.verify(clusterCoordinator).resetNodeStatuses(any(Map.class));
        inOrder.verify(clusteredController).setNodeId(nodeIdentifier);
        inOrder.verify(clusteredController).setConnectionStatus(statusCaptor.capture());
        inOrder.verify(narManager).syncWithClusterCoordinator();
        inOrder.verify(clusteredFlowService).loadFromBytes(any(), eq(true), eq(BundleUpdateStrategy.USE_SPECIFIED_OR_COMPATIBLE_OR_GHOST));
        assertEquals(nodeIdentifier, statusCaptor.getValue().getNodeIdentifier());
        assertEquals(NodeConnectionState.CONNECTING, statusCaptor.getValue().getState());
        verify(clusteredController, never()).setClustered(anyBoolean(), any());
    }

    @Test
    @Timeout(10)
    public void testOffloadWaitsForHeldStopFutureBeforeTerminatingProcessors() throws Exception {
        final TestContext context = createTestContext("10 secs");
        final CompletableFuture<Void> stopFuture = new CompletableFuture<>();
        final CountDownLatch stopRequested = new CountDownLatch(1);
        final CountDownLatch terminationRequested = new CountDownLatch(1);

        when(context.rootGroup.stopProcessing()).thenAnswer(invocation -> {
            stopRequested.countDown();
            return stopFuture;
        });

        final ProcessGroup owningGroup = mock(ProcessGroup.class);
        final ProcessorNode processor = createProcessorNode(owningGroup, ScheduledState.STOPPED, ScheduledState.STOPPED);
        when(context.rootGroup.findAllProcessors()).thenReturn(List.of(processor));
        doAnswer(invocation -> {
            terminationRequested.countDown();
            return null;
        }).when(owningGroup).terminateProcessor(same(processor));

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        final Future<?> offloadFuture = executorService.submit(() -> context.flowService.offloadNode("held-stop-future"));
        try {
            assertTrue(stopRequested.await(2, TimeUnit.SECONDS), "Expected offload to request processor stop before waiting");
            assertFalse(terminationRequested.await(200, TimeUnit.MILLISECONDS), "Termination should not begin while the aggregate stop future is incomplete");

            stopFuture.complete(null);
            offloadFuture.get(5, TimeUnit.SECONDS);
        } finally {
            stopFuture.complete(null);
            executorService.shutdownNow();
        }

        verify(owningGroup).terminateProcessor(processor);
        verify(context.clusterCoordinator).finishNodeOffload(any(NodeIdentifier.class));
        verifyOffloadStatusTransitions(context.controller);
    }

    @Test
    @Timeout(10)
    public void testOffloadTerminatesLogicallyStoppedProcessorAfterGracefulCompletionEvenWhenPhysicallyStopping() throws Exception {
        final TestContext context = createTestContext("10 secs");
        final ProcessGroup owningGroup = mock(ProcessGroup.class);
        final ProcessorNode processor = createProcessorNode(owningGroup, ScheduledState.STOPPED, ScheduledState.STOPPING);
        when(context.rootGroup.findAllProcessors()).thenReturn(List.of(processor));

        context.flowService.offloadNode("graceful-complete");

        final InOrder inOrder = inOrder(context.rootGroup, owningGroup, context.clusterCoordinator);
        inOrder.verify(context.rootGroup).stopProcessing();
        inOrder.verify(owningGroup).terminateProcessor(processor);
        inOrder.verify(context.clusterCoordinator).finishNodeOffload(any(NodeIdentifier.class));
        verifyOffloadStatusTransitions(context.controller);
    }

    @Test
    @Timeout(10)
    public void testOffloadFallsBackToTerminationWhenGracefulStopTimesOut() throws Exception {
        final TestContext context = createTestContext("0 secs");
        final ProcessGroup owningGroup = mock(ProcessGroup.class);
        final ProcessorNode processor = createProcessorNode(owningGroup, ScheduledState.STOPPED, ScheduledState.RUNNING);
        when(context.rootGroup.stopProcessing()).thenReturn(new CompletableFuture<>());
        when(context.rootGroup.findAllProcessors()).thenReturn(List.of(processor));

        context.flowService.offloadNode("timeout");

        verify(owningGroup).terminateProcessor(processor);
        verify(context.clusterCoordinator).finishNodeOffload(any(NodeIdentifier.class));
        verifyOffloadStatusTransitions(context.controller);
    }

    @Test
    @Timeout(10)
    public void testOffloadFallsBackToTerminationWhenGracefulStopCompletesExceptionally() throws Exception {
        final TestContext context = createTestContext("10 secs");
        final ProcessGroup owningGroup = mock(ProcessGroup.class);
        final ProcessorNode processor = createProcessorNode(owningGroup, ScheduledState.STOPPED, ScheduledState.RUNNING);
        final CompletableFuture<Void> stopFuture = new CompletableFuture<>();
        stopFuture.completeExceptionally(new IllegalStateException("stop failed"));
        when(context.rootGroup.stopProcessing()).thenReturn(stopFuture);
        when(context.rootGroup.findAllProcessors()).thenReturn(List.of(processor));

        context.flowService.offloadNode("exceptional");

        verify(owningGroup).terminateProcessor(processor);
        verify(context.clusterCoordinator).finishNodeOffload(any(NodeIdentifier.class));
        verifyOffloadStatusTransitions(context.controller);
    }

    @Test
    @Timeout(10)
    public void testOffloadFallsBackToTerminationWhenGracefulStopInterruptedAndRestoresInterruptStatusAfterExit() throws Exception {
        final TestContext context = createTestContext("10 secs");
        final ProcessGroup owningGroup = mock(ProcessGroup.class);
        final ProcessorNode processor = createProcessorNode(owningGroup, ScheduledState.STOPPED, ScheduledState.RUNNING);
        when(context.rootGroup.findAllProcessors()).thenReturn(List.of(processor));
        when(context.rootGroup.stopProcessing()).thenAnswer(invocation -> {
            Thread.currentThread().interrupt();
            return new CompletableFuture<>();
        });

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            final Future<Boolean> interruptedAfterReturn = executorService.submit(() -> {
                context.flowService.offloadNode("interrupt-during-grace-wait");
                return Thread.currentThread().isInterrupted();
            });

            assertTrue(interruptedAfterReturn.get(5, TimeUnit.SECONDS), "Offload thread should restore interrupt status only after the critical section exits");
        } finally {
            executorService.shutdownNow();
        }

        verify(owningGroup).terminateProcessor(processor);
        verify(context.clusterCoordinator).finishNodeOffload(any(NodeIdentifier.class));
    }

    @Test
    @Timeout(10)
    public void testOffloadContinuesWhenInterruptedDuringRemoteProcessGroupWaitAndRestoresInterruptStatus() throws Exception {
        final TestContext context = createTestContext("10 secs");
        final RemoteProcessGroup remoteProcessGroup = mock(RemoteProcessGroup.class);
        when(remoteProcessGroup.isTransmitting()).thenReturn(true);
        when(remoteProcessGroup.stopTransmitting()).thenAnswer(invocation -> {
            Thread.currentThread().interrupt();
            return new CompletableFuture<>();
        });
        when(remoteProcessGroup.getCommunicationsTimeout(TimeUnit.MILLISECONDS)).thenReturn(10_000);
        when(context.rootGroup.findAllRemoteProcessGroups()).thenReturn(List.of(remoteProcessGroup));

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            final Future<Boolean> interruptedAfterReturn = executorService.submit(() -> {
                context.flowService.offloadNode("interrupt-during-remote-process-group-wait");
                return Thread.currentThread().isInterrupted();
            });

            assertTrue(interruptedAfterReturn.get(5, TimeUnit.SECONDS));
        } finally {
            executorService.shutdownNow();
        }

        verify(context.clusterCoordinator).finishNodeOffload(any(NodeIdentifier.class));
        verifyOffloadStatusTransitions(context.controller);
    }

    @Test
    @Timeout(10)
    public void testOffloadContinuesQueueDrainingWhenInterruptedDuringQueueWaitAndReachesOffloaded() throws Exception {
        final TestContext context = createTestContext("10 secs");
        final FlowFileQueue queue = mock(FlowFileQueue.class);
        final Connection connection = mock(Connection.class);
        when(connection.getFlowFileQueue()).thenReturn(queue);
        when(context.flowManager.findAllConnections()).thenReturn(Set.of(connection));

        final ProcessGroupStatus queuedStatus = queueStatusWithQueuedCount(1);
        final ProcessGroupStatus drainedStatus = queueStatusWithQueuedCount(0);
        when(context.eventAccess.getControllerStatus()).thenAnswer(invocation -> {
            Thread.currentThread().interrupt();
            return queuedStatus;
        }).thenReturn(drainedStatus);

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            final Future<Boolean> interruptedAfterReturn = executorService.submit(() -> {
                context.flowService.offloadNode("interrupt-during-queue-wait");
                return Thread.currentThread().isInterrupted();
            });

            assertTrue(interruptedAfterReturn.get(5, TimeUnit.SECONDS), "Queue-wait interruption should be restored after offload finishes");
        } finally {
            executorService.shutdownNow();
        }

        verify(queue).offloadQueue();
        verify(queue).resetOffloadedQueue();
        verify(context.eventAccess, times(2)).getControllerStatus();
        verify(context.clusterCoordinator).finishNodeOffload(any(NodeIdentifier.class));
        verifyOffloadStatusTransitions(context.controller);
    }

    @Test
    @Timeout(10)
    public void testAwaitGracefulProcessorStopClassifiesNormalCompletion() {
        final TestContext context = createUncheckedTestContext("10 secs");
        final ProcessorNode stoppedProcessor = createProcessorNode(mock(ProcessGroup.class), ScheduledState.STOPPED, ScheduledState.STOPPED);

        final StandardFlowService.GracefulOffloadResult result = context.flowService.awaitGracefulProcessorStop(context.rootGroup, List.of(stoppedProcessor));

        assertEquals(StandardFlowService.GracefulOffloadOutcome.COMPLETED, result.outcome());
        assertEquals(1, result.processorCount());
        assertEquals(0, result.processorsNotFullyStopped());
        assertNull(result.cause());
        assertTrue(result.elapsedMillis() >= 0);
    }

    @Test
    @Timeout(10)
    public void testAwaitGracefulProcessorStopClassifiesTimeoutAndCountsProcessorsNotFullyStopped() {
        final TestContext context = createUncheckedTestContext("0 secs");
        when(context.rootGroup.stopProcessing()).thenReturn(new CompletableFuture<>());

        final ProcessorNode stoppingProcessor = createProcessorNode(mock(ProcessGroup.class), ScheduledState.STOPPED, ScheduledState.STOPPING);
        final ProcessorNode runningProcessor = createProcessorNode(mock(ProcessGroup.class), ScheduledState.RUNNING, ScheduledState.RUNNING);

        final StandardFlowService.GracefulOffloadResult result = context.flowService.awaitGracefulProcessorStop(context.rootGroup, List.of(stoppingProcessor, runningProcessor));

        assertEquals(StandardFlowService.GracefulOffloadOutcome.TIMED_OUT, result.outcome());
        assertEquals(2, result.processorCount());
        assertEquals(2, result.processorsNotFullyStopped());
        assertNull(result.cause());
        assertTrue(result.elapsedMillis() >= 0);
    }

    @Test
    @Timeout(10)
    public void testAwaitGracefulProcessorStopClassifiesExceptionalCompletionWithCause() {
        final TestContext context = createUncheckedTestContext("10 secs");
        final IllegalStateException failure = new IllegalStateException("stop failed");
        final CompletableFuture<Void> stopFuture = new CompletableFuture<>();
        stopFuture.completeExceptionally(failure);
        when(context.rootGroup.stopProcessing()).thenReturn(stopFuture);

        final StandardFlowService.GracefulOffloadResult result = context.flowService.awaitGracefulProcessorStop(context.rootGroup, List.of());

        assertEquals(StandardFlowService.GracefulOffloadOutcome.EXCEPTIONAL, result.outcome());
        assertEquals(0, result.processorCount());
        assertEquals(0, result.processorsNotFullyStopped());
        assertEquals(failure, result.cause());
        assertTrue(result.elapsedMillis() >= 0);
    }

    @Test
    @Timeout(10)
    public void testAwaitGracefulProcessorStopClassifiesCancellationAsExceptionalCompletion() {
        final TestContext context = createUncheckedTestContext("10 secs");
        final CompletableFuture<Void> stopFuture = new CompletableFuture<>();
        stopFuture.cancel(false);
        when(context.rootGroup.stopProcessing()).thenReturn(stopFuture);

        final StandardFlowService.GracefulOffloadResult result = context.flowService.awaitGracefulProcessorStop(context.rootGroup, List.of());

        assertEquals(StandardFlowService.GracefulOffloadOutcome.EXCEPTIONAL, result.outcome());
        assertTrue(result.cause() instanceof CancellationException);
    }

    @Test
    @Timeout(10)
    public void testAwaitGracefulProcessorStopClassifiesInterruption() throws Exception {
        final TestContext context = createUncheckedTestContext("10 secs");
        when(context.rootGroup.stopProcessing()).thenAnswer(invocation -> {
            Thread.currentThread().interrupt();
            return new CompletableFuture<>();
        });

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        try {
            final Future<StandardFlowService.GracefulOffloadResult> resultFuture = executorService.submit(
                    () -> context.flowService.awaitGracefulProcessorStop(context.rootGroup, List.of()));

            final StandardFlowService.GracefulOffloadResult result = resultFuture.get(5, TimeUnit.SECONDS);
            assertEquals(StandardFlowService.GracefulOffloadOutcome.INTERRUPTED, result.outcome());
            assertEquals(0, result.processorCount());
            assertEquals(0, result.processorsNotFullyStopped());
            assertNull(result.cause());
            assertTrue(result.elapsedMillis() >= 0);
        } finally {
            executorService.shutdownNow();
        }
    }

    @Test
    @Timeout(10)
    public void testAwaitGracefulProcessorStopBoundsIncompleteAggregateFutureWithoutProcessors() {
        final TestContext context = createUncheckedTestContext("0 secs");
        when(context.rootGroup.stopProcessing()).thenReturn(new CompletableFuture<>());

        final StandardFlowService.GracefulOffloadResult result = context.flowService.awaitGracefulProcessorStop(context.rootGroup, List.of());

        assertEquals(StandardFlowService.GracefulOffloadOutcome.TIMED_OUT, result.outcome());
        assertEquals(0, result.processorCount());
        assertEquals(0, result.processorsNotFullyStopped());
        assertNull(result.cause());
    }

    private DisconnectMessage createDisconnectMessage(final String explanation) {
        final NodeIdentifier nodeIdentifier = createNodeIdentifier();
        final DisconnectMessage message = new DisconnectMessage();
        message.setNodeId(nodeIdentifier);
        message.setExplanation(explanation);
        return message;
    }

    private NodeIdentifier createNodeIdentifier() {
        return new NodeIdentifier("node-1", "localhost", 8443, "localhost", 9090, "localhost", 6342, 10443, false);
    }

    private void verifyOffloadStatusTransitions(final FlowController testController) {
        final ArgumentCaptor<NodeConnectionStatus> statusCaptor = ArgumentCaptor.forClass(NodeConnectionStatus.class);
        verify(testController, times(2)).setConnectionStatus(statusCaptor.capture());
        assertEquals(NodeConnectionState.OFFLOADING, statusCaptor.getAllValues().get(0).getState());
        assertEquals(NodeConnectionState.OFFLOADED, statusCaptor.getAllValues().get(1).getState());
    }

    private ProcessGroupStatus queueStatusWithQueuedCount(final int queuedCount) {
        final ProcessGroupStatus status = mock(ProcessGroupStatus.class);
        when(status.getQueuedCount()).thenReturn(queuedCount);
        return status;
    }

    private ProcessorNode createProcessorNode(final ProcessGroup processGroup, final ScheduledState logicalState, final ScheduledState physicalState) {
        final ProcessorNode processorNode = mock(ProcessorNode.class);
        when(processorNode.getProcessGroup()).thenReturn(processGroup);
        when(processorNode.getScheduledState()).thenReturn(logicalState);
        when(processorNode.getPhysicalScheduledState()).thenReturn(physicalState);
        return processorNode;
    }

    private TestContext createUncheckedTestContext(final String gracefulShutdownPeriod) {
        try {
            return createTestContext(gracefulShutdownPeriod);
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private TestContext createTestContext(final String gracefulShutdownPeriod) throws IOException {
        final FlowController testController = mock(FlowController.class);
        final ClusterCoordinator testClusterCoordinator = mock(ClusterCoordinator.class);
        final NodeProtocolSenderListener senderListener = mock(NodeProtocolSenderListener.class);
        final RevisionManager revisionManager = mock(RevisionManager.class);
        final NarManager narManager = mock(NarManager.class);
        final AssetSynchronizer parameterContextAssetSynchronizer = mock(AssetSynchronizer.class);
        final AssetSynchronizer connectorAssetSynchronizer = mock(AssetSynchronizer.class);
        final Authorizer authorizer = mock(Authorizer.class);
        final FlowManager testFlowManager = mock(FlowManager.class);
        final ProcessGroup testRootGroup = mock(ProcessGroup.class);
        final UserAwareEventAccess eventAccess = mock(UserAwareEventAccess.class);

        final StateManagerProvider stateManagerProvider = mock(StateManagerProvider.class);
        final StateManager stateManager = mock(StateManager.class);
        when(stateManager.getState(any(Scope.class))).thenReturn(new MockStateMap(Collections.emptyMap(), 1));
        when(stateManagerProvider.getStateManager(anyString())).thenReturn(stateManager);
        when(testController.getStateManagerProvider()).thenReturn(stateManagerProvider);
        when(testController.getExtensionManager()).thenReturn(mock(ExtensionManager.class));
        when(testController.getFlowManager()).thenReturn(testFlowManager);
        when(testController.getEventAccess()).thenReturn(eventAccess);
        when(testFlowManager.getRootGroup()).thenReturn(testRootGroup);
        when(testFlowManager.findAllConnections()).thenReturn(Collections.emptySet());
        when(testRootGroup.stopProcessing()).thenReturn(CompletableFuture.completedFuture(null));
        when(testRootGroup.findAllProcessors()).thenReturn(Collections.emptyList());
        when(testRootGroup.findAllRemoteProcessGroups()).thenReturn(Collections.<RemoteProcessGroup>emptyList());
        final ProcessGroupStatus drainedStatus = queueStatusWithQueuedCount(0);
        when(eventAccess.getControllerStatus()).thenReturn(drainedStatus);

        final Path flowConfigFile = tempDir.resolve("flow-" + gracefulShutdownPeriod.replace(' ', '-') + ".json.gz");
        final NiFiProperties nifiProperties = NiFiProperties.createBasicNiFiProperties(null, Map.of(
                NiFiProperties.FLOW_CONFIGURATION_FILE, flowConfigFile.toString(),
                NiFiProperties.FLOW_CONTROLLER_GRACEFUL_SHUTDOWN_PERIOD, gracefulShutdownPeriod,
                NiFiProperties.WEB_HTTPS_HOST, "localhost",
                NiFiProperties.WEB_HTTPS_PORT, "8443",
                NiFiProperties.CLUSTER_NODE_ADDRESS, "localhost",
                NiFiProperties.CLUSTER_NODE_PROTOCOL_PORT, "9090",
                NiFiProperties.LOAD_BALANCE_HOST, "localhost",
                NiFiProperties.LOAD_BALANCE_PORT, "6342",
                NiFiProperties.FLOW_CONFIGURATION_ARCHIVE_ENABLED, "false"
        ));

        final StandardFlowService testFlowService = StandardFlowService.createClusteredInstance(testController, nifiProperties, senderListener,
                testClusterCoordinator, revisionManager, narManager, parameterContextAssetSynchronizer,
                connectorAssetSynchronizer, authorizer);

        return new TestContext(testController, testClusterCoordinator, testFlowManager, testRootGroup, eventAccess, testFlowService);
    }

    private static final class TestContext {
        private final FlowController controller;
        private final ClusterCoordinator clusterCoordinator;
        private final FlowManager flowManager;
        private final ProcessGroup rootGroup;
        private final UserAwareEventAccess eventAccess;
        private final StandardFlowService flowService;

        private TestContext(final FlowController controller, final ClusterCoordinator clusterCoordinator, final FlowManager flowManager,
                final ProcessGroup rootGroup, final UserAwareEventAccess eventAccess, final StandardFlowService flowService) {
            this.controller = controller;
            this.clusterCoordinator = clusterCoordinator;
            this.flowManager = flowManager;
            this.rootGroup = rootGroup;
            this.eventAccess = eventAccess;
            this.flowService = flowService;
        }
    }
}
