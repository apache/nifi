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
package org.apache.nifi.web.api;

import jakarta.ws.rs.HttpMethod;
import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.http.replication.AsyncClusterResponse;
import org.apache.nifi.cluster.coordination.http.replication.RequestReplicator;
import org.apache.nifi.cluster.coordination.node.NodeConnectionState;
import org.apache.nifi.cluster.coordination.node.NodeConnectionStatus;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.web.NiFiServiceFacade;
import org.apache.nifi.web.api.dto.bulkreplay.BulkReplayItemStatus;
import org.apache.nifi.web.api.dto.bulkreplay.BulkReplayJobStatus;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.context.SecurityContextImpl;

import java.lang.reflect.Field;
import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TestBulkReplayExecutionService {

    private BulkReplayExecutionService service;
    private NiFiServiceFacade serviceFacade;
    private BulkReplayJobStore jobStore;

    @BeforeEach
    void setUp() {
        service = new BulkReplayExecutionService();

        final NiFiProperties properties = mock(NiFiProperties.class);
        when(properties.getBulkReplayMaxConcurrent()).thenReturn(1);
        when(properties.getBulkReplayNodeDisconnectTimeout()).thenReturn("5 mins");
        when(properties.getSslPort()).thenReturn(8443);
        service.setProperties(properties);

        serviceFacade = mock(NiFiServiceFacade.class);
        service.setServiceFacade(serviceFacade);

        jobStore = mock(BulkReplayJobStore.class);
        service.setJobStore(jobStore);

        // Set up a SecurityContext so executeJob can capture Authentication
        final Authentication auth = mock(Authentication.class);
        SecurityContextHolder.setContext(new SecurityContextImpl(auth));
    }

    @AfterEach
    void tearDown() {
        service.shutdown();
        SecurityContextHolder.clearContext();
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    private static BulkReplayJob createJob(final String jobId, final int itemCount) {
        final List<BulkReplayJobItem> items = new ArrayList<>();
        for (int i = 0; i < itemCount; i++) {
            items.add(new BulkReplayJobItem(
                    "item-" + i, jobId, i, (long) (100 + i),
                    null, "uuid-" + i, "RECEIVE", "01/01/2024 00:00:00 UTC", "TestProcessor", 0L
            ));
        }
        return new BulkReplayJob(jobId, "Test Job", "user", Instant.now(),
                "proc-1", "TestProcessor", "org.apache.nifi.TestProcessor", "group-1", items);
    }

    private static void awaitTerminalStatus(final BulkReplayJob job, final long timeoutMs) throws InterruptedException {
        final long deadline = System.currentTimeMillis() + timeoutMs;
        while (System.currentTimeMillis() < deadline) {
            final BulkReplayJobStatus status = job.getStatus();
            if (status != BulkReplayJobStatus.QUEUED && status != BulkReplayJobStatus.RUNNING) {
                return;
            }
            Thread.sleep(10);
        }
        throw new AssertionError("Job did not reach terminal status within " + timeoutMs + "ms. Current status: " + job.getStatus());
    }

    private static BulkReplayJob createClusteredJob(final String jobId, final String... nodeIds) {
        final List<BulkReplayJobItem> items = new ArrayList<>();
        for (int i = 0; i < nodeIds.length; i++) {
            items.add(new BulkReplayJobItem(
                    "item-" + i, jobId, i, (long) (100 + i),
                    nodeIds[i], "uuid-" + i, "RECEIVE", "01/01/2024 00:00:00 UTC", "TestProcessor", 1024L
            ));
        }
        return new BulkReplayJob(jobId, "Test Job", "user", Instant.now(),
                "proc-1", "TestProcessor", "org.apache.nifi.TestProcessor", "group-1", items);
    }

    @SuppressWarnings("unchecked")
    private void setPrimaryNodeChecker(final Supplier<Boolean> checker) throws Exception {
        final Field field = BulkReplayExecutionService.class.getDeclaredField("primaryNodeChecker");
        field.setAccessible(true);
        field.set(service, checker);
    }

    private void setDisconnectTimeoutMs(final long ms) throws Exception {
        final Field field = BulkReplayExecutionService.class.getDeclaredField("disconnectTimeoutMs");
        field.setAccessible(true);
        field.set(service, ms);
    }

    private void setNodeCheckPollIntervalMs(final long ms) throws Exception {
        final Field field = BulkReplayExecutionService.class.getDeclaredField("nodeCheckPollIntervalMs");
        field.setAccessible(true);
        field.set(service, ms);
    }

    private NodeIdentifier createNodeIdentifier(final String id) {
        return new NodeIdentifier(id, "localhost", 8080, "localhost", 8081,
                "localhost", 8082, "localhost", 8083, 8084, true, null);
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    @Test
    void testRunJob_allItemsSucceed() throws Exception {
        final BulkReplayJob job = createJob("j1", 3);

        service.executeJob(job);
        awaitTerminalStatus(job, 5000);

        assertEquals(BulkReplayJobStatus.COMPLETED, job.getStatus());
        assertEquals(3, job.getSucceededItems());
        assertEquals(0, job.getFailedItems());
        for (final BulkReplayJobItem item : job.getItems()) {
            assertEquals(BulkReplayItemStatus.SUCCEEDED, item.getStatus());
        }
    }

    @Test
    void testRunJob_allItemsFail() throws Exception {
        when(serviceFacade.submitReplay(anyLong())).thenThrow(new RuntimeException("Replay failed"));

        final BulkReplayJob job = createJob("j2", 3);

        service.executeJob(job);
        awaitTerminalStatus(job, 5000);

        assertEquals(BulkReplayJobStatus.FAILED, job.getStatus());
        assertEquals(0, job.getSucceededItems());
        assertEquals(3, job.getFailedItems());
        for (final BulkReplayJobItem item : job.getItems()) {
            assertEquals(BulkReplayItemStatus.FAILED, item.getStatus());
            assertNotNull(item.getErrorMessage());
        }
    }

    @Test
    void testRunJob_mixedResults() throws Exception {
        // First call succeeds, second and third fail
        when(serviceFacade.submitReplay(100L)).thenReturn(null);
        when(serviceFacade.submitReplay(101L)).thenThrow(new RuntimeException("fail-1"));
        when(serviceFacade.submitReplay(102L)).thenThrow(new RuntimeException("fail-2"));

        final BulkReplayJob job = createJob("j3", 3);

        service.executeJob(job);
        awaitTerminalStatus(job, 5000);

        assertEquals(BulkReplayJobStatus.PARTIAL_SUCCESS, job.getStatus());
        assertEquals(1, job.getSucceededItems());
        assertEquals(2, job.getFailedItems());
    }

    @Test
    void testRunJob_emptyItems() throws Exception {
        final BulkReplayJob job = createJob("j4", 0);

        service.executeJob(job);
        awaitTerminalStatus(job, 5000);

        assertEquals(BulkReplayJobStatus.COMPLETED, job.getStatus());
        assertTrue(job.getStatusMessage().toLowerCase().contains("no items"));
    }

    @Test
    void testRunJob_cancelRequested() throws Exception {
        final BulkReplayJob job = createJob("j5", 3);

        // When first replay is called, set cancel requested
        doAnswer(invocation -> {
            job.setCancelRequested(true);
            return null;
        }).when(serviceFacade).submitReplay(100L);

        service.executeJob(job);
        awaitTerminalStatus(job, 5000);

        assertEquals(BulkReplayJobStatus.CANCELLED, job.getStatus());
        assertEquals(BulkReplayItemStatus.SUCCEEDED, job.getItems().get(0).getStatus());
        assertEquals(BulkReplayItemStatus.SKIPPED, job.getItems().get(1).getStatus());
        assertEquals(BulkReplayItemStatus.SKIPPED, job.getItems().get(2).getStatus());
    }

    @Test
    void testRunJob_cancelRequestedBeforeStart_isHonored() throws Exception {
        final BulkReplayJob job = createJob("j5b", 2);
        job.setCancelRequested(true);

        service.executeJob(job);
        awaitTerminalStatus(job, 5000);

        assertEquals(BulkReplayJobStatus.CANCELLED, job.getStatus());
        assertEquals(BulkReplayItemStatus.SKIPPED, job.getItems().get(0).getStatus());
        assertEquals(BulkReplayItemStatus.SKIPPED, job.getItems().get(1).getStatus());
        verify(serviceFacade, org.mockito.Mockito.never()).submitReplay(anyLong());
    }

    @Test
    void testRunJob_primaryNodeLost() throws Exception {
        final AtomicInteger checkCount = new AtomicInteger(0);
        // First check returns true (primary), second returns false (lost primary)
        setPrimaryNodeChecker(() -> checkCount.incrementAndGet() <= 1);

        final BulkReplayJob job = createJob("j6", 3);

        service.executeJob(job);
        awaitTerminalStatus(job, 5000);

        assertEquals(BulkReplayJobStatus.INTERRUPTED, job.getStatus());
        assertFalse(job.isSubmitted());
        // First item succeeds, remaining are skipped
        assertEquals(BulkReplayItemStatus.SUCCEEDED, job.getItems().get(0).getStatus());
        assertEquals(BulkReplayItemStatus.SKIPPED, job.getItems().get(1).getStatus());
        assertEquals(BulkReplayItemStatus.SKIPPED, job.getItems().get(2).getStatus());
    }

    @Test
    void testRunJob_unexpectedError() throws Exception {
        final AtomicInteger checkCount = new AtomicInteger(0);
        // First primaryNodeChecker call succeeds, second throws
        setPrimaryNodeChecker(() -> {
            if (checkCount.incrementAndGet() > 1) {
                throw new RuntimeException("Cluster coordinator unavailable");
            }
            return true;
        });

        final BulkReplayJob job = createJob("j7", 3);

        service.executeJob(job);
        awaitTerminalStatus(job, 5000);

        assertEquals(BulkReplayJobStatus.FAILED, job.getStatus());
        assertTrue(job.getStatusMessage().contains("Internal error"));
    }

    @Test
    void testRunJob_setsJobTimes() throws Exception {
        final BulkReplayJob job = createJob("j8", 1);

        service.executeJob(job);
        awaitTerminalStatus(job, 5000);

        assertNotNull(job.getStartTime());
        assertNotNull(job.getEndTime());
    }

    @Test
    void testRunJob_setsItemTimes() throws Exception {
        final BulkReplayJob job = createJob("j9", 2);

        service.executeJob(job);
        awaitTerminalStatus(job, 5000);

        for (final BulkReplayJobItem item : job.getItems()) {
            assertNotNull(item.getStartTime(), "Item startTime should be set");
            assertNotNull(item.getEndTime(), "Item endTime should be set");
        }
    }

    @Test
    void testRunJob_progressCounts() throws Exception {
        final BulkReplayJob job = createJob("j10", 3);

        service.executeJob(job);
        awaitTerminalStatus(job, 5000);

        assertEquals(3, job.getProcessedItems());
        assertEquals(100, job.getPercentComplete());
    }

    @Test
    void testExecuteJob_runsAsynchronously() throws Exception {
        final CountDownLatch blockLatch = new CountDownLatch(1);
        final CountDownLatch enteredLatch = new CountDownLatch(1);

        doAnswer(invocation -> {
            enteredLatch.countDown();
            blockLatch.await(5, TimeUnit.SECONDS);
            return null;
        }).when(serviceFacade).submitReplay(anyLong());

        final BulkReplayJob job = createJob("j11", 1);

        service.executeJob(job);

        // executeJob should return immediately; the job should still be running
        assertTrue(enteredLatch.await(5, TimeUnit.SECONDS), "Worker should have started");
        assertTrue(job.getStatus() == BulkReplayJobStatus.RUNNING || job.getStatus() == BulkReplayJobStatus.QUEUED,
                "Job should still be in progress");

        // Unblock the worker
        blockLatch.countDown();
        awaitTerminalStatus(job, 5000);

        assertEquals(BulkReplayJobStatus.COMPLETED, job.getStatus());
    }

    // -----------------------------------------------------------------------
    // Disconnected node tests
    // -----------------------------------------------------------------------

    private ClusterCoordinator setupClusterCoordinator(final NodeIdentifier localNode) {
        final ClusterCoordinator coordinator = mock(ClusterCoordinator.class);
        when(coordinator.getLocalNodeIdentifier()).thenReturn(localNode);
        when(coordinator.getPrimaryNode()).thenReturn(localNode);
        return coordinator;
    }

    @Test
    void testRunJob_disconnectedNode_timesOut_itemsFailedWithoutReplay() throws Exception {
        setDisconnectTimeoutMs(200);
        setNodeCheckPollIntervalMs(50);

        final NodeIdentifier localNode = createNodeIdentifier("local-node");
        final ClusterCoordinator coordinator = setupClusterCoordinator(localNode);

        // Node "remote-node" is never connected
        final Map<NodeConnectionState, List<NodeIdentifier>> stateMap = new HashMap<>();
        stateMap.put(NodeConnectionState.CONNECTED, List.of(localNode));
        when(coordinator.getConnectionStates()).thenReturn(stateMap);

        service.setClusterCoordinator(coordinator);

        // 3 items on the disconnected node — all are deferred in pass 1, then failed in pass 2 after timeout
        final BulkReplayJob job = createClusteredJob("j-dc1", "remote-node", "remote-node", "remote-node");

        service.executeJob(job);
        awaitTerminalStatus(job, 5000);

        assertEquals(BulkReplayJobStatus.FAILED, job.getStatus());
        assertEquals(0, job.getSucceededItems());
        assertEquals(3, job.getFailedItems());
        for (final BulkReplayJobItem item : job.getItems()) {
            assertTrue(item.getErrorMessage().contains("disconnected"), "Error should mention disconnected node");
        }
    }

    @Test
    void testRunJob_disconnectedNode_zeroByteFiles_alsoDeferred() throws Exception {
        setDisconnectTimeoutMs(200);
        setNodeCheckPollIntervalMs(50);

        final NodeIdentifier localNode = createNodeIdentifier("local-node");
        final ClusterCoordinator coordinator = setupClusterCoordinator(localNode);

        // Node "remote-node" is never connected
        final Map<NodeConnectionState, List<NodeIdentifier>> stateMap = new HashMap<>();
        stateMap.put(NodeConnectionState.CONNECTED, List.of(localNode));
        when(coordinator.getConnectionStates()).thenReturn(stateMap);

        service.setClusterCoordinator(coordinator);

        // Zero-byte items are still deferred — all items must replay on their original node regardless of file size
        final List<BulkReplayJobItem> zeroByteItems = new ArrayList<>();
        zeroByteItems.add(new BulkReplayJobItem("item-0", "j-dc1b", 0, 100L, "remote-node", "uuid-0", "RECEIVE", "01/01/2024 00:00:00 UTC", "TestProcessor", 0L));
        zeroByteItems.add(new BulkReplayJobItem("item-1", "j-dc1b", 1, 101L, "remote-node", "uuid-1", "RECEIVE", "01/01/2024 00:00:00 UTC", "TestProcessor", 0L));
        final BulkReplayJob job = new BulkReplayJob("j-dc1b", "Test Job", "user", Instant.now(),
                "proc-1", "TestProcessor", "org.apache.nifi.TestProcessor", "group-1", zeroByteItems);

        service.executeJob(job);
        awaitTerminalStatus(job, 5000);

        assertEquals(BulkReplayJobStatus.FAILED, job.getStatus());
        assertEquals(0, job.getSucceededItems());
        assertEquals(2, job.getFailedItems());
    }

    @Test
    void testRunJob_disconnectedNode_reconnects_itemsSucceed() throws Exception {
        setDisconnectTimeoutMs(2000);
        setNodeCheckPollIntervalMs(50);

        final NodeIdentifier localNode = createNodeIdentifier("local-node");
        final NodeIdentifier remoteNode = createNodeIdentifier("remote-node");
        final ClusterCoordinator coordinator = setupClusterCoordinator(localNode);
        when(coordinator.getNodeIdentifier("remote-node")).thenReturn(remoteNode);

        // Start disconnected, then reconnect after a short delay
        final AtomicBoolean reconnected = new AtomicBoolean(false);
        when(coordinator.getConnectionStatus(remoteNode)).thenAnswer(invocation ->
                new NodeConnectionStatus(remoteNode,
                        reconnected.get() ? NodeConnectionState.CONNECTED : NodeConnectionState.DISCONNECTED));

        when(coordinator.getConnectionStates()).thenAnswer(invocation -> {
            final Map<NodeConnectionState, List<NodeIdentifier>> stateMap = new HashMap<>();
            final List<NodeIdentifier> connected = new ArrayList<>();
            connected.add(localNode);
            if (reconnected.get()) {
                connected.add(remoteNode);
            }
            stateMap.put(NodeConnectionState.CONNECTED, connected);
            return stateMap;
        });

        // Mock RequestReplicator for remote node replays
        final RequestReplicator replicator = mock(RequestReplicator.class);
        final AsyncClusterResponse asyncResponse = mock(AsyncClusterResponse.class);
        final NodeResponse nodeResponse = mock(NodeResponse.class);
        when(nodeResponse.getStatus()).thenReturn(200);
        when(asyncResponse.awaitMergedResponse()).thenReturn(nodeResponse);
        when(replicator.replicate(
                org.mockito.ArgumentMatchers.anySet(),
                org.mockito.ArgumentMatchers.anyString(),
                org.mockito.ArgumentMatchers.any(java.net.URI.class),
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.anyMap(),
                org.mockito.ArgumentMatchers.anyBoolean(),
                org.mockito.ArgumentMatchers.anyBoolean()
        )).thenReturn(asyncResponse);
        service.setRequestReplicator(replicator);

        service.setClusterCoordinator(coordinator);

        // Schedule reconnection after 150ms
        new Thread(() -> {
            try {
                Thread.sleep(150);
                reconnected.set(true);
            } catch (final InterruptedException ignored) {
            }
        }).start();

        final BulkReplayJob job = createClusteredJob("j-dc2", "remote-node", "remote-node");

        service.executeJob(job);
        awaitTerminalStatus(job, 5000);

        assertEquals(BulkReplayJobStatus.COMPLETED, job.getStatus());
        assertEquals(2, job.getSucceededItems());
        assertEquals(0, job.getFailedItems());
    }

    @Test
    void testRunJob_mixedNodes_connectedItemsProcessedFirst_disconnectedDeferred() throws Exception {
        setDisconnectTimeoutMs(200);
        setNodeCheckPollIntervalMs(50);

        final NodeIdentifier localNode = createNodeIdentifier("local-node");
        final ClusterCoordinator coordinator = setupClusterCoordinator(localNode);

        when(coordinator.getNodeIdentifier("local-node")).thenReturn(localNode);
        when(coordinator.getConnectionStatus(localNode))
                .thenReturn(new NodeConnectionStatus(localNode, NodeConnectionState.CONNECTED));

        // Only local node is connected (used by syncItemStatusToCluster)
        final Map<NodeConnectionState, List<NodeIdentifier>> stateMap = new HashMap<>();
        stateMap.put(NodeConnectionState.CONNECTED, List.of(localNode));
        when(coordinator.getConnectionStates()).thenReturn(stateMap);

        service.setClusterCoordinator(coordinator);

        // item 0 on local node, item 1 on remote (disconnected), item 2 on local
        // Connected items should be processed immediately in pass 1; disconnected items deferred to pass 2
        final List<BulkReplayJobItem> items = new ArrayList<>();
        items.add(new BulkReplayJobItem("item-0", "j-dc3", 0, 100L, "local-node", "uuid-0", "RECEIVE", "01/01/2024 00:00:00 UTC", "TestProcessor", 1024L));
        items.add(new BulkReplayJobItem("item-1", "j-dc3", 1, 101L, "remote-node", "uuid-1", "RECEIVE", "01/01/2024 00:00:00 UTC", "TestProcessor", 1024L));
        items.add(new BulkReplayJobItem("item-2", "j-dc3", 2, 102L, "local-node", "uuid-2", "RECEIVE", "01/01/2024 00:00:00 UTC", "TestProcessor", 1024L));

        final BulkReplayJob job = new BulkReplayJob("j-dc3", "Test Job", "user", Instant.now(),
                "proc-1", "TestProcessor", "org.apache.nifi.TestProcessor", "group-1", items);

        service.executeJob(job);
        awaitTerminalStatus(job, 5000);

        assertEquals(BulkReplayJobStatus.PARTIAL_SUCCESS, job.getStatus());
        assertEquals(2, job.getSucceededItems());
        assertEquals(1, job.getFailedItems());
        // Connected items succeed, disconnected item fails with disconnect error (not replay error)
        assertEquals(BulkReplayItemStatus.SUCCEEDED, job.getItems().get(0).getStatus());
        assertEquals(BulkReplayItemStatus.FAILED, job.getItems().get(1).getStatus());
        assertTrue(job.getItems().get(1).getErrorMessage().contains("disconnected"));
        assertEquals(BulkReplayItemStatus.SUCCEEDED, job.getItems().get(2).getStatus());
    }

    @Test
    void testRunJob_multipleDisconnectedNodes_allTimeout() throws Exception {
        setDisconnectTimeoutMs(200);
        setNodeCheckPollIntervalMs(50);

        final NodeIdentifier localNode = createNodeIdentifier("local-node");
        final ClusterCoordinator coordinator = setupClusterCoordinator(localNode);

        final Map<NodeConnectionState, List<NodeIdentifier>> stateMap = new HashMap<>();
        stateMap.put(NodeConnectionState.CONNECTED, List.of(localNode));
        when(coordinator.getConnectionStates()).thenReturn(stateMap);

        // All replays fail (content on disconnected nodes)
        when(serviceFacade.submitReplay(anyLong())).thenThrow(new RuntimeException("Content not found"));

        service.setClusterCoordinator(coordinator);

        // Items on two different disconnected nodes
        final BulkReplayJob job = createClusteredJob("j-dc4", "node-a", "node-b", "node-a", "node-b");

        service.executeJob(job);
        awaitTerminalStatus(job, 10000);

        assertEquals(BulkReplayJobStatus.FAILED, job.getStatus());
        assertEquals(0, job.getSucceededItems());
        assertEquals(4, job.getFailedItems());
    }

    @Test
    void testRunJob_localNodeItems_alwaysSucceed() throws Exception {
        setDisconnectTimeoutMs(100);
        setNodeCheckPollIntervalMs(50);

        final NodeIdentifier localNode = createNodeIdentifier("local-node");
        final ClusterCoordinator coordinator = setupClusterCoordinator(localNode);

        when(coordinator.getNodeIdentifier("local-node")).thenReturn(localNode);
        when(coordinator.getConnectionStatus(localNode))
                .thenReturn(new NodeConnectionStatus(localNode, NodeConnectionState.CONNECTED));

        // Only local node is connected (used by syncItemStatusToCluster)
        final Map<NodeConnectionState, List<NodeIdentifier>> stateMap = new HashMap<>();
        stateMap.put(NodeConnectionState.CONNECTED, List.of(localNode));
        when(coordinator.getConnectionStates()).thenReturn(stateMap);

        service.setClusterCoordinator(coordinator);

        final BulkReplayJob job = createClusteredJob("j-dc5", "local-node", "local-node");

        service.executeJob(job);
        awaitTerminalStatus(job, 5000);

        assertEquals(BulkReplayJobStatus.COMPLETED, job.getStatus());
        assertEquals(2, job.getSucceededItems());
    }

    @Test
    void testRunJob_cancelDuringDisconnectWait() throws Exception {
        setDisconnectTimeoutMs(5000);
        setNodeCheckPollIntervalMs(50);

        final NodeIdentifier localNode = createNodeIdentifier("local-node");
        final ClusterCoordinator coordinator = setupClusterCoordinator(localNode);

        final Map<NodeConnectionState, List<NodeIdentifier>> stateMap = new HashMap<>();
        stateMap.put(NodeConnectionState.CONNECTED, List.of(localNode));
        when(coordinator.getConnectionStates()).thenReturn(stateMap);

        service.setClusterCoordinator(coordinator);

        final BulkReplayJob job = createClusteredJob("j-dc6", "remote-node", "remote-node");

        // Cancel after a short delay while waiting for disconnect
        new Thread(() -> {
            try {
                Thread.sleep(200);
                job.setCancelRequested(true);
            } catch (final InterruptedException ignored) {
            }
        }).start();

        service.executeJob(job);
        awaitTerminalStatus(job, 10000);

        assertEquals(BulkReplayJobStatus.CANCELLED, job.getStatus());
    }

    @Test
    void testRunJob_resumeAfterInterruption_skipsCompletedItems() throws Exception {
        final AtomicInteger replayCount = new AtomicInteger(0);
        doAnswer(invocation -> {
            replayCount.incrementAndGet();
            return null;
        }).when(serviceFacade).submitReplay(anyLong());

        final BulkReplayJob job = createJob("j-resume", 3);

        // Simulate a previous run that completed item 0 and was interrupted.
        // Item 0 = SUCCEEDED, item 1 = SKIPPED (by interruption handler), item 2 = SKIPPED
        job.getItems().get(0).setStatus(BulkReplayItemStatus.SUCCEEDED);
        job.getItems().get(0).setEndTime(Instant.now());
        job.getItems().get(1).setStatus(BulkReplayItemStatus.SKIPPED);
        job.getItems().get(2).setStatus(BulkReplayItemStatus.SKIPPED);

        service.executeJob(job);
        awaitTerminalStatus(job, 5000);

        assertEquals(BulkReplayJobStatus.COMPLETED, job.getStatus());
        // Only items 1 and 2 should have been replayed (not item 0 again)
        assertEquals(2, replayCount.get());
        assertEquals(3, job.getSucceededItems());
        assertEquals(3, job.getProcessedItems());
    }

    @Test
    void testRemoteReplay_uriContainsSchemeAndNodeAddress() throws Exception {
        final NodeIdentifier localNode = createNodeIdentifier("local-node");
        final NodeIdentifier remoteNode = new NodeIdentifier("remote-node", "remote-host", 9443,
                "remote-host", 8081, "remote-host", 8082, "remote-host", 8083, 8084, true, null);
        final ClusterCoordinator coordinator = setupClusterCoordinator(localNode);
        when(coordinator.getNodeIdentifier("remote-node")).thenReturn(remoteNode);
        when(coordinator.getConnectionStatus(remoteNode))
                .thenReturn(new NodeConnectionStatus(remoteNode, NodeConnectionState.CONNECTED));

        final Map<NodeConnectionState, List<NodeIdentifier>> stateMap = new HashMap<>();
        stateMap.put(NodeConnectionState.CONNECTED, List.of(localNode, remoteNode));
        when(coordinator.getConnectionStates()).thenReturn(stateMap);

        final RequestReplicator replicator = mock(RequestReplicator.class);
        final AsyncClusterResponse asyncResponse = mock(AsyncClusterResponse.class);
        final NodeResponse nodeResponse = mock(NodeResponse.class);
        when(nodeResponse.getStatus()).thenReturn(200);
        when(asyncResponse.awaitMergedResponse()).thenReturn(nodeResponse);
        when(replicator.replicate(
                org.mockito.ArgumentMatchers.anySet(),
                org.mockito.ArgumentMatchers.anyString(),
                org.mockito.ArgumentMatchers.any(URI.class),
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.anyMap(),
                org.mockito.ArgumentMatchers.anyBoolean(),
                org.mockito.ArgumentMatchers.anyBoolean()
        )).thenReturn(asyncResponse);

        service.setRequestReplicator(replicator);
        service.setClusterCoordinator(coordinator);

        final BulkReplayJob job = createClusteredJob("j-uri", "remote-node");

        service.executeJob(job);
        awaitTerminalStatus(job, 5000);

        assertEquals(BulkReplayJobStatus.COMPLETED, job.getStatus());

        // Capture the URI passed to the replicator and verify it has a scheme and the target node address
        final org.mockito.ArgumentCaptor<URI> uriCaptor = org.mockito.ArgumentCaptor.forClass(URI.class);
        verify(replicator).replicate(
                org.mockito.ArgumentMatchers.anySet(),
                org.mockito.ArgumentMatchers.eq("POST"),
                uriCaptor.capture(),
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.anyMap(),
                org.mockito.ArgumentMatchers.anyBoolean(),
                org.mockito.ArgumentMatchers.anyBoolean()
        );

        final URI capturedUri = uriCaptor.getValue();
        assertNotNull(capturedUri.getScheme(), "URI must have a scheme");
        assertEquals("https", capturedUri.getScheme());
        assertEquals("remote-host", capturedUri.getHost());
        assertEquals(9443, capturedUri.getPort());
        assertEquals("/nifi-api/provenance-events/replays", capturedUri.getPath());
    }

    @Test
    void testRunJob_syncsItemStatusToCluster() throws Exception {
        final NodeIdentifier localNode = createNodeIdentifier("local-node");
        final NodeIdentifier otherNode = createNodeIdentifier("other-node");
        final ClusterCoordinator coordinator = setupClusterCoordinator(localNode);

        final Map<NodeConnectionState, List<NodeIdentifier>> stateMap = new HashMap<>();
        stateMap.put(NodeConnectionState.CONNECTED, List.of(localNode, otherNode));
        when(coordinator.getConnectionStates()).thenReturn(stateMap);

        final RequestReplicator replicator = mock(RequestReplicator.class);
        final AsyncClusterResponse asyncResponse = mock(AsyncClusterResponse.class);
        final NodeResponse nodeResponse = mock(NodeResponse.class);
        when(nodeResponse.getStatus()).thenReturn(204);
        when(asyncResponse.awaitMergedResponse()).thenReturn(nodeResponse);
        when(replicator.replicate(
                org.mockito.ArgumentMatchers.anySet(),
                org.mockito.ArgumentMatchers.anyString(),
                org.mockito.ArgumentMatchers.any(URI.class),
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.anyMap(),
                org.mockito.ArgumentMatchers.anyBoolean(),
                org.mockito.ArgumentMatchers.anyBoolean()
        )).thenReturn(asyncResponse);

        service.setRequestReplicator(replicator);
        service.setClusterCoordinator(coordinator);

        final BulkReplayJob job = createJob("j-sync", 2);

        service.executeJob(job);
        awaitTerminalStatus(job, 5000);

        assertEquals(BulkReplayJobStatus.COMPLETED, job.getStatus());

        // Verify PUT calls were made for each item (sync to other nodes)
        final org.mockito.ArgumentCaptor<URI> uriCaptor = org.mockito.ArgumentCaptor.forClass(URI.class);
        verify(replicator, org.mockito.Mockito.atLeast(2)).replicate(
                org.mockito.ArgumentMatchers.anySet(),
                org.mockito.ArgumentMatchers.eq(HttpMethod.PUT),
                uriCaptor.capture(),
                org.mockito.ArgumentMatchers.any(),
                org.mockito.ArgumentMatchers.anyMap(),
                org.mockito.ArgumentMatchers.anyBoolean(),
                org.mockito.ArgumentMatchers.anyBoolean()
        );

        // Each sync URI should target the item status endpoint
        for (final URI syncUri : uriCaptor.getAllValues()) {
            assertTrue(syncUri.getPath().contains("/bulk-replay/jobs/j-sync/items/"),
                    "Sync URI should target item status endpoint");
            assertTrue(syncUri.getPath().endsWith("/status"),
                    "Sync URI should end with /status");
        }
    }

    @Test
    void testExecuteJob_maxConcurrentReached_nextJobStaysQueued() throws Exception {
        // maxConcurrent is 1 (set in setUp), so only one job runs at a time.
        final CountDownLatch blockLatch = new CountDownLatch(1);
        final CountDownLatch firstJobStarted = new CountDownLatch(1);

        doAnswer(invocation -> {
            firstJobStarted.countDown();
            blockLatch.await(10, TimeUnit.SECONDS);
            return null;
        }).when(serviceFacade).submitReplay(100L);

        final BulkReplayJob job1 = createJob("j-cap-1", 1);
        final BulkReplayJob job2 = createJob("j-cap-2", 1);

        service.executeJob(job1);
        assertTrue(firstJobStarted.await(5, TimeUnit.SECONDS), "First job should have started");

        // Submit the second job while the first is still running
        service.executeJob(job2);
        Thread.sleep(200);

        // First job should be running, second should still be QUEUED
        assertEquals(BulkReplayJobStatus.RUNNING, job1.getStatus());
        assertEquals(BulkReplayJobStatus.QUEUED, job2.getStatus());

        // Unblock the first job — the second should then execute
        blockLatch.countDown();
        awaitTerminalStatus(job1, 5000);
        awaitTerminalStatus(job2, 5000);

        assertEquals(BulkReplayJobStatus.COMPLETED, job1.getStatus());
        assertEquals(BulkReplayJobStatus.COMPLETED, job2.getStatus());
    }

    @Test
    void testRunJob_crossNodeFailover_resumesWithSyncedProgress() throws Exception {
        final AtomicInteger replayCount = new AtomicInteger(0);
        doAnswer(invocation -> {
            replayCount.incrementAndGet();
            return null;
        }).when(serviceFacade).submitReplay(anyLong());

        // Simulate resuming a job after primary failover.
        // Items 0 and 1 are pre-seeded as SUCCEEDED (synced from the previous primary).
        // Items 2 and 3 are QUEUED and should be replayed.
        final BulkReplayJob job = createJob("j-failover", 4);
        job.getItems().get(0).setStatus(BulkReplayItemStatus.SUCCEEDED);
        job.getItems().get(0).setEndTime(Instant.now());
        job.getItems().get(1).setStatus(BulkReplayItemStatus.SUCCEEDED);
        job.getItems().get(1).setEndTime(Instant.now());
        // Items 2 and 3 remain QUEUED (default)

        service.executeJob(job);
        awaitTerminalStatus(job, 5000);

        assertEquals(BulkReplayJobStatus.COMPLETED, job.getStatus());
        // Only items 2 and 3 should have been replayed
        assertEquals(2, replayCount.get());
        // All 4 items should be counted as processed
        assertEquals(4, job.getProcessedItems());
        assertEquals(4, job.getSucceededItems());
        assertEquals(0, job.getFailedItems());
        assertEquals(100, job.getPercentComplete());
    }

    @Test
    void testRunJob_deleteRequestedBeforeStart_removesJobWithoutReplay() throws Exception {
        final BulkReplayJob job = createJob("j-delete-queued", 2);
        job.setCancelRequested(true);
        job.setDeleteRequested(true);

        service.executeJob(job);
        awaitTerminalStatus(job, 5000);

        assertEquals(BulkReplayJobStatus.CANCELLED, job.getStatus());
        assertEquals("Deleted", job.getStatusMessage());
        verify(serviceFacade, org.mockito.Mockito.never()).submitReplay(anyLong());
        verify(jobStore).remove("j-delete-queued");
    }

    @Test
    void testRunJob_deleteRequestedDuringExecution_removesJobAfterCancellation() throws Exception {
        final BulkReplayJob job = createJob("j-delete-running", 3);

        doAnswer(invocation -> {
            job.setCancelRequested(true);
            job.setDeleteRequested(true);
            return null;
        }).when(serviceFacade).submitReplay(100L);

        service.executeJob(job);
        awaitTerminalStatus(job, 5000);

        assertEquals(BulkReplayJobStatus.CANCELLED, job.getStatus());
        assertEquals("Deleted", job.getStatusMessage());
        assertEquals(BulkReplayItemStatus.SUCCEEDED, job.getItems().get(0).getStatus());
        assertEquals(BulkReplayItemStatus.SKIPPED, job.getItems().get(1).getStatus());
        assertEquals(BulkReplayItemStatus.SKIPPED, job.getItems().get(2).getStatus());
        verify(jobStore).remove("j-delete-running");
    }
}
