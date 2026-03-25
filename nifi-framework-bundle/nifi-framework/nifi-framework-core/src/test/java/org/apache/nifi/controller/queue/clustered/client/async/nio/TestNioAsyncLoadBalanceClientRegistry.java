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

package org.apache.nifi.controller.queue.clustered.client.async.nio;

import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.controller.queue.LoadBalanceCompression;
import org.apache.nifi.controller.queue.clustered.client.async.AsyncLoadBalanceClient;
import org.apache.nifi.controller.queue.clustered.client.async.TransactionCompleteCallback;
import org.apache.nifi.controller.queue.clustered.client.async.TransactionFailureCallback;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TestNioAsyncLoadBalanceClientRegistry {

    private static final String NODE_UUID = "node-1-uuid";
    private static final String API_ADDRESS = "localhost";
    private static final int API_PORT = 5672;
    private static final String SOCKET_ADDRESS = "localhost";
    private static final int SOCKET_PORT = 48101;

    private static final BooleanSupplier EMPTY_SUPPLIER = () -> true;
    private static final Supplier<FlowFileRecord> FLOW_FILE_SUPPLIER = () -> null;
    private static final TransactionFailureCallback FAILURE_CALLBACK = new TransactionFailureCallback() {
        @Override
        public void onTransactionFailed(final List<FlowFileRecord> flowFiles, final Exception cause, final TransactionPhase transactionPhase) {
        }

        @Override
        public boolean isRebalanceOnFailure() {
            return false;
        }
    };
    private static final TransactionCompleteCallback SUCCESS_CALLBACK = (flowFiles, nodeId) -> { };
    private static final Supplier<LoadBalanceCompression> COMPRESSION_SUPPLIER = () -> LoadBalanceCompression.DO_NOT_COMPRESS;
    private static final BooleanSupplier BACKPRESSURE_SUPPLIER = () -> true;

    private List<StubAsyncLoadBalanceClient> createdClients;
    private NioAsyncLoadBalanceClientRegistry registry;

    @BeforeEach
    void setUp() {
        createdClients = new ArrayList<>();

        final NioAsyncLoadBalanceClientFactory factory = new NioAsyncLoadBalanceClientFactory(null, 30000, null, null, null, null) {
            @Override
            public NioAsyncLoadBalanceClient createClient(final NodeIdentifier nodeIdentifier) {
                final StubAsyncLoadBalanceClient client = new StubAsyncLoadBalanceClient(nodeIdentifier);
                createdClients.add(client);
                return client;
            }
        };

        registry = new NioAsyncLoadBalanceClientRegistry(factory, 1);
    }

    @Test
    void testRegisterCreatesNewClientForNewNode() {
        final NodeIdentifier nodeId = createNodeId("localhost", 6342);
        registry.register("conn-1", nodeId, EMPTY_SUPPLIER, FLOW_FILE_SUPPLIER,
                FAILURE_CALLBACK, SUCCESS_CALLBACK, COMPRESSION_SUPPLIER, BACKPRESSURE_SUPPLIER);

        assertEquals(1, createdClients.size());
        assertEquals(6342, createdClients.getFirst().getNodeIdentifier().getLoadBalancePort());
    }

    @Test
    void testRegisterReusesExistingClientForSamePort() {
        final NodeIdentifier nodeId = createNodeId("localhost", 6342);
        registry.register("conn-1", nodeId, EMPTY_SUPPLIER, FLOW_FILE_SUPPLIER,
                FAILURE_CALLBACK, SUCCESS_CALLBACK, COMPRESSION_SUPPLIER, BACKPRESSURE_SUPPLIER);
        registry.register("conn-2", nodeId, EMPTY_SUPPLIER, FLOW_FILE_SUPPLIER,
                FAILURE_CALLBACK, SUCCESS_CALLBACK, COMPRESSION_SUPPLIER, BACKPRESSURE_SUPPLIER);

        assertEquals(1, createdClients.size());
    }

    @Test
    void testRegisterReplacesClientOnPortChange() {
        final NodeIdentifier originalNodeId = createNodeId("localhost", 6342);
        registry.register("conn-1", originalNodeId, EMPTY_SUPPLIER, FLOW_FILE_SUPPLIER,
                FAILURE_CALLBACK, SUCCESS_CALLBACK, COMPRESSION_SUPPLIER, BACKPRESSURE_SUPPLIER);

        assertEquals(1, createdClients.size());
        final StubAsyncLoadBalanceClient originalClient = createdClients.getFirst();
        assertEquals(6342, originalClient.getNodeIdentifier().getLoadBalancePort());

        final NodeIdentifier updatedNodeId = createNodeId("localhost", 7676);
        registry.register("conn-1", updatedNodeId, EMPTY_SUPPLIER, FLOW_FILE_SUPPLIER,
                FAILURE_CALLBACK, SUCCESS_CALLBACK, COMPRESSION_SUPPLIER, BACKPRESSURE_SUPPLIER);

        assertEquals(2, createdClients.size());
        final StubAsyncLoadBalanceClient newClient = createdClients.get(1);
        assertEquals(7676, newClient.getNodeIdentifier().getLoadBalancePort());
        assertTrue(originalClient.stopped, "Original client should have been stopped");

        final Set<AsyncLoadBalanceClient> allClients = registry.getAllClients();
        assertEquals(1, allClients.size());
        assertNotEquals(originalClient, allClients.iterator().next());
    }

    @Test
    void testRegisterReplacesClientOnAddressChange() {
        final NodeIdentifier originalNodeId = createNodeId("localhost", 6342);
        registry.register("conn-1", originalNodeId, EMPTY_SUPPLIER, FLOW_FILE_SUPPLIER,
                FAILURE_CALLBACK, SUCCESS_CALLBACK, COMPRESSION_SUPPLIER, BACKPRESSURE_SUPPLIER);

        final NodeIdentifier updatedNodeId = createNodeId("127.0.0.1", 6342);
        registry.register("conn-1", updatedNodeId, EMPTY_SUPPLIER, FLOW_FILE_SUPPLIER,
                FAILURE_CALLBACK, SUCCESS_CALLBACK, COMPRESSION_SUPPLIER, BACKPRESSURE_SUPPLIER);

        assertEquals(2, createdClients.size());
        assertEquals("127.0.0.1", createdClients.get(1).getNodeIdentifier().getLoadBalanceAddress());
        assertTrue(createdClients.getFirst().stopped);
    }

    @Test
    void testStartedRegistryStartsReplacementClients() {
        registry.start();

        final NodeIdentifier originalNodeId = createNodeId("localhost", 6342);
        registry.register("conn-1", originalNodeId, EMPTY_SUPPLIER, FLOW_FILE_SUPPLIER,
                FAILURE_CALLBACK, SUCCESS_CALLBACK, COMPRESSION_SUPPLIER, BACKPRESSURE_SUPPLIER);

        final NodeIdentifier updatedNodeId = createNodeId("localhost", 7676);
        registry.register("conn-1", updatedNodeId, EMPTY_SUPPLIER, FLOW_FILE_SUPPLIER,
                FAILURE_CALLBACK, SUCCESS_CALLBACK, COMPRESSION_SUPPLIER, BACKPRESSURE_SUPPLIER);

        final StubAsyncLoadBalanceClient newClient = createdClients.get(1);
        assertTrue(newClient.started, "Replacement client should be started when registry is running");
    }

    private NodeIdentifier createNodeId(final String lbAddress, final int lbPort) {
        return new NodeIdentifier(NODE_UUID, API_ADDRESS, API_PORT, SOCKET_ADDRESS, SOCKET_PORT,
                lbAddress, lbPort, null, null, null, false);
    }

    private static class StubAsyncLoadBalanceClient extends NioAsyncLoadBalanceClient {
        boolean started = false;
        boolean stopped = false;

        StubAsyncLoadBalanceClient(final NodeIdentifier nodeIdentifier) {
            super(nodeIdentifier, null, 30000, null, null, null, null);
        }

        @Override
        public void start() {
            started = true;
        }

        @Override
        public void stop() {
            stopped = true;
        }

        @Override
        public void register(final String connectionId, final BooleanSupplier emptySupplier,
                             final Supplier<FlowFileRecord> flowFileSupplier,
                             final TransactionFailureCallback failureCallback,
                             final TransactionCompleteCallback successCallback,
                             final Supplier<LoadBalanceCompression> compressionSupplier,
                             final BooleanSupplier honorBackpressureSupplier) {
        }

        @Override
        public void unregister(final String connectionId) {
        }

        @Override
        public int getRegisteredConnectionCount() {
            return 0;
        }

        @Override
        public boolean isRunning() {
            return started && !stopped;
        }

        @Override
        public boolean isPenalized() {
            return false;
        }
    }
}
