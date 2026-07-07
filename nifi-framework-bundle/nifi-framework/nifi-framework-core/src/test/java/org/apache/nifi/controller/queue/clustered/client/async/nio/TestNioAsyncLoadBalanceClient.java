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

import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.node.NodeConnectionState;
import org.apache.nifi.cluster.coordination.node.NodeConnectionStatus;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.controller.MockFlowFileRecord;
import org.apache.nifi.controller.queue.LoadBalanceCompression;
import org.apache.nifi.controller.queue.clustered.FlowFileContentAccess;
import org.apache.nifi.controller.queue.clustered.client.StandardLoadBalanceFlowFileCodec;
import org.apache.nifi.controller.queue.clustered.client.async.TransactionFailureCallback;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.apache.nifi.events.EventReporter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestNioAsyncLoadBalanceClient {

    private static final TransactionFailureCallback NOP_FAILURE_CALLBACK = new TransactionFailureCallback() {
        @Override
        public void onTransactionFailed(final List<FlowFileRecord> flowFiles, final Exception cause, final TransactionPhase transactionPhase) {
        }

        @Override
        public boolean isRebalanceOnFailure() {
            return false;
        }
    };

    @Test
    @Timeout(30)
    public void testUnregisterOfInFlightTransactionClosesChannel() throws Exception {
        final ServerSocket serverSocket = new ServerSocket(0);
        final int port = serverSocket.getLocalPort();

        final Thread server = new Thread(() -> {
            try (final Socket socket = serverSocket.accept()) {
                final InputStream in = socket.getInputStream();
                while (in.read() != -1) {
                }
            } catch (final Exception ignored) {
            }
        });
        server.setDaemon(true);
        server.start();

        final NodeIdentifier nodeId = new NodeIdentifier("node-1", "localhost", port, "localhost", port,
            "localhost", port, "localhost", port, port, false);

        final ClusterCoordinator clusterCoordinator = mock(ClusterCoordinator.class);
        when(clusterCoordinator.getConnectionStatus(nodeId)).thenReturn(new NodeConnectionStatus(nodeId, NodeConnectionState.CONNECTED));

        final FlowFileContentAccess contentAccess = ff -> new ByteArrayInputStream(new byte[0]);

        final NioAsyncLoadBalanceClient client = new NioAsyncLoadBalanceClient(nodeId, null, 30000, contentAccess,
            new StandardLoadBalanceFlowFileCodec(), EventReporter.NO_OP, clusterCoordinator);

        try {
            client.start();
            client.register("unit-test-connection", () -> false, () -> new MockFlowFileRecord(5), NOP_FAILURE_CALLBACK,
                (flowFiles, node) -> { }, () -> LoadBalanceCompression.DO_NOT_COMPRESS, () -> true);

            final long deadline = System.currentTimeMillis() + 20_000L;
            while (!client.isConnectionEstablished() && System.currentTimeMillis() < deadline) {
                client.communicate();
                Thread.sleep(10L);
            }
            assertTrue(client.isConnectionEstablished(), "Expected the client to establish a connection and begin a transaction");

            // Unregistering while a transaction is in flight must tear down the (now desynchronized) socket so the
            // next transaction reconnects on a clean stream rather than reusing it.
            client.unregister("unit-test-connection");

            assertFalse(client.isConnectionEstablished(), "Channel must be closed after an in-flight transaction is unregistered");
        } finally {
            client.stop();
            serverSocket.close();
        }
    }
}
