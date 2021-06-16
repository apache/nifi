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
import org.apache.nifi.controller.queue.clustered.client.async.AsyncLoadBalanceClientRegistry;
import org.apache.nifi.controller.queue.clustered.client.async.TransactionCompleteCallback;
import org.apache.nifi.controller.queue.clustered.client.async.TransactionFailureCallback;
import org.apache.nifi.controller.repository.FlowFileRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

public class NioAsyncLoadBalanceClientRegistry implements AsyncLoadBalanceClientRegistry {
    private static final Logger logger = LoggerFactory.getLogger(NioAsyncLoadBalanceClientRegistry.class);

    private final NioAsyncLoadBalanceClientFactory clientFactory;
    private final int clientsPerNode;

    private Map<NodeIdentifier, Set<AsyncLoadBalanceClient>> clientMap = new HashMap<>();
    private Set<AsyncLoadBalanceClient> allClients = new CopyOnWriteArraySet<>();
    private boolean running = false;

    public NioAsyncLoadBalanceClientRegistry(final NioAsyncLoadBalanceClientFactory clientFactory, final int clientsPerNode) {
        this.clientFactory = clientFactory;
        this.clientsPerNode = clientsPerNode;
    }

    @Override
    public synchronized void register(final String connectionId, final NodeIdentifier nodeId, final BooleanSupplier emptySupplier, final Supplier<FlowFileRecord> flowFileSupplier,
                                      final TransactionFailureCallback failureCallback, final TransactionCompleteCallback successCallback,
                                      final Supplier<LoadBalanceCompression> compressionSupplier, final BooleanSupplier honorBackpressureSupplier) {

        Set<AsyncLoadBalanceClient> clients = clientMap.get(nodeId);
        if (clients == null) {
            clients = registerClients(nodeId);
        }

        clients.forEach(client -> client.register(connectionId, emptySupplier, flowFileSupplier, failureCallback, successCallback, compressionSupplier, honorBackpressureSupplier));
        logger.debug("Registered Connection with ID {} to send to Node {}", connectionId, nodeId);
    }


    @Override
    public synchronized void unregister(final String connectionId, final NodeIdentifier nodeId) {
        final Set<AsyncLoadBalanceClient> clients = clientMap.get(nodeId);
        if (clients == null) {
            return;
        }

        final Set<AsyncLoadBalanceClient> toRemove = new HashSet<>();
        for (final AsyncLoadBalanceClient client : clients) {
            client.unregister(connectionId);
            if (client.getRegisteredConnectionCount() == 0) {
                toRemove.add(client);
            }
        }

        clients.removeAll(toRemove);
        allClients.removeAll(toRemove);

        if (clients.isEmpty()) {
            clientMap.remove(nodeId);
        }

        logger.debug("Un-registered Connection with ID {} so that it will no longer send data to Node {}; {} clients were removed", connectionId, nodeId, toRemove.size());
    }

    private Set<AsyncLoadBalanceClient> registerClients(final NodeIdentifier nodeId) {
        final Set<AsyncLoadBalanceClient> clients = new HashSet<>();

        for (int i=0; i < clientsPerNode; i++) {
            final AsyncLoadBalanceClient client = clientFactory.createClient(nodeId);
            clients.add(client);

            logger.debug("Added client {} for communicating with Node {}", client, nodeId);
        }

        clientMap.put(nodeId, clients);
        allClients.addAll(clients);

        if (running) {
            clients.forEach(AsyncLoadBalanceClient::start);
        }

        return clients;
    }

    public synchronized Set<AsyncLoadBalanceClient> getAllClients() {
        return allClients;
    }

    public synchronized void start() {
        if (running) {
            return;
        }

        running = true;
        allClients.forEach(AsyncLoadBalanceClient::start);
    }

    public synchronized void stop() {
        if (!running) {
            return;
        }

        running = false;
        allClients.forEach(AsyncLoadBalanceClient::stop);
    }
}
