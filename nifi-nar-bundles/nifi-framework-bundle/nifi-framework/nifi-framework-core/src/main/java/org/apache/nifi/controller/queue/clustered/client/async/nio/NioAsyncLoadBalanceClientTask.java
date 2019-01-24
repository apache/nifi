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
import org.apache.nifi.controller.queue.clustered.client.async.AsyncLoadBalanceClient;
import org.apache.nifi.events.EventReporter;
import org.apache.nifi.reporting.Severity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NioAsyncLoadBalanceClientTask implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(NioAsyncLoadBalanceClientTask.class);
    private static final String EVENT_CATEGORY = "Load-Balanced Connection";

    private final NioAsyncLoadBalanceClientRegistry clientRegistry;
    private final ClusterCoordinator clusterCoordinator;
    private final EventReporter eventReporter;
    private volatile boolean running = true;

    public NioAsyncLoadBalanceClientTask(final NioAsyncLoadBalanceClientRegistry clientRegistry, final ClusterCoordinator clusterCoordinator, final EventReporter eventReporter) {
        this.clientRegistry = clientRegistry;
        this.clusterCoordinator = clusterCoordinator;
        this.eventReporter = eventReporter;
    }

    @Override
    public void run() {
        while (running) {
            try {
                boolean success = false;
                for (final AsyncLoadBalanceClient client : clientRegistry.getAllClients()) {
                    if (!client.isRunning()) {
                        logger.trace("Client {} is not running so will not communicate with it", client);
                        continue;
                    }

                    if (client.isPenalized()) {
                        logger.trace("Client {} is penalized so will not communicate with it", client);
                        continue;
                    }

                    final NodeIdentifier clientNodeId = client.getNodeIdentifier();
                    final NodeConnectionStatus connectionStatus = clusterCoordinator.getConnectionStatus(clientNodeId);
                    if (connectionStatus == null) {
                        logger.debug("Could not determine Connection Status for Node with ID {}; will not communicate with it", clientNodeId);
                        continue;
                    }

                    final NodeConnectionState connectionState = connectionStatus.getState();
                    if (connectionState != NodeConnectionState.CONNECTED) {
                        logger.debug("Notifying Client {} that node is not connected because current state is {}", client, connectionState);
                        client.nodeDisconnected();
                        continue;
                    }

                    try {
                        while (client.communicate()) {
                            success = true;
                            logger.trace("Client {} was able to make progress communicating with peer. Will continue to communicate with peer.", client);
                        }
                    } catch (final Exception e) {
                        eventReporter.reportEvent(Severity.ERROR, EVENT_CATEGORY, "Failed to communicate with Peer "
                                + client.getNodeIdentifier() + " while trying to load balance data across the cluster due to " + e.toString());
                        logger.error("Failed to communicate with Peer {} while trying to load balance data across the cluster.", client.getNodeIdentifier(), e);
                    }

                    logger.trace("Client {} was no longer able to make progress communicating with peer. Will move on to the next client", client);
                }

                if (!success) {
                    logger.trace("Was unable to communicate with any client. Will sleep for 10 milliseconds.");
                    Thread.sleep(10L);
                }
            } catch (final Exception e) {
                logger.error("Failed to communicate with peer while trying to load balance data across the cluster", e);
                eventReporter.reportEvent(Severity.ERROR, EVENT_CATEGORY, "Failed to comunicate with Peer while trying to load balance data across the cluster due to " + e);
            }
        }
    }

    public void stop() {
        running = false;
    }
}
