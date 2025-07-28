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

package org.apache.nifi.cluster.lifecycle;

import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.node.DisconnectionCode;
import org.apache.nifi.cluster.coordination.node.NodeConnectionState;
import org.apache.nifi.cluster.coordination.node.NodeConnectionStatus;
import org.apache.nifi.cluster.coordination.node.OffloadCode;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.controller.DecommissionTask;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.FlowController.GroupStatusCounts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ClusterDecommissionTask implements DecommissionTask {
    private static final Logger logger = LoggerFactory.getLogger(ClusterDecommissionTask.class);
    private static final int DELAY_SECONDS = 3;

    private final ClusterCoordinator clusterCoordinator;
    private final FlowController flowController;
    private NodeIdentifier localNodeIdentifier;

    public ClusterDecommissionTask(final ClusterCoordinator clusterCoordinator, final FlowController flowController) {
        this.clusterCoordinator = clusterCoordinator;
        this.flowController = flowController;
    }

    @Override
    public synchronized void decommission() throws InterruptedException {
        if (clusterCoordinator == null) {
            throw new IllegalStateException("Cannot decommission Node because it is not part of a cluster");
        }

        logger.info("Decommissioning Node...");
        localNodeIdentifier = clusterCoordinator.getLocalNodeIdentifier();
        if (localNodeIdentifier == null) {
            throw new IllegalStateException("Node has not yet connected to the cluster");
        }

        final boolean wasCoordinator = clusterCoordinator.isActiveClusterCoordinator();
        final NodeIdentifier coordinatorId = clusterCoordinator.getElectedActiveCoordinatorNode();
        if (wasCoordinator) {
            logger.info("Decommissioning Cluster Coordinator [{}]...", coordinatorId);
        }

        flowController.stopHeartbeating();
        flowController.setClustered(false, null);
        logger.info("Instructed FlowController to stop sending heartbeats to Cluster Coordinator and take Cluster Disconnect actions");

        disconnectNode();
        logger.info("Requested that node be disconnected from cluster");

        waitForDisconnection();
        logger.info("Successfully disconnected node from cluster");

        if (wasCoordinator) {
            waitForNotActiveClusterCoordinator(coordinatorId);
        }

        offloadNode();
        logger.info("Successfully triggered Node Offload. Will wait for offload to complete");

        waitForOffloadToFinish();
        logger.info("Offload has successfully completed.");

        removeFromCluster();
        logger.info("Requested that node be removed from cluster.");

        waitForRemoval();
        logger.info("Node successfully removed from cluster. Decommission is complete.");
    }

    private void disconnectNode() throws InterruptedException {
        logger.info("Requesting that Node disconnect from cluster");

        while (true) {
            final Future<Void> future = clusterCoordinator.requestNodeDisconnect(localNodeIdentifier, DisconnectionCode.USER_DISCONNECTED, "Node is being decommissioned");
            try {
                future.get();
                return;
            } catch (final ExecutionException e) {
                final Throwable cause = e.getCause();
                logger.error("Failed when attempting to disconnect node from cluster", cause);
            }
        }
    }

    private void waitForDisconnection() throws InterruptedException {
        logger.info("Waiting for Node to be completely disconnected from cluster");
        waitForState(Collections.singleton(NodeConnectionState.DISCONNECTED));
    }

    private void offloadNode() throws InterruptedException {
        logger.info("Requesting that Node be offloaded");

        while (true) {
            final Future<Void> future = clusterCoordinator.requestNodeOffload(localNodeIdentifier, OffloadCode.OFFLOADED, "Node is being decommissioned");
            try {
                future.get();
                break;
            } catch (final ExecutionException e) {
                final Throwable cause = e.getCause();
                logger.error("Failed when attempting to disconnect node from cluster", cause);
            }
        }

        // Wait until status changes to either OFFLOADING or OFFLOADED.
        waitForState(new HashSet<>(Arrays.asList(NodeConnectionState.OFFLOADING, NodeConnectionState.OFFLOADED)));
    }

    private void waitForState(final Set<NodeConnectionState> acceptableStates) throws InterruptedException {
        while (true) {
            final NodeConnectionStatus status = clusterCoordinator.getConnectionStatus(localNodeIdentifier);
            final NodeConnectionState state = status.getState();
            logger.debug("Node state is {}", state);

            if (acceptableStates.contains(state)) {
                return;
            }

            TimeUnit.SECONDS.sleep(DELAY_SECONDS);
        }
    }

    private void waitForOffloadToFinish() throws InterruptedException {
        logger.info("Waiting for Node to finish offloading");

        int iterations = 0;
        while (true) {
            final NodeConnectionStatus status = clusterCoordinator.getConnectionStatus(localNodeIdentifier);
            final NodeConnectionState state = status.getState();
            if (state == NodeConnectionState.OFFLOADED) {
                return;
            }

            if (state != NodeConnectionState.OFFLOADING) {
                throw new IllegalStateException("Expected state of Node to be OFFLOADING but Node is now in a state of " + state);
            }

            // Every 10th iteration log how many FlowFiles are left
            if (++iterations % 10 == 0) {
                final GroupStatusCounts statusCounts = flowController.getGroupStatusCounts(flowController.getFlowManager().getRootGroup());
                final int flowFileCount = statusCounts.getQueuedCount();
                final long byteCount = statusCounts.getQueuedContentSize();
                logger.info("Node state is OFFLOADING. Currently, there are {} FlowFiles ({} bytes) left on node.", flowFileCount, byteCount);
            } else {
                logger.debug("Node state is OFFLOADING. Will wait {} seconds and check again", DELAY_SECONDS);
            }

            TimeUnit.SECONDS.sleep(DELAY_SECONDS);
        }
    }

    private void removeFromCluster() {
        clusterCoordinator.removeNode(localNodeIdentifier, "<Local Decommission>");
    }

    private void waitForRemoval() throws InterruptedException {
        logger.info("Waiting for Node to be completely removed from cluster");

        while (true) {
            final NodeConnectionStatus status = clusterCoordinator.getConnectionStatus(localNodeIdentifier);
            if (status == null) {
                return;
            }

            final NodeConnectionState state = status.getState();
            if (state == NodeConnectionState.REMOVED) {
                return;
            }

            logger.debug("Node state is {}. Will wait {} seconds and check again", state, DELAY_SECONDS);
            TimeUnit.SECONDS.sleep(DELAY_SECONDS);
        }
    }

    private void waitForNotActiveClusterCoordinator(final NodeIdentifier nodeIdentifier) throws InterruptedException {
        logger.info("Waiting to no longer be the Active Cluster Coordinator");

        // We check getElectedActiveCoordinatorNode here instead of calling isActiveClusterCoordinator because in the case of Kubernetes leader election,
        // isActiveClusterCoordinator will return false since the current node unregistered from election for the coordinator role, but it still may hold
        // the lease in Kubernetes, so we need to check the elected coordinator node id directly
        while (true) {
            final NodeIdentifier coordinatorId = clusterCoordinator.getElectedActiveCoordinatorNode();
            logger.debug("Elected Cluster Coordinator is {}", coordinatorId);

            if (coordinatorId != null && !coordinatorId.equals(nodeIdentifier)) {
                logger.info("New Cluster Coordinator elected: {}", clusterCoordinator.getElectedActiveCoordinatorNode());
                return;
            }

            logger.debug("Still considered the Active Cluster Coordinator. Will wait {} seconds and check again", DELAY_SECONDS);
            TimeUnit.SECONDS.sleep(DELAY_SECONDS);
        }
    }

}
