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

package org.apache.nifi.cluster.integration;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.nifi.cluster.coordination.node.ClusterRoles;
import org.apache.nifi.cluster.coordination.node.DisconnectionCode;
import org.apache.nifi.cluster.coordination.node.NodeConnectionState;
import org.apache.nifi.cluster.coordination.node.NodeConnectionStatus;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ClusterConnectionIT {
    private Cluster cluster;

    @BeforeClass
    public static void setup() {
        System.setProperty("nifi.properties.file.path", "src/test/resources/conf/nifi.properties");
    }

    @Before
    public void createCluster() throws IOException {
        cluster = new Cluster();
        cluster.start();
    }

    @After
    public void destroyCluster() {
        if (cluster != null) {
            cluster.stop();
        }
    }

    @Test(timeout = 20000)
    public void testSingleNode() throws InterruptedException {
        final Node firstNode = cluster.createNode();
        firstNode.waitUntilConnected(10, TimeUnit.SECONDS);

        firstNode.waitUntilElectedForRole(ClusterRoles.CLUSTER_COORDINATOR, 10, TimeUnit.SECONDS);
        firstNode.waitUntilElectedForRole(ClusterRoles.PRIMARY_NODE, 10, TimeUnit.SECONDS);
    }

    @Test(timeout = 60000)
    public void testThreeNodeCluster() throws InterruptedException {
        cluster.createNode();
        cluster.createNode();
        cluster.createNode();

        cluster.waitUntilAllNodesConnected(10, TimeUnit.SECONDS);

        final Node clusterCoordinator = cluster.waitForClusterCoordinator(10, TimeUnit.SECONDS);
        final Node primaryNode = cluster.waitForPrimaryNode(10, TimeUnit.SECONDS);
        System.out.println("\n\n");
        System.out.println("Cluster Coordinator = " + clusterCoordinator);
        System.out.println("Primary Node = " + primaryNode);
        System.out.println("\n\n");
    }

    @Test(timeout = 60000)
    public void testNewCoordinatorElected() throws IOException {
        final Node firstNode = cluster.createNode();
        final Node secondNode = cluster.createNode();

        cluster.waitUntilAllNodesConnected(10, TimeUnit.SECONDS);

        final Node clusterCoordinator = cluster.waitForClusterCoordinator(10, TimeUnit.SECONDS);
        clusterCoordinator.stop();

        final Node otherNode = firstNode == clusterCoordinator ? secondNode : firstNode;
        otherNode.waitUntilElectedForRole(ClusterRoles.CLUSTER_COORDINATOR, 10, TimeUnit.SECONDS);
    }

    @Test(timeout = 60000)
    public void testReconnectGetsCorrectClusterTopology() throws IOException {
        final Node firstNode = cluster.createNode();
        final Node secondNode = cluster.createNode();
        final Node thirdNode = cluster.createNode();

        cluster.waitUntilAllNodesConnected(10, TimeUnit.SECONDS);

        // shutdown node
        secondNode.stop();

        System.out.println("\n\nNode 2 Shut Down\n\n");

        // wait for node 1 and 3 to recognize that node 2 is gone
        Stream.of(firstNode, thirdNode).forEach(node -> {
            node.assertNodeDisconnects(secondNode.getIdentifier(), 10, TimeUnit.SECONDS);
        });

        // restart node
        secondNode.start();
        System.out.println("\n\nNode 2 Restarted\n\n");

        secondNode.waitUntilConnected(20, TimeUnit.SECONDS);
        System.out.println("\n\nNode 2 Reconnected\n\n");

        // wait for all 3 nodes to agree that node 2 is connected
        Stream.of(firstNode, secondNode, thirdNode).forEach(node -> {
            ClusterUtils.waitUntilConditionMet(5, TimeUnit.SECONDS,
                () -> firstNode.getClusterCoordinator().getConnectionStatus(secondNode.getIdentifier()).getState() == NodeConnectionState.CONNECTED);
        });

        // Ensure that all 3 nodes see a cluster of 3 connected nodes.
        Stream.of(firstNode, secondNode, thirdNode).forEach(node -> {
            node.assertNodeIsConnected(firstNode.getIdentifier());
            node.assertNodeIsConnected(secondNode.getIdentifier());
            node.assertNodeIsConnected(thirdNode.getIdentifier());
        });

        // Ensure that we get both a cluster coordinator and a primary node elected
        cluster.waitForClusterCoordinator(10, TimeUnit.SECONDS);
        cluster.waitForPrimaryNode(10, TimeUnit.SECONDS);
    }

    @Test(timeout = 60000)
    public void testRestartAllNodes() throws IOException, InterruptedException {
        final Node firstNode = cluster.createNode();
        final Node secondNode = cluster.createNode();
        final Node thirdNode = cluster.createNode();

        firstNode.waitUntilConnected(10, TimeUnit.SECONDS);
        System.out.println("**** Node 1 Connected ****");
        secondNode.waitUntilConnected(10, TimeUnit.SECONDS);
        System.out.println("**** Node 2 Connected ****");
        thirdNode.waitUntilConnected(10, TimeUnit.SECONDS);
        System.out.println("**** Node 3 Connected ****");

        // shutdown node
        firstNode.stop();
        secondNode.stop();
        thirdNode.stop();

        System.out.println("\n\nRestarting all nodes\n\n");
        thirdNode.start();
        firstNode.start();
        secondNode.start();


        firstNode.waitUntilConnected(20, TimeUnit.SECONDS);
        System.out.println("\n\n\n**** Node 1 Re-Connected ****\n\n\n");
        secondNode.waitUntilConnected(10, TimeUnit.SECONDS);
        System.out.println("**** Node 2 Re-Connected ****");
        thirdNode.waitUntilConnected(10, TimeUnit.SECONDS);
        System.out.println("**** Node 3 Re-Connected ****");

        // wait for all 3 nodes to agree that node 2 is connected
        Stream.of(firstNode, secondNode, thirdNode).forEach(node -> {
            ClusterUtils.waitUntilConditionMet(5, TimeUnit.SECONDS,
                () -> firstNode.getClusterCoordinator().getConnectionStatus(secondNode.getIdentifier()).getState() == NodeConnectionState.CONNECTED);
        });

        // Ensure that all 3 nodes see a cluster of 3 connected nodes.
        Stream.of(firstNode, secondNode, thirdNode).forEach(node -> {
            node.assertNodeConnects(firstNode.getIdentifier(), 10, TimeUnit.SECONDS);
            node.assertNodeConnects(secondNode.getIdentifier(), 10, TimeUnit.SECONDS);
            node.assertNodeConnects(thirdNode.getIdentifier(), 10, TimeUnit.SECONDS);
        });

        // Ensure that we get both a cluster coordinator and a primary node elected
        cluster.waitForClusterCoordinator(10, TimeUnit.SECONDS);
        cluster.waitForPrimaryNode(10, TimeUnit.SECONDS);
    }


    @Test(timeout = 30000)
    public void testHeartbeatsMonitored() throws IOException {
        final Node firstNode = cluster.createNode();
        final Node secondNode = cluster.createNode();

        cluster.waitUntilAllNodesConnected(10, TimeUnit.SECONDS);

        final Node nodeToSuspend = firstNode;
        final Node otherNode = secondNode;

        nodeToSuspend.suspendHeartbeating();

        // Heartbeat interval in nifi.properties is set to 1 sec. This means that the node should be kicked out
        // due to lack of heartbeat after 8 times this amount of time, or 8 seconds.
        otherNode.assertNodeDisconnects(nodeToSuspend.getIdentifier(), 12, TimeUnit.SECONDS);

        nodeToSuspend.resumeHeartbeating();
        otherNode.assertNodeConnects(nodeToSuspend.getIdentifier(), 10, TimeUnit.SECONDS);
    }

    @Test(timeout = 60000)
    public void testNodeInheritsClusterTopologyOnHeartbeat() throws InterruptedException {
        final Node node1 = cluster.createNode();
        final Node node2 = cluster.createNode();
        final Node node3 = cluster.createNode();

        cluster.waitUntilAllNodesConnected(10, TimeUnit.SECONDS);
        final Node coordinator = cluster.waitForClusterCoordinator(10, TimeUnit.SECONDS);

        final NodeIdentifier node4NotReallyInCluster = new NodeIdentifier(UUID.randomUUID().toString(), "localhost", 9283, "localhost", 9284, "localhost", 9286, "localhost", 9285, null, false, null);

        final Map<NodeIdentifier, NodeConnectionStatus> replacementStatuses = new HashMap<>();
        replacementStatuses.put(node1.getIdentifier(), new NodeConnectionStatus(node1.getIdentifier(), DisconnectionCode.USER_DISCONNECTED));
        replacementStatuses.put(node4NotReallyInCluster, new NodeConnectionStatus(node4NotReallyInCluster, NodeConnectionState.CONNECTING));

        // reset coordinator status so that other nodes with get its now-fake view of the cluster
        coordinator.getClusterCoordinator().resetNodeStatuses(replacementStatuses);
        final List<NodeConnectionStatus> expectedStatuses = coordinator.getClusterCoordinator().getConnectionStatuses();

        // give nodes a bit to heartbeat in. We need to wait long enough that each node heartbeats.
        // But we need to not wait more than 8 seconds because that's when nodes start getting kicked out.
        Thread.sleep(6000L);

        for (final Node node : new Node[] {node1, node2, node3}) {
            assertEquals(expectedStatuses, node.getClusterCoordinator().getConnectionStatuses());
        }
    }
}
