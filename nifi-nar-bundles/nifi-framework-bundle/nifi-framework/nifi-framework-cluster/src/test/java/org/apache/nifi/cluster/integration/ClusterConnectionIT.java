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

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.apache.nifi.cluster.coordination.node.ClusterRoles;
import org.apache.nifi.cluster.coordination.node.NodeConnectionState;
import org.junit.BeforeClass;
import org.junit.Test;

public class ClusterConnectionIT {

    @BeforeClass
    public static void setup() {
        System.setProperty("nifi.properties.file.path", "src/test/resources/conf/nifi.properties");
    }

    @Test(timeout = 20000)
    public void testSingleNode() throws InterruptedException {
        final Cluster cluster = new Cluster();
        cluster.start();

        try {
            final Node firstNode = cluster.createNode();
            firstNode.waitUntilConnected(10, TimeUnit.SECONDS);

            firstNode.waitUntilElectedForRole(ClusterRoles.CLUSTER_COORDINATOR, 10, TimeUnit.SECONDS);
            firstNode.waitUntilElectedForRole(ClusterRoles.PRIMARY_NODE, 10, TimeUnit.SECONDS);
        } finally {
            cluster.stop();
        }
    }

    @Test(timeout = 60000)
    public void testThreeNodeCluster() throws InterruptedException {
        final Cluster cluster = new Cluster();
        cluster.start();

        try {
            final Node firstNode = cluster.createNode();
            final Node secondNode = cluster.createNode();
            final Node thirdNode = cluster.createNode();

            firstNode.waitUntilConnected(10, TimeUnit.SECONDS);
            System.out.println("**** Node 1 Connected ****");
            secondNode.waitUntilConnected(10, TimeUnit.SECONDS);
            System.out.println("**** Node 2 Connected ****");
            thirdNode.waitUntilConnected(10, TimeUnit.SECONDS);
            System.out.println("**** Node 3 Connected ****");

            final Node clusterCoordinator = cluster.waitForClusterCoordinator(10, TimeUnit.SECONDS);
            final Node primaryNode = cluster.waitForPrimaryNode(10, TimeUnit.SECONDS);
            System.out.println("\n\n");
            System.out.println("Cluster Coordinator = " + clusterCoordinator);
            System.out.println("Primary Node = " + primaryNode);
            System.out.println("\n\n");
        } finally {
            cluster.stop();
        }
    }

    @Test(timeout = 60000)
    public void testNewCoordinatorElected() throws IOException {
        final Cluster cluster = new Cluster();
        cluster.start();

        try {
            final Node firstNode = cluster.createNode();
            final Node secondNode = cluster.createNode();

            firstNode.waitUntilConnected(10, TimeUnit.SECONDS);
            System.out.println("**** Node 1 Connected ****");
            secondNode.waitUntilConnected(10, TimeUnit.SECONDS);
            System.out.println("**** Node 2 Connected ****");

            final Node clusterCoordinator = cluster.waitForClusterCoordinator(10, TimeUnit.SECONDS);
            clusterCoordinator.stop();

            final Node otherNode = firstNode == clusterCoordinator ? secondNode : firstNode;
            otherNode.waitUntilElectedForRole(ClusterRoles.CLUSTER_COORDINATOR, 10, TimeUnit.SECONDS);
        } finally {
            cluster.stop();
        }
    }

    @Test(timeout = 60000)
    public void testReconnectGetsCorrectClusterTopology() throws IOException {
        final Cluster cluster = new Cluster();
        cluster.start();

        try {
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
            secondNode.stop();

            System.out.println("\n\nNode 2 Shut Down\n\n");

            // wait for node 1 and 3 to recognize that node 2 is gone
            Stream.of(firstNode, thirdNode).forEach(node -> {
                node.assertNodeDisconnects(secondNode.getIdentifier(), 5, TimeUnit.SECONDS);
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
        } finally {
            cluster.stop();
        }
    }


    @Test(timeout = 60000)
    public void testRestartAllNodes() throws IOException {
        final Cluster cluster = new Cluster();
        cluster.start();

        try {
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

            Stream.of(firstNode, secondNode, thirdNode).forEach(node -> {
                node.waitUntilConnected(10, TimeUnit.SECONDS);
            });

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
        } finally {
            cluster.stop();
        }
    }


    @Test(timeout = 30000)
    public void testHeartbeatsMonitored() throws IOException {
        final Cluster cluster = new Cluster();
        cluster.start();

        try {
            final Node firstNode = cluster.createNode();
            final Node secondNode = cluster.createNode();

            firstNode.waitUntilConnected(10, TimeUnit.SECONDS);
            secondNode.waitUntilConnected(10, TimeUnit.SECONDS);

            secondNode.suspendHeartbeating();

            // Heartbeat interval in nifi.properties is set to 1 sec. This means that the node should be kicked out
            // due to lack of heartbeat after 8 times this amount of time, or 8 seconds.
            firstNode.assertNodeDisconnects(secondNode.getIdentifier(), 12, TimeUnit.SECONDS);

            secondNode.resumeHeartbeating();
            firstNode.assertNodeConnects(secondNode.getIdentifier(), 10, TimeUnit.SECONDS);
        } finally {
            cluster.stop();
        }
    }

}
