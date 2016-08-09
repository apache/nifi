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

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.curator.test.TestingServer;
import org.apache.nifi.cluster.coordination.node.ClusterRoles;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Cluster {
    private static final Logger logger = LoggerFactory.getLogger(Cluster.class);

    private final Set<Node> nodes = new HashSet<>();
    private final TestingServer zookeeperServer;

    public Cluster() {
        try {
            zookeeperServer = new TestingServer();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }


    public void start() {
        try {
            zookeeperServer.start();
        } catch (final RuntimeException e) {
            throw e;
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        while (getZooKeeperConnectString() == null) {
            try {
                Thread.sleep(100L);
            } catch (InterruptedException e) {
            }
        }

        logger.info("Start ZooKeeper Server on Port {}, with temporary directory {}", zookeeperServer.getPort(), zookeeperServer.getTempDirectory());
    }

    public void stop() {
        for (final Node node : nodes) {
            try {
                if (node.isRunning()) {
                    node.stop();
                }
            } catch (Exception e) {
                logger.error("Failed to shut down " + node, e);
            }
        }

        try {
            zookeeperServer.stop();
            zookeeperServer.close();
        } catch (final Exception e) {
        }
    }


    public String getZooKeeperConnectString() {
        return zookeeperServer.getConnectString();
    }

    public Set<Node> getNodes() {
        return Collections.unmodifiableSet(nodes);
    }


    public Node createNode() {
        NiFiProperties.getInstance().setProperty(NiFiProperties.ZOOKEEPER_CONNECT_STRING, getZooKeeperConnectString());
        NiFiProperties.getInstance().setProperty(NiFiProperties.CLUSTER_IS_NODE, "true");

        final NiFiProperties properties = NiFiProperties.getInstance().copy();
        final Node node = new Node(properties);
        node.start();
        nodes.add(node);

        return node;
    }

    public Node waitForClusterCoordinator(final long time, final TimeUnit timeUnit) {
        return ClusterUtils.waitUntilNonNull(time, timeUnit,
            () -> getNodes().stream().filter(node -> node.getRoles().contains(ClusterRoles.CLUSTER_COORDINATOR)).findFirst().orElse(null));
    }

    public Node waitForPrimaryNode(final long time, final TimeUnit timeUnit) {
        return ClusterUtils.waitUntilNonNull(time, timeUnit,
            () -> getNodes().stream().filter(node -> node.getRoles().contains(ClusterRoles.PRIMARY_NODE)).findFirst().orElse(null));
    }
}
