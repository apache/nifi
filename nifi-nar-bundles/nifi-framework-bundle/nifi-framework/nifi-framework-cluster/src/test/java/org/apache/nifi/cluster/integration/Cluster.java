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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.test.TestingServer;
import org.apache.nifi.cluster.coordination.flow.FlowElection;
import org.apache.nifi.cluster.coordination.flow.PopularVoteFlowElection;
import org.apache.nifi.cluster.coordination.node.ClusterRoles;
import org.apache.nifi.encrypt.StringEncryptor;
import org.apache.nifi.fingerprint.FingerprintFactory;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Cluster {
    private static final Logger logger = LoggerFactory.getLogger(Cluster.class);

    private final Set<Node> nodes = new HashSet<>();
    private final TestingServer zookeeperServer;

    private final long flowElectionTimeoutMillis;
    private final Integer flowElectionMaxNodes;

    public Cluster() throws IOException {
        this(3, TimeUnit.SECONDS, 3);
    }

    public Cluster(final long flowElectionTimeout, final TimeUnit flowElectionTimeUnit, final Integer flowElectionMaxNodes) throws IOException {
        try {
            zookeeperServer = new TestingServer();
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }

        this.flowElectionTimeoutMillis = flowElectionTimeUnit.toMillis(flowElectionTimeout);
        this.flowElectionMaxNodes = flowElectionMaxNodes;
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

    public CuratorFramework createCuratorClient() {
        final RetryPolicy retryPolicy = new RetryNTimes(20, 500);
        final CuratorFramework curatorClient = CuratorFrameworkFactory.builder()
            .connectString(getZooKeeperConnectString())
            .sessionTimeoutMs(3000)
            .connectionTimeoutMs(3000)
            .retryPolicy(retryPolicy)
            .defaultData(new byte[0])
            .build();

        curatorClient.start();
        return curatorClient;
    }

    public Node createNode() {
        final Map<String, String> addProps = new HashMap<>();
        addProps.put(NiFiProperties.ZOOKEEPER_CONNECT_STRING, getZooKeeperConnectString());
        addProps.put(NiFiProperties.CLUSTER_IS_NODE, "true");

        final NiFiProperties nifiProperties = NiFiProperties.createBasicNiFiProperties("src/test/resources/conf/nifi.properties", addProps);

        final FingerprintFactory fingerprintFactory = new FingerprintFactory(StringEncryptor.createEncryptor(nifiProperties));
        final FlowElection flowElection = new PopularVoteFlowElection(flowElectionTimeoutMillis, TimeUnit.MILLISECONDS, flowElectionMaxNodes, fingerprintFactory);

        final Node node = new Node(nifiProperties, flowElection);
        node.start();
        nodes.add(node);

        return node;
    }

    public Node waitForClusterCoordinator(final long time, final TimeUnit timeUnit) {
        return ClusterUtils.waitUntilNonNull(time, timeUnit,
            () -> getNodes().stream().filter(node -> node.hasRole(ClusterRoles.CLUSTER_COORDINATOR)).findFirst().orElse(null));
    }

    public Node waitForPrimaryNode(final long time, final TimeUnit timeUnit) {
        return ClusterUtils.waitUntilNonNull(time, timeUnit,
            () -> getNodes().stream().filter(node -> node.hasRole(ClusterRoles.PRIMARY_NODE)).findFirst().orElse(null));
    }

    /**
     * Waits for each node in the cluster to connect. The time given is the maximum amount of time to wait for each node to connect, not for
     * the entire cluster to connect.
     *
     * @param time the max amount of time to wait for a node to connect
     * @param timeUnit the unit of time that the given <code>time</code> value represents
     */
    public void waitUntilAllNodesConnected(final long time, final TimeUnit timeUnit) {
        for (final Node node : nodes) {
            node.waitUntilConnected(time, timeUnit);
        }
    }
}
