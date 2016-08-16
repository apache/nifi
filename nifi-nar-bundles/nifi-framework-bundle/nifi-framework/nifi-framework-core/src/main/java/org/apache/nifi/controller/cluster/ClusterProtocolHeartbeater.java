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
package org.apache.nifi.controller.cluster;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.nifi.cluster.protocol.NodeProtocolSender;
import org.apache.nifi.cluster.protocol.ProtocolException;
import org.apache.nifi.cluster.protocol.message.HeartbeatMessage;
import org.apache.nifi.util.NiFiProperties;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Uses ZooKeeper in order to determine which node is the elected Cluster
 * Coordinator and to indicate that this node is part of the cluster. However,
 * once the Cluster Coordinator is known, heartbeats are sent directly to the
 * Cluster Coordinator.
 */
public class ClusterProtocolHeartbeater implements Heartbeater {

    private static final Logger logger = LoggerFactory.getLogger(ClusterProtocolHeartbeater.class);

    private final NodeProtocolSender protocolSender;
    private final CuratorFramework curatorClient;
    private final String nodesPathPrefix;

    private final String coordinatorPath;
    private volatile String coordinatorAddress;

    public ClusterProtocolHeartbeater(final NodeProtocolSender protocolSender, final NiFiProperties nifiProperties) {
        this.protocolSender = protocolSender;

        final RetryPolicy retryPolicy = new RetryNTimes(10, 500);
        final ZooKeeperClientConfig zkConfig = ZooKeeperClientConfig.createConfig(nifiProperties);

        curatorClient = CuratorFrameworkFactory.newClient(zkConfig.getConnectString(),
                zkConfig.getSessionTimeoutMillis(), zkConfig.getConnectionTimeoutMillis(), retryPolicy);

        curatorClient.start();
        nodesPathPrefix = zkConfig.resolvePath("cluster/nodes");
        coordinatorPath = nodesPathPrefix + "/coordinator";
    }

    @Override
    public String getHeartbeatAddress() throws IOException {
        final String curAddress = coordinatorAddress;
        if (curAddress != null) {
            return curAddress;
        }

        try {
            // Get coordinator address and add watcher to change who we are heartbeating to if the value changes.
            final byte[] coordinatorAddressBytes = curatorClient.getData().usingWatcher(new Watcher() {
                @Override
                public void process(final WatchedEvent event) {
                    coordinatorAddress = null;
                }
            }).forPath(coordinatorPath);
            final String address = coordinatorAddress = new String(coordinatorAddressBytes, StandardCharsets.UTF_8);

            logger.info("Determined that Cluster Coordinator is located at {}; will use this address for sending heartbeat messages", address);
            return address;
        } catch (Exception e) {
            throw new IOException("Unable to determine Cluster Coordinator from ZooKeeper", e);
        }
    }

    @Override
    public synchronized void send(final HeartbeatMessage heartbeatMessage) throws IOException {
        final String heartbeatAddress = getHeartbeatAddress();

        try {
            protocolSender.heartbeat(heartbeatMessage, heartbeatAddress);
        } catch (final ProtocolException pe) {
            // a ProtocolException is likely the result of not being able to communicate
            // with the coordinator. If we do get an IOException communicating with the coordinator,
            // it will be the cause of the Protocol Exception. In this case, set coordinatorAddress
            // to null so that we double-check next time that the coordinator has not changed.
            if (pe.getCause() instanceof IOException) {
                coordinatorAddress = null;
            }

            throw pe;
        }
    }

    @Override
    public void close() throws IOException {
        if (curatorClient != null) {
            curatorClient.close();
        }

        logger.info("ZooKeeper heartbeater closed. Will no longer send Heartbeat messages to ZooKeeper");
    }
}
