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
package org.apache.nifi.cluster.coordination.node;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.nifi.cluster.exception.NoClusterCoordinatorException;
import org.apache.nifi.cluster.protocol.AbstractNodeProtocolSender;
import org.apache.nifi.cluster.protocol.ProtocolContext;
import org.apache.nifi.cluster.protocol.ProtocolException;
import org.apache.nifi.cluster.protocol.message.ProtocolMessage;
import org.apache.nifi.controller.cluster.ZooKeeperClientConfig;
import org.apache.nifi.io.socket.SocketConfiguration;
import org.apache.nifi.util.NiFiProperties;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Uses Apache Curator to determine the address of the current cluster
 * coordinator
 */
public class CuratorNodeProtocolSender extends AbstractNodeProtocolSender {

    private static final Logger logger = LoggerFactory.getLogger(CuratorNodeProtocolSender.class);

    private final String coordinatorPath;
    private final ZooKeeperClientConfig zkConfig;
    private InetSocketAddress coordinatorAddress;

    public CuratorNodeProtocolSender(final SocketConfiguration socketConfig, final ProtocolContext<ProtocolMessage> protocolContext, final NiFiProperties nifiProperties) {
        super(socketConfig, protocolContext);
        zkConfig = ZooKeeperClientConfig.createConfig(nifiProperties);
        coordinatorPath = zkConfig.resolvePath("cluster/nodes/coordinator");
    }

    @Override
    protected synchronized InetSocketAddress getServiceAddress() throws IOException {
        if (coordinatorAddress != null) {
            return coordinatorAddress;
        }

        final RetryPolicy retryPolicy = new RetryNTimes(0, 0);
        final CuratorFramework curatorClient = CuratorFrameworkFactory.newClient(zkConfig.getConnectString(),
                zkConfig.getSessionTimeoutMillis(), zkConfig.getConnectionTimeoutMillis(), retryPolicy);
        curatorClient.start();

        try {
            // Get coordinator address and add watcher to change who we are heartbeating to if the value changes.
            final byte[] coordinatorAddressBytes = curatorClient.getData().usingWatcher(new Watcher() {
                @Override
                public void process(final WatchedEvent event) {
                    coordinatorAddress = null;
                }
            }).forPath(coordinatorPath);

            if (coordinatorAddressBytes == null || coordinatorAddressBytes.length == 0) {
                throw new NoClusterCoordinatorException("No node has yet been elected Cluster Coordinator. Cannot establish connection to cluster yet.");
            }

            final String address = new String(coordinatorAddressBytes, StandardCharsets.UTF_8);

            final String[] splits = address.split(":");
            if (splits.length != 2) {
                final String message = String.format("Attempted to determine Cluster Coordinator address. Zookeeper indicates "
                        + "that address is %s, but this is not in the expected format of <hostname>:<port>", address);
                logger.error(message);
                throw new ProtocolException(message);
            }

            logger.info("Determined that Cluster Coordinator is located at {}; will use this address for sending heartbeat messages", address);

            final String hostname = splits[0];
            final int port;
            try {
                port = Integer.parseInt(splits[1]);
                if (port < 1 || port > 65535) {
                    throw new NumberFormatException("Port must be in the range of 1 - 65535 but got " + port);
                }
            } catch (final NumberFormatException nfe) {
                final String message = String.format("Attempted to determine Cluster Coordinator address. Zookeeper indicates "
                        + "that address is %s, but the port is not a valid port number", address);
                logger.error(message);
                throw new ProtocolException(message);
            }

            final InetSocketAddress socketAddress = InetSocketAddress.createUnresolved(hostname, port);
            coordinatorAddress = socketAddress;
            return socketAddress;
        } catch (final NoNodeException nne) {
            logger.info("No node has yet been elected Cluster Coordinator. Cannot establish connection to cluster yet.");
            throw new NoClusterCoordinatorException("No node has yet been elected Cluster Coordinator. Cannot establish connection to cluster yet.");
        } catch (final NoClusterCoordinatorException ncce) {
            throw ncce;
        } catch (Exception e) {
            throw new IOException("Unable to determine Cluster Coordinator from ZooKeeper", e);
        } finally {
            curatorClient.close();
        }
    }

}
