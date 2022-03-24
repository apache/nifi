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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.cluster.exception.NoClusterCoordinatorException;
import org.apache.nifi.cluster.protocol.AbstractNodeProtocolSender;
import org.apache.nifi.cluster.protocol.ProtocolContext;
import org.apache.nifi.cluster.protocol.ProtocolException;
import org.apache.nifi.cluster.protocol.message.ProtocolMessage;
import org.apache.nifi.controller.leader.election.LeaderElectionManager;
import org.apache.nifi.io.socket.SocketConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaderElectionNodeProtocolSender extends AbstractNodeProtocolSender {
    private static final Logger logger = LoggerFactory.getLogger(LeaderElectionNodeProtocolSender.class);

    private final LeaderElectionManager electionManager;

    public LeaderElectionNodeProtocolSender(final SocketConfiguration socketConfiguration, final ProtocolContext<ProtocolMessage> protocolContext, final LeaderElectionManager electionManager) {
        super(socketConfiguration, protocolContext);
        this.electionManager = electionManager;
    }

    @Override
    protected InetSocketAddress getServiceAddress() throws IOException {
        final String address = electionManager.getLeader(ClusterRoles.CLUSTER_COORDINATOR);

        if (StringUtils.isEmpty(address)) {
            throw new NoClusterCoordinatorException("No node has yet been elected Cluster Coordinator. Cannot establish connection to cluster yet.");
        }

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
        return socketAddress;
    }

}
