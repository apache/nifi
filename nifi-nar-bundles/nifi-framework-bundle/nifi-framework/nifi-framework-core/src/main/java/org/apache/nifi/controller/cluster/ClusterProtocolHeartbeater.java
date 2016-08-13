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

import org.apache.nifi.cluster.coordination.node.ClusterRoles;
import org.apache.nifi.cluster.protocol.NodeProtocolSender;
import org.apache.nifi.cluster.protocol.ProtocolException;
import org.apache.nifi.cluster.protocol.message.HeartbeatMessage;
import org.apache.nifi.controller.leader.election.LeaderElectionManager;

/**
 * Uses Leader Election Manager in order to determine which node is the elected Cluster Coordinator and to indicate
 * that this node is part of the cluster. Once the Cluster Coordinator is known, heartbeats are
 * sent directly to the Cluster Coordinator.
 */
public class ClusterProtocolHeartbeater implements Heartbeater {
    private final NodeProtocolSender protocolSender;
    private final LeaderElectionManager electionManager;

    public ClusterProtocolHeartbeater(final NodeProtocolSender protocolSender, final LeaderElectionManager electionManager) {
        this.protocolSender = protocolSender;
        this.electionManager = electionManager;
    }

    @Override
    public String getHeartbeatAddress() throws IOException {
        final String heartbeatAddress = electionManager.getLeader(ClusterRoles.CLUSTER_COORDINATOR);
        if (heartbeatAddress == null) {
            throw new ProtocolException("Cannot send heartbeat because there is no Cluster Coordinator currently elected");
        }

        return heartbeatAddress;
    }

    @Override
    public synchronized void send(final HeartbeatMessage heartbeatMessage) throws IOException {
        final String heartbeatAddress = getHeartbeatAddress();
        protocolSender.heartbeat(heartbeatMessage, heartbeatAddress);
    }

    @Override
    public void close() throws IOException {
    }
}
