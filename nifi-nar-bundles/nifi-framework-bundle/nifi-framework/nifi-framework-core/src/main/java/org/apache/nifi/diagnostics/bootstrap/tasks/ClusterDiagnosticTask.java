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
package org.apache.nifi.diagnostics.bootstrap.tasks;

import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.node.NodeConnectionState;
import org.apache.nifi.cluster.coordination.node.NodeConnectionStatus;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.controller.FlowController;
import org.apache.nifi.controller.leader.election.LeaderElectionManager;
import org.apache.nifi.diagnostics.DiagnosticTask;
import org.apache.nifi.diagnostics.DiagnosticsDumpElement;
import org.apache.nifi.diagnostics.StandardDiagnosticsDumpElement;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ClusterDiagnosticTask implements DiagnosticTask {
    private final FlowController flowController;

    public ClusterDiagnosticTask(final FlowController flowController) {
        this.flowController = flowController;
    }

    @Override
    public DiagnosticsDumpElement captureDump(final boolean verbose) {
        final List<String> details = new ArrayList<>();

        if (!flowController.isClustered()) {
            details.add("This instance is not clustered");
            return new StandardDiagnosticsDumpElement("Cluster Details", details);
        }

        final ClusterCoordinator clusterCoordinator = flowController.getClusterCoordinator();
        for (final NodeConnectionStatus status : clusterCoordinator.getConnectionStatuses()) {
            final StringBuilder sb = new StringBuilder();

            final NodeIdentifier nodeId = status.getNodeIdentifier();
            sb.append(nodeId.getFullDescription());

            final NodeConnectionState state = status.getState();
            sb.append("; State = ").append(state);

            if (state == NodeConnectionState.OFFLOADED || state == NodeConnectionState.OFFLOADING) {
                sb.append("; Offload Code = ").append(status.getOffloadCode()).append("; Reason = ").append(status.getReason());
            } else if (state == NodeConnectionState.DISCONNECTED || state == NodeConnectionState.DISCONNECTING) {
                sb.append("; Disconnection Code = ").append(status.getDisconnectCode()).append("; Reason = ").append(status.getReason());
            }

            details.add(sb.toString());
        }

        details.add("Primary Node : " + clusterCoordinator.getPrimaryNode());
        details.add("Coordinator Node : " + clusterCoordinator.getElectedActiveCoordinatorNode());
        details.add("Local Node : " + clusterCoordinator.getLocalNodeIdentifier());

        final LeaderElectionManager leaderElectionManager = flowController.getLeaderElectionManager();
        if (leaderElectionManager != null) {
            final Map<String, Integer> changeCounts = leaderElectionManager.getLeadershipChangeCount(24, TimeUnit.HOURS);
            changeCounts.forEach((role, count) -> details.add("Leadership for Role <" + role + "> has changed " + count + " times in the last 24 hours."));

            details.add("In the past 5 minutes, the Leader Election service has been polled " + leaderElectionManager.getPollCount() + " times");
            details.add("In the past 5 minutes, the minimum time taken to communicate with the Leader Election service has been "
                + leaderElectionManager.getMinPollTime(TimeUnit.MILLISECONDS) + " millis");
            details.add("In the past 5 minutes, the maximum time taken to communicate with the Leader Election service has been "
                + leaderElectionManager.getMaxPollTime(TimeUnit.MILLISECONDS) + " millis");
            details.add("In the past 5 minutes, the average time taken to communicate with the Leader Election service has been "
                + leaderElectionManager.getAveragePollTime(TimeUnit.MILLISECONDS) + " millis");
        }

        return new StandardDiagnosticsDumpElement("Cluster Details", details);
    }
}
