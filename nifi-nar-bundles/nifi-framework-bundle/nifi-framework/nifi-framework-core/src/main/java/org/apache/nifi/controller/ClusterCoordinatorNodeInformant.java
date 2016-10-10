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

package org.apache.nifi.controller;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.remote.cluster.ClusterNodeInformation;
import org.apache.nifi.remote.cluster.NodeInformant;
import org.apache.nifi.remote.cluster.NodeInformation;

public class ClusterCoordinatorNodeInformant implements NodeInformant {
    private final ClusterCoordinator clusterCoordinator;

    public ClusterCoordinatorNodeInformant(final ClusterCoordinator coordinator) {
        this.clusterCoordinator = coordinator;
    }

    @Override
    public ClusterNodeInformation getNodeInformation() {
        final List<NodeInformation> nodeInfoCollection;
        try {
            nodeInfoCollection = clusterCoordinator.getClusterWorkload().entrySet().stream().map(entry -> {
                final NodeIdentifier nodeId = entry.getKey();
                final NodeInformation nodeInfo = new NodeInformation(nodeId.getSiteToSiteAddress(), nodeId.getSiteToSitePort(),
                        nodeId.getSiteToSiteHttpApiPort(), nodeId.getApiPort(), nodeId.isSiteToSiteSecure(), entry.getValue().getFlowFileCount());
                return nodeInfo;
            }).collect(Collectors.toList());
        } catch (IOException e) {
            throw new RuntimeException("Failed to retrieve cluster workload due to " + e, e);
        }

        final ClusterNodeInformation nodeInfo = new ClusterNodeInformation();
        nodeInfo.setNodeInformation(nodeInfoCollection);
        return nodeInfo;
    }

}
