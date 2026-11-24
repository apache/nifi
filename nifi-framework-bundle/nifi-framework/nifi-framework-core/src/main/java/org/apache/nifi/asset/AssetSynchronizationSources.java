/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *  contributor license agreements.  See the NOTICE file distributed with
 *  this work for additional information regarding copyright ownership.
 *  The ASF licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.nifi.asset;

import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.protocol.NodeIdentifier;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

/**
 * Determines the nodes from which the Cluster Coordinator synchronizes assets.
 */
final class AssetSynchronizationSources {

    private static final Comparator<NodeIdentifier> NODE_IDENTIFIER_COMPARATOR = Comparator.comparing(NodeIdentifier::getApiAddress).thenComparingInt(NodeIdentifier::getApiPort);

    private AssetSynchronizationSources() {
    }

    /**
     * Returns the nodes from which the Cluster Coordinator should synchronize missing assets. The list is empty when the
     * local node voted for the elected flow, because the local node then already holds the assets that the flow references.
     *
     * @param clusterCoordinator the Cluster Coordinator
     * @param localNodeIdentifier the identifier of the local node, or null if not yet known
     * @return the nodes from which to synchronize missing assets, ordered by API address and then API port
     */
    static List<NodeIdentifier> getCoordinatorSourceNodes(final ClusterCoordinator clusterCoordinator, final NodeIdentifier localNodeIdentifier) {
        if (localNodeIdentifier == null) {
            return List.of();
        }

        final Set<NodeIdentifier> electedFlowNodeIdentifiers = clusterCoordinator.getElectedFlowNodeIdentifiers();
        if (electedFlowNodeIdentifiers.contains(localNodeIdentifier)) {
            return List.of();
        }

        final List<NodeIdentifier> sourceNodeIds = new ArrayList<>(electedFlowNodeIdentifiers);
        sourceNodeIds.sort(NODE_IDENTIFIER_COMPARATOR);
        return sourceNodeIds;
    }
}
