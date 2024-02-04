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

package org.apache.nifi.cluster;

import org.apache.nifi.cluster.coordination.ClusterCoordinator;
import org.apache.nifi.cluster.coordination.node.NodeConnectionStatus;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StandardClusterDetailsFactory implements ClusterDetailsFactory {
    private static final Logger logger = LoggerFactory.getLogger(StandardClusterDetailsFactory.class);

    private final ClusterCoordinator clusterCoordinator;

    /**
     * Constructor marked as never used because it is constructed via Spring
     */
    public StandardClusterDetailsFactory(final ClusterCoordinator clusterCoordinator) {
        this.clusterCoordinator = clusterCoordinator;
    }

    @Override
    public ConnectionState getConnectionState() {
        if (clusterCoordinator == null) {
            logger.debug("No Cluster Coordinator has been configured; returning Connection State of NOT_CLUSTERED");
            return ConnectionState.NOT_CLUSTERED;
        }

        final NodeIdentifier nodeIdentifier = clusterCoordinator.getLocalNodeIdentifier();
        if (nodeIdentifier == null) {
            logger.info("Local Node Identifier has not yet been established; returning Connection State of UNKNOWN");
            return ConnectionState.UNKNOWN;
        }

        final NodeConnectionStatus connectionStatus = clusterCoordinator.getConnectionStatus(nodeIdentifier);
        if (connectionStatus == null) {
            logger.info("Cluster connection status is not currently known for Node Identifier {}; returning Connection State of UNKNOWN", nodeIdentifier.getId());
            return ConnectionState.UNKNOWN;
        }

        final String stateName = connectionStatus.getState().name();
        try {
            final ConnectionState connectionState = ConnectionState.valueOf(stateName);
            logger.debug("Returning Connection State of {}", connectionState);
            return connectionState;
        } catch (final IllegalArgumentException iae) {
            logger.warn("Cluster Coordinator reports Connection State of {}, which is not a known state; returning UNKNOWN", stateName);
            return ConnectionState.UNKNOWN;
        }
    }
}
