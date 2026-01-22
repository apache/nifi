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

package org.apache.nifi.components.connector;

import java.io.IOException;
import java.time.Duration;
import java.util.Set;

/**
 * <p>
 * Provides cluster coordination capabilities for connector operations. This interface
 * is implemented by the framework and provided to {@link ConnectorLifecycleManager}
 * implementations to enable coordination across cluster nodes.
 * </p>
 *
 * <p>
 * This interface is not intended to be implemented by third parties. The framework
 * provides a standard implementation that handles cluster communication.
 * </p>
 */
public interface ConnectorClusterCoordinator {

    /**
     * Gets the connector state as reported by the cluster. This queries other nodes
     * in the cluster to determine the consensus state of the connector.
     *
     * @param connectorId the identifier of the connector
     * @return the cluster-wide connector state
     * @throws IOException if an I/O error occurs during cluster communication
     */
    ConnectorState getClusterState(String connectorId) throws IOException;

    /**
     * Waits for the cluster to reach one of the desired states for the specified connector.
     * This method polls the cluster nodes until all nodes report a state in the desired set,
     * or until the timeout is reached.
     *
     * @param connectorId the identifier of the connector
     * @param desiredStates the set of acceptable final states to wait for
     * @param allowableIntermediateStates the set of acceptable intermediate states while waiting
     * @param timeout the maximum time to wait for the cluster to reach the desired state
     * @throws IOException if an I/O error occurs during cluster communication
     * @throws InterruptedException if the wait is interrupted
     * @throws ConnectorLifecycleException if the cluster fails to reach the desired state
     *         within the timeout or if an unexpected state is encountered
     */
    void awaitClusterState(String connectorId, Set<ConnectorState> desiredStates,
                           Set<ConnectorState> allowableIntermediateStates,
                           Duration timeout) throws IOException, InterruptedException, ConnectorLifecycleException;

    /**
     * Indicates whether this node is currently connected to a cluster.
     *
     * @return true if this node is clustered and connected, false otherwise
     */
    boolean isClustered();

    /**
     * Indicates whether this node is the primary node in the cluster.
     *
     * @return true if this node is the primary node, false otherwise
     */
    boolean isPrimaryNode();

    /**
     * Returns a no-op implementation of ConnectorClusterCoordinator for standalone mode.
     *
     * @return a standalone (no-op) cluster coordinator
     */
    static ConnectorClusterCoordinator standalone() {
        return StandaloneConnectorClusterCoordinator.INSTANCE;
    }
}
