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
 * Callback interface for coordinating persistence and cluster state during connector updates.
 * This interface provides methods that the {@link ConnectorLifecycleManager} can use to
 * persist connector state and coordinate with the cluster.
 */
public interface ConnectorUpdateCallback {

    /**
     * Persists the current connector state. This method should be called after making
     * changes to the connector's configuration to ensure the changes are durably stored.
     *
     * @throws IOException if an I/O error occurs during persistence
     */
    void persist() throws IOException;

    /**
     * Waits for the cluster to reach one of the desired states for the connector.
     * This is used during update operations to ensure all nodes in the cluster
     * are synchronized before proceeding.
     *
     * @param connectorId the identifier of the connector
     * @param desiredStates the set of acceptable final states to wait for
     * @param allowableIntermediateStates the set of acceptable intermediate states while waiting
     * @param timeout the maximum time to wait for the cluster to reach the desired state
     * @throws IOException if an I/O error occurs during cluster communication
     * @throws InterruptedException if the wait is interrupted
     * @throws ConnectorLifecycleException if the cluster fails to reach the desired state
     */
    void awaitClusterState(String connectorId, Set<ConnectorState> desiredStates,
                           Set<ConnectorState> allowableIntermediateStates,
                           Duration timeout) throws IOException, InterruptedException, ConnectorLifecycleException;
}
