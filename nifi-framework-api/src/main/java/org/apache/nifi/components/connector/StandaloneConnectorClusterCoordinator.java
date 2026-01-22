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

import java.time.Duration;
import java.util.Set;

/**
 * A no-op implementation of {@link ConnectorClusterCoordinator} for standalone (non-clustered) mode.
 * All cluster coordination operations are no-ops in this implementation.
 */
final class StandaloneConnectorClusterCoordinator implements ConnectorClusterCoordinator {

    static final StandaloneConnectorClusterCoordinator INSTANCE = new StandaloneConnectorClusterCoordinator();

    private StandaloneConnectorClusterCoordinator() {
    }

    @Override
    public ConnectorState getClusterState(final String connectorId) {
        throw new UnsupportedOperationException("Cluster state is not available in standalone mode");
    }

    @Override
    public void awaitClusterState(final String connectorId, final Set<ConnectorState> desiredStates,
                                  final Set<ConnectorState> allowableIntermediateStates, final Duration timeout) {
        // No-op in standalone mode - no cluster coordination needed
    }

    @Override
    public boolean isClustered() {
        return false;
    }

    @Override
    public boolean isPrimaryNode() {
        return false;
    }
}
