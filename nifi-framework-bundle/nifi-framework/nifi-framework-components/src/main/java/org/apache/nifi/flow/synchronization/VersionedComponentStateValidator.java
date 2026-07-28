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
package org.apache.nifi.flow.synchronization;

import org.apache.nifi.flow.VersionedComponentState;
import org.apache.nifi.flow.VersionedConfigurableExtension;
import org.apache.nifi.flow.VersionedProcessGroup;

import java.util.ArrayList;
import java.util.List;

/**
 * Shared helpers that inspect the LOCAL component state contained in an exported {@link VersionedProcessGroup}
 * and validate it against the destination cluster topology before importing or migrating the flow.
 *
 * <p>This is the single source of truth for the cluster-topology rule used by both the standard versioned-flow
 * synchronization path and the connector migration path. Both paths must reject a flow whose LOCAL state
 * contains entries for more nodes than the destination cluster currently has connected, because there is no way
 * to attribute the extra source-node states to a destination node.</p>
 */
public final class VersionedComponentStateValidator {

    private VersionedComponentStateValidator() {
    }

    /**
     * Validates that the LOCAL component state inside the given proposed {@link VersionedProcessGroup} can be
     * imported into a cluster that currently has the given number of connected nodes.
     *
     * @param proposed the proposed flow to validate
     * @param connectedNodeCount the number of nodes that are currently connected to the destination cluster
     * @throws IllegalStateException when any stateful component in the proposed flow has more local-node-state entries
     *         than the destination cluster has connected nodes
     */
    public static void validateLocalStateTopology(final VersionedProcessGroup proposed, final int connectedNodeCount) {
        if (proposed == null || connectedNodeCount <= 0) {
            return;
        }

        final int maxSourceNodes = findMaxLocalStateNodeCount(proposed);
        if (maxSourceNodes > connectedNodeCount) {
            throw new IllegalStateException(
                    "Cannot import flow with component state: the flow definition contains local state from %d source node(s) but the destination cluster has only %d connected node(s). "
                            .formatted(maxSourceNodes, connectedNodeCount)
                    + "Import into a cluster with at least %d node(s), or export without component state.".formatted(maxSourceNodes));
        }
    }

    /**
     * Returns the maximum number of local-node-state entries declared by any stateful component anywhere in the
     * given {@link VersionedProcessGroup} hierarchy. Returns 0 when no component declares LOCAL state.
     *
     * @param group the root process group to inspect
     * @return the maximum local-node-state count across the group hierarchy
     */
    public static int findMaxLocalStateNodeCount(final VersionedProcessGroup group) {
        if (group == null) {
            return 0;
        }

        int max = 0;
        for (final VersionedConfigurableExtension extension : getStatefulExtensions(group)) {
            final VersionedComponentState state = extension.getComponentState();
            if (state != null && state.getLocalNodeStates() != null) {
                max = Math.max(max, state.getLocalNodeStates().size());
            }
        }

        if (group.getProcessGroups() != null) {
            for (final VersionedProcessGroup child : group.getProcessGroups()) {
                max = Math.max(max, findMaxLocalStateNodeCount(child));
            }
        }

        return max;
    }

    private static List<VersionedConfigurableExtension> getStatefulExtensions(final VersionedProcessGroup group) {
        final List<VersionedConfigurableExtension> extensions = new ArrayList<>();
        if (group.getProcessors() != null) {
            extensions.addAll(group.getProcessors());
        }
        if (group.getControllerServices() != null) {
            extensions.addAll(group.getControllerServices());
        }
        return extensions;
    }
}
