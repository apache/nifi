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
package org.apache.nifi.registry.flow;

import org.apache.nifi.flow.ParameterProviderReference;
import org.apache.nifi.flow.VersionedParameter;
import org.apache.nifi.flow.VersionedParameterContext;
import org.apache.nifi.flow.VersionedProcessGroup;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Holds the results of recursively fetching the contents of a registered flow snapshot where some child groups may
 * also be under version control. The top level flow snapshot will have the contents of all the child groups populated,
 * and the child snapshots here provide a mechanism to obtain the corresponding metadata for a versioned controlled
 * child group, such as the external service references, etc.
 */
public class FlowSnapshotContainer {

    private final RegisteredFlowSnapshot flowSnapshot;

    /**
     * The key of this Map is the id of the corresponding VersionedProcessGroup within the contents of the top level snapshot
     * (i.e. within flowSnapshot.getFlowContents()). Any child process group under version control will have an entry in this Map.
     */
    private final Map<String, RegisteredFlowSnapshot> childSnapshotsByGroupId;

    public FlowSnapshotContainer(final RegisteredFlowSnapshot flowSnapshot) {
        this.flowSnapshot = Objects.requireNonNull(flowSnapshot);
        if (this.flowSnapshot.getParameterContexts() == null) {
            flowSnapshot.setParameterContexts(new HashMap<>());
        }
        if (this.flowSnapshot.getParameterProviders() == null) {
            flowSnapshot.setParameterProviders(new HashMap<>());
        }
        this.childSnapshotsByGroupId = new HashMap<>();
    }

    /**
     * @return the top level snapshot
     */
    public RegisteredFlowSnapshot getFlowSnapshot() {
        return flowSnapshot;
    }

    /**
     * Get the snapshot that was used to populate the given group in the top level snapshot.
     *
     * @param groupId the id of a versioned controlled group within the top level snapshot
     * @return the snapshot used to populate that group
     */
    public RegisteredFlowSnapshot getChildSnapshot(final String groupId) {
        return childSnapshotsByGroupId.get(groupId);
    }

    /**
     * Adds a child snapshot to the container.
     *
     * @param childSnapshot the snapshot for the inner child PG
     * @param destinationGroup the VersionedProcessGroup in the top level snapshot where the child exists
     */
    public void addChildSnapshot(final RegisteredFlowSnapshot childSnapshot, final VersionedProcessGroup destinationGroup) {
        // We need to use the id of the group that the snapshot is being copied into because that is how this Map
        // will be accessed later, the id from the child snapshot's flow contents group is not the same
        childSnapshotsByGroupId.put(destinationGroup.getIdentifier(), childSnapshot);

        // Merge any parameter contexts and parameter providers from the child into the top level snapshot
        mergeParameterContexts(childSnapshot);
        mergeParameterProviders(childSnapshot);
    }

    /**
     * For each parameter context in the child snapshot:
     *   - Check if the top level snapshot has a parameter context with the same name
     *   - If a context with the same name does not exist, then add the context
     *   - If a context with the same name does exist, then add any parameters that don't already exist in the context
     */
    private void mergeParameterContexts(final RegisteredFlowSnapshot childSnapshot) {
        final Map<String, VersionedParameterContext> childContexts = childSnapshot.getParameterContexts();
        if (childContexts == null) {
            return;
        }

        for (final Map.Entry<String, VersionedParameterContext> childContextEntry : childContexts.entrySet()) {
            final String childContextName = childContextEntry.getKey();
            final VersionedParameterContext childContext  = childContextEntry.getValue();

            final VersionedParameterContext matchingContext = flowSnapshot.getParameterContexts().get(childContextName);
            if (matchingContext == null) {
                flowSnapshot.getParameterContexts().put(childContextName, childContext);
            } else {
                if (matchingContext.getParameters() == null) {
                    matchingContext.setParameters(new HashSet<>());
                }
                final Set<VersionedParameter> childParameters = childContext.getParameters() == null ? Collections.emptySet() : childContext.getParameters();
                for (final VersionedParameter childParameter : childParameters) {
                    if (!matchingContext.getParameters().contains(childParameter)) {
                        matchingContext.getParameters().add(childParameter);
                    }
                }
            }
        }
    }

    /**
     * For each parameter provider reference in the child snapshot:
     *   - Check if the top level snapshot has a parameter provider reference with the same id
     *   - If a provider reference does not exist with the same id, then add the provider reference
     */
    private void mergeParameterProviders(final RegisteredFlowSnapshot childSnapshot) {
        final Map<String, ParameterProviderReference> childParamProviders = childSnapshot.getParameterProviders();
        if (childParamProviders == null) {
            return;
        }

        for (final Map.Entry<String, ParameterProviderReference> childProviderEntry : childParamProviders.entrySet()) {
            final String childProviderId = childProviderEntry.getKey();
            final ParameterProviderReference childProvider = childProviderEntry.getValue();
            flowSnapshot.getParameterProviders().putIfAbsent(childProviderId, childProvider);
        }
    }
}
