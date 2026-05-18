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

package org.apache.nifi.cluster.coordination.http.endpoints;

import org.apache.nifi.cluster.coordination.http.EndpointResponseMerger;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.flow.VersionedComponentState;
import org.apache.nifi.flow.VersionedConfigurableExtension;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedNodeState;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.registry.flow.RegisteredFlowSnapshot;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Merges responses for {@code GET /nifi-api/process-groups/{uuid}/download}.
 * When component state is included in the export, each cluster node contributes its own LOCAL state
 * keyed by ordinal position. This merger combines the {@code localNodeStates} maps from every node's
 * snapshot into a single response.
 */
public class ExportProcessGroupEndpointMerger implements EndpointResponseMerger {

    public static final Pattern PROCESS_GROUP_DOWNLOAD_URI_PATTERN =
            Pattern.compile("/nifi-api/process-groups/(?:(?:root)|(?:[a-f0-9\\-]{36}))/download");

    @Override
    public boolean canHandle(final URI uri, final String method) {
        return "GET".equalsIgnoreCase(method) && PROCESS_GROUP_DOWNLOAD_URI_PATTERN.matcher(uri.getPath()).matches();
    }

    @Override
    public NodeResponse merge(final URI uri, final String method, final Set<NodeResponse> successfulResponses, final Set<NodeResponse> problematicResponses, final NodeResponse clientResponse) {
        if (!canHandle(uri, method)) {
            throw new IllegalArgumentException("Cannot use Endpoint Mapper of type " + getClass().getSimpleName() + " to map responses for URI " + uri + ", HTTP Method " + method);
        }

        final RegisteredFlowSnapshot mergedSnapshot = clientResponse.getClientResponse().readEntity(RegisteredFlowSnapshot.class);
        for (final NodeResponse nodeResponse : successfulResponses) {
            if (nodeResponse == clientResponse) {
                continue;
            }
            final RegisteredFlowSnapshot nodeSnapshot = nodeResponse.getClientResponse().readEntity(RegisteredFlowSnapshot.class);
            mergeLocalNodeStates(mergedSnapshot.getFlowContents(), nodeSnapshot.getFlowContents());
        }

        return new NodeResponse(clientResponse, new ExportedFlowSnapshotEntity(mergedSnapshot));
    }

    /**
     * Recursively merges localNodeStates from a source process group into a target process group.
     * For each stateful component (processor or controller service), the LOCAL state entries from
     * the source are added to the target's localNodeStates list. Cluster state is identical across
     * nodes and is already present in the target.
     *
     * @param target the process group to merge into
     * @param source the process group to merge from
     */
    private void mergeLocalNodeStates(final VersionedProcessGroup target, final VersionedProcessGroup source) {
        if (target == null || source == null) {
            return;
        }

        if (target.getProcessors() != null && source.getProcessors() != null) {
            final Map<String, VersionedProcessor> sourceProcessors = new HashMap<>();
            for (final VersionedProcessor sp : source.getProcessors()) {
                sourceProcessors.put(sp.getIdentifier(), sp);
            }
            for (final VersionedProcessor tp : target.getProcessors()) {
                mergeComponentState(tp, sourceProcessors.get(tp.getIdentifier()));
            }
        }

        if (target.getControllerServices() != null && source.getControllerServices() != null) {
            final Map<String, VersionedControllerService> sourceServices = new HashMap<>();
            for (final VersionedControllerService ss : source.getControllerServices()) {
                sourceServices.put(ss.getIdentifier(), ss);
            }
            for (final VersionedControllerService ts : target.getControllerServices()) {
                mergeComponentState(ts, sourceServices.get(ts.getIdentifier()));
            }
        }

        if (target.getProcessGroups() != null && source.getProcessGroups() != null) {
            final Map<String, VersionedProcessGroup> sourceGroups = new HashMap<>();
            for (final VersionedProcessGroup sg : source.getProcessGroups()) {
                sourceGroups.put(sg.getIdentifier(), sg);
            }
            for (final VersionedProcessGroup tg : target.getProcessGroups()) {
                mergeLocalNodeStates(tg, sourceGroups.get(tg.getIdentifier()));
            }
        }
    }

    /**
     * Merges localNodeStates from a source component into a target component. Each node contributes its
     * own ordinal entry and the merge fills in any missing slots in the target's list.
     *
     * @param target the target component (already contains state from first node)
     * @param source the source component (contains state from another node), may be {@code null}
     */
    private void mergeComponentState(final VersionedConfigurableExtension target, final VersionedConfigurableExtension source) {
        if (source == null) {
            return;
        }

        final VersionedComponentState sourceState = source.getComponentState();
        if (sourceState == null || sourceState.getLocalNodeStates() == null) {
            return;
        }

        VersionedComponentState targetState = target.getComponentState();
        if (targetState == null) {
            targetState = new VersionedComponentState();
            target.setComponentState(targetState);
        }

        final List<VersionedNodeState> sourceList = sourceState.getLocalNodeStates();
        if (targetState.getLocalNodeStates() == null) {
            targetState.setLocalNodeStates(new ArrayList<>(sourceList));
            return;
        }

        final List<VersionedNodeState> targetList = targetState.getLocalNodeStates();
        while (targetList.size() < sourceList.size()) {
            targetList.add(null);
        }
        for (int i = 0; i < sourceList.size(); i++) {
            if (sourceList.get(i) != null) {
                targetList.set(i, sourceList.get(i));
            }
        }
    }
}
