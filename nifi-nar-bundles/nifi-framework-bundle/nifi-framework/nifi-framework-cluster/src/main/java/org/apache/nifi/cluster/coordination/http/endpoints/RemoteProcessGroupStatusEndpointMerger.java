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

import org.apache.nifi.cluster.manager.ComponentEntityStatusMerger;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.manager.StatusMerger;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.dto.status.NodeRemoteProcessGroupStatusSnapshotDTO;
import org.apache.nifi.web.api.dto.status.RemoteProcessGroupStatusDTO;
import org.apache.nifi.web.api.entity.RemoteProcessGroupStatusEntity;

import java.net.URI;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class RemoteProcessGroupStatusEndpointMerger extends AbstractSingleEntityEndpoint<RemoteProcessGroupStatusEntity> implements ComponentEntityStatusMerger<RemoteProcessGroupStatusDTO> {
    public static final Pattern REMOTE_PROCESS_GROUP_STATUS_URI_PATTERN = Pattern.compile("/nifi-api/flow/remote-process-groups/[a-f0-9\\-]{36}/status");

    @Override
    public boolean canHandle(URI uri, String method) {
        return "GET".equalsIgnoreCase(method) && REMOTE_PROCESS_GROUP_STATUS_URI_PATTERN.matcher(uri.getPath()).matches();
    }

    @Override
    protected Class<RemoteProcessGroupStatusEntity> getEntityClass() {
        return RemoteProcessGroupStatusEntity.class;
    }

    @Override
    protected void mergeResponses(RemoteProcessGroupStatusEntity clientEntity, Map<NodeIdentifier, RemoteProcessGroupStatusEntity> entityMap, Set<NodeResponse> successfulResponses,
                                  Set<NodeResponse> problematicResponses) {
        final RemoteProcessGroupStatusDTO mergedRemoteProcessGroupStatus = clientEntity.getRemoteProcessGroupStatus();
        mergedRemoteProcessGroupStatus.setNodeSnapshots(new ArrayList<>());

        final NodeIdentifier selectedNodeId = entityMap.entrySet().stream()
                .filter(e -> e.getValue() == clientEntity)
                .map(e -> e.getKey())
                .findFirst()
                .orElse(null);

        final NodeRemoteProcessGroupStatusSnapshotDTO selectedNodeSnapshot = new NodeRemoteProcessGroupStatusSnapshotDTO();
        selectedNodeSnapshot.setStatusSnapshot(mergedRemoteProcessGroupStatus.getAggregateSnapshot().clone());
        selectedNodeSnapshot.setAddress(selectedNodeId.getApiAddress());
        selectedNodeSnapshot.setApiPort(selectedNodeId.getApiPort());
        selectedNodeSnapshot.setNodeId(selectedNodeId.getId());

        mergedRemoteProcessGroupStatus.getNodeSnapshots().add(selectedNodeSnapshot);

        // merge the other nodes
        for (final Map.Entry<NodeIdentifier, RemoteProcessGroupStatusEntity> entry : entityMap.entrySet()) {
            final NodeIdentifier nodeId = entry.getKey();
            final RemoteProcessGroupStatusEntity nodeRemoteProcessGroupStatusEntity = entry.getValue();
            final RemoteProcessGroupStatusDTO nodeRemoteProcessGroupStatus = nodeRemoteProcessGroupStatusEntity.getRemoteProcessGroupStatus();
            if (nodeRemoteProcessGroupStatus == mergedRemoteProcessGroupStatus) {
                continue;
            }

            mergeStatus(mergedRemoteProcessGroupStatus, clientEntity.getCanRead(), nodeRemoteProcessGroupStatus, nodeRemoteProcessGroupStatusEntity.getCanRead(), nodeId);
        }
    }

    @Override
    public void mergeStatus(RemoteProcessGroupStatusDTO clientStatus, boolean clientStatusReadablePermission, RemoteProcessGroupStatusDTO status, boolean statusReadablePermission,
                            NodeIdentifier statusNodeIdentifier) {
        StatusMerger.merge(clientStatus, clientStatusReadablePermission, status, statusReadablePermission, statusNodeIdentifier.getId(), statusNodeIdentifier.getApiAddress(),
                statusNodeIdentifier.getApiPort());
    }
}
