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
import org.apache.nifi.web.api.dto.status.NodeProcessGroupStatusSnapshotDTO;
import org.apache.nifi.web.api.dto.status.ProcessGroupStatusDTO;
import org.apache.nifi.web.api.entity.ProcessGroupStatusEntity;

import java.net.URI;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class GroupStatusEndpointMerger extends AbstractSingleEntityEndpoint<ProcessGroupStatusEntity> implements ComponentEntityStatusMerger<ProcessGroupStatusDTO> {
    public static final Pattern GROUP_STATUS_URI_PATTERN = Pattern.compile("/nifi-api/flow/process-groups/(?:(?:root)|(?:[a-f0-9\\-]{36}))/status");

    @Override
    public boolean canHandle(URI uri, String method) {
        return "GET".equalsIgnoreCase(method) && GROUP_STATUS_URI_PATTERN.matcher(uri.getPath()).matches();
    }

    @Override
    protected Class<ProcessGroupStatusEntity> getEntityClass() {
        return ProcessGroupStatusEntity.class;
    }

    @Override
    protected void mergeResponses(ProcessGroupStatusEntity clientEntity, Map<NodeIdentifier, ProcessGroupStatusEntity> entityMap, Set<NodeResponse> successfulResponses,
                                  Set<NodeResponse> problematicResponses) {
        final ProcessGroupStatusDTO mergedProcessGroupStatus = clientEntity.getProcessGroupStatus();
        mergedProcessGroupStatus.setNodeSnapshots(new ArrayList<>());

        final NodeIdentifier selectedNodeId = entityMap.entrySet().stream()
                .filter(e -> e.getValue() == clientEntity)
                .map(e -> e.getKey())
                .findFirst()
                .orElse(null);

        final NodeProcessGroupStatusSnapshotDTO selectedNodeSnapshot = new NodeProcessGroupStatusSnapshotDTO();
        selectedNodeSnapshot.setStatusSnapshot(mergedProcessGroupStatus.getAggregateSnapshot().clone());
        selectedNodeSnapshot.setAddress(selectedNodeId.getApiAddress());
        selectedNodeSnapshot.setApiPort(selectedNodeId.getApiPort());
        selectedNodeSnapshot.setNodeId(selectedNodeId.getId());

        mergedProcessGroupStatus.getNodeSnapshots().add(selectedNodeSnapshot);

        for (final Map.Entry<NodeIdentifier, ProcessGroupStatusEntity> entry : entityMap.entrySet()) {
            final NodeIdentifier nodeId = entry.getKey();
            final ProcessGroupStatusEntity nodeProcessGroupStatusEntity = entry.getValue();
            final ProcessGroupStatusDTO nodeProcessGroupStatus = nodeProcessGroupStatusEntity.getProcessGroupStatus();
            if (nodeProcessGroupStatus == mergedProcessGroupStatus) {
                continue;
            }

            mergeStatus(mergedProcessGroupStatus, clientEntity.getCanRead(), nodeProcessGroupStatus, nodeProcessGroupStatusEntity.getCanRead(), nodeId);
        }
    }

    @Override
    public void mergeStatus(ProcessGroupStatusDTO clientStatus, boolean clientStatusReadablePermission, ProcessGroupStatusDTO status, boolean statusReadablePermission,
                            NodeIdentifier statusNodeIdentifier) {
        StatusMerger.merge(clientStatus, clientStatusReadablePermission, status, statusReadablePermission, statusNodeIdentifier.getId(), statusNodeIdentifier.getApiAddress(),
                statusNodeIdentifier.getApiPort());
    }
}
