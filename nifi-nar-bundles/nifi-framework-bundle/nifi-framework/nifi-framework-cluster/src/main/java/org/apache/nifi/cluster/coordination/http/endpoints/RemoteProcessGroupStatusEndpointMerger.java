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

import java.net.URI;
import java.util.ArrayList;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.nifi.cluster.manager.StatusMerger;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.dto.status.NodeRemoteProcessGroupStatusSnapshotDTO;
import org.apache.nifi.web.api.dto.status.RemoteProcessGroupStatusDTO;
import org.apache.nifi.web.api.entity.RemoteProcessGroupStatusEntity;

public class RemoteProcessGroupStatusEndpointMerger extends AbstractNodeStatusEndpoint<RemoteProcessGroupStatusEntity, RemoteProcessGroupStatusDTO> {
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
    protected RemoteProcessGroupStatusDTO getDto(RemoteProcessGroupStatusEntity entity) {
        return entity.getRemoteProcessGroupStatus();
    }

    @Override
    protected void mergeResponses(RemoteProcessGroupStatusDTO clientDto, Map<NodeIdentifier, RemoteProcessGroupStatusDTO> dtoMap, NodeIdentifier selectedNodeId) {
        final RemoteProcessGroupStatusDTO mergedRemoteProcessGroupStatus = clientDto;
        mergedRemoteProcessGroupStatus.setNodeSnapshots(new ArrayList<NodeRemoteProcessGroupStatusSnapshotDTO>());

        final NodeRemoteProcessGroupStatusSnapshotDTO selectedNodeSnapshot = new NodeRemoteProcessGroupStatusSnapshotDTO();
        selectedNodeSnapshot.setStatusSnapshot(clientDto.getAggregateSnapshot().clone());
        selectedNodeSnapshot.setAddress(selectedNodeId.getApiAddress());
        selectedNodeSnapshot.setApiPort(selectedNodeId.getApiPort());
        selectedNodeSnapshot.setNodeId(selectedNodeId.getId());

        mergedRemoteProcessGroupStatus.getNodeSnapshots().add(selectedNodeSnapshot);

        // merge the other nodes
        for (final Map.Entry<NodeIdentifier, RemoteProcessGroupStatusDTO> entry : dtoMap.entrySet()) {
            final NodeIdentifier nodeId = entry.getKey();
            final RemoteProcessGroupStatusDTO nodeRemoteProcessGroupStatus = entry.getValue();
            if (nodeRemoteProcessGroupStatus == clientDto) {
                continue;
            }

            StatusMerger.merge(mergedRemoteProcessGroupStatus, nodeRemoteProcessGroupStatus, nodeId.getId(), nodeId.getApiAddress(), nodeId.getApiPort());
        }
    }

}
