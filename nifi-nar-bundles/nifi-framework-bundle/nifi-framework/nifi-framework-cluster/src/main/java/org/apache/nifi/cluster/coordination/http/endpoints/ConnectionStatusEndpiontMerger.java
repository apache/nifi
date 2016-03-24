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
import org.apache.nifi.web.api.dto.status.ConnectionStatusDTO;
import org.apache.nifi.web.api.dto.status.NodeConnectionStatusSnapshotDTO;
import org.apache.nifi.web.api.entity.ConnectionStatusEntity;

public class ConnectionStatusEndpiontMerger extends AbstractNodeStatusEndpoint<ConnectionStatusEntity, ConnectionStatusDTO> {
    public static final Pattern CONNECTION_STATUS_URI_PATTERN = Pattern.compile("/nifi-api/flow/connections/[a-f0-9\\-]{36}/status");

    @Override
    public boolean canHandle(URI uri, String method) {
        return "GET".equalsIgnoreCase(method) && CONNECTION_STATUS_URI_PATTERN.matcher(uri.getPath()).matches();
    }

    @Override
    protected Class<ConnectionStatusEntity> getEntityClass() {
        return ConnectionStatusEntity.class;
    }

    @Override
    protected ConnectionStatusDTO getDto(ConnectionStatusEntity entity) {
        return entity.getConnectionStatus();
    }

    @Override
    protected void mergeResponses(ConnectionStatusDTO clientDto, Map<NodeIdentifier, ConnectionStatusDTO> dtoMap, NodeIdentifier selectedNodeId) {
        final ConnectionStatusDTO mergedConnectionStatus = clientDto;
        mergedConnectionStatus.setNodeSnapshots(new ArrayList<NodeConnectionStatusSnapshotDTO>());

        final NodeConnectionStatusSnapshotDTO selectedNodeSnapshot = new NodeConnectionStatusSnapshotDTO();
        selectedNodeSnapshot.setStatusSnapshot(clientDto.getAggregateSnapshot().clone());
        selectedNodeSnapshot.setAddress(selectedNodeId.getApiAddress());
        selectedNodeSnapshot.setApiPort(selectedNodeId.getApiPort());
        selectedNodeSnapshot.setNodeId(selectedNodeId.getId());

        mergedConnectionStatus.getNodeSnapshots().add(selectedNodeSnapshot);

        // merge the other nodes
        for (final Map.Entry<NodeIdentifier, ConnectionStatusDTO> entry : dtoMap.entrySet()) {
            final NodeIdentifier nodeId = entry.getKey();
            final ConnectionStatusDTO nodeConnectionStatus = entry.getValue();
            if (nodeConnectionStatus == clientDto) {
                continue;
            }

            StatusMerger.merge(mergedConnectionStatus, nodeConnectionStatus, nodeId.getId(), nodeId.getApiAddress(), nodeId.getApiPort());
        }
    }

}
