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
import org.apache.nifi.web.api.dto.status.ConnectionStatusDTO;
import org.apache.nifi.web.api.dto.status.NodeConnectionStatusSnapshotDTO;
import org.apache.nifi.web.api.entity.ConnectionStatusEntity;

import java.net.URI;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class ConnectionStatusEndpiontMerger extends AbstractSingleEntityEndpoint<ConnectionStatusEntity> implements ComponentEntityStatusMerger<ConnectionStatusDTO> {
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
    protected void mergeResponses(ConnectionStatusEntity clientEntity, Map<NodeIdentifier, ConnectionStatusEntity> entityMap, Set<NodeResponse> successfulResponses,
                                  Set<NodeResponse> problematicResponses) {
        final ConnectionStatusDTO mergedConnectionStatus = clientEntity.getConnectionStatus();
        mergedConnectionStatus.setNodeSnapshots(new ArrayList<>());

        final NodeIdentifier selectedNodeId = entityMap.entrySet().stream()
                .filter(e -> e.getValue() == clientEntity)
                .map(e -> e.getKey())
                .findFirst()
                .orElse(null);

        final NodeConnectionStatusSnapshotDTO selectedNodeSnapshot = new NodeConnectionStatusSnapshotDTO();
        selectedNodeSnapshot.setStatusSnapshot(mergedConnectionStatus.getAggregateSnapshot().clone());
        selectedNodeSnapshot.setAddress(selectedNodeId.getApiAddress());
        selectedNodeSnapshot.setApiPort(selectedNodeId.getApiPort());
        selectedNodeSnapshot.setNodeId(selectedNodeId.getId());

        mergedConnectionStatus.getNodeSnapshots().add(selectedNodeSnapshot);

        // merge the other nodes
        for (final Map.Entry<NodeIdentifier, ConnectionStatusEntity> entry : entityMap.entrySet()) {
            final NodeIdentifier nodeId = entry.getKey();
            final ConnectionStatusEntity nodeConnectionStatusEntity = entry.getValue();
            final ConnectionStatusDTO nodeConnectionStatus = nodeConnectionStatusEntity.getConnectionStatus();
            if (nodeConnectionStatus == mergedConnectionStatus) {
                continue;
            }

            mergeStatus(mergedConnectionStatus, clientEntity.getCanRead(), nodeConnectionStatus, nodeConnectionStatusEntity.getCanRead(), nodeId);
        }
    }

    @Override
    public void mergeStatus(ConnectionStatusDTO clientStatus, boolean clientStatusReadablePermission, ConnectionStatusDTO status, boolean statusReadablePermission,
                            NodeIdentifier statusNodeIdentifier) {
        StatusMerger.merge(clientStatus, clientStatusReadablePermission, status, statusReadablePermission, statusNodeIdentifier.getId(), statusNodeIdentifier.getApiAddress(),
                statusNodeIdentifier.getApiPort());
    }
}
