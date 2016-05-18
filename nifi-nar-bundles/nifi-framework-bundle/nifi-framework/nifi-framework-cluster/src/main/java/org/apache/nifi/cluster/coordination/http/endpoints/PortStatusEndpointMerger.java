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
import org.apache.nifi.web.api.dto.status.NodePortStatusSnapshotDTO;
import org.apache.nifi.web.api.dto.status.PortStatusDTO;
import org.apache.nifi.web.api.entity.PortStatusEntity;

public class PortStatusEndpointMerger extends AbstractNodeStatusEndpoint<PortStatusEntity, PortStatusDTO> {
    public static final Pattern INPUT_PORT_STATUS_URI_PATTERN = Pattern.compile("/nifi-api/flow/input-ports/[a-f0-9\\-]{36}/status");
    public static final Pattern OUTPUT_PORT_STATUS_URI_PATTERN = Pattern.compile("/nifi-api/flow/output-ports/[a-f0-9\\-]{36}/status");

    @Override
    public boolean canHandle(URI uri, String method) {
        return "GET".equalsIgnoreCase(method) && (INPUT_PORT_STATUS_URI_PATTERN.matcher(uri.getPath()).matches() || OUTPUT_PORT_STATUS_URI_PATTERN.matcher(uri.getPath()).matches());
    }

    @Override
    protected Class<PortStatusEntity> getEntityClass() {
        return PortStatusEntity.class;
    }

    @Override
    protected PortStatusDTO getDto(PortStatusEntity entity) {
        return entity.getPortStatus();
    }

    @Override
    protected void mergeResponses(PortStatusDTO clientDto, Map<NodeIdentifier, PortStatusDTO> dtoMap, NodeIdentifier selectedNodeId) {
        final PortStatusDTO mergedPortStatus = clientDto;
        mergedPortStatus.setNodeSnapshots(new ArrayList<NodePortStatusSnapshotDTO>());

        final NodePortStatusSnapshotDTO selectedNodeSnapshot = new NodePortStatusSnapshotDTO();
        selectedNodeSnapshot.setStatusSnapshot(clientDto.getAggregateSnapshot().clone());
        selectedNodeSnapshot.setAddress(selectedNodeId.getApiAddress());
        selectedNodeSnapshot.setApiPort(selectedNodeId.getApiPort());
        selectedNodeSnapshot.setNodeId(selectedNodeId.getId());

        mergedPortStatus.getNodeSnapshots().add(selectedNodeSnapshot);

        // merge the other nodes
        for (final Map.Entry<NodeIdentifier, PortStatusDTO> entry : dtoMap.entrySet()) {
            final NodeIdentifier nodeId = entry.getKey();
            final PortStatusDTO nodePortStatus = entry.getValue();
            if (nodePortStatus == clientDto) {
                continue;
            }

            StatusMerger.merge(mergedPortStatus, nodePortStatus, nodeId.getId(), nodeId.getApiAddress(), nodeId.getApiPort());
        }
    }

}
