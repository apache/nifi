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
import org.apache.nifi.web.api.dto.status.NodeProcessorStatusSnapshotDTO;
import org.apache.nifi.web.api.dto.status.ProcessorStatusDTO;
import org.apache.nifi.web.api.entity.ProcessorStatusEntity;

import java.net.URI;
import java.util.ArrayList;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class ProcessorStatusEndpointMerger extends AbstractSingleEntityEndpoint<ProcessorStatusEntity> implements ComponentEntityStatusMerger<ProcessorStatusDTO> {
    public static final Pattern PROCESSOR_STATUS_URI_PATTERN = Pattern.compile("/nifi-api/flow/processors/[a-f0-9\\-]{36}/status");

    @Override
    public boolean canHandle(URI uri, String method) {
        return "GET".equalsIgnoreCase(method) && PROCESSOR_STATUS_URI_PATTERN.matcher(uri.getPath()).matches();
    }

    @Override
    protected Class<ProcessorStatusEntity> getEntityClass() {
        return ProcessorStatusEntity.class;
    }

    @Override
    protected void mergeResponses(ProcessorStatusEntity clientEntity, Map<NodeIdentifier, ProcessorStatusEntity> entityMap, Set<NodeResponse> successfulResponses,
                                  Set<NodeResponse> problematicResponses) {
        final ProcessorStatusDTO mergedProcessorStatus = clientEntity.getProcessorStatus();
        mergedProcessorStatus.setNodeSnapshots(new ArrayList<>());

        final NodeIdentifier selectedNodeId = entityMap.entrySet().stream()
                .filter(e -> e.getValue() == clientEntity)
                .map(e -> e.getKey())
                .findFirst()
                .orElse(null);

        final NodeProcessorStatusSnapshotDTO selectedNodeSnapshot = new NodeProcessorStatusSnapshotDTO();
        selectedNodeSnapshot.setStatusSnapshot(mergedProcessorStatus.getAggregateSnapshot().clone());
        selectedNodeSnapshot.setAddress(selectedNodeId.getApiAddress());
        selectedNodeSnapshot.setApiPort(selectedNodeId.getApiPort());
        selectedNodeSnapshot.setNodeId(selectedNodeId.getId());

        mergedProcessorStatus.getNodeSnapshots().add(selectedNodeSnapshot);

        // merge the other nodes
        for (final Map.Entry<NodeIdentifier, ProcessorStatusEntity> entry : entityMap.entrySet()) {
            final NodeIdentifier nodeId = entry.getKey();
            final ProcessorStatusEntity nodeProcessorStatusEntity = entry.getValue();
            final ProcessorStatusDTO nodeProcessorStatus = nodeProcessorStatusEntity.getProcessorStatus();
            if (nodeProcessorStatus == mergedProcessorStatus) {
                continue;
            }

            mergeStatus(mergedProcessorStatus, clientEntity.getCanRead(), nodeProcessorStatus, nodeProcessorStatusEntity.getCanRead(), nodeId);
        }
    }

    @Override
    public void mergeStatus(ProcessorStatusDTO clientStatus, boolean clientStatusReadablePermission, ProcessorStatusDTO status, boolean statusReadablePermission, NodeIdentifier statusNodeIdentifier) {
        StatusMerger.merge(clientStatus, clientStatusReadablePermission, status, statusReadablePermission, statusNodeIdentifier.getId(), statusNodeIdentifier.getApiAddress(),
                statusNodeIdentifier.getApiPort());
    }
}
