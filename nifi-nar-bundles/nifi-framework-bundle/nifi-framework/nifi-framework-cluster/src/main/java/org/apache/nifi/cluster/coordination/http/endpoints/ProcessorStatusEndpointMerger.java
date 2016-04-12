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
import org.apache.nifi.web.api.dto.status.NodeProcessorStatusSnapshotDTO;
import org.apache.nifi.web.api.dto.status.ProcessorStatusDTO;
import org.apache.nifi.web.api.entity.ProcessorStatusEntity;

public class ProcessorStatusEndpointMerger extends AbstractNodeStatusEndpoint<ProcessorStatusEntity, ProcessorStatusDTO> {
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
    protected ProcessorStatusDTO getDto(ProcessorStatusEntity entity) {
        return entity.getProcessorStatus();
    }

    @Override
    protected void mergeResponses(ProcessorStatusDTO clientDto, Map<NodeIdentifier, ProcessorStatusDTO> dtoMap, NodeIdentifier selectedNodeId) {
        final ProcessorStatusDTO mergedProcessorStatus = clientDto;
        mergedProcessorStatus.setNodeSnapshots(new ArrayList<NodeProcessorStatusSnapshotDTO>());

        final NodeProcessorStatusSnapshotDTO selectedNodeSnapshot = new NodeProcessorStatusSnapshotDTO();
        selectedNodeSnapshot.setStatusSnapshot(clientDto.getAggregateSnapshot().clone());
        selectedNodeSnapshot.setAddress(selectedNodeId.getApiAddress());
        selectedNodeSnapshot.setApiPort(selectedNodeId.getApiPort());
        selectedNodeSnapshot.setNodeId(selectedNodeId.getId());

        mergedProcessorStatus.getNodeSnapshots().add(selectedNodeSnapshot);

        // merge the other nodes
        for (final Map.Entry<NodeIdentifier, ProcessorStatusDTO> entry : dtoMap.entrySet()) {
            final NodeIdentifier nodeId = entry.getKey();
            final ProcessorStatusDTO nodeProcessorStatus = entry.getValue();
            if (nodeProcessorStatus == clientDto) {
                continue;
            }

            StatusMerger.merge(mergedProcessorStatus, nodeProcessorStatus, nodeId.getId(), nodeId.getApiAddress(), nodeId.getApiPort());
        }
    }

}
