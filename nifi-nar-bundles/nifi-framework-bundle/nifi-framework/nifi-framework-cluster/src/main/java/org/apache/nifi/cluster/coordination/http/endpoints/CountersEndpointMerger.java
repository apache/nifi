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
import org.apache.nifi.web.api.dto.CountersDTO;
import org.apache.nifi.web.api.dto.NodeCountersSnapshotDTO;
import org.apache.nifi.web.api.entity.CountersEntity;

public class CountersEndpointMerger extends AbstractNodeStatusEndpoint<CountersEntity, CountersDTO> {
    public static final Pattern COUNTERS_URI_PATTERN = Pattern.compile("/nifi-api/counters");

    @Override
    public boolean canHandle(URI uri, String method) {
        return "GET".equalsIgnoreCase(method) && COUNTERS_URI_PATTERN.matcher(uri.getPath()).matches();
    }

    @Override
    protected Class<CountersEntity> getEntityClass() {
        return CountersEntity.class;
    }

    @Override
    protected CountersDTO getDto(CountersEntity entity) {
        return entity.getCounters();
    }

    @Override
    protected void mergeResponses(CountersDTO clientDto, Map<NodeIdentifier, CountersDTO> dtoMap, NodeIdentifier selectedNodeId) {
        final CountersDTO mergedCounters = clientDto;
        mergedCounters.setNodeSnapshots(new ArrayList<NodeCountersSnapshotDTO>());

        final NodeCountersSnapshotDTO selectedNodeSnapshot = new NodeCountersSnapshotDTO();
        selectedNodeSnapshot.setSnapshot(clientDto.getAggregateSnapshot().clone());
        selectedNodeSnapshot.setAddress(selectedNodeId.getApiAddress());
        selectedNodeSnapshot.setApiPort(selectedNodeId.getApiPort());
        selectedNodeSnapshot.setNodeId(selectedNodeId.getId());

        mergedCounters.getNodeSnapshots().add(selectedNodeSnapshot);

        for (final Map.Entry<NodeIdentifier, CountersDTO> entry : dtoMap.entrySet()) {
            final NodeIdentifier nodeId = entry.getKey();
            final CountersDTO toMerge = entry.getValue();
            if (toMerge == clientDto) {
                continue;
            }

            StatusMerger.merge(mergedCounters, toMerge, nodeId.getId(), nodeId.getApiAddress(), nodeId.getApiPort());
        }
    }

}
