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
package org.apache.nifi.cluster.manager;

import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.dto.status.ProcessGroupStatusDTO;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;

import java.util.Map;
import java.util.stream.Collectors;

public class ProcessGroupEntityMerger implements ComponentEntityMerger<ProcessGroupEntity>, ComponentEntityStatusMerger<ProcessGroupStatusDTO> {

    @Override
    public void merge(ProcessGroupEntity clientEntity, Map<NodeIdentifier, ProcessGroupEntity> entityMap) {
        ComponentEntityMerger.super.merge(clientEntity, entityMap);
        merge(clientEntity.getStatus(), entityMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getStatus())));
    }

    /**
     * Merges the ProcessorGroupEntity responses.
     *
     * @param clientEntityStatus the entity being returned to the client
     * @param entityMap all node responses
     */
    public void merge(final ProcessGroupStatusDTO clientEntityStatus, final Map<NodeIdentifier, ProcessGroupStatusDTO> entityMap) {
        for (final Map.Entry<NodeIdentifier, ProcessGroupStatusDTO> entry : entityMap.entrySet()) {
            final NodeIdentifier nodeId = entry.getKey();
            final ProcessGroupStatusDTO entityStatus = entry.getValue();
            if (entityStatus != clientEntityStatus) {
                StatusMerger.merge(clientEntityStatus, entityStatus, nodeId.getId(), nodeId.getApiAddress(), nodeId.getApiPort());
            }
        }
    }
}
