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
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.dto.status.PortStatusDTO;
import org.apache.nifi.web.api.entity.PortEntity;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class PortEntityMerger implements ComponentEntityMerger<PortEntity>, ComponentEntityStatusMerger<PortStatusDTO> {

    @Override
    public void merge(PortEntity clientEntity, Map<NodeIdentifier, PortEntity> entityMap) {
        ComponentEntityMerger.super.merge(clientEntity, entityMap);
        for (Map.Entry<NodeIdentifier, PortEntity> entry : entityMap.entrySet()) {
            final NodeIdentifier nodeId = entry.getKey();
            final PortEntity entityStatus = entry.getValue();
            if (entityStatus != clientEntity) {
                mergeStatus(clientEntity.getStatus(), clientEntity.getPermissions().getCanRead(), entry.getValue().getStatus(), entry.getValue().getPermissions().getCanRead(), entry.getKey());
            }
        }
    }

    /**
     * Merges the PortEntity responses.
     *
     * @param clientEntity the entity being returned to the client
     * @param entityMap all node responses
     */
    @Override
    public void mergeComponents(PortEntity clientEntity, Map<NodeIdentifier, PortEntity> entityMap) {
        final PortDTO clientDto = clientEntity.getComponent();
        final Map<NodeIdentifier, PortDTO> dtoMap = new HashMap<>();
        for (final Map.Entry<NodeIdentifier, PortEntity> entry : entityMap.entrySet()) {
            final PortEntity nodePortEntity = entry.getValue();
            final PortDTO nodePortDto = nodePortEntity.getComponent();
            dtoMap.put(entry.getKey(), nodePortDto);
        }

        mergeDtos(clientDto, dtoMap);
    }

    @Override
    public void mergeStatus(PortStatusDTO clientStatus, boolean clientStatusReadablePermission, PortStatusDTO status, boolean statusReadablePermission, NodeIdentifier
            statusNodeIdentifier) {
        StatusMerger.merge(clientStatus, clientStatusReadablePermission, status, statusReadablePermission, statusNodeIdentifier.getId(), statusNodeIdentifier.getApiAddress(),
                statusNodeIdentifier.getApiPort());
    }

    public static void mergeDtos(final PortDTO clientDto, final Map<NodeIdentifier, PortDTO> dtoMap) {
        // if unauthorized for the client dto, simple return
        if (clientDto == null) {
            return;
        }

        final Map<String, Set<NodeIdentifier>> validationErrorMap = new HashMap<>();

        for (final Map.Entry<NodeIdentifier, PortDTO> nodeEntry : dtoMap.entrySet()) {
            final PortDTO nodePort = nodeEntry.getValue();

            // merge the validation errors if authorized
            if (nodePort != null) {
                final NodeIdentifier nodeId = nodeEntry.getKey();
                ErrorMerger.mergeErrors(validationErrorMap, nodeId, nodePort.getValidationErrors());
            }
        }

        // set the merged the validation errors
        clientDto.setValidationErrors(ErrorMerger.normalizedMergedErrors(validationErrorMap, dtoMap.size()));
    }
}
