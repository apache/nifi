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
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.status.ConnectionStatusDTO;
import org.apache.nifi.web.api.entity.ConnectionEntity;

import java.util.Map;
import java.util.Objects;

public class ConnectionEntityMerger implements ComponentEntityMerger<ConnectionEntity>, ComponentEntityStatusMerger<ConnectionStatusDTO> {

    @Override
    public void merge(ConnectionEntity clientEntity, Map<NodeIdentifier, ConnectionEntity> entityMap) {
        ComponentEntityMerger.super.merge(clientEntity, entityMap);
        for (Map.Entry<NodeIdentifier, ConnectionEntity> entry : entityMap.entrySet()) {
            final ConnectionEntity entityStatus = entry.getValue();
            if (entityStatus != clientEntity) {
                mergeStatus(clientEntity.getStatus(), clientEntity.getPermissions().getCanRead(), entry.getValue().getStatus(), entry.getValue().getPermissions().getCanRead(), entry.getKey());
            }
        }

        // If Load Balancing is configured but client entity indicates that data is not being transferred, we need to check if any other
        // node is actively transferring data. If Client Entity is transferring data, we already know the correct value for the Status,
        // and if the Connection is not configured for Load Balancing, then we also know the correct value, so no need to look at all of
        // the values of the other nodes.
        if (clientEntity.getComponent() != null && ConnectionDTO.LOAD_BALANCE_INACTIVE.equals(clientEntity.getComponent().getLoadBalanceStatus())) {
            final boolean anyActive = entityMap.values().stream()
                .map(ConnectionEntity::getComponent)
                .filter(Objects::nonNull)
                .map(ConnectionDTO::getLoadBalanceStatus)
                .anyMatch(status -> status.equals(ConnectionDTO.LOAD_BALANCE_ACTIVE));

            if (anyActive) {
                clientEntity.getComponent().setLoadBalanceStatus(ConnectionDTO.LOAD_BALANCE_ACTIVE);
            }
        }
    }

    @Override
    public void mergeStatus(ConnectionStatusDTO clientStatus, boolean clientStatusReadablePermission, ConnectionStatusDTO status, boolean statusReadablePermission,
                            NodeIdentifier statusNodeIdentifier) {
        StatusMerger.merge(clientStatus, clientStatusReadablePermission, status, statusReadablePermission, statusNodeIdentifier.getId(), statusNodeIdentifier.getApiAddress(),
                statusNodeIdentifier.getApiPort());
    }
}
