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

public class ProcessGroupEntityMerger implements ComponentEntityMerger<ProcessGroupEntity>, ComponentEntityStatusMerger<ProcessGroupStatusDTO> {

    @Override
    public void merge(ProcessGroupEntity clientEntity, Map<NodeIdentifier, ProcessGroupEntity> entityMap) {
        ComponentEntityMerger.super.merge(clientEntity, entityMap);
        for (Map.Entry<NodeIdentifier, ProcessGroupEntity> entry : entityMap.entrySet()) {
            final ProcessGroupEntity entityStatus = entry.getValue();
            if (entityStatus != clientEntity) {
                mergeStatus(clientEntity.getStatus(), clientEntity.getPermissions().getCanRead(), entry.getValue().getStatus(), entry.getValue().getPermissions().getCanRead(), entry.getKey());
            }
        }
    }

    @Override
    public void mergeStatus(ProcessGroupStatusDTO clientStatus, boolean clientStatusReadablePermission, ProcessGroupStatusDTO status, boolean statusReadablePermission,
                            NodeIdentifier statusNodeIdentifier) {
        StatusMerger.merge(clientStatus, clientStatusReadablePermission, status, statusReadablePermission, statusNodeIdentifier.getId(), statusNodeIdentifier.getApiAddress(),
                statusNodeIdentifier.getApiPort());
    }
}
