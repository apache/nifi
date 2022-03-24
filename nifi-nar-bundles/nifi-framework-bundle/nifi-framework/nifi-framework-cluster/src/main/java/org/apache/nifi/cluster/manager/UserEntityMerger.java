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
import org.apache.nifi.web.api.dto.UserDTO;
import org.apache.nifi.web.api.entity.AccessPolicySummaryEntity;
import org.apache.nifi.web.api.entity.TenantEntity;
import org.apache.nifi.web.api.entity.UserEntity;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class UserEntityMerger implements ComponentEntityMerger<UserEntity> {

    @Override
    public void merge(UserEntity clientEntity, Map<NodeIdentifier, UserEntity> entityMap) {
        ComponentEntityMerger.super.merge(clientEntity, entityMap);
    }

    /**
     * Merges the UserEntity responses.
     *
     * @param clientEntity the entity being returned to the client
     * @param entityMap all node responses
     */
    public void mergeComponents(final UserEntity clientEntity, final Map<NodeIdentifier, UserEntity> entityMap) {
        final UserDTO clientDto = clientEntity.getComponent();
        final Map<NodeIdentifier, UserDTO> dtoMap = new HashMap<>();
        for (final Map.Entry<NodeIdentifier, UserEntity> entry : entityMap.entrySet()) {
            final UserEntity nodeUserEntity = entry.getValue();
            final UserDTO nodeUserDto = nodeUserEntity.getComponent();
            dtoMap.put(entry.getKey(), nodeUserDto);
        }

        mergeDtos(clientDto, dtoMap);
    }

    private static void mergeDtos(final UserDTO clientDto, final Map<NodeIdentifier, UserDTO> dtoMap) {
        // if unauthorized for the client dto, simple return
        if (clientDto == null) {
            return;
        }

        final Set<AccessPolicySummaryEntity> accessPolicyEntities = new HashSet<>(clientDto.getAccessPolicies());
        final Set<TenantEntity> tenantEntities = new HashSet<>(clientDto.getUserGroups());

        for (final Map.Entry<NodeIdentifier, UserDTO> nodeEntry : dtoMap.entrySet()) {
            final UserDTO nodeUser = nodeEntry.getValue();

            if (nodeUser != null) {
                accessPolicyEntities.retainAll(nodeUser.getAccessPolicies());
                tenantEntities.retainAll(nodeUser.getUserGroups());
            }
        }

        clientDto.setAccessPolicies(accessPolicyEntities);
        clientDto.setUserGroups(tenantEntities);
    }
}
