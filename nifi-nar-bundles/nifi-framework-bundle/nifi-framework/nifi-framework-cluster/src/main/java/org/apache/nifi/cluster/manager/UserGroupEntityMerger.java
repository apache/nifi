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
import org.apache.nifi.web.api.dto.UserGroupDTO;
import org.apache.nifi.web.api.entity.AccessPolicyEntity;
import org.apache.nifi.web.api.entity.TenantEntity;
import org.apache.nifi.web.api.entity.UserGroupEntity;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class UserGroupEntityMerger implements ComponentEntityMerger<UserGroupEntity> {
    @Override
    public void merge(UserGroupEntity clientEntity, Map<NodeIdentifier, UserGroupEntity> entityMap) {
        ComponentEntityMerger.super.merge(clientEntity, entityMap);
    }

    /**
     * Merges the UserGroupEntity responses.
     *
     * @param clientEntity the entity being returned to the client
     * @param entityMap all node responses
     */
    public void mergeComponents(final UserGroupEntity clientEntity, final Map<NodeIdentifier, UserGroupEntity> entityMap) {
        final UserGroupDTO clientDto = clientEntity.getComponent();
        final Map<NodeIdentifier, UserGroupDTO> dtoMap = new HashMap<>();
        for (final Map.Entry<NodeIdentifier, UserGroupEntity> entry : entityMap.entrySet()) {
            final UserGroupEntity nodeUserGroupEntity = entry.getValue();
            final UserGroupDTO nodeUserGroupDto = nodeUserGroupEntity.getComponent();
            dtoMap.put(entry.getKey(), nodeUserGroupDto);
        }

        mergeDtos(clientDto, dtoMap);
    }

    private static void mergeDtos(final UserGroupDTO clientDto, final Map<NodeIdentifier, UserGroupDTO> dtoMap) {
        // if unauthorized for the client dto, simple return
        if (clientDto == null) {
            return;
        }

        final Set<AccessPolicyEntity> accessPolicyEntities = new HashSet<>(clientDto.getAccessPolicies());
        final Set<TenantEntity> userEntities = new HashSet<>(clientDto.getUsers());

        for (final Map.Entry<NodeIdentifier, UserGroupDTO> nodeEntry : dtoMap.entrySet()) {
            final UserGroupDTO nodeUserGroup = nodeEntry.getValue();

            if (nodeUserGroup != null) {
                accessPolicyEntities.retainAll(nodeUserGroup.getAccessPolicies());
                userEntities.retainAll(nodeUserGroup.getUsers());
            }
        }

        clientDto.setAccessPolicies(accessPolicyEntities);
        clientDto.setUsers(userEntities);
    }
}
