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
import org.apache.nifi.web.api.dto.AccessPolicyDTO;
import org.apache.nifi.web.api.entity.AccessPolicyEntity;
import org.apache.nifi.web.api.entity.TenantEntity;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class AccessPolicyEntityMerger implements ComponentEntityMerger<AccessPolicyEntity> {
    @Override
    public void merge(AccessPolicyEntity clientEntity, Map<NodeIdentifier, AccessPolicyEntity> entityMap) {
        ComponentEntityMerger.super.merge(clientEntity, entityMap);
    }

    /**
     * Merges the AccessPolicyEntity responses.
     *
     * @param clientEntity the entity being returned to the client
     * @param entityMap all node responses
     */
    public void mergeComponents(final AccessPolicyEntity clientEntity, final Map<NodeIdentifier, AccessPolicyEntity> entityMap) {
        final AccessPolicyDTO clientDto = clientEntity.getComponent();
        final Map<NodeIdentifier, AccessPolicyDTO> dtoMap = new HashMap<>();
        for (final Map.Entry<NodeIdentifier, AccessPolicyEntity> entry : entityMap.entrySet()) {
            final AccessPolicyEntity nodeAccessPolicyEntity = entry.getValue();
            final AccessPolicyDTO nodeAccessPolicyDto = nodeAccessPolicyEntity.getComponent();
            dtoMap.put(entry.getKey(), nodeAccessPolicyDto);
        }

        mergeDtos(clientDto, dtoMap);
    }

    private static void mergeDtos(final AccessPolicyDTO clientDto, final Map<NodeIdentifier, AccessPolicyDTO> dtoMap) {
        // if unauthorized for the client dto, simple return
        if (clientDto == null) {
            return;
        }

        final Set<TenantEntity> users = new HashSet<>(clientDto.getUsers());
        final Set<TenantEntity> userGroups = new HashSet<>(clientDto.getUserGroups());

        for (final Map.Entry<NodeIdentifier, AccessPolicyDTO> nodeEntry : dtoMap.entrySet()) {
            final AccessPolicyDTO nodeAccessPolicy = nodeEntry.getValue();

            if (nodeAccessPolicy != null) {
                users.retainAll(nodeAccessPolicy.getUsers());
                userGroups.retainAll(nodeAccessPolicy.getUserGroups());
            }
        }

        clientDto.setUsers(users);
        clientDto.setUserGroups(userGroups);
    }
}
