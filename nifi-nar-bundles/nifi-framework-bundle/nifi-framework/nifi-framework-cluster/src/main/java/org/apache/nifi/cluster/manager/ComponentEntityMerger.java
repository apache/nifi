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
import org.apache.nifi.web.api.entity.BulletinEntity;
import org.apache.nifi.web.api.entity.ComponentEntity;
import org.apache.nifi.web.api.entity.OperationPermissible;
import org.apache.nifi.web.api.entity.Permissible;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.nifi.cluster.manager.BulletinMerger.BULLETIN_COMPARATOR;
import static org.apache.nifi.reporting.BulletinRepository.MAX_BULLETINS_PER_COMPONENT;

public interface ComponentEntityMerger<EntityType extends ComponentEntity & Permissible> {

    /**
     * Merges the ComponentEntity responses according to their {@link org.apache.nifi.web.api.dto.PermissionsDTO}s.  Responsible for invoking
     * {@link ComponentEntityMerger#mergeComponents(EntityType, Map)}.
     *
     * @param clientEntity the entity being returned to the client
     * @param entityMap    all node responses
     */
    @SuppressWarnings("unchecked")
    default void merge(final EntityType clientEntity, final Map<NodeIdentifier, EntityType> entityMap) {
        for (final Map.Entry<NodeIdentifier, EntityType> entry : entityMap.entrySet()) {
            final EntityType entity = entry.getValue();
            PermissionsDtoMerger.mergePermissions(clientEntity.getPermissions(), entity.getPermissions());
            if (clientEntity instanceof OperationPermissible && entity instanceof  OperationPermissible) {
                PermissionsDtoMerger.mergePermissions(
                        ((OperationPermissible) clientEntity).getOperatePermissions(),
                        ((OperationPermissible) entity).getOperatePermissions());
            }
        }

        if (clientEntity.getPermissions().getCanRead()) {
            final Map<NodeIdentifier, List<BulletinEntity>> bulletinEntities = new HashMap<>();
            for (final Map.Entry<NodeIdentifier, ? extends ComponentEntity> entry : entityMap.entrySet()) {
                final NodeIdentifier nodeIdentifier = entry.getKey();
                final ComponentEntity entity = entry.getValue();

                // consider the bulletins if present and authorized
                if (entity.getBulletins() != null) {
                    entity.getBulletins().forEach(bulletin -> {
                        bulletinEntities.computeIfAbsent(nodeIdentifier, nodeId -> new ArrayList<>()).add(bulletin);
                    });
                }
            }
            clientEntity.setBulletins(BulletinMerger.mergeBulletins(bulletinEntities, entityMap.size()));

            // sort the results
            Collections.sort(clientEntity.getBulletins(), BULLETIN_COMPARATOR);

            // prune the response to only include the max number of bulletins
            if (clientEntity.getBulletins().size() > MAX_BULLETINS_PER_COMPONENT) {
                clientEntity.setBulletins(clientEntity.getBulletins().subList(0, MAX_BULLETINS_PER_COMPONENT));
            }

            mergeComponents(clientEntity, entityMap);
        } else {
            clientEntity.setBulletins(null);
            clientEntity.setComponent(null); // unchecked warning suppressed
        }
    }

    /**
     * Performs the merging of the entities.  This method should not be called directly, it will be called by {@link ComponentEntityMerger#merge}.
     * @param clientEntity the entity being returned to the client
     * @param entityMap    all node responses
     */
    default void mergeComponents(final EntityType clientEntity, final Map<NodeIdentifier, EntityType> entityMap) {
    }
}
