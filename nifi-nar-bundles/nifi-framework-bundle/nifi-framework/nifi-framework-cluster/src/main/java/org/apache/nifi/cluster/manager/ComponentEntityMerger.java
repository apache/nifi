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
import org.apache.nifi.web.api.dto.BulletinDTO;
import org.apache.nifi.web.api.entity.ComponentEntity;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ComponentEntityMerger {

    /**
     * Merges the ComponentEntity responses.
     *
     * @param clientEntity the entity being returned to the client
     * @param entityMap all node responses
     */
    public static void mergeComponents(final ComponentEntity clientEntity, final Map<NodeIdentifier, ? extends ComponentEntity> entityMap) {
        final Map<NodeIdentifier, List<BulletinDTO>> bulletinDtos = new HashMap<>();
        for (final Map.Entry<NodeIdentifier, ? extends ComponentEntity> entry : entityMap.entrySet()) {
            final NodeIdentifier nodeIdentifier = entry.getKey();
            final ComponentEntity entity = entry.getValue();

            // consider the bulletins if present and authorized
            if (entity.getBulletins() != null) {
                entity.getBulletins().forEach(bulletin -> {
                    bulletinDtos.computeIfAbsent(nodeIdentifier, nodeId -> new ArrayList<>()).add(bulletin);
                });
            }
        }
        clientEntity.setBulletins(BulletinMerger.mergeBulletins(bulletinDtos));
    }
}
