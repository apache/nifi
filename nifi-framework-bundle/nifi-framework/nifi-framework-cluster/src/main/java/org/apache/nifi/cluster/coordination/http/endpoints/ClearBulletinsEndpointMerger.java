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

import org.apache.nifi.cluster.manager.BulletinMerger;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.entity.BulletinEntity;
import org.apache.nifi.web.api.entity.ClearBulletinsResultEntity;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class ClearBulletinsEndpointMerger extends AbstractSingleEntityEndpoint<ClearBulletinsResultEntity> {
    public static final Pattern CLEAR_BULLETINS_URI_PATTERN =
            Pattern.compile("/nifi-api/(?:processors|controller-services|reporting-tasks|parameter-providers|registry-clients|flow-analysis-rules|remote-process-groups)/[a-f0-9\\-]{36}/bulletins");

    @Override
    public boolean canHandle(URI uri, String method) {
        return "POST".equalsIgnoreCase(method) && CLEAR_BULLETINS_URI_PATTERN.matcher(uri.getPath()).matches();
    }

    @Override
    protected Class<ClearBulletinsResultEntity> getEntityClass() {
        return ClearBulletinsResultEntity.class;
    }

    @Override
    protected void mergeResponses(ClearBulletinsResultEntity clientEntity, Map<NodeIdentifier, ClearBulletinsResultEntity> entityMap,
                                  Set<NodeResponse> successfulResponses, Set<NodeResponse> problematicResponses) {

        // Sum up the total bulletins cleared across all nodes
        int totalBulletinsCleared = 0;
        for (final ClearBulletinsResultEntity entity : entityMap.values()) {
            totalBulletinsCleared += entity.getBulletinsCleared();
        }
        clientEntity.setBulletinsCleared(totalBulletinsCleared);

        // Merge the remaining bulletins from all nodes
        final Map<NodeIdentifier, List<BulletinEntity>> bulletinMap = new HashMap<>();
        for (final Map.Entry<NodeIdentifier, ClearBulletinsResultEntity> entry : entityMap.entrySet()) {
            final NodeIdentifier nodeIdentifier = entry.getKey();
            final ClearBulletinsResultEntity entity = entry.getValue();
            final String nodeAddress = nodeIdentifier.getApiAddress() + ":" + nodeIdentifier.getApiPort();

            if (entity.getBulletins() != null) {
                final List<BulletinEntity> nodeBulletins = new ArrayList<>();
                for (final BulletinEntity bulletinEntity : entity.getBulletins()) {
                    if (bulletinEntity.getNodeAddress() == null) {
                        bulletinEntity.setNodeAddress(nodeAddress);
                    }
                    if (bulletinEntity.getCanRead() && bulletinEntity.getBulletin() != null && bulletinEntity.getBulletin().getNodeAddress() == null) {
                        bulletinEntity.getBulletin().setNodeAddress(nodeAddress);
                    }
                    nodeBulletins.add(bulletinEntity);
                }
                bulletinMap.put(nodeIdentifier, nodeBulletins);
            }
        }

        // Use the BulletinMerger to merge the bulletins properly
        final List<BulletinEntity> mergedBulletins = BulletinMerger.mergeBulletins(bulletinMap, entityMap.size());
        clientEntity.setBulletins(mergedBulletins);
    }
}
