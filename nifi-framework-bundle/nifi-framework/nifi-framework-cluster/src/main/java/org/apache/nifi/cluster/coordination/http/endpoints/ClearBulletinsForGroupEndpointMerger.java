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

import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.entity.ClearBulletinsForGroupResultsEntity;

import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class ClearBulletinsForGroupEndpointMerger extends AbstractSingleEntityEndpoint<ClearBulletinsForGroupResultsEntity> {
    public static final Pattern CLEAR_BULLETINS_FOR_GROUP_URI_PATTERN = Pattern.compile("/nifi-api/process-groups/[a-f0-9\\-]{36}/bulletins");

    @Override
    public boolean canHandle(URI uri, String method) {
        return "POST".equalsIgnoreCase(method) && CLEAR_BULLETINS_FOR_GROUP_URI_PATTERN.matcher(uri.getPath()).matches();
    }

    @Override
    protected Class<ClearBulletinsForGroupResultsEntity> getEntityClass() {
        return ClearBulletinsForGroupResultsEntity.class;
    }

    @Override
    protected void mergeResponses(ClearBulletinsForGroupResultsEntity clientEntity, Map<NodeIdentifier, ClearBulletinsForGroupResultsEntity> entityMap,
                                  Set<NodeResponse> successfulResponses, Set<NodeResponse> problematicResponses) {

        // Sum up the total bulletins cleared across all nodes
        int totalBulletinsCleared = 0;
        for (final ClearBulletinsForGroupResultsEntity entity : entityMap.values()) {
            totalBulletinsCleared += entity.getBulletinsCleared();
        }
        clientEntity.setBulletinsCleared(totalBulletinsCleared);
    }
}
