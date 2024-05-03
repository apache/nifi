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
import org.apache.nifi.web.api.dto.BulletinBoardDTO;
import org.apache.nifi.web.api.entity.BulletinBoardEntity;
import org.apache.nifi.web.api.entity.BulletinEntity;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class BulletinBoardEndpointMerger extends AbstractSingleDTOEndpoint<BulletinBoardEntity, BulletinBoardDTO> {
    public static final Pattern BULLETIN_BOARD_URI_PATTERN = Pattern.compile("/nifi-api/flow/bulletin-board");

    @Override
    public boolean canHandle(URI uri, String method) {
        return "GET".equalsIgnoreCase(method) && BULLETIN_BOARD_URI_PATTERN.matcher(uri.getPath()).matches();
    }

    @Override
    protected Class<BulletinBoardEntity> getEntityClass() {
        return BulletinBoardEntity.class;
    }

    @Override
    protected BulletinBoardDTO getDto(BulletinBoardEntity entity) {
        return entity.getBulletinBoard();
    }

    @Override
    protected void mergeResponses(BulletinBoardDTO clientDto, Map<NodeIdentifier, BulletinBoardDTO> dtoMap, Set<NodeResponse> successfulResponses, Set<NodeResponse> problematicResponses) {
        final Map<NodeIdentifier, List<BulletinEntity>> bulletinEntities = new HashMap<>();
        for (final Map.Entry<NodeIdentifier, BulletinBoardDTO> entry : dtoMap.entrySet()) {
            final NodeIdentifier nodeIdentifier = entry.getKey();
            final BulletinBoardDTO boardDto = entry.getValue();
            boardDto.getBulletins().forEach(bulletin -> {
                bulletinEntities.computeIfAbsent(nodeIdentifier, nodeId -> new ArrayList<>()).add(bulletin);
            });
        }

        clientDto.setBulletins(BulletinMerger.mergeBulletins(bulletinEntities, dtoMap.size()));
    }

}
