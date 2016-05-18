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

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.dto.BulletinBoardDTO;
import org.apache.nifi.web.api.dto.BulletinDTO;
import org.apache.nifi.web.api.entity.BulletinBoardEntity;

public class BulletinBoardEndpointMerger extends AbstractSingleEntityEndpoint<BulletinBoardEntity, BulletinBoardDTO> {
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
        final List<BulletinDTO> bulletinDtos = new ArrayList<>();
        for (final Map.Entry<NodeIdentifier, BulletinBoardDTO> entry : dtoMap.entrySet()) {
            final NodeIdentifier nodeId = entry.getKey();
            final BulletinBoardDTO boardDto = entry.getValue();
            final String nodeAddress = nodeId.getApiAddress() + ":" + nodeId.getApiPort();

            for (final BulletinDTO bulletin : boardDto.getBulletins()) {
                bulletin.setNodeAddress(nodeAddress);
                bulletinDtos.add(bulletin);
            }
        }

        Collections.sort(bulletinDtos, new Comparator<BulletinDTO>() {
            @Override
            public int compare(final BulletinDTO o1, final BulletinDTO o2) {
                final int timeComparison = o1.getTimestamp().compareTo(o2.getTimestamp());
                if (timeComparison != 0) {
                    return timeComparison;
                }

                return o1.getNodeAddress().compareTo(o2.getNodeAddress());
            }
        });

        clientDto.setBulletins(bulletinDtos);
    }

}
