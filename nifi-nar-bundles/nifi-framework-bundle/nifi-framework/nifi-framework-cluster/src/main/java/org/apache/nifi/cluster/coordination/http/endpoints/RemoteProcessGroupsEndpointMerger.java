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
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.entity.RemoteProcessGroupsEntity;

public class RemoteProcessGroupsEndpointMerger extends AbstractMultiEntityEndpoint<RemoteProcessGroupsEntity, RemoteProcessGroupDTO> {
    public static final Pattern REMOTE_PROCESS_GROUPS_URI_PATTERN = Pattern.compile("/nifi-api/controller/process-groups/(?:(?:root)|(?:[a-f0-9\\-]{36}))/remote-process-groups");

    @Override
    public boolean canHandle(final URI uri, final String method) {
        return "GET".equalsIgnoreCase(method) && REMOTE_PROCESS_GROUPS_URI_PATTERN.matcher(uri.getPath()).matches();
    }

    @Override
    protected Class<RemoteProcessGroupsEntity> getEntityClass() {
        return RemoteProcessGroupsEntity.class;
    }

    @Override
    protected Set<RemoteProcessGroupDTO> getDtos(final RemoteProcessGroupsEntity entity) {
        return entity.getRemoteProcessGroups();
    }

    @Override
    protected String getComponentId(final RemoteProcessGroupDTO dto) {
        return dto.getId();
    }

    @Override
    protected void mergeResponses(RemoteProcessGroupDTO clientDto, Map<NodeIdentifier, RemoteProcessGroupDTO> dtoMap, Set<NodeResponse> successfulResponses, Set<NodeResponse> problematicResponses) {
        new RemoteProcessGroupEndpointMerger().mergeResponses(clientDto, dtoMap, successfulResponses, problematicResponses);
    }
}
