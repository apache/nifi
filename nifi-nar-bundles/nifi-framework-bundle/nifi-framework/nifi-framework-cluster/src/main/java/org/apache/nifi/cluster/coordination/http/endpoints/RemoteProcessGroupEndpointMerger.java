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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.dto.RemoteProcessGroupContentsDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupPortDTO;
import org.apache.nifi.web.api.entity.RemoteProcessGroupEntity;

public class RemoteProcessGroupEndpointMerger extends AbstractSingleDTOEndpoint<RemoteProcessGroupEntity, RemoteProcessGroupDTO> {
    public static final Pattern REMOTE_PROCESS_GROUPS_URI_PATTERN = Pattern.compile("/nifi-api/remote-process-groups");
    public static final Pattern REMOTE_PROCESS_GROUP_URI_PATTERN = Pattern.compile("/nifi-api/remote-process-groups/[a-f0-9\\-]{36}");

    @Override
    public boolean canHandle(final URI uri, final String method) {
        if (("GET".equalsIgnoreCase(method) || "PUT".equalsIgnoreCase(method)) && REMOTE_PROCESS_GROUP_URI_PATTERN.matcher(uri.getPath()).matches()) {
            return true;
        } else if ("POST".equalsIgnoreCase(method) && REMOTE_PROCESS_GROUPS_URI_PATTERN.matcher(uri.getPath()).matches()) {
            return true;
        }

        return false;
    }

    @Override
    protected Class<RemoteProcessGroupEntity> getEntityClass() {
        return RemoteProcessGroupEntity.class;
    }

    @Override
    protected RemoteProcessGroupDTO getDto(final RemoteProcessGroupEntity entity) {
        return entity.getComponent();
    }

    @Override
    protected void mergeResponses(RemoteProcessGroupDTO clientDto, Map<NodeIdentifier, RemoteProcessGroupDTO> dtoMap, Set<NodeResponse> successfulResponses, Set<NodeResponse> problematicResponses) {
        final RemoteProcessGroupContentsDTO remoteProcessGroupContents = clientDto.getContents();

        Boolean mergedIsTargetSecure = null;
        final List<String> mergedAuthorizationIssues = new ArrayList<>();
        final Set<RemoteProcessGroupPortDTO> mergedInputPorts = new HashSet<>();
        final Set<RemoteProcessGroupPortDTO> mergedOutputPorts = new HashSet<>();

        for (final Map.Entry<NodeIdentifier, RemoteProcessGroupDTO> nodeEntry : dtoMap.entrySet()) {
            final NodeIdentifier nodeId = nodeEntry.getKey();
            final RemoteProcessGroupDTO nodeRemoteProcessGroupDto = nodeEntry.getValue();

            // merge the issues
            final List<String> nodeAuthorizationIssues = nodeRemoteProcessGroupDto.getAuthorizationIssues();
            if (nodeAuthorizationIssues != null && !nodeAuthorizationIssues.isEmpty()) {
                for (final String nodeAuthorizationIssue : nodeAuthorizationIssues) {
                    mergedAuthorizationIssues.add(nodeId.getApiAddress() + ":" + nodeId.getApiPort() + " -- " + nodeAuthorizationIssue);
                }
            }

            // use the first target secure flag since they will all be the same
            final Boolean nodeIsTargetSecure = nodeRemoteProcessGroupDto.isTargetSecure();
            if (mergedIsTargetSecure == null) {
                mergedIsTargetSecure = nodeIsTargetSecure;
            }

            // merge the ports in the contents
            final RemoteProcessGroupContentsDTO nodeRemoteProcessGroupContentsDto = nodeRemoteProcessGroupDto.getContents();
            if (remoteProcessGroupContents != null && nodeRemoteProcessGroupContentsDto != null) {
                if (nodeRemoteProcessGroupContentsDto.getInputPorts() != null) {
                    mergedInputPorts.addAll(nodeRemoteProcessGroupContentsDto.getInputPorts());
                }
                if (nodeRemoteProcessGroupContentsDto.getOutputPorts() != null) {
                    mergedOutputPorts.addAll(nodeRemoteProcessGroupContentsDto.getOutputPorts());
                }
            }
        }

        if (remoteProcessGroupContents != null) {
            if (!mergedInputPorts.isEmpty()) {
                remoteProcessGroupContents.setInputPorts(mergedInputPorts);
            }
            if (!mergedOutputPorts.isEmpty()) {
                remoteProcessGroupContents.setOutputPorts(mergedOutputPorts);
            }
        }

        if (mergedIsTargetSecure != null) {
            clientDto.setTargetSecure(mergedIsTargetSecure);
        }

        if (!mergedAuthorizationIssues.isEmpty()) {
            clientDto.setAuthorizationIssues(mergedAuthorizationIssues);
        }
    }

    protected void mergeResponses(RemoteProcessGroupEntity clientEntity, Map<NodeIdentifier, RemoteProcessGroupEntity> entityMap,
        Set<NodeResponse> successfulResponses, Set<NodeResponse> problematicResponses) {

        final RemoteProcessGroupDTO clientDto = clientEntity.getComponent();
        final Map<NodeIdentifier, RemoteProcessGroupDTO> dtoMap = new HashMap<>();
        for (final Map.Entry<NodeIdentifier, RemoteProcessGroupEntity> entry : entityMap.entrySet()) {
            dtoMap.put(entry.getKey(), entry.getValue().getComponent());
        }

        mergeResponses(clientDto, dtoMap, successfulResponses, problematicResponses);
    }
}
