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
import org.apache.nifi.web.api.dto.RemoteProcessGroupContentsDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupPortDTO;
import org.apache.nifi.web.api.dto.status.RemoteProcessGroupStatusDTO;
import org.apache.nifi.web.api.entity.RemoteProcessGroupEntity;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RemoteProcessGroupEntityMerger implements ComponentEntityMerger<RemoteProcessGroupEntity>, ComponentEntityStatusMerger<RemoteProcessGroupStatusDTO> {
    @Override
    public void merge(RemoteProcessGroupEntity clientEntity, Map<NodeIdentifier, RemoteProcessGroupEntity> entityMap) {
        ComponentEntityMerger.super.merge(clientEntity, entityMap);
        for (Map.Entry<NodeIdentifier, RemoteProcessGroupEntity> entry : entityMap.entrySet()) {
            final RemoteProcessGroupEntity entityStatus = entry.getValue();
            if (entityStatus != clientEntity) {
                mergeStatus(clientEntity.getStatus(), clientEntity.getPermissions().getCanRead(), entry.getValue().getStatus(), entry.getValue().getPermissions().getCanRead(), entry.getKey());
            }
        }
    }

    /**
     * Merges the RemoteProcessGroupEntity responses.
     *
     * @param clientEntity the entity being returned to the client
     * @param entityMap all node responses
     */
    @Override
    public void mergeComponents(final RemoteProcessGroupEntity clientEntity, final Map<NodeIdentifier, RemoteProcessGroupEntity> entityMap) {
        final RemoteProcessGroupDTO clientDto = clientEntity.getComponent();
        final Map<NodeIdentifier, RemoteProcessGroupDTO> dtoMap = new HashMap<>();
        for (final Map.Entry<NodeIdentifier, RemoteProcessGroupEntity> entry : entityMap.entrySet()) {
            final RemoteProcessGroupEntity nodeProcEntity = entry.getValue();
            final RemoteProcessGroupDTO nodeProcDto = nodeProcEntity.getComponent();
            dtoMap.put(entry.getKey(), nodeProcDto);
        }

        mergeDtos(clientDto, dtoMap);
    }

    @Override
    public void mergeStatus(RemoteProcessGroupStatusDTO clientStatus, boolean clientStatusReadablePermission, RemoteProcessGroupStatusDTO status,
                            boolean statusReadablePermission, NodeIdentifier statusNodeIdentifier) {
        StatusMerger.merge(clientStatus, clientStatusReadablePermission, status, statusReadablePermission, statusNodeIdentifier.getId(), statusNodeIdentifier.getApiAddress(),
                statusNodeIdentifier.getApiPort());
    }

    private static void mergeDtos(final RemoteProcessGroupDTO clientDto, final Map<NodeIdentifier, RemoteProcessGroupDTO> dtoMap) {
        // if unauthorized for the client dto, simple return
        if (clientDto == null) {
            return;
        }

        final RemoteProcessGroupContentsDTO remoteProcessGroupContents = clientDto.getContents();

        final Map<String, Set<NodeIdentifier>> authorizationErrorMap = new HashMap<>();
        final Map<String, Set<NodeIdentifier>> validationErrorMap = new HashMap<>();
        Boolean mergedIsTargetSecure = null;
        final Set<RemoteProcessGroupPortDTO> mergedInputPorts = new HashSet<>();
        final Set<RemoteProcessGroupPortDTO> mergedOutputPorts = new HashSet<>();

        for (final Map.Entry<NodeIdentifier, RemoteProcessGroupDTO> nodeEntry : dtoMap.entrySet()) {
            final RemoteProcessGroupDTO nodeRemoteProcessGroup = nodeEntry.getValue();

            // consider the node remote process group when authorized
            if (nodeRemoteProcessGroup != null) {
                final NodeIdentifier nodeId = nodeEntry.getKey();

                // merge the authorization errors
                ErrorMerger.mergeErrors(authorizationErrorMap, nodeId, nodeRemoteProcessGroup.getAuthorizationIssues());
                ErrorMerger.mergeErrors(validationErrorMap, nodeId, nodeRemoteProcessGroup.getValidationErrors());

                // use the first target secure flag since they will all be the same
                final Boolean nodeIsTargetSecure = nodeRemoteProcessGroup.isTargetSecure();
                if (mergedIsTargetSecure == null) {
                    mergedIsTargetSecure = nodeIsTargetSecure;
                }

                // merge the ports in the contents
                final RemoteProcessGroupContentsDTO nodeRemoteProcessGroupContentsDto = nodeRemoteProcessGroup.getContents();
                if (remoteProcessGroupContents != null && nodeRemoteProcessGroupContentsDto != null) {
                    if (nodeRemoteProcessGroupContentsDto.getInputPorts() != null) {
                        mergedInputPorts.addAll(nodeRemoteProcessGroupContentsDto.getInputPorts());
                    }
                    if (nodeRemoteProcessGroupContentsDto.getOutputPorts() != null) {
                        mergedOutputPorts.addAll(nodeRemoteProcessGroupContentsDto.getOutputPorts());
                    }
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

        // set the merged the validation errors
        clientDto.setAuthorizationIssues(ErrorMerger.normalizedMergedErrors(authorizationErrorMap, dtoMap.size()));
        clientDto.setValidationErrors(ErrorMerger.normalizedMergedErrors(validationErrorMap, dtoMap.size()));
    }
}
