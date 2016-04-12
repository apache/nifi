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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.nifi.cluster.coordination.http.EndpointResponseMerger;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;

public class ProcessGroupEndpointMerger implements EndpointResponseMerger {
    public static final Pattern PROCESS_GROUP_URI_PATTERN = Pattern.compile("/nifi-api/process-groups/(?:(?:root)|(?:[a-f0-9\\-]{36}))");

    @Override
    public boolean canHandle(final URI uri, final String method) {
        return ("GET".equalsIgnoreCase(method) || "PUT".equalsIgnoreCase(method)) && PROCESS_GROUP_URI_PATTERN.matcher(uri.getPath()).matches();
    }

    @Override
    public NodeResponse merge(final URI uri, final String method, final Set<NodeResponse> successfulResponses, final Set<NodeResponse> problematicResponses, final NodeResponse clientResponse) {
        if (!canHandle(uri, method)) {
            throw new IllegalArgumentException("Cannot use Endpoint Mapper of type " + getClass().getSimpleName() + " to map responses for URI " + uri + ", HTTP Method " + method);
        }

        final ProcessGroupEntity responseEntity = clientResponse.getClientResponse().getEntity(ProcessGroupEntity.class);
        final ProcessGroupDTO responseDto = responseEntity.getComponent();

        final FlowSnippetDTO contents = responseDto.getContents();
        if (contents == null) {
            return new NodeResponse(clientResponse, responseEntity);
        } else {
            final Map<String, Map<NodeIdentifier, ProcessorDTO>> processorMap = new HashMap<>();
            final Map<String, Map<NodeIdentifier, RemoteProcessGroupDTO>> remoteProcessGroupMap = new HashMap<>();

            for (final NodeResponse nodeResponse : successfulResponses) {
                final ProcessGroupEntity nodeResponseEntity = nodeResponse == clientResponse ? responseEntity : nodeResponse.getClientResponse().getEntity(ProcessGroupEntity.class);
                final ProcessGroupDTO nodeProcessGroup = nodeResponseEntity.getComponent();

                for (final ProcessorDTO nodeProcessor : nodeProcessGroup.getContents().getProcessors()) {
                    Map<NodeIdentifier, ProcessorDTO> innerMap = processorMap.get(nodeProcessor.getId());
                    if (innerMap == null) {
                        innerMap = new HashMap<>();
                        processorMap.put(nodeProcessor.getId(), innerMap);
                    }

                    innerMap.put(nodeResponse.getNodeId(), nodeProcessor);
                }

                for (final RemoteProcessGroupDTO nodeRemoteProcessGroup : nodeProcessGroup.getContents().getRemoteProcessGroups()) {
                    Map<NodeIdentifier, RemoteProcessGroupDTO> innerMap = remoteProcessGroupMap.get(nodeRemoteProcessGroup.getId());
                    if (innerMap == null) {
                        innerMap = new HashMap<>();
                        remoteProcessGroupMap.put(nodeRemoteProcessGroup.getId(), innerMap);
                    }

                    innerMap.put(nodeResponse.getNodeId(), nodeRemoteProcessGroup);
                }
            }

            final ProcessorEndpointMerger procMerger = new ProcessorEndpointMerger();
            for (final ProcessorDTO processor : contents.getProcessors()) {
                final String procId = processor.getId();
                final Map<NodeIdentifier, ProcessorDTO> mergeMap = processorMap.get(procId);

                procMerger.mergeResponses(processor, mergeMap, successfulResponses, problematicResponses);
            }

            final RemoteProcessGroupEndpointMerger rpgMerger = new RemoteProcessGroupEndpointMerger();
            for (final RemoteProcessGroupDTO remoteProcessGroup : contents.getRemoteProcessGroups()) {
                if (remoteProcessGroup.getContents() != null) {
                    final String remoteProcessGroupId = remoteProcessGroup.getId();
                    final Map<NodeIdentifier, RemoteProcessGroupDTO> mergeMap = remoteProcessGroupMap.get(remoteProcessGroupId);

                    rpgMerger.mergeResponses(remoteProcessGroup, mergeMap, successfulResponses, problematicResponses);
                }
            }
        }

        // create a new client response
        return new NodeResponse(clientResponse, responseEntity);
    }

}
