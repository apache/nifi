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

import org.apache.nifi.cluster.coordination.http.EndpointResponseMerger;
import org.apache.nifi.cluster.manager.LabelsEntityMerger;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.entity.LabelEntity;
import org.apache.nifi.web.api.entity.LabelsEntity;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class LabelsEndpointMerger implements EndpointResponseMerger {
    public static final Pattern LABELS_URI_PATTERN = Pattern.compile("/nifi-api/process-groups/(?:(?:root)|(?:[a-f0-9\\-]{36}))/labels");

    @Override
    public boolean canHandle(URI uri, String method) {
        return "GET".equalsIgnoreCase(method) && LABELS_URI_PATTERN.matcher(uri.getPath()).matches();
    }

    @Override
    public NodeResponse merge(URI uri, String method, Set<NodeResponse> successfulResponses, Set<NodeResponse> problematicResponses, NodeResponse clientResponse) {
        if (!canHandle(uri, method)) {
            throw new IllegalArgumentException("Cannot use Endpoint Mapper of type " + getClass().getSimpleName() + " to map responses for URI " + uri + ", HTTP Method " + method);
        }

        final LabelsEntity responseEntity = clientResponse.getClientResponse().getEntity(LabelsEntity.class);
        final Set<LabelEntity> labelEntities = responseEntity.getLabels();

        final Map<String, Map<NodeIdentifier, LabelEntity>> entityMap = new HashMap<>();
        for (final NodeResponse nodeResponse : successfulResponses) {
            final LabelsEntity nodeResponseEntity = nodeResponse == clientResponse ? responseEntity : nodeResponse.getClientResponse().getEntity(LabelsEntity.class);
            final Set<LabelEntity> nodeLabelEntities = nodeResponseEntity.getLabels();

            for (final LabelEntity nodeLabelEntity : nodeLabelEntities) {
                final String nodeLabelEntityId = nodeLabelEntity.getId();
                Map<NodeIdentifier, LabelEntity> innerMap = entityMap.get(nodeLabelEntityId);
                if (innerMap == null) {
                    innerMap = new HashMap<>();
                    entityMap.put(nodeLabelEntityId, innerMap);
                }

                innerMap.put(nodeResponse.getNodeId(), nodeLabelEntity);
            }
        }

        LabelsEntityMerger.mergeLabels(labelEntities, entityMap);

        // create a new client response
        return new NodeResponse(clientResponse, responseEntity);
    }
}
