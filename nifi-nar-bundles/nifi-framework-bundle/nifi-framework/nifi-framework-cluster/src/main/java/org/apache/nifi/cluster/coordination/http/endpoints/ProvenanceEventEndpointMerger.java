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
import org.apache.nifi.web.api.dto.provenance.ProvenanceEventDTO;
import org.apache.nifi.web.api.entity.ProvenanceEventEntity;

public class ProvenanceEventEndpointMerger extends AbstractSingleEntityEndpoint<ProvenanceEventEntity, ProvenanceEventDTO> {
    public static final Pattern PROVENANCE_EVENT_URI = Pattern.compile("/nifi-api/provenance/events/[0-9]+");

    @Override
    public boolean canHandle(URI uri, String method) {
        return "GET".equalsIgnoreCase(method) && PROVENANCE_EVENT_URI.matcher(uri.getPath()).matches();
    }

    @Override
    protected Class<ProvenanceEventEntity> getEntityClass() {
        return ProvenanceEventEntity.class;
    }

    @Override
    protected ProvenanceEventDTO getDto(ProvenanceEventEntity entity) {
        return entity.getProvenanceEvent();
    }

    @Override
    protected void mergeResponses(ProvenanceEventDTO clientDto, Map<NodeIdentifier, ProvenanceEventDTO> dtoMap, Set<NodeResponse> successfulResponses, Set<NodeResponse> problematicResponses) {
        // The request for a Provenance Event is replicated to a single Node. We simply update its cluster node info.
        final NodeIdentifier nodeId = successfulResponses.iterator().next().getNodeId();
        clientDto.setClusterNodeId(nodeId.getId());
        clientDto.setClusterNodeAddress(nodeId.getApiAddress() + ":" + nodeId.getApiPort());
    }
}
