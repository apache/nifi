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
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.dto.provenance.LatestProvenanceEventsDTO;
import org.apache.nifi.web.api.dto.provenance.ProvenanceEventDTO;
import org.apache.nifi.web.api.entity.LatestProvenanceEventsEntity;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

public class LatestProvenanceEventsMerger implements EndpointResponseMerger {
    public static final Pattern LATEST_EVENTS_URI = Pattern.compile("/nifi-api/provenance-events/latest/[a-f0-9\\-]{36}");

    @Override
    public boolean canHandle(final URI uri, final String method) {
        if ("GET".equalsIgnoreCase(method) && LATEST_EVENTS_URI.matcher(uri.getPath()).matches()) {
            return true;
        }

        return false;
    }

    @Override
    public NodeResponse merge(final URI uri, final String method, final Set<NodeResponse> successfulResponses, final Set<NodeResponse> problematicResponses, final NodeResponse clientResponse) {
        if (!canHandle(uri, method)) {
            throw new IllegalArgumentException("Cannot use Endpoint Mapper of type " + getClass().getSimpleName() + " to map responses for URI " + uri + ", HTTP Method " + method);
        }

        final LatestProvenanceEventsEntity responseEntity = clientResponse.getClientResponse().readEntity(LatestProvenanceEventsEntity.class);
        final LatestProvenanceEventsDTO dto = responseEntity.getLatestProvenanceEvents();
        final List<ProvenanceEventDTO> mergedEvents = new ArrayList<>();

        for (final NodeResponse nodeResponse : successfulResponses) {
            final NodeIdentifier nodeId = nodeResponse.getNodeId();

            final LatestProvenanceEventsEntity nodeResponseEntity = nodeResponse.getClientResponse().readEntity(LatestProvenanceEventsEntity.class);
            final List<ProvenanceEventDTO> nodeEvents = nodeResponseEntity.getLatestProvenanceEvents().getProvenanceEvents();

            // if the cluster node id or node address is not set, then we need to populate them. If they
            // are already set, we don't want to populate them because it will be the case that they were populated
            // by the Cluster Coordinator when it federated the request, and we are now just receiving the response
            // from the Cluster Coordinator.
            for (final ProvenanceEventDTO eventDto : nodeEvents) {
                if (eventDto.getClusterNodeId() == null || eventDto.getClusterNodeAddress() == null) {
                    eventDto.setClusterNodeId(nodeId.getId());
                    eventDto.setClusterNodeAddress(nodeId.getApiAddress() + ":" + nodeId.getApiPort());
                }
            }

            mergedEvents.addAll(nodeEvents);
        }

        dto.setProvenanceEvents(mergedEvents);

        return new NodeResponse(clientResponse, responseEntity);
    }

}
