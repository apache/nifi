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
import org.apache.nifi.cluster.manager.FlowRegistryClientsEntityMerger;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.entity.FlowRegistryClientEntity;
import org.apache.nifi.web.api.entity.FlowRegistryClientsEntity;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class FlowRegistryClientsEndpointMerger implements EndpointResponseMerger {
    public static final String FLOW_REGISTRY_URI = "/nifi-api/flow/registries";
    public static final String CONTROLLER_REGISTRY_URI = "/nifi-api/controller/registry-clients";

    @Override
    public boolean canHandle(URI uri, String method) {
        return "GET".equalsIgnoreCase(method) && (FLOW_REGISTRY_URI.equals(uri.getPath()) || CONTROLLER_REGISTRY_URI.equals(uri.getPath()));
    }

    @Override
    public NodeResponse merge(final URI uri, final String method, final Set<NodeResponse> successfulResponses, final Set<NodeResponse> problematicResponses, final NodeResponse clientResponse) {
        if (!canHandle(uri, method)) {
            throw new IllegalArgumentException("Cannot use Endpoint Mapper of type " + getClass().getSimpleName() + " to map responses for URI " + uri + ", HTTP Method " + method);
        }

        final FlowRegistryClientsEntity responseEntity = clientResponse.getClientResponse().readEntity(FlowRegistryClientsEntity.class);
        final Set<FlowRegistryClientEntity> clientEntities = responseEntity.getRegistries();

        final Map<String, Map<NodeIdentifier, FlowRegistryClientEntity>> entityMap = new HashMap<>();

        for (final NodeResponse nodeResponse : successfulResponses) {
            final FlowRegistryClientsEntity nodeResponseEntity = nodeResponse == clientResponse ? responseEntity : nodeResponse.getClientResponse().readEntity(FlowRegistryClientsEntity.class);
            final Set<FlowRegistryClientEntity> nodeFlowRegistryClientEntities = nodeResponseEntity.getRegistries();

            for (final FlowRegistryClientEntity nodeFlowRegistryClientEntity : nodeFlowRegistryClientEntities) {
                final Map<NodeIdentifier, FlowRegistryClientEntity> innerMap = entityMap.computeIfAbsent(nodeFlowRegistryClientEntity.getId(), k -> new HashMap<>());
                innerMap.put(nodeResponse.getNodeId(), nodeFlowRegistryClientEntity);
            }
        }

        FlowRegistryClientsEntityMerger.mergeFlowRegistryClients(clientEntities, entityMap);

        // create a new client response
        return new NodeResponse(clientResponse, responseEntity);
    }
}
