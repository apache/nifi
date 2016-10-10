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
import org.apache.nifi.cluster.manager.ConnectionsEntityMerger;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.ConnectionsEntity;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class ConnectionsEndpointMerger implements EndpointResponseMerger {
    public static final Pattern CONNECTIONS_URI_PATTERN = Pattern.compile("/nifi-api/process-groups/(?:(?:root)|(?:[a-f0-9\\-]{36}))/connections");

    @Override
    public boolean canHandle(final URI uri, final String method) {
        return "GET".equalsIgnoreCase(method) && CONNECTIONS_URI_PATTERN.matcher(uri.getPath()).matches();
    }

    @Override
    public final NodeResponse merge(final URI uri, final String method, final Set<NodeResponse> successfulResponses, final Set<NodeResponse> problematicResponses, final NodeResponse clientResponse) {
        if (!canHandle(uri, method)) {
            throw new IllegalArgumentException("Cannot use Endpoint Mapper of type " + getClass().getSimpleName() + " to map responses for URI " + uri + ", HTTP Method " + method);
        }

        final ConnectionsEntity responseEntity = clientResponse.getClientResponse().getEntity(ConnectionsEntity.class);
        final Set<ConnectionEntity> connectionEntities = responseEntity.getConnections();

        final Map<String, Map<NodeIdentifier, ConnectionEntity>> entityMap = new HashMap<>();
        for (final NodeResponse nodeResponse : successfulResponses) {
            final ConnectionsEntity nodeResponseEntity = nodeResponse == clientResponse ? responseEntity : nodeResponse.getClientResponse().getEntity(ConnectionsEntity.class);
            final Set<ConnectionEntity> nodeConnectionEntities = nodeResponseEntity.getConnections();

            for (final ConnectionEntity nodeConnectionEntity : nodeConnectionEntities) {
                final String nodeConnectionEntityId = nodeConnectionEntity.getId();
                Map<NodeIdentifier, ConnectionEntity> innerMap = entityMap.get(nodeConnectionEntityId);
                if (innerMap == null) {
                    innerMap = new HashMap<>();
                    entityMap.put(nodeConnectionEntityId, innerMap);
                }

                innerMap.put(nodeResponse.getNodeId(), nodeConnectionEntity);
            }
        }

        ConnectionsEntityMerger.mergeConnections(connectionEntities, entityMap);

        // create a new client response
        return new NodeResponse(clientResponse, responseEntity);
    }
}
