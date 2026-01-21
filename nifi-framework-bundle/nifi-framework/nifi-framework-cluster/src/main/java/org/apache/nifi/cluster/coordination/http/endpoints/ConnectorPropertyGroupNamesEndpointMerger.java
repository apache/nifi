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
import org.apache.nifi.cluster.manager.ConfigurationStepNamesEntityMerger;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.entity.ConfigurationStepNamesEntity;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class ConnectorPropertyGroupNamesEndpointMerger implements EndpointResponseMerger {
    public static final Pattern CONNECTOR_PROPERTY_GROUPS_URI_PATTERN = Pattern.compile("/nifi-api/connectors/[a-f0-9\\-]{36}/configuration-steps");

    @Override
    public boolean canHandle(final URI uri, final String method) {
        return "GET".equalsIgnoreCase(method) && CONNECTOR_PROPERTY_GROUPS_URI_PATTERN.matcher(uri.getPath()).matches();
    }

    @Override
    public final NodeResponse merge(final URI uri, final String method, final Set<NodeResponse> successfulResponses,
                                  final Set<NodeResponse> problematicResponses, final NodeResponse clientResponse) {
        if (!canHandle(uri, method)) {
            throw new IllegalArgumentException("Cannot use Endpoint Mapper of type " + getClass().getSimpleName()
                                             + " to map responses for URI " + uri + ", HTTP Method " + method);
        }

        final ConfigurationStepNamesEntity responseEntity = clientResponse.getClientResponse().readEntity(ConfigurationStepNamesEntity.class);

        final Map<NodeIdentifier, ConfigurationStepNamesEntity> entityMap = new HashMap<>();
        for (final NodeResponse nodeResponse : successfulResponses) {
            final ConfigurationStepNamesEntity nodeResponseEntity = nodeResponse == clientResponse
                    ? responseEntity
                    : nodeResponse.getClientResponse().readEntity(ConfigurationStepNamesEntity.class);
            entityMap.put(nodeResponse.getNodeId(), nodeResponseEntity);
        }

        ConfigurationStepNamesEntityMerger.merge(responseEntity, entityMap);

        // create a new client response
        return new NodeResponse(clientResponse, responseEntity);
    }
}
