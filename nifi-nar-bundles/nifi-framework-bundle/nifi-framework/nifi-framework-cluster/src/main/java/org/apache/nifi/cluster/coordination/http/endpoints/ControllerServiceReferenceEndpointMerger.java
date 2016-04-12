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
import org.apache.nifi.web.api.dto.ControllerServiceReferencingComponentDTO;
import org.apache.nifi.web.api.entity.ControllerServiceReferencingComponentEntity;
import org.apache.nifi.web.api.entity.ControllerServiceReferencingComponentsEntity;

public class ControllerServiceReferenceEndpointMerger implements EndpointResponseMerger {
    public static final Pattern CONTROLLER_SERVICE_REFERENCES_URI_PATTERN = Pattern.compile("/nifi-api/controller-services/node/[a-f0-9\\-]{36}/references");

    @Override
    public boolean canHandle(URI uri, String method) {
        if (("GET".equalsIgnoreCase(method) || "PUT".equalsIgnoreCase(method)) && CONTROLLER_SERVICE_REFERENCES_URI_PATTERN.matcher(uri.getPath()).matches()) {
            return true;
        }

        return false;
    }

    @Override
    public NodeResponse merge(URI uri, String method, Set<NodeResponse> successfulResponses, Set<NodeResponse> problematicResponses, NodeResponse clientResponse) {
        if (!canHandle(uri, method)) {
            throw new IllegalArgumentException("Cannot use Endpoint Mapper of type " + getClass().getSimpleName() + " to map responses for URI " + uri + ", HTTP Method " + method);
        }

        final ControllerServiceReferencingComponentsEntity responseEntity = clientResponse.getClientResponse().getEntity(ControllerServiceReferencingComponentsEntity.class);
        final Set<ControllerServiceReferencingComponentEntity> referencingComponents = responseEntity.getControllerServiceReferencingComponents();

        final Map<NodeIdentifier, Set<ControllerServiceReferencingComponentEntity>> resultsMap = new HashMap<>();
        for (final NodeResponse nodeResponse : successfulResponses) {
            final ControllerServiceReferencingComponentsEntity nodeResponseEntity = nodeResponse == clientResponse ? responseEntity
                : nodeResponse.getClientResponse().getEntity(ControllerServiceReferencingComponentsEntity.class);
            final Set<ControllerServiceReferencingComponentEntity> nodeReferencingComponents = nodeResponseEntity.getControllerServiceReferencingComponents();

            resultsMap.put(nodeResponse.getNodeId(), nodeReferencingComponents);
        }

        ControllerServiceEndpointMerger.mergeControllerServiceReferences(referencingComponents, resultsMap);

        return new NodeResponse(clientResponse, responseEntity);
    }

}
