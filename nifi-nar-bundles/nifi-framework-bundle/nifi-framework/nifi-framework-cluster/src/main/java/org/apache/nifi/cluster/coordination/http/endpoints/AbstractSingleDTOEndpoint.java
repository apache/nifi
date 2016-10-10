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
import org.apache.nifi.web.api.entity.Entity;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public abstract class AbstractSingleDTOEndpoint<EntityType extends Entity, DtoType> implements EndpointResponseMerger {

    @Override
    public final NodeResponse merge(final URI uri, final String method, final Set<NodeResponse> successfulResponses, final Set<NodeResponse> problematicResponses, final NodeResponse clientResponse) {
        if (!canHandle(uri, method)) {
            throw new IllegalArgumentException("Cannot use Endpoint Mapper of type " + getClass().getSimpleName() + " to map responses for URI " + uri + ", HTTP Method " + method);
        }

        final EntityType responseEntity = clientResponse.getClientResponse().getEntity(getEntityClass());
        final DtoType dto = getDto(responseEntity);

        final Map<NodeIdentifier, DtoType> dtoMap = new HashMap<>();
        for (final NodeResponse nodeResponse : successfulResponses) {
            final EntityType nodeResponseEntity = nodeResponse == clientResponse ? responseEntity : nodeResponse.getClientResponse().getEntity(getEntityClass());
            final DtoType nodeDto = getDto(nodeResponseEntity);
            dtoMap.put(nodeResponse.getNodeId(), nodeDto);
        }

        mergeResponses(dto, dtoMap, successfulResponses, problematicResponses);
        return new NodeResponse(clientResponse, responseEntity);
    }

    /**
     * @return the class that represents the type of Entity that is expected by this response mapper
     */
    protected abstract Class<EntityType> getEntityClass();

    /**
     * Extracts the DTO from the given entity
     *
     * @param entity the entity to extract the DTO from
     * @return the DTO from the given entity
     */
    protected abstract DtoType getDto(EntityType entity);

    /**
     * Merges the responses from all nodes in the given map into the single given DTO
     *
     * @param clientDto the DTO to merge responses into
     * @param dtoMap the responses from all nodes
     * @param successfulResponses the responses from nodes that completed the request successfully
     * @param problematicResponses the responses from nodes that did not complete the request successfully
     */
    protected abstract void mergeResponses(DtoType clientDto, Map<NodeIdentifier, DtoType> dtoMap, Set<NodeResponse> successfulResponses, Set<NodeResponse> problematicResponses);

}
