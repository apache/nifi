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

import org.apache.nifi.cluster.coordination.http.EndpointResponseMerger;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.entity.Entity;

public abstract class AbstractMultiEntityEndpoint<EntityType extends Entity, DtoType> implements EndpointResponseMerger {

    @Override
    public final NodeResponse merge(final URI uri, final String method, final Set<NodeResponse> successfulResponses, final Set<NodeResponse> problematicResponses, final NodeResponse clientResponse) {
        if (!canHandle(uri, method)) {
            throw new IllegalArgumentException("Cannot use Endpoint Mapper of type " + getClass().getSimpleName() + " to map responses for URI " + uri + ", HTTP Method " + method);
        }

        final EntityType responseEntity = clientResponse.getClientResponse().getEntity(getEntityClass());
        final Set<DtoType> dtos = getDtos(responseEntity);

        final Map<String, Map<NodeIdentifier, DtoType>> dtoMap = new HashMap<>();
        for (final NodeResponse nodeResponse : successfulResponses) {
            final EntityType nodeResponseEntity = nodeResponse == clientResponse ? responseEntity : nodeResponse.getClientResponse().getEntity(getEntityClass());
            final Set<DtoType> nodeDtos = getDtos(nodeResponseEntity);

            for (final DtoType nodeDto : nodeDtos) {
                final NodeIdentifier nodeId = nodeResponse.getNodeId();
                Map<NodeIdentifier, DtoType> innerMap = dtoMap.get(nodeId);
                if (innerMap == null) {
                    innerMap = new HashMap<>();
                    dtoMap.put(getComponentId(nodeDto), innerMap);
                }

                innerMap.put(nodeResponse.getNodeId(), nodeDto);
            }
        }

        for (final DtoType dto : dtos) {
            final String componentId = getComponentId(dto);
            final Map<NodeIdentifier, DtoType> mergeMap = dtoMap.get(componentId);

            mergeResponses(dto, mergeMap, successfulResponses, problematicResponses);
        }

        // create a new client response
        return new NodeResponse(clientResponse, responseEntity);
    }


    /**
     * @return the class that represents the type of Entity that is expected by this response mapper
     */
    protected abstract Class<EntityType> getEntityClass();

    /**
     * Extracts the DTOs from the given entity
     *
     * @param entity the entity to extract the DTOs from
     * @return the DTOs from the given entity
     */
    protected abstract Set<DtoType> getDtos(EntityType entity);

    /**
     * Extracts the ID of the component that the DTO refers to
     * @param dto the DTO to extract the ID from
     * @return the ID of the component that the DTO refers to
     */
    protected abstract String getComponentId(DtoType dto);

    /**
     * Merges the responses from all nodes in the given map into the single given DTO
     *
     * @param clientDto the DTO to merge responses into
     * @param dtoMap the responses from all nodes
     * @param successfulResponses the responses from nodes that completed the request successfully
     * @param problematicResponses the responses from nodes that did not complete the request successfully
     */
    protected abstract void mergeResponses(final DtoType clientDto, Map<NodeIdentifier, DtoType> dtoMap, Set<NodeResponse> successfulResponses, Set<NodeResponse> problematicResponses);
}
