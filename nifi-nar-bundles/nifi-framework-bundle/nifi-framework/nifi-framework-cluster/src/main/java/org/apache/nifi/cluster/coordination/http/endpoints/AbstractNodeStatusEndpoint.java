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

import java.util.Map;
import java.util.Set;

import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.entity.Entity;

public abstract class AbstractNodeStatusEndpoint<EntityType extends Entity, DtoType> extends AbstractSingleDTOEndpoint<EntityType, DtoType> {

    @Override
    protected final void mergeResponses(DtoType clientDto, Map<NodeIdentifier, DtoType> dtoMap, Set<NodeResponse> successfulResponses, Set<NodeResponse> problematicResponses) {
        final NodeIdentifier selectedNodeId = dtoMap.entrySet().stream()
            .filter(e -> e.getValue() == clientDto)
            .map(e -> e.getKey())
            .findFirst()
            .orElse(null);

        if (selectedNodeId == null) {
            throw new IllegalArgumentException("Attempted to merge Status request but could not find the appropriate Node Identifier");
        }

        mergeResponses(clientDto, dtoMap, selectedNodeId);
    }

    protected abstract void mergeResponses(DtoType clientDto, Map<NodeIdentifier, DtoType> dtoMap, NodeIdentifier selectedNodeId);
}
