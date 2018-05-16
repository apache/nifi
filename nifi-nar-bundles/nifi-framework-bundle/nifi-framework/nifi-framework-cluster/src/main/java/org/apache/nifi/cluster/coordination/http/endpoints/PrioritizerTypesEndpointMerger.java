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

import org.apache.nifi.cluster.manager.DocumentedTypesMerger;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.dto.DocumentedTypeDTO;
import org.apache.nifi.web.api.entity.PrioritizerTypesEntity;

import java.net.URI;
import java.util.Map;
import java.util.Set;

public class PrioritizerTypesEndpointMerger extends AbstractNodeStatusEndpoint<PrioritizerTypesEntity, Set<DocumentedTypeDTO>> {
    public static final String PRIORITIZER_TYPES_URI_PATTERN = "/nifi-api/flow/prioritizers";

    @Override
    public boolean canHandle(URI uri, String method) {
        return "GET".equalsIgnoreCase(method) && PRIORITIZER_TYPES_URI_PATTERN.equals(uri.getPath());
    }

    @Override
    protected Class<PrioritizerTypesEntity> getEntityClass() {
        return PrioritizerTypesEntity.class;
    }

    @Override
    protected Set<DocumentedTypeDTO> getDto(PrioritizerTypesEntity entity) {
        return entity.getPrioritizerTypes();
    }

    @Override
    protected void mergeResponses(Set<DocumentedTypeDTO> clientDto, Map<NodeIdentifier, Set<DocumentedTypeDTO>> dtoMap, NodeIdentifier selectedNodeId) {
        DocumentedTypesMerger.mergeDocumentedTypes(clientDto, dtoMap);
    }
}
