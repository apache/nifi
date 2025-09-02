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

import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.dto.flow.ProcessGroupFlowDTO;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;

import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class ConnectorFlowEndpointMerger extends AbstractSingleDTOEndpoint<ProcessGroupFlowEntity, ProcessGroupFlowDTO> {
    public static final Pattern CONNECTOR_FLOW_URI_PATTERN = Pattern.compile("/nifi-api/connectors/[a-f0-9\\-]{36}/flow");

    @Override
    public boolean canHandle(final URI uri, final String method) {
        return "GET".equalsIgnoreCase(method) && CONNECTOR_FLOW_URI_PATTERN.matcher(uri.getPath()).matches();
    }

    @Override
    protected Class<ProcessGroupFlowEntity> getEntityClass() {
        return ProcessGroupFlowEntity.class;
    }

    @Override
    protected ProcessGroupFlowDTO getDto(final ProcessGroupFlowEntity entity) {
        return entity.getProcessGroupFlow();
    }

    @Override
    protected void mergeResponses(final ProcessGroupFlowDTO clientDto, final Map<NodeIdentifier, ProcessGroupFlowDTO> dtoMap,
        final Set<NodeResponse> successfulResponses, final Set<NodeResponse> problematicResponses) {

        // Delegate to the shared flow merging logic
        FlowMerger.mergeProcessGroupFlowDto(clientDto, dtoMap);
    }
}

