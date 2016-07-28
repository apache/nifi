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

import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.dto.FlowConfigurationDTO;
import org.apache.nifi.web.api.entity.FlowConfigurationEntity;

import java.net.URI;
import java.util.Map;
import java.util.regex.Pattern;

public class FlowConfigurationEndpointMerger extends AbstractNodeStatusEndpoint<FlowConfigurationEntity, FlowConfigurationDTO> {
    public static final Pattern FLOW_CONFIGURATION_URI_PATTERN = Pattern.compile("/nifi-api/flow/config");

    @Override
    public boolean canHandle(URI uri, String method) {
        return "GET".equalsIgnoreCase(method) && FLOW_CONFIGURATION_URI_PATTERN.matcher(uri.getPath()).matches();
    }

    @Override
    protected Class<FlowConfigurationEntity> getEntityClass() {
        return FlowConfigurationEntity.class;
    }

    @Override
    protected FlowConfigurationDTO getDto(FlowConfigurationEntity entity) {
        return entity.getFlowConfiguration();
    }

    @Override
    protected void mergeResponses(FlowConfigurationDTO clientDto, Map<NodeIdentifier, FlowConfigurationDTO> dtoMap, NodeIdentifier selectedNodeId) {

        for (final Map.Entry<NodeIdentifier, FlowConfigurationDTO> entry : dtoMap.entrySet()) {
            final NodeIdentifier nodeId = entry.getKey();
            final FlowConfigurationDTO toMerge = entry.getValue();
            if (toMerge != clientDto) {
                clientDto.setSupportsConfigurableAuthorizer(clientDto.getSupportsConfigurableAuthorizer() && toMerge.getSupportsConfigurableAuthorizer());
            }
        }
    }

}
