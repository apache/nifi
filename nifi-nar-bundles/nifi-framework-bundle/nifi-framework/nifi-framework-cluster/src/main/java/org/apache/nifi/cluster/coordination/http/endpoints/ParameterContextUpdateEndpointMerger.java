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
import org.apache.nifi.web.api.dto.ParameterContextDTO;
import org.apache.nifi.web.api.dto.ParameterContextUpdateRequestDTO;
import org.apache.nifi.web.api.entity.ParameterContextUpdateRequestEntity;

import java.net.URI;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class ParameterContextUpdateEndpointMerger extends AbstractSingleEntityEndpoint<ParameterContextUpdateRequestEntity> implements EndpointResponseMerger {
    private static final Pattern PARAMETER_CONTEXT_URI = Pattern.compile("/nifi-api/parameter-contexts/[a-f0-9\\-]{36}");
    private static final String PARAMETER_CONTEXTS_URI = "/nifi-api/parameter-contexts";

    @Override
    public boolean canHandle(final URI uri, final String method) {
        return ("GET".equalsIgnoreCase(method) || "DELETE".equalsIgnoreCase(method)) && PARAMETER_CONTEXT_URI.matcher(uri.getPath()).matches()
            || "POST".equalsIgnoreCase(method) && PARAMETER_CONTEXTS_URI.equals(method);
    }

    @Override
    protected Class<ParameterContextUpdateRequestEntity> getEntityClass() {
        return ParameterContextUpdateRequestEntity.class;
    }

    @Override
    protected void mergeResponses(final ParameterContextUpdateRequestEntity clientEntity, final Map<NodeIdentifier, ParameterContextUpdateRequestEntity> entityMap,
                                  final Set<NodeResponse> successfulResponses, final Set<NodeResponse> problematicResponses) {

        final ParameterContextUpdateRequestDTO clientUpdateRequestDto = clientEntity.getRequest();

        for (final ParameterContextUpdateRequestEntity requestEntity : entityMap.values()) {
            final ParameterContextUpdateRequestDTO updateRequestDto = requestEntity.getRequest();
            clientUpdateRequestDto.setComplete(clientUpdateRequestDto.isComplete() && updateRequestDto.isComplete());

            if (updateRequestDto.getFailureReason() != null) {
                clientUpdateRequestDto.setFailureReason(updateRequestDto.getFailureReason());
            }

            clientUpdateRequestDto.setLastUpdated(new Date(Math.min(clientUpdateRequestDto.getLastUpdated().getTime(), updateRequestDto.getLastUpdated().getTime())));
            clientUpdateRequestDto.setPercentCompleted(Math.min(clientUpdateRequestDto.getPercentCompleted(), updateRequestDto.getPercentCompleted()));
        }

        final Map<NodeIdentifier, ParameterContextDTO> contextDtoMap = new HashMap<>();
        entityMap.forEach( (nodeId, entity) -> contextDtoMap.put(nodeId, entity.getRequest().getParameterContext()));

        ParameterContextMerger.merge(clientUpdateRequestDto.getParameterContext(), contextDtoMap);
    }

}
