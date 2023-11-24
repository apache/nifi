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
import org.apache.nifi.web.api.dto.AnalyzeFlowRequestDTO;
import org.apache.nifi.web.api.entity.AnalyzeFlowRequestEntity;

import java.net.URI;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class AnalyzeFlowRequestEndpointMerger extends AbstractSingleDTOEndpoint<AnalyzeFlowRequestEntity, AnalyzeFlowRequestDTO> {
    public static final Pattern ANALYZE_FLOW_URI_PATTERN = Pattern.compile("/nifi-api/process-groups/flow-analysis/[a-f0-9\\-]{36}");

    @Override
    public boolean canHandle(URI uri, String method) {
        if (
            ("POST".equalsIgnoreCase(method) || "GET".equalsIgnoreCase(method) || "DELETE".equalsIgnoreCase(method))
                && ANALYZE_FLOW_URI_PATTERN.matcher(uri.getPath()).matches()
        ) {
            return true;
        }

        return false;
    }

    @Override
    protected Class<AnalyzeFlowRequestEntity> getEntityClass() {
        return AnalyzeFlowRequestEntity.class;
    }

    @Override
    protected AnalyzeFlowRequestDTO getDto(AnalyzeFlowRequestEntity entity) {
        AnalyzeFlowRequestDTO dto = entity.getAnalyzeFlowRequest();

        return dto;
    }

    @Override
    protected void mergeResponses(
        AnalyzeFlowRequestDTO clientDto,
        Map<NodeIdentifier, AnalyzeFlowRequestDTO> dtoMap,
        Set<NodeResponse> successfulResponses,
        Set<NodeResponse> problematicResponses
    ) {
        Collection<String> failureReasons = new HashSet<>();
        if (clientDto.getFailureReason() != null) {
            failureReasons.add(clientDto.getFailureReason());
        }

        for (final AnalyzeFlowRequestDTO requestDto : dtoMap.values()) {
            if (!requestDto.isComplete()) {
                clientDto.setComplete(false);
            }
            if (requestDto.getFailureReason() != null) {
                failureReasons.add(requestDto.getFailureReason());
            }
            if (requestDto.getLastUpdated() != null && (clientDto.getLastUpdated() == null || requestDto.getLastUpdated().after(clientDto.getLastUpdated()))) {
                clientDto.setLastUpdated(requestDto.getLastUpdated());
            }
            if (requestDto.getPercentCompleted() < clientDto.getPercentCompleted()) {
                clientDto.setPercentCompleted(requestDto.getPercentCompleted());
                clientDto.setState(requestDto.getState());
            }
        }

        String failureReason = failureReasons.stream()
            .filter(Objects::nonNull)
            .collect(Collectors.joining("\n"));
        if (!failureReason.isEmpty()) {
            clientDto.setFailureReason(failureReason);
        }
    }
}
