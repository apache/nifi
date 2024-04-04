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
import org.apache.nifi.cluster.manager.FlowAnalysisRulesEntityMerger;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.entity.FlowAnalysisRuleEntity;
import org.apache.nifi.web.api.entity.FlowAnalysisRulesEntity;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class FlowAnalysisRulesEndpointMerger implements EndpointResponseMerger {
    public static final String FLOW_ANALYSIS_RULES_URI = "/nifi-api/controller/flow-analysis-rules";

    @Override
    public boolean canHandle(URI uri, String method) {
        return "GET".equalsIgnoreCase(method) && FLOW_ANALYSIS_RULES_URI.equals(uri.getPath());
    }

    @Override
    public final NodeResponse merge(final URI uri, final String method, final Set<NodeResponse> successfulResponses, final Set<NodeResponse> problematicResponses, final NodeResponse clientResponse) {
        if (!canHandle(uri, method)) {
            throw new IllegalArgumentException("Cannot use Endpoint Mapper of type " + getClass().getSimpleName() + " to map responses for URI " + uri + ", HTTP Method " + method);
        }

        final FlowAnalysisRulesEntity responseEntity = clientResponse.getClientResponse().readEntity(FlowAnalysisRulesEntity.class);
        final Set<FlowAnalysisRuleEntity> flowAnalysisRulesEntities = responseEntity.getFlowAnalysisRules();

        final Map<String, Map<NodeIdentifier, FlowAnalysisRuleEntity>> entityMap = new HashMap<>();
        for (final NodeResponse nodeResponse : successfulResponses) {
            final FlowAnalysisRulesEntity nodeResponseEntity = nodeResponse == clientResponse ? responseEntity : nodeResponse.getClientResponse().readEntity(FlowAnalysisRulesEntity.class);
            final Set<FlowAnalysisRuleEntity> nodeFlowAnalysisRuleEntities = nodeResponseEntity.getFlowAnalysisRules();

            for (final FlowAnalysisRuleEntity nodeFlowAnalysisRuleEntity : nodeFlowAnalysisRuleEntities) {
                final NodeIdentifier nodeId = nodeResponse.getNodeId();
                Map<NodeIdentifier, FlowAnalysisRuleEntity> innerMap = entityMap.get(nodeId);
                if (innerMap == null) {
                    innerMap = new HashMap<>();
                    entityMap.put(nodeFlowAnalysisRuleEntity.getId(), innerMap);
                }

                innerMap.put(nodeResponse.getNodeId(), nodeFlowAnalysisRuleEntity);
            }
        }

        FlowAnalysisRulesEntityMerger.mergeFlowAnalysisRules(flowAnalysisRulesEntities, entityMap);

        // create a new client response
        return new NodeResponse(clientResponse, responseEntity);
    }
}
