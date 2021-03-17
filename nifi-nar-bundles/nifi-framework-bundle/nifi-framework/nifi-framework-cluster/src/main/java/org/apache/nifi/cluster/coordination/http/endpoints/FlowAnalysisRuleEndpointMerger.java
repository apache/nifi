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
import org.apache.nifi.cluster.manager.FlowAnalysisRuleEntityMerger;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.entity.FlowAnalysisRuleEntity;

import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class FlowAnalysisRuleEndpointMerger extends AbstractSingleEntityEndpoint<FlowAnalysisRuleEntity> implements EndpointResponseMerger {
    public static final String FLOW_ANALYSIS_RULES_URI = "/nifi-api/controller/flow-analysis-rules";
    public static final Pattern FLOW_ANALYSIS_RULE_URI_PATTERN = Pattern.compile("/nifi-api/flow-analysis-rules/[a-f0-9\\-]{36}");
    public static final Pattern FLOW_ANALYSIS_RULE_RUN_STATUS_URI_PATTERN = Pattern.compile("/nifi-api/flow-analysis-rules/[a-f0-9\\-]{36}/run-status");

    private final FlowAnalysisRuleEntityMerger flowAnalysisRuleEntityMerger = new FlowAnalysisRuleEntityMerger();

    @Override
    public boolean canHandle(URI uri, String method) {
        if (("GET".equalsIgnoreCase(method) || "PUT".equalsIgnoreCase(method)) && FLOW_ANALYSIS_RULE_URI_PATTERN.matcher(uri.getPath()).matches()) {
            return true;
        } else if ("PUT".equalsIgnoreCase(method) && FLOW_ANALYSIS_RULE_RUN_STATUS_URI_PATTERN.matcher(uri.getPath()).matches()) {
            return true;
        } else if ("POST".equalsIgnoreCase(method) && FLOW_ANALYSIS_RULES_URI.equals(uri.getPath())) {
            return true;
        }

        return false;
    }

    @Override
    protected Class<FlowAnalysisRuleEntity> getEntityClass() {
        return FlowAnalysisRuleEntity.class;
    }

    @Override
    protected void mergeResponses(
        FlowAnalysisRuleEntity clientEntity,
        Map<NodeIdentifier, FlowAnalysisRuleEntity> entityMap,
        Set<NodeResponse> successfulResponses,
        Set<NodeResponse> problematicResponses
    ) {
        flowAnalysisRuleEntityMerger.merge(clientEntity, entityMap);
    }
}
