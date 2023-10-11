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
import org.apache.nifi.cluster.manager.FlowAnalysisResultEntityMerger;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.entity.FlowAnalysisResultEntity;

import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class FlowAnalysisEndpointMerger extends AbstractSingleEntityEndpoint<FlowAnalysisResultEntity> implements EndpointResponseMerger {
    public static final String GET_ALL_FLOW_ANALYSIS_RESULTS_URI = "/nifi-api/flow/flow-analysis/result";
    public static final Pattern GET_GROUP_FLOW_ANALYSIS_RESULTS_URI_PATTERN = Pattern.compile("/nifi-api/flow/flow-analysis/result/[a-f0-9\\-]{36}");

    private final FlowAnalysisResultEntityMerger flowAnalysisResultEntityMerger = new FlowAnalysisResultEntityMerger();

    @Override
    public boolean canHandle(URI uri, String method) {
        if ("GET".equalsIgnoreCase(method)
            && (GET_ALL_FLOW_ANALYSIS_RESULTS_URI.equals(uri.getPath()) ||  GET_GROUP_FLOW_ANALYSIS_RESULTS_URI_PATTERN.matcher(uri.getPath()).matches())
        ) {
            return true;
        }

        return false;
    }

    @Override
    protected Class<FlowAnalysisResultEntity> getEntityClass() {
        return FlowAnalysisResultEntity.class;
    }

    @Override
    protected void mergeResponses(
        FlowAnalysisResultEntity clientEntity,
        Map<NodeIdentifier, FlowAnalysisResultEntity> entityMap,
        Set<NodeResponse> successfulResponses,
        Set<NodeResponse> problematicResponses
    ) {
        flowAnalysisResultEntityMerger.merge(clientEntity, entityMap);
    }
}
