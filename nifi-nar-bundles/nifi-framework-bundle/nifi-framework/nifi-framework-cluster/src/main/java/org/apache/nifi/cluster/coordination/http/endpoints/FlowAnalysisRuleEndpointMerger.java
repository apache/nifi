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
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class FlowAnalysisRuleEndpointMerger extends AbstractSingleEntityEndpoint<FlowAnalysisRuleEntity> implements EndpointResponseMerger {
    private static final Collection<Endpoint> SUPPORTED_ENDPOINTS = Arrays.asList(
      new Endpoint("/nifi-api/controller/flow-analysis-rules", "POST"),
      new Endpoint(Pattern.compile("/nifi-api/controller/flow-analysis-rules/[a-f0-9\\-]{36}"), "GET", "PUT", "DELETE"),
      new Endpoint(Pattern.compile("/nifi-api/controller/flow-analysis-rules/[a-f0-9\\-]{36}/run-status"), "PUT")
    );


    private final FlowAnalysisRuleEntityMerger flowAnalysisRuleEntityMerger = new FlowAnalysisRuleEntityMerger();

    @Override
    public boolean canHandle(URI uri, String method) {
        boolean canHandle = SUPPORTED_ENDPOINTS.stream()
                .filter(supportedEndpoint -> supportedEndpoint.canHandle(uri, method))
                .findAny()
                .isPresent();

        return canHandle;
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

    private static class Endpoint {
        final List<String> httpMethods;
        final String uri;
        final Pattern uriPattern;

        public Endpoint(final String uri, final String... httpMethods) {
            this.httpMethods = Arrays.asList(httpMethods);
            this.uri = uri;
            this.uriPattern = null;
        }

        public Endpoint(Pattern uriPattern, String... httpMethods) {
            this.httpMethods = Arrays.asList(httpMethods);
            this.uri = null;
            this.uriPattern = uriPattern;
        }

        public boolean canHandle(URI uri, String method) {
            boolean canHandle =
                    httpMethods.stream().filter(httpMethod -> httpMethod.equalsIgnoreCase(method)).findAny().isPresent()
                    && (
                            this.uri != null && this.uri.equals(uri.getPath())
                            ||
                            this.uriPattern != null && this.uriPattern.matcher(uri.getPath()).matches()
                    );

            return canHandle;
        }
    }
}
