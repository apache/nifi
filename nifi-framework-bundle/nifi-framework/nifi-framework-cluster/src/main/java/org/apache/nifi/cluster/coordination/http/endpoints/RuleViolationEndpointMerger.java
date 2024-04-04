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
import org.apache.nifi.web.api.entity.RuleViolationEntity;

import java.net.URI;
import java.util.Map;
import java.util.Set;

public class RuleViolationEndpointMerger extends AbstractSingleEntityEndpoint<RuleViolationEntity> implements EndpointResponseMerger {
    public static final String UPDATE_RULE_VIOLATION_URI = "/nifi-api/controller/analyze-flow/update-rule-violation";

    @Override
    public boolean canHandle(URI uri, String method) {
        if ("PUT".equalsIgnoreCase(method) && UPDATE_RULE_VIOLATION_URI.equals(uri.getPath())) {
            return true;
        }

        return false;
    }

    @Override
    protected Class<RuleViolationEntity> getEntityClass() {
        return RuleViolationEntity.class;
    }

    @Override
    protected void mergeResponses(RuleViolationEntity clientEntity, Map<NodeIdentifier, RuleViolationEntity> entityMap, Set<NodeResponse> successfulResponses, Set<NodeResponse> problematicResponses) {
        // Nothing to do, if there are no issues the entities are the same
    }
}
