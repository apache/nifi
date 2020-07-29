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
import org.apache.nifi.cluster.manager.AccessPolicyEntityMerger;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.entity.AccessPolicyEntity;

import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class AccessPolicyEndpointMerger extends AbstractSingleEntityEndpoint<AccessPolicyEntity> implements EndpointResponseMerger {
    public static final Pattern ACCESS_POLICIES_URI_PATTERN = Pattern.compile("/nifi-api/policies");
    public static final Pattern ACCESS_POLICY_URI_PATTERN = Pattern.compile("/nifi-api/policies/[a-f0-9\\-]{36}");
    public static final Pattern ACCESS_POLICY_LOOKUP_URI_PATTERN = Pattern.compile("/nifi-api/policies/(?:read|write)/(?:[\\w-]+?/?)+");
    private final AccessPolicyEntityMerger accessPolicyEntityMerger = new AccessPolicyEntityMerger();

    @Override
    public boolean canHandle(final URI uri, final String method) {
        if (("GET".equalsIgnoreCase(method) || "PUT".equalsIgnoreCase(method)) && (ACCESS_POLICY_URI_PATTERN.matcher(uri.getPath()).matches())) {
            return true;
        } else if ("GET".equalsIgnoreCase(method) && ACCESS_POLICY_LOOKUP_URI_PATTERN.matcher(uri.getPath()).matches()) {
            return true;
        } else if ("POST".equalsIgnoreCase(method) && ACCESS_POLICIES_URI_PATTERN.matcher(uri.getPath()).matches()) {
            return true;
        }

        return false;
    }

    @Override
    protected Class<AccessPolicyEntity> getEntityClass() {
        return AccessPolicyEntity.class;
    }

    @Override
    protected void mergeResponses(final AccessPolicyEntity clientEntity, final Map<NodeIdentifier, AccessPolicyEntity> entityMap,
                                  final Set<NodeResponse> successfulResponses, final Set<NodeResponse> problematicResponses) {

        accessPolicyEntityMerger.merge(clientEntity, entityMap);
    }
}
