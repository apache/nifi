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

import org.apache.nifi.cluster.manager.FunnelEntityMerger;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.entity.FunnelEntity;

import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class FunnelEndpointMerger extends AbstractSingleEntityEndpoint<FunnelEntity> {
    public static final Pattern FUNNELS_URI_PATTERN = Pattern.compile("/nifi-api/process-groups/(?:(?:root)|(?:[a-f0-9\\-]{36}))/funnels");
    public static final Pattern FUNNEL_URI_PATTERN = Pattern.compile("/nifi-api/funnels/[a-f0-9\\-]{36}");

    final private FunnelEntityMerger funnelEntityMerger = new FunnelEntityMerger();

    @Override
    protected Class<FunnelEntity> getEntityClass() {
        return FunnelEntity.class;
    }

    @Override
    protected void mergeResponses(FunnelEntity clientEntity, Map<NodeIdentifier, FunnelEntity> entityMap, Set<NodeResponse> successfulResponses, Set<NodeResponse> problematicResponses) {
        funnelEntityMerger.merge(clientEntity, entityMap);
    }

    @Override
    public boolean canHandle(URI uri, String method) {
        if (("GET".equalsIgnoreCase(method) || "PUT".equalsIgnoreCase(method)) && (FUNNEL_URI_PATTERN.matcher(uri.getPath()).matches())) {
            return true;
        } else if ("POST".equalsIgnoreCase(method) && FUNNELS_URI_PATTERN.matcher(uri.getPath()).matches()) {
            return true;
        }

        return false;
    }
}
