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
import org.apache.nifi.cluster.manager.PermissionsDtoMerger;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.entity.ParameterContextEntity;
import org.apache.nifi.web.api.entity.ParameterContextsEntity;

import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ParameterContextsEndpointMerger extends AbstractSingleEntityEndpoint<ParameterContextsEntity> implements EndpointResponseMerger {
    private static final String PARAMETER_CONTEXTS_URI = "/nifi-api/parameter-contexts";

    @Override
    public boolean canHandle(final URI uri, final String method) {
        return "GET".equalsIgnoreCase(method) && PARAMETER_CONTEXTS_URI.equals(uri.getPath());
    }

    @Override
    protected Class<ParameterContextsEntity> getEntityClass() {
        return ParameterContextsEntity.class;
    }

    @Override
    protected void mergeResponses(final ParameterContextsEntity clientEntity, final Map<NodeIdentifier, ParameterContextsEntity> entityMap, final Set<NodeResponse> successfulResponses,
                                  final Set<NodeResponse> problematicResponses) {

        final Map<String, ParameterContextEntity> contextEntities = new HashMap<>();
        for (final ParameterContextsEntity contextsEntity : entityMap.values()) {
            for (final ParameterContextEntity entity : contextsEntity.getParameterContexts()) {
                final ParameterContextEntity mergedEntity = contextEntities.get(entity.getId());
                if (mergedEntity == null) {
                    contextEntities.put(entity.getId(), entity);
                    continue;
                }

                merge(mergedEntity, entity);
            }
        }

        clientEntity.setParameterContexts(new HashSet<>(contextEntities.values()));
    }

    private void merge(final ParameterContextEntity merged, final ParameterContextEntity additional) {
        PermissionsDtoMerger.mergePermissions(merged.getPermissions(), additional.getPermissions());

        if (merged.getComponent() == null || additional.getComponent() == null) {
            merged.setComponent(null);
        }
    }
}
