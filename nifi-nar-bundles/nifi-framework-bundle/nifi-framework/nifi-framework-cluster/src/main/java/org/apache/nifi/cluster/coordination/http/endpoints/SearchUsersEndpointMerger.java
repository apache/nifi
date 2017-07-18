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
import org.apache.nifi.web.api.entity.TenantEntity;
import org.apache.nifi.web.api.entity.TenantsEntity;

import java.net.URI;
import java.util.Collection;
import java.util.Set;
import java.util.regex.Pattern;

public class SearchUsersEndpointMerger implements EndpointResponseMerger {
    public static final Pattern SEARCH_TENANTS_URI_PATTERN = Pattern.compile("/nifi-api/tenants/search-results");

    @Override
    public boolean canHandle(final URI uri, final String method) {
        return "GET".equalsIgnoreCase(method) && SEARCH_TENANTS_URI_PATTERN.matcher(uri.getPath()).matches();
    }

    @Override
    public final NodeResponse merge(final URI uri, final String method, final Set<NodeResponse> successfulResponses, final Set<NodeResponse> problematicResponses, final NodeResponse clientResponse) {
        if (!canHandle(uri, method)) {
            throw new IllegalArgumentException("Cannot use Endpoint Mapper of type " + getClass().getSimpleName() + " to map responses for URI " + uri + ", HTTP Method " + method);
        }

        final TenantsEntity responseEntity = clientResponse.getClientResponse().getEntity(TenantsEntity.class);
        final Collection<TenantEntity> userEntities = responseEntity.getUsers();
        final Collection<TenantEntity> userGroupEntities = responseEntity.getUserGroups();

        for (final NodeResponse nodeResponse : successfulResponses) {
            final TenantsEntity nodeResponseEntity = nodeResponse == clientResponse ? responseEntity : nodeResponse.getClientResponse().getEntity(TenantsEntity.class);

            // only retain users/groups that all nodes agree on
            userEntities.retainAll(nodeResponseEntity.getUsers());
            userGroupEntities.retainAll(nodeResponseEntity.getUserGroups());
        }

        // create a new client response
        return new NodeResponse(clientResponse, responseEntity);
    }
}
