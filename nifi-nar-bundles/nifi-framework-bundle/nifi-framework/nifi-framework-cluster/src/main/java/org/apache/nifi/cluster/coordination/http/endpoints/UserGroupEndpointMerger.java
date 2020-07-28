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
import org.apache.nifi.cluster.manager.UserGroupEntityMerger;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.entity.UserGroupEntity;

import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class UserGroupEndpointMerger extends AbstractSingleEntityEndpoint<UserGroupEntity> implements EndpointResponseMerger {
    public static final Pattern USER_GROUPS_URI_PATTERN = Pattern.compile("/nifi-api/tenants/user-groups");
    public static final Pattern USER_GROUP_URI_PATTERN = Pattern.compile("/nifi-api/tenants/user-groups/[a-f0-9\\-]{36}");
    private final UserGroupEntityMerger userGroupEntityMerger = new UserGroupEntityMerger();

    @Override
    public boolean canHandle(final URI uri, final String method) {
        if (("GET".equalsIgnoreCase(method) || "PUT".equalsIgnoreCase(method)) && (USER_GROUP_URI_PATTERN.matcher(uri.getPath()).matches())) {
            return true;
        } else if ("POST".equalsIgnoreCase(method) && USER_GROUPS_URI_PATTERN.matcher(uri.getPath()).matches()) {
            return true;
        }

        return false;
    }

    @Override
    protected Class<UserGroupEntity> getEntityClass() {
        return UserGroupEntity.class;
    }

    @Override
    protected void mergeResponses(final UserGroupEntity clientEntity, final Map<NodeIdentifier, UserGroupEntity> entityMap,
                                  final Set<NodeResponse> successfulResponses, final Set<NodeResponse> problematicResponses) {

        userGroupEntityMerger.merge(clientEntity, entityMap);
    }
}
