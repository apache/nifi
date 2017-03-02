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

import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.dto.PermissionsDTO;
import org.apache.nifi.web.api.entity.CurrentUserEntity;

import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class CurrentUserEndpointMerger extends AbstractSingleEntityEndpoint<CurrentUserEntity> {
    public static final Pattern CURRENT_USER_URI_PATTERN = Pattern.compile("/nifi-api/flow/current-user");

    @Override
    public boolean canHandle(URI uri, String method) {
        return "GET".equalsIgnoreCase(method) && CURRENT_USER_URI_PATTERN.matcher(uri.getPath()).matches();
    }

    @Override
    protected Class<CurrentUserEntity> getEntityClass() {
        return CurrentUserEntity.class;
    }


    @Override
    protected void mergeResponses(final CurrentUserEntity clientEntity, final Map<NodeIdentifier, CurrentUserEntity> entityMap,
                                  final Set<NodeResponse> successfulResponses, final Set<NodeResponse> problematicResponses) {

        for (final Map.Entry<NodeIdentifier, CurrentUserEntity> entry : entityMap.entrySet()) {
            final CurrentUserEntity entity = entry.getValue();
            if (entity != clientEntity) {
                mergePermissions(clientEntity.getControllerPermissions(), entity.getControllerPermissions());
                mergePermissions(clientEntity.getCountersPermissions(), entity.getCountersPermissions());
                mergePermissions(clientEntity.getPoliciesPermissions(), entity.getPoliciesPermissions());
                mergePermissions(clientEntity.getProvenancePermissions(), entity.getProvenancePermissions());
                mergePermissions(clientEntity.getTenantsPermissions(), entity.getTenantsPermissions());
            }
        }
    }

    private void mergePermissions(PermissionsDTO clientPermissions, PermissionsDTO permissions) {
        clientPermissions.setCanRead(clientPermissions.getCanRead() && permissions.getCanRead());
        clientPermissions.setCanWrite(clientPermissions.getCanWrite() && permissions.getCanWrite());
    }

}
