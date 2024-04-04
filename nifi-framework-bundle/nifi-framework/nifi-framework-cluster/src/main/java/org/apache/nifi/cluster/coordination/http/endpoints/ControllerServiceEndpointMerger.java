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
import org.apache.nifi.cluster.manager.ControllerServiceEntityMerger;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.entity.ControllerServiceEntity;

import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class ControllerServiceEndpointMerger extends AbstractSingleEntityEndpoint<ControllerServiceEntity> implements EndpointResponseMerger {
    public static final String CONTROLLER_CONTROLLER_SERVICES_URI = "/nifi-api/controller/controller-services";
    public static final Pattern PROCESS_GROUPS_CONTROLLER_SERVICES_URI = Pattern.compile("/nifi-api/process-groups/(?:(?:root)|(?:[a-f0-9\\-]{36}))/controller-services");
    public static final Pattern CONTROLLER_SERVICE_URI_PATTERN = Pattern.compile("/nifi-api/controller-services/[a-f0-9\\-]{36}");
    public static final Pattern CONTROLLER_SERVICE_RUN_STATUS_URI_PATTERN = Pattern.compile("/nifi-api/controller-services/[a-f0-9\\-]{36}/run-status");
    private final ControllerServiceEntityMerger controllerServiceEntityMerger = new ControllerServiceEntityMerger();

    @Override
    public boolean canHandle(URI uri, String method) {
        if (("GET".equalsIgnoreCase(method) || "PUT".equalsIgnoreCase(method)) && CONTROLLER_SERVICE_URI_PATTERN.matcher(uri.getPath()).matches()) {
            return true;
        } else if ("PUT".equalsIgnoreCase(method) && CONTROLLER_SERVICE_RUN_STATUS_URI_PATTERN.matcher(uri.getPath()).matches()) {
            return true;
        } else if ("POST".equalsIgnoreCase(method) && (CONTROLLER_CONTROLLER_SERVICES_URI.equals(uri.getPath()) || PROCESS_GROUPS_CONTROLLER_SERVICES_URI.matcher(uri.getPath()).matches())) {
            return true;
        }

        return false;
    }

    @Override
    protected Class<ControllerServiceEntity> getEntityClass() {
        return ControllerServiceEntity.class;
    }

    @Override
    protected void mergeResponses(ControllerServiceEntity clientEntity, Map<NodeIdentifier, ControllerServiceEntity> entityMap,
                                  Set<NodeResponse> successfulResponses, Set<NodeResponse> problematicResponses) {

        controllerServiceEntityMerger.merge(clientEntity, entityMap);
    }
}
