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
import org.apache.nifi.cluster.manager.PortEntityMerger;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.entity.PortEntity;

import java.net.URI;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class PortEndpointMerger extends AbstractSingleEntityEndpoint<PortEntity> implements EndpointResponseMerger {
    public static final Pattern INPUT_PORTS_URI_PATTERN = Pattern.compile("/nifi-api/process-groups/(?:(?:root)|(?:[a-f0-9\\-]{36}))/input-ports");
    public static final Pattern INPUT_PORT_URI_PATTERN = Pattern.compile("/nifi-api/input-ports/[a-f0-9\\-]{36}");
    public static final Pattern INPUT_PORT_RUN_STATUS_URI_PATTERN = Pattern.compile("/nifi-api/input-ports/[a-f0-9\\-]{36}/run-status");

    public static final Pattern OUTPUT_PORTS_URI_PATTERN = Pattern.compile("/nifi-api/process-groups/(?:(?:root)|(?:[a-f0-9\\-]{36}))/output-ports");
    public static final Pattern OUTPUT_PORT_URI_PATTERN = Pattern.compile("/nifi-api/output-ports/[a-f0-9\\-]{36}");
    public static final Pattern OUTPUT_PORT_RUN_STATUS_URI_PATTERN = Pattern.compile("/nifi-api/output-ports/[a-f0-9\\-]{36}/run-status");
    private final PortEntityMerger portEntityMerger = new PortEntityMerger();

    @Override
    public boolean canHandle(final URI uri, final String method) {
        return canHandleInputPort(uri, method) || canHandleOutputPort(uri, method);
    }

    private boolean canHandleInputPort(final URI uri, final String method) {
        if (("GET".equalsIgnoreCase(method) || "PUT".equalsIgnoreCase(method)) && (INPUT_PORT_URI_PATTERN.matcher(uri.getPath()).matches())) {
            return true;
        } else if ("PUT".equalsIgnoreCase(method) && INPUT_PORT_RUN_STATUS_URI_PATTERN.matcher(uri.getPath()).matches()) {
            return true;
        } else if ("POST".equalsIgnoreCase(method) && INPUT_PORTS_URI_PATTERN.matcher(uri.getPath()).matches()) {
            return true;
        }

        return false;
    }

    private boolean canHandleOutputPort(final URI uri, final String method) {
        if (("GET".equalsIgnoreCase(method) || "PUT".equalsIgnoreCase(method)) && (OUTPUT_PORT_URI_PATTERN.matcher(uri.getPath()).matches())) {
            return true;
        } else if ("PUT".equalsIgnoreCase(method) && OUTPUT_PORT_RUN_STATUS_URI_PATTERN.matcher(uri.getPath()).matches()) {
            return true;
        } else if ("POST".equalsIgnoreCase(method) && OUTPUT_PORTS_URI_PATTERN.matcher(uri.getPath()).matches()) {
            return true;
        }

        return false;
    }

    @Override
    protected Class<PortEntity> getEntityClass() {
        return PortEntity.class;
    }

    @Override
    protected void mergeResponses(final PortEntity clientEntity, final Map<NodeIdentifier, PortEntity> entityMap,
                                  final Set<NodeResponse> successfulResponses, final Set<NodeResponse> problematicResponses) {

        portEntityMerger.merge(clientEntity, entityMap);
    }
}
