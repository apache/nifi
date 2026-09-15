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
import org.apache.nifi.web.api.dto.RevisionDTO;
import org.apache.nifi.web.api.entity.ScheduleComponentsEntity;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class StopSourcesEndpointMerger extends AbstractSingleEntityEndpoint<ScheduleComponentsEntity> {
    public static final Pattern STOP_SOURCES_URI_PATTERN = Pattern.compile("/nifi-api/flow/process-groups/(?:(?:root)|(?:[a-f0-9\\-]{36}))/sources");

    @Override
    public boolean canHandle(final URI uri, final String method) {
        return "PUT".equalsIgnoreCase(method) && STOP_SOURCES_URI_PATTERN.matcher(uri.getPath()).matches();
    }

    @Override
    protected Class<ScheduleComponentsEntity> getEntityClass() {
        return ScheduleComponentsEntity.class;
    }

    @Override
    protected void mergeResponses(final ScheduleComponentsEntity clientEntity, final Map<NodeIdentifier, ScheduleComponentsEntity> entityMap,
                                  final Set<NodeResponse> successfulResponses, final Set<NodeResponse> problematicResponses) {
        if (clientEntity.getComponents() == null) {
            clientEntity.setComponents(new HashMap<>());
        }

        for (final ScheduleComponentsEntity nodeEntity : entityMap.values()) {
            if (nodeEntity.getComponents() == null) {
                continue;
            }

            for (final Map.Entry<String, RevisionDTO> entry : nodeEntity.getComponents().entrySet()) {
                clientEntity.getComponents().putIfAbsent(entry.getKey(), entry.getValue());
            }
        }
    }
}
