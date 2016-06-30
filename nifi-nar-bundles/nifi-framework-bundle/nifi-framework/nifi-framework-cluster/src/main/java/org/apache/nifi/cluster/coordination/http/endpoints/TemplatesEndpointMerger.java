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

import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.nifi.cluster.coordination.http.EndpointResponseMerger;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.web.api.dto.TemplateDTO;
import org.apache.nifi.web.api.entity.TemplatesEntity;

public class TemplatesEndpointMerger implements EndpointResponseMerger {
    public static final Pattern TEMPLATES_URI_PATTERN = Pattern.compile("/nifi-api/process-groups/(?:(?:root)|(?:[a-f0-9\\-]{36}))/templates");

    @Override
    public boolean canHandle(final URI uri, final String method) {
        return "GET".equalsIgnoreCase(method) && TEMPLATES_URI_PATTERN.matcher(uri.getPath()).matches();
    }

    protected Class<TemplatesEntity> getEntityClass() {
        return TemplatesEntity.class;
    }

    protected Set<TemplateDTO> getDtos(final TemplatesEntity entity) {
        return entity.getTemplates();
    }

    protected String getComponentId(final TemplateDTO dto) {
        return dto.getId();
    }

    @Override
    public final NodeResponse merge(final URI uri, final String method, final Set<NodeResponse> successfulResponses, final Set<NodeResponse> problematicResponses, final NodeResponse clientResponse) {
        if (!canHandle(uri, method)) {
            throw new IllegalArgumentException("Cannot use Endpoint Mapper of type " + getClass().getSimpleName() + " to map responses for URI " + uri + ", HTTP Method " + method);
        }

        final TemplatesEntity responseEntity = clientResponse.getClientResponse().getEntity(getEntityClass());

        // Find the templates that all nodes know about. We do this by mapping Template ID to Template and
        // then for each node, removing any template whose ID is not known to that node. After iterating over
        // all of the nodes, we are left with a Map whose contents are those Templates known by all nodes.
        Map<String, TemplateDTO> templatesById = null;
        for (final NodeResponse nodeResponse : successfulResponses) {
            final TemplatesEntity entity = nodeResponse == clientResponse ? responseEntity : nodeResponse.getClientResponse().getEntity(TemplatesEntity.class);
            final Set<TemplateDTO> templateDtos = entity.getTemplates();
            final Map<String, TemplateDTO> nodeTemplatesById = templateDtos.stream().collect(Collectors.toMap(dto -> dto.getId(), dto -> dto));

            if (templatesById == null) {
                // Create new HashMap so that the map that we have is modifiable.
                templatesById = new HashMap<>(nodeTemplatesById);
            } else {
                // Only keep templates that are known by this node.
                templatesById.keySet().retainAll(nodeTemplatesById.keySet());
            }
        }

        // Set the templates to the set of templates that all nodes know about
        responseEntity.setTemplates(new HashSet<>(templatesById.values()));

        // create a new client response
        return new NodeResponse(clientResponse, responseEntity);
    }

}
