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
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.entity.ProcessorsEntity;

public class ProcessorsEndpointMerger extends AbstractMultiEntityEndpoint<ProcessorsEntity, ProcessorDTO> {
    public static final Pattern PROCESSORS_URI_PATTERN = Pattern.compile("/nifi-api/controller/process-groups/(?:(?:root)|(?:[a-f0-9\\-]{36}))/processors");

    @Override
    public boolean canHandle(final URI uri, final String method) {
        return "GET".equalsIgnoreCase(method) && PROCESSORS_URI_PATTERN.matcher(uri.getPath()).matches();
    }

    @Override
    protected Class<ProcessorsEntity> getEntityClass() {
        return ProcessorsEntity.class;
    }

    @Override
    protected Set<ProcessorDTO> getDtos(final ProcessorsEntity entity) {
        return entity.getProcessors();
    }

    @Override
    protected String getComponentId(final ProcessorDTO dto) {
        return dto.getId();
    }

    @Override
    protected void mergeResponses(ProcessorDTO clientDto, Map<NodeIdentifier, ProcessorDTO> dtoMap, Set<NodeResponse> successfulResponses, Set<NodeResponse> problematicResponses) {
        new ProcessorEndpointMerger().mergeResponses(clientDto, dtoMap, successfulResponses, problematicResponses);
    }
}
