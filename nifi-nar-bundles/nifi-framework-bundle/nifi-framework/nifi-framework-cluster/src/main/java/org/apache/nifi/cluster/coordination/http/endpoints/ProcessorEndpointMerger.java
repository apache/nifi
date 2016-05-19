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
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.nifi.cluster.coordination.http.EndpointResponseMerger;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.manager.StatusMerger;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.entity.ProcessorEntity;

public class ProcessorEndpointMerger extends AbstractSingleEntityEndpoint<ProcessorEntity> implements EndpointResponseMerger {
    public static final Pattern PROCESSORS_URI_PATTERN = Pattern.compile("/nifi-api/processors");
    public static final Pattern PROCESSOR_URI_PATTERN = Pattern.compile("/nifi-api/processors/[a-f0-9\\-]{36}");
    public static final Pattern CLUSTER_PROCESSOR_URI_PATTERN = Pattern.compile("/nifi-api/cluster/processors/[a-f0-9\\-]{36}");

    @Override
    public boolean canHandle(final URI uri, final String method) {
        if (("GET".equalsIgnoreCase(method) || "PUT".equalsIgnoreCase(method))
            && (PROCESSOR_URI_PATTERN.matcher(uri.getPath()).matches() || CLUSTER_PROCESSOR_URI_PATTERN.matcher(uri.getPath()).matches())) {
            return true;
        } else if ("POST".equalsIgnoreCase(method) && PROCESSORS_URI_PATTERN.matcher(uri.getPath()).matches()) {
            return true;
        }

        return false;
    }

    @Override
    protected Class<ProcessorEntity> getEntityClass() {
        return ProcessorEntity.class;
    }


    protected void mergeResponses(final ProcessorDTO clientDto, final Map<NodeIdentifier, ProcessorDTO> dtoMap, final Set<NodeResponse> successfulResponses,
        final Set<NodeResponse> problematicResponses) {
        final Map<String, Set<NodeIdentifier>> validationErrorMap = new HashMap<>();

        for (final Map.Entry<NodeIdentifier, ProcessorDTO> nodeEntry : dtoMap.entrySet()) {
            final NodeIdentifier nodeId = nodeEntry.getKey();
            final ProcessorDTO nodeProcessor = nodeEntry.getValue();

            // merge the validation errors
            mergeValidationErrors(validationErrorMap, nodeId, nodeProcessor.getValidationErrors());
        }

        // set the merged the validation errors
        clientDto.setValidationErrors(normalizedMergedValidationErrors(validationErrorMap, dtoMap.size()));
    }

    @Override
    protected void mergeResponses(final ProcessorEntity clientEntity, final Map<NodeIdentifier, ProcessorEntity> entityMap, final Set<NodeResponse> successfulResponses,
        final Set<NodeResponse> problematicResponses) {

        final ProcessorDTO clientDto = clientEntity.getComponent();
        final Map<NodeIdentifier, ProcessorDTO> dtoMap = new HashMap<>();
        for (final Map.Entry<NodeIdentifier, ProcessorEntity> entry : entityMap.entrySet()) {
            final ProcessorEntity nodeProcEntity = entry.getValue();
            final ProcessorDTO nodeProcDto = nodeProcEntity.getComponent();
            dtoMap.put(entry.getKey(), nodeProcDto);
        }

        for (final Map.Entry<NodeIdentifier, ProcessorEntity> entry : entityMap.entrySet()) {
            final NodeIdentifier nodeId = entry.getKey();
            final ProcessorEntity entity = entry.getValue();
            if (entity != clientEntity) {
                StatusMerger.merge(clientEntity.getStatus(), entity.getStatus(), nodeId.getId(), nodeId.getApiAddress(), nodeId.getApiPort());
            }
        }

        mergeResponses(clientDto, dtoMap, successfulResponses, problematicResponses);
    }
}
