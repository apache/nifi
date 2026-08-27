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
import org.apache.nifi.web.api.dto.DropRequestDTO;
import org.apache.nifi.web.api.entity.DropRequestEntity;

import java.net.URI;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class ConnectorPurgeRequestEndpointMerger extends AbstractSingleDTOEndpoint<DropRequestEntity, DropRequestDTO> {
    private static final int TOTAL_PERCENT_COMPLETED = 100;
    private static final Pattern CONNECTOR_PURGE_REQUESTS_URI = Pattern.compile("/nifi-api/connectors/[a-f0-9\\-]{36}/purge-requests");
    private static final Pattern CONNECTOR_PURGE_REQUEST_URI = Pattern.compile("/nifi-api/connectors/[a-f0-9\\-]{36}/purge-requests/[a-f0-9\\-]{36}");

    @Override
    public boolean canHandle(final URI uri, final String method) {
        final String path = uri.getPath();
        if ("POST".equalsIgnoreCase(method)) {
            return CONNECTOR_PURGE_REQUESTS_URI.matcher(path).matches();
        }

        return ("GET".equalsIgnoreCase(method) || "DELETE".equalsIgnoreCase(method))
                && CONNECTOR_PURGE_REQUEST_URI.matcher(path).matches();
    }

    @Override
    protected Class<DropRequestEntity> getEntityClass() {
        return DropRequestEntity.class;
    }

    @Override
    protected DropRequestDTO getDto(final DropRequestEntity entity) {
        return entity.getDropRequest();
    }

    @Override
    protected void mergeResponses(final DropRequestDTO clientDto, final Map<NodeIdentifier, DropRequestDTO> dtoMap,
                                  final Set<NodeResponse> successfulResponses, final Set<NodeResponse> problematicResponses) {
        final Set<String> failureReasons = new LinkedHashSet<>();
        boolean allFinished = true;
        int percentCompleted = TOTAL_PERCENT_COMPLETED;

        for (final DropRequestDTO nodeDto : dtoMap.values()) {
            if (nodeDto.getFailureReason() != null) {
                failureReasons.add(nodeDto.getFailureReason());
            }

            allFinished &= nodeDto.isFinished();
            percentCompleted = Math.min(percentCompleted, nodeDto.getPercentCompleted());
        }

        if (failureReasons.isEmpty()) {
            percentCompleted = allFinished ? TOTAL_PERCENT_COMPLETED : percentCompleted;
            clientDto.setState(allFinished ? "Complete" : "In Progress");
        } else {
            final String failureReason = String.join("; ", failureReasons);
            allFinished = true;
            percentCompleted = TOTAL_PERCENT_COMPLETED;
            clientDto.setFailureReason(failureReason);
            clientDto.setState("Failed: " + failureReason);
        }

        clientDto.setFinished(allFinished);
        clientDto.setPercentCompleted(percentCompleted);
    }
}
