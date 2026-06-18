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
import org.apache.nifi.web.api.dto.MigrationRequestDTO;
import org.apache.nifi.web.api.dto.MigrationUpdateStepDTO;
import org.apache.nifi.web.api.entity.MigrationRequestEntity;

import java.net.URI;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class MigrationRequestEndpointMerger extends AbstractSingleEntityEndpoint<MigrationRequestEntity> {
    public static final Pattern MIGRATION_REQUEST_URI_PATTERN =
            Pattern.compile("/nifi-api/connectors/[a-f0-9\\-]{36}/migration-requests(/[a-f0-9\\-]{36})?");

    @Override
    protected Class<MigrationRequestEntity> getEntityClass() {
        return MigrationRequestEntity.class;
    }

    @Override
    public boolean canHandle(final URI uri, final String method) {
        return MIGRATION_REQUEST_URI_PATTERN.matcher(uri.getPath()).matches();
    }

    @Override
    protected void mergeResponses(final MigrationRequestEntity clientEntity, final Map<NodeIdentifier, MigrationRequestEntity> entityMap,
            final Set<NodeResponse> successfulResponses, final Set<NodeResponse> problematicResponses) {
        final MigrationRequestDTO clientRequest = clientEntity.getRequest();
        if (clientRequest == null) {
            return;
        }

        final List<MigrationUpdateStepDTO> clientUpdateSteps = clientRequest.getUpdateSteps() == null ? List.of() : clientRequest.getUpdateSteps();

        for (final Map.Entry<NodeIdentifier, MigrationRequestEntity> nodeEntry : entityMap.entrySet()) {
            final NodeIdentifier nodeId = nodeEntry.getKey();
            final MigrationRequestDTO nodeRequest = nodeEntry.getValue().getRequest();
            if (nodeRequest == null) {
                continue;
            }

            clientRequest.setComplete(clientRequest.isComplete() && nodeRequest.isComplete());

            if (nodeRequest.getFailureReason() != null) {
                final String nodeReason = "Node " + nodeId.getApiAddress() + ":" + nodeId.getApiPort() + ": " + nodeRequest.getFailureReason();
                final String existingReason = clientRequest.getFailureReason();
                clientRequest.setFailureReason(existingReason == null ? nodeReason : existingReason + "; " + nodeReason);
            }

            final Date clientLastUpdated = clientRequest.getLastUpdated();
            final Date nodeLastUpdated = nodeRequest.getLastUpdated();
            if (nodeLastUpdated != null && (clientLastUpdated == null || nodeLastUpdated.before(clientLastUpdated))) {
                clientRequest.setLastUpdated(nodeLastUpdated);
            }

            clientRequest.setPercentCompleted(Math.min(clientRequest.getPercentCompleted(), nodeRequest.getPercentCompleted()));

            mergeUpdateSteps(clientUpdateSteps, nodeRequest.getUpdateSteps());
        }

        // Recompute the human-readable state to reflect any failure surfaced by a remote node so that polling clients
        // see a consistent picture rather than the arbitrary client node's view.
        if (clientRequest.getFailureReason() != null) {
            clientRequest.setState("Failed: " + clientRequest.getFailureReason());
        } else if (clientRequest.isComplete()) {
            clientRequest.setState("Complete");
        }
    }

    private void mergeUpdateSteps(final List<MigrationUpdateStepDTO> clientSteps, final List<MigrationUpdateStepDTO> nodeSteps) {
        if (clientSteps == null || nodeSteps == null) {
            return;
        }

        final int sharedStepCount = Math.min(clientSteps.size(), nodeSteps.size());
        for (int stepIndex = 0; stepIndex < sharedStepCount; stepIndex++) {
            final MigrationUpdateStepDTO clientStep = clientSteps.get(stepIndex);
            final MigrationUpdateStepDTO nodeStep = nodeSteps.get(stepIndex);

            clientStep.setComplete(clientStep.isComplete() && nodeStep.isComplete());
            if (nodeStep.getFailureReason() != null && clientStep.getFailureReason() == null) {
                clientStep.setFailureReason(nodeStep.getFailureReason());
            }
        }
    }
}
