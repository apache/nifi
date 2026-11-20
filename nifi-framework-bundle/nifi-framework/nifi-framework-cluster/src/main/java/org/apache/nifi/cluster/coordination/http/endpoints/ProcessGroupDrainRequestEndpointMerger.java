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
import org.apache.nifi.groups.ProcessGroupDrainState;
import org.apache.nifi.web.api.dto.DrainBlockingComponentDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDrainRequestDTO;
import org.apache.nifi.web.api.entity.ProcessGroupDrainRequestEntity;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * Merges the drain request of each node, summing the counts and sizes. A node that has no record of the drain, such as a node that restarted,
 * is no longer draining its data, so the merged drain is reported as failed.
 */
public class ProcessGroupDrainRequestEndpointMerger extends AbstractSingleEntityEndpoint<ProcessGroupDrainRequestEntity> {
    private static final Pattern DRAIN_REQUESTS_URI = Pattern.compile("/nifi-api/process-groups/(?:root|[a-f0-9\\-]{36})/drain-requests");
    private static final Pattern DRAIN_REQUEST_URI = Pattern.compile("/nifi-api/process-groups/(?:root|[a-f0-9\\-]{36})/drain-requests/[a-f0-9\\-]{36}");

    private static final String NO_RECORD_REASON = "The node has no record of the drain, for example because it restarted, so the FlowFile counts do not include its FlowFiles";

    @Override
    public boolean canHandle(final URI uri, final String method) {
        final String path = uri.getPath();
        if ("POST".equalsIgnoreCase(method)) {
            return DRAIN_REQUESTS_URI.matcher(path).matches();
        }

        return ("GET".equalsIgnoreCase(method) || "DELETE".equalsIgnoreCase(method)) && DRAIN_REQUEST_URI.matcher(path).matches();
    }

    @Override
    protected Class<ProcessGroupDrainRequestEntity> getEntityClass() {
        return ProcessGroupDrainRequestEntity.class;
    }

    @Override
    protected void mergeResponses(final ProcessGroupDrainRequestEntity clientEntity, final Map<NodeIdentifier, ProcessGroupDrainRequestEntity> entityMap,
                                  final Set<NodeResponse> successfulResponses, final Set<NodeResponse> problematicResponses) {
        final Map<NodeIdentifier, ProcessGroupDrainRequestDTO> requestsByNode = new LinkedHashMap<>();
        final List<String> failureReasons = new ArrayList<>();
        for (final Map.Entry<NodeIdentifier, ProcessGroupDrainRequestEntity> entry : entityMap.entrySet()) {
            final ProcessGroupDrainRequestDTO nodeRequest = entry.getValue().getDrainRequest();
            if (nodeRequest == null) {
                failureReasons.add(formatFailureReason(entry.getKey(), NO_RECORD_REASON));
            } else {
                requestsByNode.put(entry.getKey(), nodeRequest);
            }
        }

        if (requestsByNode.isEmpty()) {
            clientEntity.setDrainRequest(null);
            return;
        }

        final ProcessGroupDrainRequestDTO clientRequest = clientEntity.getDrainRequest();
        final ProcessGroupDrainRequestDTO identifyingRequest = clientRequest == null ? requestsByNode.values().iterator().next() : clientRequest;

        int originalCount = 0;
        long originalSize = 0L;
        int currentCount = 0;
        long currentSize = 0L;
        boolean anyFailed = !failureReasons.isEmpty();
        boolean anyDraining = false;
        boolean allPrepared = true;

        for (final Map.Entry<NodeIdentifier, ProcessGroupDrainRequestDTO> entry : requestsByNode.entrySet()) {
            final NodeIdentifier nodeId = entry.getKey();
            final ProcessGroupDrainRequestDTO nodeRequest = entry.getValue();

            originalCount += nodeRequest.getOriginalCount();
            originalSize += nodeRequest.getOriginalSize();
            currentCount += nodeRequest.getCurrentCount();
            currentSize += nodeRequest.getCurrentSize();
            anyFailed |= ProcessGroupDrainState.FAILED.name().equals(nodeRequest.getState());
            anyDraining |= ProcessGroupDrainState.DRAINING.name().equals(nodeRequest.getState());
            allPrepared &= Boolean.TRUE.equals(nodeRequest.getPrepared());

            if (nodeRequest.getFailureReason() != null) {
                failureReasons.add(formatFailureReason(nodeId, nodeRequest.getFailureReason()));
            }
        }

        // The order in which the nodes respond varies, so the reasons are sorted to keep the merged reason the same between requests
        failureReasons.sort(null);

        final ProcessGroupDrainState mergedState;
        if (anyFailed) {
            mergedState = ProcessGroupDrainState.FAILED;
        } else if (anyDraining) {
            mergedState = ProcessGroupDrainState.DRAINING;
        } else {
            mergedState = ProcessGroupDrainState.ENDED;
        }

        final ProcessGroupDrainRequestDTO mergedRequest = new ProcessGroupDrainRequestDTO();
        mergedRequest.setId(identifyingRequest.getId());
        mergedRequest.setProcessGroupId(identifyingRequest.getProcessGroupId());
        mergedRequest.setUri(identifyingRequest.getUri());
        mergedRequest.setState(mergedState.name());
        mergedRequest.setPrepared(allPrepared);
        mergedRequest.setFailureReason(failureReasons.isEmpty() ? null : String.join("\n", failureReasons));
        mergedRequest.setOriginalCount(originalCount);
        mergedRequest.setOriginalSize(originalSize);
        mergedRequest.setCurrentCount(currentCount);
        mergedRequest.setCurrentSize(currentSize);
        mergedRequest.setBlockingComponents(mergeBlockingComponents(requestsByNode.values()));

        clientEntity.setDrainRequest(mergedRequest);
    }

    private static String formatFailureReason(final NodeIdentifier nodeId, final String reason) {
        return nodeId.getApiAddress() + ":" + nodeId.getApiPort() + " -- " + reason;
    }

    /**
     * Every node checks the permissions of the same user, so the first copy of each component is used and only the queued counts are summed.
     * The components are sorted so that their order does not change between requests.
     */
    private static List<DrainBlockingComponentDTO> mergeBlockingComponents(final Collection<ProcessGroupDrainRequestDTO> nodeRequests) {
        final Map<String, DrainBlockingComponentDTO> mergedComponentsById = new TreeMap<>();
        for (final ProcessGroupDrainRequestDTO nodeRequest : nodeRequests) {
            for (final DrainBlockingComponentDTO nodeComponent : nodeRequest.getBlockingComponents()) {
                final DrainBlockingComponentDTO mergedComponent = mergedComponentsById.putIfAbsent(nodeComponent.getId(), nodeComponent);
                if (mergedComponent != null) {
                    mergedComponent.setQueuedCount(mergedComponent.getQueuedCount() + nodeComponent.getQueuedCount());
                }
            }
        }

        return new ArrayList<>(mergedComponentsById.values());
    }
}
