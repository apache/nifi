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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.controller.queue.ListFlowFileState;
import org.apache.nifi.web.api.dto.FlowFileSummaryDTO;
import org.apache.nifi.web.api.dto.ListingRequestDTO;
import org.apache.nifi.web.api.dto.QueueSizeDTO;
import org.apache.nifi.web.api.entity.ListingRequestEntity;

public class ListFlowFilesEndpointMerger extends AbstractSingleDTOEndpoint<ListingRequestEntity, ListingRequestDTO> {
    public static final Pattern LISTING_REQUESTS_URI = Pattern.compile("/nifi-api/flowfile-queues/[a-f0-9\\-]{36}/listing-requests");
    public static final Pattern LISTING_REQUEST_URI = Pattern.compile("/nifi-api/flowfile-queues/[a-f0-9\\-]{36}/listing-requests/[a-f0-9\\-]{36}");

    @Override
    public boolean canHandle(URI uri, String method) {
        if (("GET".equalsIgnoreCase(method) || "DELETE".equalsIgnoreCase(method)) && LISTING_REQUEST_URI.matcher(uri.getPath()).matches()) {
            return true;
        } else if ("POST".equalsIgnoreCase(method) && LISTING_REQUESTS_URI.matcher(uri.getPath()).matches()) {
            return true;
        }

        return false;
    }

    @Override
    protected Class<ListingRequestEntity> getEntityClass() {
        return ListingRequestEntity.class;
    }

    @Override
    protected ListingRequestDTO getDto(ListingRequestEntity entity) {
        return entity.getListingRequest();
    }

    @Override
    protected void mergeResponses(ListingRequestDTO clientDto, Map<NodeIdentifier, ListingRequestDTO> dtoMap, Set<NodeResponse> successfulResponses, Set<NodeResponse> problematicResponses) {
        final Comparator<FlowFileSummaryDTO> comparator = new Comparator<FlowFileSummaryDTO>() {
            @Override
            public int compare(final FlowFileSummaryDTO dto1, final FlowFileSummaryDTO dto2) {
                int positionCompare = dto1.getPosition().compareTo(dto2.getPosition());
                if (positionCompare != 0) {
                    return positionCompare;
                }

                final String address1 = dto1.getClusterNodeAddress();
                final String address2 = dto2.getClusterNodeAddress();
                if (address1 == null && address2 == null) {
                    return 0;
                }
                if (address1 == null) {
                    return 1;
                }
                if (address2 == null) {
                    return -1;
                }
                return address1.compareTo(address2);
            }
        };

        final NavigableSet<FlowFileSummaryDTO> flowFileSummaries = new TreeSet<>(comparator);

        ListFlowFileState state = null;
        int numStepsCompleted = 0;
        int numStepsTotal = 0;
        int objectCount = 0;
        long byteCount = 0;
        boolean finished = true;
        for (final Map.Entry<NodeIdentifier, ListingRequestDTO> entry : dtoMap.entrySet()) {
            final NodeIdentifier nodeIdentifier = entry.getKey();
            final String nodeAddress = nodeIdentifier.getApiAddress() + ":" + nodeIdentifier.getApiPort();

            final ListingRequestDTO nodeRequest = entry.getValue();

            numStepsTotal++;
            if (Boolean.TRUE.equals(nodeRequest.getFinished())) {
                numStepsCompleted++;
            }

            final QueueSizeDTO nodeQueueSize = nodeRequest.getQueueSize();
            objectCount += nodeQueueSize.getObjectCount();
            byteCount += nodeQueueSize.getByteCount();

            if (!nodeRequest.getFinished()) {
                finished = false;
            }

            if (nodeRequest.getLastUpdated().after(clientDto.getLastUpdated())) {
                clientDto.setLastUpdated(nodeRequest.getLastUpdated());
            }

            // Keep the state with the lowest ordinal value (the "least completed").
            final ListFlowFileState nodeState = ListFlowFileState.valueOfDescription(nodeRequest.getState());
            if (state == null || state.compareTo(nodeState) > 0) {
                state = nodeState;
            }

            if (nodeRequest.getFlowFileSummaries() != null) {
                for (final FlowFileSummaryDTO summaryDTO : nodeRequest.getFlowFileSummaries()) {
                    if (summaryDTO.getClusterNodeId() == null || summaryDTO.getClusterNodeAddress() == null) {
                        summaryDTO.setClusterNodeId(nodeIdentifier.getId());
                        summaryDTO.setClusterNodeAddress(nodeAddress);
                    }

                    flowFileSummaries.add(summaryDTO);

                    // Keep the set from growing beyond our max
                    if (flowFileSummaries.size() > clientDto.getMaxResults()) {
                        flowFileSummaries.pollLast();
                    }
                }
            }

            if (nodeRequest.getFailureReason() != null) {
                clientDto.setFailureReason(nodeRequest.getFailureReason());
            }
        }

        final List<FlowFileSummaryDTO> summaryDTOs = new ArrayList<>(flowFileSummaries);
        clientDto.setFlowFileSummaries(summaryDTOs);
        // depends on invariant if numStepsTotal is 0, so is numStepsCompleted, all steps being completed
        // would be 1
        final int percentCompleted = (numStepsTotal == 0) ? 1 : numStepsCompleted / numStepsTotal;
        clientDto.setPercentCompleted(percentCompleted);
        clientDto.setFinished(finished);

        clientDto.getQueueSize().setByteCount(byteCount);
        clientDto.getQueueSize().setObjectCount(objectCount);
    }

}
