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
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.nifi.cluster.coordination.http.EndpointResponseMerger;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.util.FormatUtils;
import org.apache.nifi.web.api.dto.provenance.ProvenanceDTO;
import org.apache.nifi.web.api.dto.provenance.ProvenanceEventDTO;
import org.apache.nifi.web.api.dto.provenance.ProvenanceRequestDTO;
import org.apache.nifi.web.api.dto.provenance.ProvenanceResultsDTO;
import org.apache.nifi.web.api.entity.ProvenanceEntity;

public class ProvenanceQueryEndpointMerger implements EndpointResponseMerger {
    public static final String PROVENANCE_URI = "/nifi-api/provenance";
    public static final Pattern PROVENANCE_QUERY_URI = Pattern.compile("/nifi-api/provenance/[a-f0-9\\-]{36}");

    @Override
    public boolean canHandle(URI uri, String method) {
        if ("POST".equalsIgnoreCase(method) && PROVENANCE_URI.equals(uri.getPath())) {
            return true;
        } else if ("GET".equalsIgnoreCase(method) && PROVENANCE_QUERY_URI.matcher(uri.getPath()).matches()) {
            return true;
        }
        return false;
    }


    @Override
    public NodeResponse merge(URI uri, String method, Set<NodeResponse> successfulResponses, Set<NodeResponse> problematicResponses, NodeResponse clientResponse) {
        if (!canHandle(uri, method)) {
            throw new IllegalArgumentException("Cannot use Endpoint Mapper of type " + getClass().getSimpleName() + " to map responses for URI " + uri + ", HTTP Method " + method);
        }

        final ProvenanceEntity responseEntity = clientResponse.getClientResponse().getEntity(ProvenanceEntity.class);
        final ProvenanceDTO dto = responseEntity.getProvenance();

        final Map<NodeIdentifier, ProvenanceDTO> dtoMap = new HashMap<>();
        for (final NodeResponse nodeResponse : successfulResponses) {
            final ProvenanceEntity nodeResponseEntity = nodeResponse == clientResponse ? responseEntity : nodeResponse.getClientResponse().getEntity(ProvenanceEntity.class);
            final ProvenanceDTO nodeDto = nodeResponseEntity.getProvenance();
            dtoMap.put(nodeResponse.getNodeId(), nodeDto);
        }

        mergeResponses(dto, dtoMap, successfulResponses, problematicResponses);
        return new NodeResponse(clientResponse, responseEntity);
    }


    protected void mergeResponses(ProvenanceDTO clientDto, Map<NodeIdentifier, ProvenanceDTO> dtoMap, Set<NodeResponse> successfulResponses, Set<NodeResponse> problematicResponses) {
        final ProvenanceResultsDTO results = clientDto.getResults();
        final ProvenanceRequestDTO request = clientDto.getRequest();
        final List<ProvenanceEventDTO> allResults = new ArrayList<>(1024);

        final Set<String> errors = new HashSet<>();
        Date oldestEventDate = new Date();
        int percentageComplete = 0;
        boolean finished = true;

        long totalRecords = 0;
        for (final Map.Entry<NodeIdentifier, ProvenanceDTO> entry : dtoMap.entrySet()) {
            final NodeIdentifier nodeIdentifier = entry.getKey();
            final String nodeAddress = nodeIdentifier.getApiAddress() + ":" + nodeIdentifier.getApiPort();

            final ProvenanceDTO nodeDto = entry.getValue();
            final ProvenanceResultsDTO nodeResultDto = nodeDto.getResults();
            if (nodeResultDto != null && nodeResultDto.getProvenanceEvents() != null) {
                // increment the total number of records
                totalRecords += nodeResultDto.getTotalCount();

                // populate the cluster identifier
                for (final ProvenanceEventDTO eventDto : nodeResultDto.getProvenanceEvents()) {
                    // if the cluster node id or node address is not set, then we need to populate them. If they
                    // are already set, we don't want to populate them because it will be the case that they were populated
                    // by the Cluster Coordinator when it federated the request, and we are now just receiving the response
                    // from the Cluster Coordinator.
                    if (eventDto.getClusterNodeId() == null || eventDto.getClusterNodeAddress() == null) {
                        eventDto.setClusterNodeId(nodeIdentifier.getId());
                        eventDto.setClusterNodeAddress(nodeAddress);
                        // add node identifier to the event's id so that it is unique across cluster
                        eventDto.setId(nodeIdentifier.getId() + eventDto.getId());
                    }

                    allResults.add(eventDto);
                }
            }

            if (nodeResultDto.getOldestEvent() != null && nodeResultDto.getOldestEvent().before(oldestEventDate)) {
                oldestEventDate = nodeResultDto.getOldestEvent();
            }

            if (nodeResultDto.getErrors() != null) {
                for (final String error : nodeResultDto.getErrors()) {
                    errors.add(nodeAddress + " -- " + error);
                }
            }

            percentageComplete += nodeDto.getPercentCompleted();
            if (!nodeDto.isFinished()) {
                finished = false;
            }
        }
        percentageComplete /= dtoMap.size();

        // consider any problematic responses as errors
        for (final NodeResponse problematicResponse : problematicResponses) {
            final NodeIdentifier problemNode = problematicResponse.getNodeId();
            final String problemNodeAddress = problemNode.getApiAddress() + ":" + problemNode.getApiPort();
            errors.add(String.format("%s -- Request did not complete successfully (Status code: %s)", problemNodeAddress, problematicResponse.getStatus()));
        }

        // Since we get back up to the maximum number of results from each node, we need to sort those values and then
        // grab only the first X number of them. We do a sort based on time, such that the newest are included.
        // If 2 events have the same timestamp, we do a secondary sort based on Cluster Node Identifier. If those are
        // equal, we perform a terciary sort based on the the event id
        Collections.sort(allResults, new Comparator<ProvenanceEventDTO>() {
            @Override
            public int compare(final ProvenanceEventDTO o1, final ProvenanceEventDTO o2) {
                final int eventTimeComparison = o1.getEventTime().compareTo(o2.getEventTime());
                if (eventTimeComparison != 0) {
                    return -eventTimeComparison;
                }

                final String nodeId1 = o1.getClusterNodeId();
                final String nodeId2 = o2.getClusterNodeId();
                final int nodeIdComparison;
                if (nodeId1 == null && nodeId2 == null) {
                    nodeIdComparison = 0;
                } else if (nodeId1 == null) {
                    nodeIdComparison = 1;
                } else if (nodeId2 == null) {
                    nodeIdComparison = -1;
                } else {
                    nodeIdComparison = -nodeId1.compareTo(nodeId2);
                }

                if (nodeIdComparison != 0) {
                    return nodeIdComparison;
                }

                return -Long.compare(o1.getEventId(), o2.getEventId());
            }
        });

        final int maxResults = request.getMaxResults().intValue();
        final List<ProvenanceEventDTO> selectedResults;
        if (allResults.size() < maxResults) {
            selectedResults = allResults;
        } else {
            selectedResults = allResults.subList(0, maxResults);
        }

        // include any errors
        if (errors.size() > 0) {
            results.setErrors(errors);
        }

        if (clientDto.getRequest().getMaxResults() != null && totalRecords >= clientDto.getRequest().getMaxResults()) {
            results.setTotalCount(clientDto.getRequest().getMaxResults().longValue());
            results.setTotal(FormatUtils.formatCount(clientDto.getRequest().getMaxResults().longValue()) + "+");
        } else {
            results.setTotal(FormatUtils.formatCount(totalRecords));
            results.setTotalCount(totalRecords);
        }

        results.setProvenanceEvents(selectedResults);
        results.setOldestEvent(oldestEventDate);
        results.setGenerated(new Date());
        clientDto.setPercentCompleted(percentageComplete);
        clientDto.setFinished(finished);
    }
}
