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
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.entity.NodeReplayLastEventSnapshotDTO;
import org.apache.nifi.web.api.entity.ReplayLastEventResponseEntity;
import org.apache.nifi.web.api.entity.ReplayLastEventSnapshotDTO;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ReplayLastEventEndpointMerger extends AbstractSingleEntityEndpoint<ReplayLastEventResponseEntity> implements EndpointResponseMerger {
    public static final String REPLAY_URI = "/nifi-api/provenance-events/latest/replays";

    @Override
    public boolean canHandle(final URI uri, final String method) {
        return "POST".equals(method) && REPLAY_URI.equals(uri.getPath());
    }

    @Override
    protected Class<ReplayLastEventResponseEntity> getEntityClass() {
        return ReplayLastEventResponseEntity.class;
    }

    @Override
    protected void mergeResponses(final ReplayLastEventResponseEntity clientEntity, final Map<NodeIdentifier, ReplayLastEventResponseEntity> entityMap, final Set<NodeResponse> successfulResponses,
                                  final Set<NodeResponse> problematicResponses) {

        // Move all aggregate snapshots into the node snapshots.
        final Set<Long> replayedEventIds = new HashSet<>();
        final Set<String> failureExplanations = new HashSet<>();
        boolean eventAvailable = false;
        for (final Map.Entry<NodeIdentifier, ReplayLastEventResponseEntity> entry : entityMap.entrySet()) {
            final NodeIdentifier nodeId = entry.getKey();
            final ReplayLastEventResponseEntity nodeEntity = entry.getValue();

            final ReplayLastEventSnapshotDTO nodeSnapshot = nodeEntity.getAggregateSnapshot();
            final NodeReplayLastEventSnapshotDTO nodeResponseDto = new NodeReplayLastEventSnapshotDTO();
            nodeResponseDto.setAddress(nodeId.getApiAddress());
            nodeResponseDto.setApiPort(nodeId.getApiPort());
            nodeResponseDto.setNodeId(nodeId.getId());
            nodeResponseDto.setSnapshot(nodeSnapshot);

            if (clientEntity.getNodeSnapshots() == null) {
                clientEntity.setNodeSnapshots(new ArrayList<>());
            }
            clientEntity.getNodeSnapshots().add(nodeResponseDto);

            final Collection<Long> eventsReplayed = nodeSnapshot.getEventsReplayed();
            if (eventsReplayed != null) {
                replayedEventIds.addAll(eventsReplayed);
            }

            final String failureExplanation = nodeSnapshot.getFailureExplanation();
            if (failureExplanation != null) {
                failureExplanations.add(nodeId.getApiAddress() + ":" + nodeId.getApiPort() + " - " + failureExplanation);
            }

            eventAvailable = eventAvailable || nodeSnapshot.getEventAvailable() == Boolean.TRUE;
        }

        // Update the aggregate snapshot
        clientEntity.getAggregateSnapshot().setEventsReplayed(replayedEventIds);
        clientEntity.getAggregateSnapshot().setEventAvailable(eventAvailable);

        if (failureExplanations.isEmpty()) {
            return;
        }
        if (failureExplanations.size() == 1) {
            clientEntity.getAggregateSnapshot().setFailureExplanation("One node failed to replay the latest event: " + failureExplanations.iterator().next());
        } else {
            clientEntity.getAggregateSnapshot().setFailureExplanation(failureExplanations.size() + " nodes failed to replay the latest events. See logs for more details.");
        }
    }
}
