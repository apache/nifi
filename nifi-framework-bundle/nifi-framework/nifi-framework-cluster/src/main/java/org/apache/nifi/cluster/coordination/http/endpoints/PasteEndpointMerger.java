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

import org.apache.nifi.cluster.manager.ConnectionsEntityMerger;
import org.apache.nifi.cluster.manager.FunnelsEntityMerger;
import org.apache.nifi.cluster.manager.LabelsEntityMerger;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.manager.PortsEntityMerger;
import org.apache.nifi.cluster.manager.ProcessGroupsEntityMerger;
import org.apache.nifi.cluster.manager.ProcessorsEntityMerger;
import org.apache.nifi.cluster.manager.RemoteProcessGroupsEntityMerger;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.dto.flow.FlowDTO;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.FunnelEntity;
import org.apache.nifi.web.api.entity.LabelEntity;
import org.apache.nifi.web.api.entity.PasteResponseEntity;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupEntity;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class PasteEndpointMerger extends AbstractSingleDTOEndpoint<PasteResponseEntity, FlowDTO> {
    public static final Pattern PASTE_INSTANCE_URI_PATTERN = Pattern.compile("/nifi-api/process-groups/(?:(?:root)|(?:[a-f0-9\\-]{36}))/paste");

    @Override
    public boolean canHandle(final URI uri, final String method) {
        return "PUT".equalsIgnoreCase(method) && PASTE_INSTANCE_URI_PATTERN.matcher(uri.getPath()).matches();
    }

    @Override
    protected Class<PasteResponseEntity> getEntityClass() {
        return PasteResponseEntity.class;
    }

    @Override
    protected FlowDTO getDto(PasteResponseEntity entity) {
        return entity.getFlow();
    }

    @Override
    protected void mergeResponses(final FlowDTO clientDto, final Map<NodeIdentifier, FlowDTO> dtoMap, final Set<NodeResponse> successfulResponses, final Set<NodeResponse> problematicResponses) {
        final Set<ConnectionEntity> clientConnections = clientDto.getConnections();
        final Set<ProcessorEntity> clientProcessors = clientDto.getProcessors();
        final Set<PortEntity> clientInputPorts = clientDto.getInputPorts();
        final Set<PortEntity> clientOutputPorts = clientDto.getOutputPorts();
        final Set<RemoteProcessGroupEntity> clientRemoteProcessGroups = clientDto.getRemoteProcessGroups();
        final Set<ProcessGroupEntity> clientProcessGroups = clientDto.getProcessGroups();
        final Set<LabelEntity> clientLabels = clientDto.getLabels();
        final Set<FunnelEntity> clientFunnels = clientDto.getFunnels();

        final Map<String, Map<NodeIdentifier, ConnectionEntity>> connections = new HashMap<>();
        final Map<String, Map<NodeIdentifier, FunnelEntity>> funnels = new HashMap<>();
        final Map<String, Map<NodeIdentifier, PortEntity>> inputPorts = new HashMap<>();
        final Map<String, Map<NodeIdentifier, LabelEntity>> labels = new HashMap<>();
        final Map<String, Map<NodeIdentifier, PortEntity>> outputPorts = new HashMap<>();
        final Map<String, Map<NodeIdentifier, ProcessorEntity>> processors = new HashMap<>();
        final Map<String, Map<NodeIdentifier, RemoteProcessGroupEntity>> rpgs = new HashMap<>();
        final Map<String, Map<NodeIdentifier, ProcessGroupEntity>> processGroups = new HashMap<>();

        // Create mapping of ComponentID -> [nodeId, entity on that node]
        for (final Map.Entry<NodeIdentifier, FlowDTO> nodeFlowEntry : dtoMap.entrySet()) {
            final NodeIdentifier nodeIdentifier = nodeFlowEntry.getKey();
            final FlowDTO nodeFlowDto = nodeFlowEntry.getValue();

            for (final ConnectionEntity entity : nodeFlowDto.getConnections()) {
                connections.computeIfAbsent(entity.getId(), id -> new HashMap<>()).computeIfAbsent(nodeIdentifier, nodeId -> entity);
            }

            for (final FunnelEntity entity : nodeFlowDto.getFunnels()) {
                funnels.computeIfAbsent(entity.getId(), id -> new HashMap<>()).computeIfAbsent(nodeIdentifier, nodeId -> entity);
            }

            for (final PortEntity entity : nodeFlowDto.getInputPorts()) {
                inputPorts.computeIfAbsent(entity.getId(), id -> new HashMap<>()).computeIfAbsent(nodeIdentifier, nodeId -> entity);
            }

            for (final PortEntity entity : nodeFlowDto.getOutputPorts()) {
                outputPorts.computeIfAbsent(entity.getId(), id -> new HashMap<>()).computeIfAbsent(nodeIdentifier, nodeId -> entity);
            }

            for (final LabelEntity entity : nodeFlowDto.getLabels()) {
                labels.computeIfAbsent(entity.getId(), id -> new HashMap<>()).computeIfAbsent(nodeIdentifier, nodeId -> entity);
            }

            for (final ProcessorEntity entity : nodeFlowDto.getProcessors()) {
                processors.computeIfAbsent(entity.getId(), id -> new HashMap<>()).computeIfAbsent(nodeIdentifier, nodeId -> entity);
            }

            for (final RemoteProcessGroupEntity entity : nodeFlowDto.getRemoteProcessGroups()) {
                rpgs.computeIfAbsent(entity.getId(), id -> new HashMap<>()).computeIfAbsent(nodeIdentifier, nodeId -> entity);
            }

            for (final ProcessGroupEntity entity : nodeFlowDto.getProcessGroups()) {
                processGroups.computeIfAbsent(entity.getId(), id -> new HashMap<>()).computeIfAbsent(nodeIdentifier, nodeId -> entity);
            }
        }

        // Merge connections
        ConnectionsEntityMerger.mergeConnections(clientConnections, connections);

        // Merge funnel statuses
        FunnelsEntityMerger.mergeFunnels(clientFunnels, funnels);

        // Merge input ports
        PortsEntityMerger.mergePorts(clientInputPorts, inputPorts);

        // Merge output ports
        PortsEntityMerger.mergePorts(clientOutputPorts, outputPorts);

        // Merge labels
        LabelsEntityMerger.mergeLabels(clientLabels, labels);

        // Merge processors
        ProcessorsEntityMerger.mergeProcessors(clientProcessors, processors);

        // Merge Remote Process Groups
        RemoteProcessGroupsEntityMerger.mergeRemoteProcessGroups(clientRemoteProcessGroups, rpgs);

        // Merge Process Groups
        ProcessGroupsEntityMerger.mergeProcessGroups(clientProcessGroups, processGroups);
    }
}
