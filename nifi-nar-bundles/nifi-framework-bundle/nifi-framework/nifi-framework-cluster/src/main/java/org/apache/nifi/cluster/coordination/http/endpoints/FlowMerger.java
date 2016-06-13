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
import org.apache.nifi.cluster.manager.StatusMerger;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.dto.flow.FlowDTO;
import org.apache.nifi.web.api.dto.flow.ProcessGroupFlowDTO;
import org.apache.nifi.web.api.dto.status.ConnectionStatusDTO;
import org.apache.nifi.web.api.dto.status.PortStatusDTO;
import org.apache.nifi.web.api.dto.status.ProcessGroupStatusDTO;
import org.apache.nifi.web.api.dto.status.ProcessorStatusDTO;
import org.apache.nifi.web.api.dto.status.RemoteProcessGroupStatusDTO;
import org.apache.nifi.web.api.entity.ConnectionEntity;
import org.apache.nifi.web.api.entity.FunnelEntity;
import org.apache.nifi.web.api.entity.LabelEntity;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.apache.nifi.web.api.entity.ProcessorEntity;
import org.apache.nifi.web.api.entity.RemoteProcessGroupEntity;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class FlowMerger extends AbstractSingleDTOEndpoint<ProcessGroupFlowEntity, ProcessGroupFlowDTO> {
    public static final Pattern FLOW_URI_PATTERN = Pattern.compile("/nifi-api/flow/process-groups/(?:(?:root)|(?:[a-f0-9\\-]{36}))");

    @Override
    public boolean canHandle(final URI uri, final String method) {
        return "GET".equalsIgnoreCase(method) && FLOW_URI_PATTERN.matcher(uri.getPath()).matches();
    }

    @Override
    protected Class<ProcessGroupFlowEntity> getEntityClass() {
        return ProcessGroupFlowEntity.class;
    }

    @Override
    protected ProcessGroupFlowDTO getDto(final ProcessGroupFlowEntity entity) {
        return entity.getProcessGroupFlow();
    }

    @Override
    protected void mergeResponses(final ProcessGroupFlowDTO clientDto, final Map<NodeIdentifier, ProcessGroupFlowDTO> dtoMap,
        final Set<NodeResponse> successfulResponses, final Set<NodeResponse> problematicResponses) {

        final FlowDTO flowDto = clientDto.getFlow();

        final Map<String, List<ConnectionEntity>> connections = new HashMap<>();
        final Map<String, List<FunnelEntity>> funnels = new HashMap<>();
        final Map<String, List<PortEntity>> inputPorts = new HashMap<>();
        final Map<String, List<LabelEntity>> labels = new HashMap<>();
        final Map<String, List<PortEntity>> outputPorts = new HashMap<>();
        final Map<String, List<ProcessorEntity>> processors = new HashMap<>();
        final Map<String, List<RemoteProcessGroupEntity>> rpgs = new HashMap<>();
        final Map<String, List<ProcessGroupEntity>> processGroups = new HashMap<>();

        // Create mapping of ComponentID -> all components with that ID (one per node)
        for (final ProcessGroupFlowDTO nodeGroupFlowDto : dtoMap.values()) {
            final FlowDTO nodeFlowDto = nodeGroupFlowDto.getFlow();

            // Merge connection statuses
            for (final ConnectionEntity entity : nodeFlowDto.getConnections()) {
                connections.computeIfAbsent(entity.getId(), id -> new ArrayList<>()).add(entity);
            }

            for (final FunnelEntity entity : nodeFlowDto.getFunnels()) {
                funnels.computeIfAbsent(entity.getId(), id -> new ArrayList<>()).add(entity);
            }

            for (final PortEntity entity : nodeFlowDto.getInputPorts()) {
                inputPorts.computeIfAbsent(entity.getId(), id -> new ArrayList<>()).add(entity);
            }

            for (final PortEntity entity : nodeFlowDto.getOutputPorts()) {
                outputPorts.computeIfAbsent(entity.getId(), id -> new ArrayList<>()).add(entity);
            }

            for (final LabelEntity entity : nodeFlowDto.getLabels()) {
                labels.computeIfAbsent(entity.getId(), id -> new ArrayList<>()).add(entity);
            }

            for (final ProcessorEntity entity : nodeFlowDto.getProcessors()) {
                processors.computeIfAbsent(entity.getId(), id -> new ArrayList<>()).add(entity);
            }

            for (final RemoteProcessGroupEntity entity : nodeFlowDto.getRemoteProcessGroups()) {
                rpgs.computeIfAbsent(entity.getId(), id -> new ArrayList<>()).add(entity);
            }

            for (final ProcessGroupEntity entity : nodeFlowDto.getProcessGroups()) {
                processGroups.computeIfAbsent(entity.getId(), id -> new ArrayList<>()).add(entity);
            }
        }

        //
        // Merge the components that are grouped together by ID
        //

        // Merge connections
        final Set<ConnectionEntity> mergedConnections = new HashSet<>();
        for (final List<ConnectionEntity> connectionList : connections.values()) {
            mergedConnections.add(mergeConnections(connectionList));
        }
        flowDto.setConnections(mergedConnections);

        // Merge funnel statuses
        final Set<FunnelEntity> mergedFunnels = new HashSet<>();
        for (final List<FunnelEntity> funnelList : funnels.values()) {
            mergedFunnels.add(mergeFunnels(funnelList));
        }
        flowDto.setFunnels(mergedFunnels);

        // Merge input ports
        final Set<PortEntity> mergedInputPorts = new HashSet<>();
        for (final List<PortEntity> portList : inputPorts.values()) {
            mergedInputPorts.add(mergePorts(portList));
        }
        flowDto.setInputPorts(mergedInputPorts);

        // Merge output ports
        final Set<PortEntity> mergedOutputPorts = new HashSet<>();
        for (final List<PortEntity> portList : outputPorts.values()) {
            mergedOutputPorts.add(mergePorts(portList));
        }
        flowDto.setOutputPorts(mergedOutputPorts);

        // Merge labels
        final Set<LabelEntity> mergedLabels = new HashSet<>();
        for (final List<LabelEntity> labelList : labels.values()) {
            mergedLabels.add(mergeLabels(labelList));
        }
        flowDto.setLabels(mergedLabels);


        // Merge processors
        final Set<ProcessorEntity> mergedProcessors = new HashSet<>();
        for (final List<ProcessorEntity> processorList : processors.values()) {
            mergedProcessors.add(mergeProcessors(processorList));
        }
        flowDto.setProcessors(mergedProcessors);


        // Merge Remote Process Groups
        final Set<RemoteProcessGroupEntity> mergedRpgs = new HashSet<>();
        for (final List<RemoteProcessGroupEntity> rpgList : rpgs.values()) {
            mergedRpgs.add(mergeRemoteProcessGroups(rpgList));
        }
        flowDto.setRemoteProcessGroups(mergedRpgs);


        // Merge Process Groups
        final Set<ProcessGroupEntity> mergedGroups = new HashSet<>();
        for (final List<ProcessGroupEntity> groupList : processGroups.values()) {
            mergedGroups.add(mergeProcessGroups(groupList));
        }
        flowDto.setProcessGroups(mergedGroups);
    }

    private ConnectionEntity mergeConnections(final List<ConnectionEntity> connections) {
        final ConnectionEntity merged = connections.get(0);
        final ConnectionStatusDTO statusDto = merged.getStatus();
        statusDto.setNodeSnapshots(null);

        for (final ConnectionEntity entity : connections) {
            if (entity != merged) {
                StatusMerger.merge(merged.getStatus().getAggregateSnapshot(), entity.getStatus().getAggregateSnapshot());
            }
        }

        return merged;
    }

    private PortEntity mergePorts(final List<PortEntity> ports) {
        final PortEntity merged = ports.get(0);
        final PortStatusDTO statusDto = merged.getStatus();
        statusDto.setNodeSnapshots(null);

        for (final PortEntity entity : ports) {
            if (entity != merged) {
                StatusMerger.merge(merged.getStatus().getAggregateSnapshot(), entity.getStatus().getAggregateSnapshot());
            }
        }

        return merged;
    }

    private FunnelEntity mergeFunnels(final List<FunnelEntity> funnels) {
        return funnels.get(0);
    }

    private LabelEntity mergeLabels(final List<LabelEntity> labels) {
        return labels.get(0);
    }

    private ProcessorEntity mergeProcessors(final List<ProcessorEntity> processors) {
        final ProcessorEntity merged = processors.get(0);
        final ProcessorStatusDTO statusDto = merged.getStatus();
        statusDto.setNodeSnapshots(null);

        for (final ProcessorEntity entity : processors) {
            if (entity != merged) {
                StatusMerger.merge(merged.getStatus().getAggregateSnapshot(), entity.getStatus().getAggregateSnapshot());
            }
        }

        return merged;
    }


    private RemoteProcessGroupEntity mergeRemoteProcessGroups(final List<RemoteProcessGroupEntity> rpgs) {
        final RemoteProcessGroupEntity merged = rpgs.get(0);
        final RemoteProcessGroupStatusDTO statusDto = merged.getStatus();
        statusDto.setNodeSnapshots(null);

        for (final RemoteProcessGroupEntity entity : rpgs) {
            if (entity != merged) {
                StatusMerger.merge(merged.getStatus().getAggregateSnapshot(), entity.getStatus().getAggregateSnapshot());
            }
        }

        return merged;
    }

    private ProcessGroupEntity mergeProcessGroups(final List<ProcessGroupEntity> groups) {
        final ProcessGroupEntity merged = groups.get(0);
        final ProcessGroupStatusDTO statusDto = merged.getStatus();
        statusDto.setNodeSnapshots(null);

        for (final ProcessGroupEntity entity : groups) {
            if (entity != merged) {
                StatusMerger.merge(merged.getStatus().getAggregateSnapshot(), entity.getStatus().getAggregateSnapshot());
            }
        }

        // We merge only the statuses of the Process Groups. The child components are not
        // necessary for a FlowProcessGroupDTO, so we just ensure that they are null
        if (merged.getComponent() != null) {
            merged.getComponent().setContents(null);
        }

        return merged;
    }
}
