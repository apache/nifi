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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.components.state.Scope;
import org.apache.nifi.controller.state.SortedStateUtils;
import org.apache.nifi.web.api.dto.ComponentStateDTO;
import org.apache.nifi.web.api.dto.StateEntryDTO;
import org.apache.nifi.web.api.dto.StateMapDTO;
import org.apache.nifi.web.api.entity.ComponentStateEntity;

public class ComponentStateEndpointMerger extends AbstractSingleDTOEndpoint<ComponentStateEntity, ComponentStateDTO> {
    public static final Pattern PROCESSOR_STATE_URI_PATTERN = Pattern.compile("/nifi-api/processors/[a-f0-9\\-]{36}/state");
    public static final Pattern CONTROLLER_SERVICE_STATE_URI_PATTERN = Pattern.compile("/nifi-api/controller-services/[a-f0-9\\-]{36}/state");
    public static final Pattern REPORTING_TASK_STATE_URI_PATTERN = Pattern.compile("/nifi-api/reporting-tasks/[a-f0-9\\-]{36}/state");

    @Override
    public boolean canHandle(URI uri, String method) {
        if (!"GET".equalsIgnoreCase(method)) {
            return false;
        }

        return PROCESSOR_STATE_URI_PATTERN.matcher(uri.getPath()).matches()
            || CONTROLLER_SERVICE_STATE_URI_PATTERN.matcher(uri.getPath()).matches()
            || REPORTING_TASK_STATE_URI_PATTERN.matcher(uri.getPath()).matches();
    }

    @Override
    protected Class<ComponentStateEntity> getEntityClass() {
        return ComponentStateEntity.class;
    }

    @Override
    protected ComponentStateDTO getDto(ComponentStateEntity entity) {
        return entity.getComponentState();
    }

    @Override
    public void mergeResponses(ComponentStateDTO clientDto, Map<NodeIdentifier, ComponentStateDTO> dtoMap,
                               Set<NodeResponse> successfulResponses, Set<NodeResponse> problematicResponses) {

        // If there're more than 1 node returning external state, then it's a per node external state.
        final boolean externalPerNode = dtoMap.values().stream()
                .filter(component -> (component.getExternalState() != null && component.getExternalState().getState() != null))
                .count() > 1;

        final StateMapDTOMerger[] mergers = {
                new StateMapDTOMerger(Scope.LOCAL, node -> node.getLocalState(), (merged, state) -> merged.setLocalState(state), true),
                new StateMapDTOMerger(Scope.CLUSTER, node -> node.getClusterState(), (merged, state) -> merged.setClusterState(state), false),
                new StateMapDTOMerger(Scope.EXTERNAL, node -> node.getExternalState(), (merged, state) -> merged.setExternalState(state), externalPerNode)
        };

        // Loop through nodes
        for (final Map.Entry<NodeIdentifier, ComponentStateDTO> nodeEntry : dtoMap.entrySet()) {
            final ComponentStateDTO nodeComponentState = nodeEntry.getValue();
            final NodeIdentifier nodeId = nodeEntry.getKey();
            final String nodeAddress = nodeId.getApiAddress() + ":" + nodeId.getApiPort();

            for (final StateMapDTOMerger merger : mergers) {
                final StateMapDTO nodeStateMapDTO = merger.stateGetter.apply(nodeComponentState);
                if (nodeStateMapDTO != null && nodeStateMapDTO.getState() != null) {
                    final StateMapDTO mergedStateMapDTO = merger.getMergedStateMapDTO();
                    final List<StateEntryDTO> stateEntries = mergedStateMapDTO.getState();
                    mergedStateMapDTO.setTotalEntryCount(mergedStateMapDTO.getTotalEntryCount() + nodeStateMapDTO.getTotalEntryCount());

                    for (final StateEntryDTO nodeStateEntry : nodeStateMapDTO.getState()) {
                        if (merger.perNode
                                && (nodeStateEntry.getClusterNodeId() == null || nodeStateEntry.getClusterNodeAddress() == null)) {
                            nodeStateEntry.setClusterNodeId(nodeId.getId());
                            nodeStateEntry.setClusterNodeAddress(nodeAddress);
                        }

                        stateEntries.add(nodeStateEntry);
                    }
                }
            }
        }

        Arrays.stream(mergers).filter(m -> m.mergedStateMapDTO != null).forEach(m -> {
            // ensure appropriate sort
            final List<StateEntryDTO> stateEntries = m.mergedStateMapDTO.getState();
            Collections.sort(stateEntries, SortedStateUtils.getEntryDtoComparator());

            // sublist if necessary
            if (stateEntries.size() > SortedStateUtils.MAX_COMPONENT_STATE_ENTRIES) {
                m.mergedStateMapDTO.setState(stateEntries.subList(0, SortedStateUtils.MAX_COMPONENT_STATE_ENTRIES));
            }

            m.stateSetter.apply(clientDto, m.mergedStateMapDTO);
        });

    }

    private interface StateGetter {
        StateMapDTO apply(ComponentStateDTO node);
    }
    private interface StateSetter {
        void apply(ComponentStateDTO merged, StateMapDTO state);
    }

    private static class StateMapDTOMerger {
        private final StateGetter stateGetter;
        private final StateSetter stateSetter;
        private final Scope scope;
        private final boolean perNode;
        private StateMapDTO mergedStateMapDTO;

        public StateMapDTOMerger(final Scope scope, final StateGetter stateGetter, final StateSetter stateSetter, final boolean perNode) {
            this.scope = scope;
            this.stateGetter = stateGetter;
            this.stateSetter = stateSetter;
            this.perNode = perNode;
        }

        public StateMapDTO getMergedStateMapDTO() {
            if (mergedStateMapDTO == null) {
                mergedStateMapDTO = new StateMapDTO();
                mergedStateMapDTO.setScope(scope.toString());
                mergedStateMapDTO.setTotalEntryCount(0);
                mergedStateMapDTO.setState(new ArrayList<>());
            }
            return mergedStateMapDTO;
        }
    }

}
