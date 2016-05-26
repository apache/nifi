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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.controller.state.SortedStateUtils;
import org.apache.nifi.web.api.dto.ComponentStateDTO;
import org.apache.nifi.web.api.dto.StateEntryDTO;
import org.apache.nifi.web.api.dto.StateMapDTO;
import org.apache.nifi.web.api.entity.ComponentStateEntity;

public class ComponentStateEndpointMerger extends AbstractSingleEntityEndpoint<ComponentStateEntity, ComponentStateDTO> {
    public static final Pattern PROCESSOR_STATE_URI_PATTERN = Pattern.compile("/nifi-api/processors/[a-f0-9\\-]{36}/state");
    public static final Pattern CONTROLLER_SERVICE_STATE_URI_PATTERN = Pattern.compile("/nifi-api/controller-services/node/[a-f0-9\\-]{36}/state");
    public static final Pattern REPORTING_TASK_STATE_URI_PATTERN = Pattern.compile("/nifi-api/reporting-tasks/node/[a-f0-9\\-]{36}/state");

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
    protected void mergeResponses(ComponentStateDTO clientDto, Map<NodeIdentifier, ComponentStateDTO> dtoMap, Set<NodeResponse> successfulResponses, Set<NodeResponse> problematicResponses) {
        List<StateEntryDTO> localStateEntries = new ArrayList<>();

        int totalStateEntries = 0;
        for (final Map.Entry<NodeIdentifier, ComponentStateDTO> nodeEntry : dtoMap.entrySet()) {
            final ComponentStateDTO nodeComponentState = nodeEntry.getValue();
            final NodeIdentifier nodeId = nodeEntry.getKey();
            final String nodeAddress = nodeId.getApiAddress() + ":" + nodeId.getApiPort();

            final StateMapDTO nodeLocalStateMap = nodeComponentState.getLocalState();
            if (nodeLocalStateMap.getState() != null) {
                totalStateEntries += nodeLocalStateMap.getTotalEntryCount();

                for (final StateEntryDTO nodeStateEntry : nodeLocalStateMap.getState()) {
                    nodeStateEntry.setClusterNodeId(nodeId.getId());
                    nodeStateEntry.setClusterNodeAddress(nodeAddress);
                    localStateEntries.add(nodeStateEntry);
                }
            }
        }

        // ensure appropriate sort
        Collections.sort(localStateEntries, SortedStateUtils.getEntryDtoComparator());

        // sublist if necessary
        if (localStateEntries.size() > SortedStateUtils.MAX_COMPONENT_STATE_ENTRIES) {
            localStateEntries = localStateEntries.subList(0, SortedStateUtils.MAX_COMPONENT_STATE_ENTRIES);
        }

        // add all the local state entries
        clientDto.getLocalState().setTotalEntryCount(totalStateEntries);
        clientDto.getLocalState().setState(localStateEntries);
    }

}
