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

import com.google.common.collect.Sets;
import org.apache.nifi.cluster.manager.NodeResponse;
import org.apache.nifi.cluster.manager.PortEntityMerger;
import org.apache.nifi.cluster.protocol.NodeIdentifier;
import org.apache.nifi.web.api.dto.ControllerDTO;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.entity.ControllerEntity;

import java.net.URI;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class ControllerEndpointMerger extends AbstractSingleDTOEndpoint<ControllerEntity, ControllerDTO> {
    public static final Pattern CONTROLLER_URI_PATTERN = Pattern.compile("/nifi-api/site-to-site");
    private PortEntityMerger portMerger = new PortEntityMerger();

    @Override
    protected Class<ControllerEntity> getEntityClass() {
        return ControllerEntity.class;
    }

    @Override
    protected ControllerDTO getDto(ControllerEntity entity) {
        return entity.getController();
    }

    @Override
    protected void mergeResponses(ControllerDTO clientDto, Map<NodeIdentifier, ControllerDTO> dtoMap, Set<NodeResponse> successfulResponses, Set<NodeResponse> problematicResponses) {
        ControllerDTO mergedController = clientDto;
        final Map<String, Map<NodeIdentifier, PortDTO>> inputPortMap = new HashMap<>(); // map of port id to map of node id to port dto
        final Map<String, Map<NodeIdentifier, PortDTO>> outputPortMap = new HashMap<>(); // map of port id to map of node id to port dto

        for (final Map.Entry<NodeIdentifier, ControllerDTO> entry : dtoMap.entrySet()) {
            final NodeIdentifier nodeId = entry.getKey();
            final ControllerDTO nodeController = entry.getValue();

            // gather all input and output ports for merging, including the ports from clientDto
            nodeController.getInputPorts().stream().forEach(inputPort -> inputPortMap.computeIfAbsent(inputPort.getId(), nodeIdToInputPort -> new HashMap<>()).put(nodeId, inputPort));
            nodeController.getOutputPorts().stream().forEach(outputPort -> outputPortMap.computeIfAbsent(outputPort.getId(), nodeIdToOutputPort -> new HashMap<>()).put(nodeId, outputPort));
        }

        /*
         * Note on port merging: only merge the ports if they exist in the client response and all node responses.  Due to authorization possibly different per node, only ports that have been
         * returned from every node need to be merged.  If a node doesn't return a port DTO due to authorization issues, the responses for that port ID should be dropped from the client response.
         */

        // merge input ports
        for (Map<NodeIdentifier, PortDTO> inputPortByNodeId : inputPortMap.values()) {
            final Collection<PortDTO> nodeInputPorts = inputPortByNodeId.values();
            if (!nodeInputPorts.isEmpty()) {
                final PortDTO inputPort = nodeInputPorts.iterator().next();
                final PortDTO clientInputPort = clientDto.getInputPorts().stream().filter(p -> p.getId().equals(inputPort.getId())).findFirst().orElse(null);
                if (clientInputPort != null) {
                    PortEntityMerger.mergeDtos(clientInputPort, inputPortByNodeId);
                }
            }
        }

        // merge output ports
        for (Map<NodeIdentifier, PortDTO> outputPortByNodeId : outputPortMap.values()) {
            final Collection<PortDTO> nodeOutputPorts = outputPortByNodeId.values();
            if (!nodeOutputPorts.isEmpty()) {
                final PortDTO outputPort = nodeOutputPorts.iterator().next();
                final PortDTO clientOutputPort = clientDto.getInputPorts().stream().filter(p -> p.getId().equals(outputPort.getId())).findFirst().orElse(null);
                if (clientOutputPort != null) {
                    PortEntityMerger.mergeDtos(clientOutputPort, outputPortByNodeId);
                }
            }
        }

        // get intersection of input and output ports
        final Set<PortDTO> clientInputPorts = Sets.newHashSet(clientDto.getInputPorts());
        final Set<PortDTO> clientOutputPorts = Sets.newHashSet(clientDto.getOutputPorts());
        dtoMap.values().forEach(controller -> {
            clientInputPorts.retainAll(controller.getInputPorts());
            clientOutputPorts.retainAll(controller.getOutputPorts());
        });

        clientDto.setInputPorts(clientInputPorts);
        clientDto.setInputPortCount(clientInputPorts.size());
        clientDto.setOutputPorts(clientOutputPorts);
        clientDto.setOutputPortCount(clientOutputPorts.size());
    }

    @Override
    public boolean canHandle(URI uri, String method) {
        return "GET".equalsIgnoreCase(method) && CONTROLLER_URI_PATTERN.matcher(uri.getPath()).matches();
    }
}
