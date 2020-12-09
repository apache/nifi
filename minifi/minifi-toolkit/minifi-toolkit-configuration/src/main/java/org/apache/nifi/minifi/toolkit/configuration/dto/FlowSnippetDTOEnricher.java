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
package org.apache.nifi.minifi.toolkit.configuration.dto;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.connectable.ConnectableType;
import org.apache.nifi.minifi.commons.schema.common.StringUtil;
import org.apache.nifi.web.api.dto.ComponentDTO;
import org.apache.nifi.web.api.dto.ConnectableDTO;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupContentsDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupPortDTO;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.nifi.minifi.commons.schema.common.CollectionUtil.nullToEmpty;

public class FlowSnippetDTOEnricher {

    public void enrich(FlowSnippetDTO flowSnippetDTO, final String encodingVersion) {
        List<FlowSnippetDTO> allFlowSnippets = getAllFlowSnippets(flowSnippetDTO);

        Set<RemoteProcessGroupDTO> remoteProcessGroups = getAll(allFlowSnippets, FlowSnippetDTO::getRemoteProcessGroups).collect(Collectors.toSet());

        Map<String, String> connectableNameMap = getAll(allFlowSnippets, FlowSnippetDTO::getProcessors).collect(Collectors.toMap(ComponentDTO::getId, ProcessorDTO::getName));
        Map<String, String> rpgIdToTargetIdMap = new HashMap<>();

        for (RemoteProcessGroupDTO remoteProcessGroupDTO : remoteProcessGroups) {
            final RemoteProcessGroupContentsDTO contents = remoteProcessGroupDTO.getContents();
            final Set<RemoteProcessGroupPortDTO> rpgInputPortDtos = nullToEmpty(contents.getInputPorts());
            final Set<RemoteProcessGroupPortDTO> rpgOutputPortDtos = nullToEmpty(contents.getOutputPorts());

            /*
             *  Templates created prior to version 1.0 of NiFi did not have an encoding-version specified
             *   There are no material changes to the flow representation that affect MiNiFi prior to the 1.2 encoding-version introduced in NiFi 1.5, so we avoid evaluating
             */
            if (encodingVersion != null) {
                switch (encodingVersion) {
                    // Perform a no-op for the first two versions of templates
                    case "1.0":
                    case "1.1":
                        break;
                    // Starting in 1.2, the way in which RPGs are mapped has changed
                    default:
                        // Map all port DTOs to their respective targetIds
                        rpgIdToTargetIdMap.putAll(
                                Stream.concat(rpgInputPortDtos.stream(), rpgOutputPortDtos.stream())
                                        .collect(Collectors.toMap(RemoteProcessGroupPortDTO::getId, RemoteProcessGroupPortDTO::getTargetId)));
                        break;
                }
            }

            addConnectables(connectableNameMap, rpgInputPortDtos, RemoteProcessGroupPortDTO::getId, RemoteProcessGroupPortDTO::getId);
            addConnectables(connectableNameMap, rpgOutputPortDtos, RemoteProcessGroupPortDTO::getId, RemoteProcessGroupPortDTO::getId);
        }


        addConnectables(connectableNameMap, getAll(allFlowSnippets, FlowSnippetDTO::getInputPorts).collect(Collectors.toList()), PortDTO::getId, PortDTO::getName);
        addConnectables(connectableNameMap, getAll(allFlowSnippets, FlowSnippetDTO::getOutputPorts).collect(Collectors.toList()), PortDTO::getId, PortDTO::getName);

        final Set<ConnectionDTO> connections = getAll(allFlowSnippets, FlowSnippetDTO::getConnections).collect(Collectors.toSet());

        // Enrich connection endpoints using known names and overriding with targetIds for remote ports
        for (ConnectionDTO connection : connections) {
            setName(connectableNameMap, connection.getSource(), rpgIdToTargetIdMap);
            setName(connectableNameMap, connection.getDestination(), rpgIdToTargetIdMap);
        }

        // Override any ids that are for Remote Ports to use their target Ids where available
        connections.stream()
                .flatMap(connectionDTO -> Stream.of(connectionDTO.getSource(), connectionDTO.getDestination()))
                .filter(connectable -> connectable.getType().equals(ConnectableType.REMOTE_OUTPUT_PORT.toString()) || connectable.getType().equals(ConnectableType.REMOTE_INPUT_PORT.toString()))
                .forEach(connectable -> connectable.setId(Optional.ofNullable(rpgIdToTargetIdMap.get(connectable.getId())).orElse(connectable.getId())));

        // Establish unique names for connections
        for (ConnectionDTO connection : connections) {
            if (StringUtil.isNullOrEmpty(connection.getName())) {
                StringBuilder name = new StringBuilder();
                ConnectableDTO connectionSource = connection.getSource();
                name.append(determineValueForConnectable(connectionSource, rpgIdToTargetIdMap));

                name.append("/");
                if (connection.getSelectedRelationships() != null && connection.getSelectedRelationships().size() > 0) {
                    name.append(connection.getSelectedRelationships().iterator().next());
                }

                name.append("/");
                ConnectableDTO connectionDestination = connection.getDestination();
                name.append(determineValueForConnectable(connectionDestination, rpgIdToTargetIdMap));

                connection.setName(name.toString());
            }
        }
        nullToEmpty(flowSnippetDTO.getProcessGroups()).stream().map(ProcessGroupDTO::getContents).forEach(snippetDTO -> enrich(snippetDTO, encodingVersion));
    }

    private static String determineValueForConnectable(ConnectableDTO connectable, Map<String, String> idOverrideMap) {
        String connectionName = "";
        if (connectable != null) {
            connectionName = connectable.getName();
            // If no name is specified, determine the appropriate id to use, preferring any overrides specified
            if (StringUtils.isBlank(connectionName)) {
                connectionName = idOverrideMap.containsKey(connectable.getId()) ? idOverrideMap.get(connectable.getId()) : connectable.getId();
            }
        }
        return connectionName;
    }

    private static <T> Stream<T> getAll(List<FlowSnippetDTO> allFlowSnippets, Function<FlowSnippetDTO, Collection<T>> accessor) {
        return allFlowSnippets.stream().flatMap(f -> accessor.apply(f).stream()).filter(Objects::nonNull);
    }

    private static List<FlowSnippetDTO> getAllFlowSnippets(FlowSnippetDTO flowSnippetDTO) {
        List<FlowSnippetDTO> result = new ArrayList<>();
        getAllFlowSnippets(flowSnippetDTO, result);
        return result;
    }

    private static void getAllFlowSnippets(FlowSnippetDTO flowSnippetDTO, List<FlowSnippetDTO> result) {
        result.add(flowSnippetDTO);
        nullToEmpty(flowSnippetDTO.getProcessGroups()).stream().map(ProcessGroupDTO::getContents).forEach(f -> getAllFlowSnippets(f, result));
    }

    private static void setName(Map<String, String> connectableNameMap, ConnectableDTO connectableDTO, Map<String, String> nameOverrides) {
        if (connectableDTO != null) {
            final String name = connectableNameMap.get(connectableDTO.getId());
            if (name != null) {
                connectableDTO.setName(Optional.ofNullable(nameOverrides.get(connectableDTO.getId())).orElse(name));
            }
        }
    }

    private static <T> void addConnectables(Map<String, String> connectableNameMap, Collection<T> hasIdAndNames, Function<T, String> idGetter, Function<T, String> nameGetter) {
        if (hasIdAndNames != null) {
            for (T hasIdAndName : hasIdAndNames) {
                String id = idGetter.apply(hasIdAndName);
                String name = nameGetter.apply(hasIdAndName);
                if (!StringUtil.isNullOrEmpty(name)) {
                    connectableNameMap.put(id, name);
                }
            }
        }
    }

}
