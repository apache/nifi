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
package org.apache.nifi.minifi.toolkit.configuration.registry;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.minifi.commons.schema.common.StringUtil;
import org.apache.nifi.registry.flow.ConnectableComponent;
import org.apache.nifi.registry.flow.ConnectableComponentType;
import org.apache.nifi.registry.flow.VersionedComponent;
import org.apache.nifi.registry.flow.VersionedConnection;
import org.apache.nifi.registry.flow.VersionedPort;
import org.apache.nifi.registry.flow.VersionedProcessGroup;
import org.apache.nifi.registry.flow.VersionedRemoteGroupPort;
import org.apache.nifi.registry.flow.VersionedRemoteProcessGroup;

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

public class VersionedProcessGroupEnricher {

    public void enrich(final VersionedProcessGroup versionedProcessGroup) {
        List<VersionedProcessGroup> allVersionedProcessGroups = getAllVersionedProcessGroups(versionedProcessGroup);

        Set<VersionedRemoteProcessGroup> remoteProcessGroups = getAll(allVersionedProcessGroups, VersionedProcessGroup::getRemoteProcessGroups).collect(Collectors.toSet());

        Map<String, String> connectableNameMap = getAll(allVersionedProcessGroups, VersionedProcessGroup::getProcessors)
                .collect(Collectors.toMap(VersionedComponent::getIdentifier, VersionedComponent::getName));

        Map<String, String> rpgIdToTargetIdMap = new HashMap<>();

        for (VersionedRemoteProcessGroup remoteProcessGroup : remoteProcessGroups) {
            final Set<VersionedRemoteGroupPort> rpgInputPorts = nullToEmpty(remoteProcessGroup.getInputPorts());
            final Set<VersionedRemoteGroupPort> rpgOutputPorts = nullToEmpty(remoteProcessGroup.getOutputPorts());

            // Map all port DTOs to their respective targetIds
            rpgIdToTargetIdMap.putAll(
                    Stream.concat(rpgInputPorts.stream(), rpgOutputPorts.stream())
                            .collect(Collectors.toMap(VersionedRemoteGroupPort::getIdentifier, VersionedRemoteGroupPort::getTargetId)));

            addConnectables(connectableNameMap, rpgInputPorts, VersionedRemoteGroupPort::getIdentifier, VersionedRemoteGroupPort::getIdentifier);
            addConnectables(connectableNameMap, rpgOutputPorts, VersionedRemoteGroupPort::getIdentifier, VersionedRemoteGroupPort::getIdentifier);
        }

        addConnectables(connectableNameMap, getAll(allVersionedProcessGroups, VersionedProcessGroup::getInputPorts)
                .collect(Collectors.toList()), VersionedPort::getIdentifier, VersionedPort::getName);

        addConnectables(connectableNameMap, getAll(allVersionedProcessGroups, VersionedProcessGroup::getOutputPorts)
                .collect(Collectors.toList()), VersionedPort::getIdentifier, VersionedPort::getName);

        final Set<VersionedConnection> connections = getAll(allVersionedProcessGroups, VersionedProcessGroup::getConnections).collect(Collectors.toSet());

        // Enrich connection endpoints using known names and overriding with targetIds for remote ports
        for (VersionedConnection connection : connections) {
            setName(connectableNameMap, connection.getSource(), rpgIdToTargetIdMap);
            setName(connectableNameMap, connection.getDestination(), rpgIdToTargetIdMap);
        }

        // Override any ids that are for Remote Ports to use their target Ids where available
        connections.stream()
                .flatMap(connectionDTO -> Stream.of(connectionDTO.getSource(), connectionDTO.getDestination()))
                .filter(connectable -> (connectable.getType() == ConnectableComponentType.REMOTE_OUTPUT_PORT || connectable.getType() == ConnectableComponentType.REMOTE_INPUT_PORT))
                .forEach(connectable -> connectable.setId(Optional.ofNullable(rpgIdToTargetIdMap.get(connectable.getId())).orElse(connectable.getId())));

        // Establish unique names for connections
        for (VersionedConnection connection : connections) {
            if (StringUtil.isNullOrEmpty(connection.getName())) {
                StringBuilder name = new StringBuilder();
                ConnectableComponent connectionSource = connection.getSource();
                name.append(determineValueForConnectable(connectionSource, rpgIdToTargetIdMap));

                name.append("/");
                if (connection.getSelectedRelationships() != null && connection.getSelectedRelationships().size() > 0) {
                    name.append(connection.getSelectedRelationships().iterator().next());
                }

                name.append("/");
                ConnectableComponent connectionDestination = connection.getDestination();
                name.append(determineValueForConnectable(connectionDestination, rpgIdToTargetIdMap));

                connection.setName(name.toString());
            }
        }
        nullToEmpty(versionedProcessGroup.getProcessGroups()).stream().forEach(pg -> enrich(pg));
    }

    private static String determineValueForConnectable(ConnectableComponent connectable, Map<String, String> idOverrideMap) {
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

    private static <T> Stream<T> getAll(List<VersionedProcessGroup> allVersionedProcessGroups, Function<VersionedProcessGroup, Collection<T>> accessor) {
        return allVersionedProcessGroups.stream().flatMap(f -> accessor.apply(f).stream()).filter(Objects::nonNull);
    }

    private static List<VersionedProcessGroup> getAllVersionedProcessGroups(VersionedProcessGroup versionedProcessGroup) {
        List<VersionedProcessGroup> result = new ArrayList<>();
        getAllVersionedProcessGroups(versionedProcessGroup, result);
        return result;
    }

    private static void getAllVersionedProcessGroups(VersionedProcessGroup versionedProcessGroup, List<VersionedProcessGroup> result) {
        result.add(versionedProcessGroup);
        nullToEmpty(versionedProcessGroup.getProcessGroups()).stream().forEach(f -> getAllVersionedProcessGroups(f, result));
    }

    private static void setName(Map<String, String> connectableNameMap, ConnectableComponent connectable, Map<String, String> nameOverrides) {
        if (connectable != null) {
            final String name = connectableNameMap.get(connectable.getId());
            if (name != null) {
                connectable.setName(Optional.ofNullable(nameOverrides.get(connectable.getId())).orElse(name));
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
