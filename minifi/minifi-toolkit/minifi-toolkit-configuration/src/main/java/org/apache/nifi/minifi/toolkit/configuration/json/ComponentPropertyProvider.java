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

package org.apache.nifi.minifi.toolkit.configuration.json;

import static java.util.Objects.nonNull;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Stream.concat;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.nifi.flow.ConnectableComponentType;
import org.apache.nifi.minifi.toolkit.schema.ConfigSchema;
import org.apache.nifi.minifi.toolkit.schema.ProcessGroupSchema;
import org.apache.nifi.minifi.toolkit.schema.RemoteProcessGroupSchema;
import org.apache.nifi.minifi.toolkit.schema.common.BaseSchemaWithId;

/**
 * Helper class to support flow conversion from YAML to JSON format
 * When creating connection objects in the JSON definition, the component's type and parentId are required
 * However these attributes are not available in the YAML definition on those objects.
 * We iterate through the YAML definition, and store the related data in HashMaps, so it can be queried during
 * the connection build easily
 */
public class ComponentPropertyProvider {

    private static final Map<ConnectableComponentType, Function<ProcessGroupSchema, List<? extends BaseSchemaWithId>>> PG_COMPONENT_CONNECTABLE_TYPE_WITH_ACCESSOR = Map.of(
        ConnectableComponentType.PROCESSOR, ProcessGroupSchema::getProcessors,
        ConnectableComponentType.INPUT_PORT, ProcessGroupSchema::getInputPortSchemas,
        ConnectableComponentType.OUTPUT_PORT, ProcessGroupSchema::getOutputPortSchemas,
        ConnectableComponentType.FUNNEL, ProcessGroupSchema::getFunnels
    );
    private static final Map<ConnectableComponentType, Function<RemoteProcessGroupSchema, List<? extends BaseSchemaWithId>>> RPG_COMPONENT_CONNECTABLE_TYPE_WITH_ACCESSOR = Map.of(
        ConnectableComponentType.REMOTE_INPUT_PORT, RemoteProcessGroupSchema::getInputPorts,
        ConnectableComponentType.REMOTE_OUTPUT_PORT, RemoteProcessGroupSchema::getOutputPorts
    );

    private final Map<String, String> componentIdToParentIdMap;
    private final Map<String, ConnectableComponentType> componentTypeMap;

    public ComponentPropertyProvider(ConfigSchema configSchema) {
        this.componentIdToParentIdMap = createComponentIdToParentIdMap(configSchema);
        this.componentTypeMap = createComponentTypeMap(configSchema);
    }

    public String parentId(String componentId) {
        return componentIdToParentIdMap.get(componentId);
    }

    public ConnectableComponentType connectableComponentType(String componentId) {
        return componentTypeMap.get(componentId);
    }

    private Map<String, String> createComponentIdToParentIdMap(ConfigSchema configSchema) {
        return concat(
            fetchControllerServicesProperties(configSchema.getProcessGroupSchema()),
            concat(
                fetchProcessGroupComponentProperties(configSchema.getProcessGroupSchema()),
                fetchRemoteProcessGroupComponentsProperties(configSchema.getProcessGroupSchema()))
                .map(triple -> Pair.of(triple.getLeft(), triple.getMiddle())))
            .collect(toMap(Pair::getLeft, Pair::getRight));
    }

    private Map<String, ConnectableComponentType> createComponentTypeMap(ConfigSchema configSchema) {
        return concat(
            fetchProcessGroupComponentProperties(configSchema.getProcessGroupSchema()),
            fetchRemoteProcessGroupComponentsProperties(configSchema.getProcessGroupSchema()))
            .filter(triple -> nonNull(triple.getRight()))
            .collect(toMap(Triple::getLeft, Triple::getRight));
    }

    private Stream<Triple<String, String, ConnectableComponentType>> fetchProcessGroupComponentProperties(ProcessGroupSchema processGroupSchema) {
        return
            concat(
                concat(
                    // adding child process groups to component property map
                    ofNullable(processGroupSchema.getProcessGroupSchemas()).orElse(List.of())
                        .stream()
                        .map(ProcessGroupSchema::getId)
                        .map(childProcessGroupId -> Triple.of(childProcessGroupId, processGroupSchema.getId(), null)),
                    // adding components in process group to component property map
                    PG_COMPONENT_CONNECTABLE_TYPE_WITH_ACCESSOR.entrySet()
                        .stream()
                        .flatMap(schemaAccessorAndComponentType ->
                            getProcessGroupComponentsProperties(processGroupSchema, schemaAccessorAndComponentType.getValue(), schemaAccessorAndComponentType.getKey()))
                ),
                // recursively processing child process groups
                ofNullable(processGroupSchema.getProcessGroupSchemas()).orElse(List.of())
                    .stream()
                    .flatMap(this::fetchProcessGroupComponentProperties)
            );
    }

    private Stream<Triple<String, String, ConnectableComponentType>> getProcessGroupComponentsProperties(ProcessGroupSchema processGroupSchema,
                                                                                                         Function<ProcessGroupSchema, List<? extends BaseSchemaWithId>> schemaAccessor,
                                                                                                         ConnectableComponentType componentType) {
        return ofNullable(schemaAccessor.apply(processGroupSchema)).orElse(List.of())
            .stream()
            .map(component -> Triple.of(component.getId(), processGroupSchema.getId(), componentType));
    }

    private Stream<Triple<String, String, ConnectableComponentType>> fetchRemoteProcessGroupComponentsProperties(ProcessGroupSchema processGroupSchema) {
        return concat(
            concat(
                // adding remote process groups to component property map
                ofNullable(processGroupSchema.getRemoteProcessGroups()).orElse(List.of())
                    .stream()
                    .map(RemoteProcessGroupSchema::getId)
                    .map(remoteProcessGroupId -> Triple.of(remoteProcessGroupId, processGroupSchema.getId(), null)),
                // adding components from all remote process groups in the process group to component property map
                ofNullable(processGroupSchema.getRemoteProcessGroups()).orElse(List.of())
                    .stream()
                    .flatMap(remoteProcessGroupSchema -> RPG_COMPONENT_CONNECTABLE_TYPE_WITH_ACCESSOR.entrySet()
                        .stream()
                        .flatMap(schemaAccessorAndComponentType -> getRemoteProcessGroupComponentsProperties(
                            remoteProcessGroupSchema, schemaAccessorAndComponentType.getValue(), schemaAccessorAndComponentType.getKey())))
            ),
            // recursively processing child process groups
            ofNullable(processGroupSchema.getProcessGroupSchemas()).orElse(List.of())
                .stream()
                .flatMap(this::fetchRemoteProcessGroupComponentsProperties)
        );
    }

    private Stream<Triple<String, String, ConnectableComponentType>> getRemoteProcessGroupComponentsProperties(RemoteProcessGroupSchema remoteProcessGroupSchema,
                                                                                                               Function<RemoteProcessGroupSchema, List<? extends BaseSchemaWithId>> schemaAccessor,
                                                                                                               ConnectableComponentType componentType) {
        return ofNullable(schemaAccessor.apply(remoteProcessGroupSchema)).orElse(List.of())
            .stream()
            .map(component -> Triple.of(component.getId(), remoteProcessGroupSchema.getId(), componentType));
    }

    private Stream<Pair<String, String>> fetchControllerServicesProperties(ProcessGroupSchema processGroupSchema) {
        return concat(
            // adding controller services id and parent id for this process group
            ofNullable(processGroupSchema.getControllerServices()).orElse(List.of())
                .stream()
                .map(controllerServiceSchema -> Pair.of(controllerServiceSchema.getId(), processGroupSchema.getId())),
            // recursively processing child process groups
            ofNullable(processGroupSchema.getProcessGroupSchemas()).orElse(List.of())
                .stream()
                .flatMap(this::fetchControllerServicesProperties)
        );
    }
}
