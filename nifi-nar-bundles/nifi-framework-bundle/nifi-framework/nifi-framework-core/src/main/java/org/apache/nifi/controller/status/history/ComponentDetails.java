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
package org.apache.nifi.controller.status.history;

import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.controller.status.RemoteProcessGroupStatus;

import java.util.HashMap;
import java.util.Map;

public class ComponentDetails {
    private final String componentId;
    private final String groupId;
    private final String componentName;
    private final String componentType;
    private final String sourceName;
    private final String destinationName;
    private final String targetUri;


    public ComponentDetails(final String id, final String groupId, final String componentName, final String componentType,
                            final String sourceName, final String destinationName, final String remoteUri) {
        this.componentId = id;
        this.groupId = groupId;
        this.componentName = componentName;
        this.componentType = componentType;
        this.sourceName = sourceName;
        this.destinationName = destinationName;
        this.targetUri = remoteUri;
    }

    public static ComponentDetails forProcessor(final ProcessorStatus status) {
        return forProcessor(status.getId(), status.getGroupId(), status.getName(), status.getType());
    }

    public static ComponentDetails forProcessor(final String id, final String groupId, final String processorName, final String processorType) {
        return new ComponentDetails(id, groupId, processorName, processorType, null, null, null);
    }

    public static ComponentDetails forConnection(final ConnectionStatus status) {
        return forConnection(status.getId(), status.getGroupId(), status.getName(), status.getSourceName(), status.getDestinationName());
    }

    public static ComponentDetails forConnection(final String id, final String groupId, final String connectionName, final String sourceName, final String destinationName) {
        return new ComponentDetails(id, groupId, connectionName, sourceName, destinationName, null, null);
    }

    public static ComponentDetails forProcessGroup(final ProcessGroupStatus status) {
        return forProcessGroup(status.getId(), status.getName());
    }

    public static ComponentDetails forProcessGroup(final String id, final String groupName) {
        return new ComponentDetails(id,null, groupName, null, null, null, null);
    }

    public static ComponentDetails forRemoteProcessGroup(final RemoteProcessGroupStatus status) {
        return forRemoteProcessGroup(status.getId(), status.getGroupId(), status.getName(), status.getTargetUri());
    }

    public static ComponentDetails forRemoteProcessGroup(final String id, final String parentGroupId, final String rpgName, final String remoteUri) {
        return new ComponentDetails(id, parentGroupId, rpgName, null, null, null, remoteUri);
    }

    public String getComponentId() {
        return componentId;
    }

    public String getGroupId() {
        return groupId;
    }

    public String getComponentName() {
        return componentName;
    }

    public String getComponentType() {
        return componentType;
    }

    public String getSourceName() {
        return sourceName;
    }

    public String getDestinationName() {
        return destinationName;
    }

    public String getTargetUri() {
        return targetUri;
    }

    /**
     * Returns a {@Link Map} whose keys are those values defined by {@link ComponentStatusRepository#COMPONENT_DETAIL_GROUP_ID ComponentStatusRepository.COMPONENT_DETAIL_*}
     * and values are the values that are populated for this ComponentDetails object.
     */
    public Map<String, String> toMap() {
        final Map<String, String> map = new HashMap<>();
        map.put(ComponentStatusRepository.COMPONENT_DETAIL_ID, componentId);
        map.put(ComponentStatusRepository.COMPONENT_DETAIL_GROUP_ID, groupId);
        map.put(ComponentStatusRepository.COMPONENT_DETAIL_NAME, componentName);
        map.put(ComponentStatusRepository.COMPONENT_DETAIL_TYPE, componentType);
        map.put(ComponentStatusRepository.COMPONENT_DETAIL_SOURCE_NAME, sourceName);
        map.put(ComponentStatusRepository.COMPONENT_DETAIL_DESTINATION_NAME, destinationName);
        map.put(ComponentStatusRepository.COMPONENT_DETAIL_URI, targetUri);
        return map;
    }
}
