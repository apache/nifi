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
package org.apache.nifi.util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.PortStatus;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.controller.status.RemoteProcessGroupStatus;

/**
 * ComponentStatusReport is a util class that can be used to "flatten" a ProcessGroupStatus into a collection of Map's so that retrieval of a Status for a particular component is very efficient
 */
public class ComponentStatusReport {

    private final Map<String, ProcessGroupStatus> groupMap = new HashMap<>();
    private final Map<String, ProcessorStatus> processorMap = new HashMap<>();
    private final Map<String, ConnectionStatus> connectionMap = new HashMap<>();
    private final Map<String, RemoteProcessGroupStatus> remoteGroupMap = new HashMap<>();
    private final Map<String, PortStatus> inputPortMap = new HashMap<>();
    private final Map<String, PortStatus> outputPortMap = new HashMap<>();

    private ComponentStatusReport() {
    }

    public static ComponentStatusReport createEmpty() {
        return new ComponentStatusReport();
    }

    public static ComponentStatusReport fromProcessGroupStatus(final ProcessGroupStatus status) {
        return fromProcessGroupStatus(status, ComponentType.values());
    }

    public static ComponentStatusReport fromProcessGroupStatus(final ProcessGroupStatus status, final ComponentType... componentTypes) {
        final Set<ComponentType> componentTypeSet = new HashSet<>();
        for (final ComponentType type : componentTypes) {
            componentTypeSet.add(type);
        }

        final ComponentStatusReport report = new ComponentStatusReport();
        report.populate(status, componentTypeSet);
        return report;
    }

    private void populate(final ProcessGroupStatus status, final Set<ComponentType> componentTypes) {
        if (componentTypes.contains(ComponentType.PROCESS_GROUP)) {
            groupMap.put(status.getId(), status);
        }

        if (componentTypes.contains(ComponentType.PROCESSOR)) {
            for (final ProcessorStatus procStatus : status.getProcessorStatus()) {
                processorMap.put(procStatus.getId(), procStatus);
            }
        }

        if (componentTypes.contains(ComponentType.CONNECTION)) {
            for (final ConnectionStatus connStatus : status.getConnectionStatus()) {
                connectionMap.put(connStatus.getId(), connStatus);
            }
        }

        if (componentTypes.contains(ComponentType.REMOTE_PROCESS_GROUP)) {
            for (final RemoteProcessGroupStatus rpgStatus : status.getRemoteProcessGroupStatus()) {
                remoteGroupMap.put(rpgStatus.getId(), rpgStatus);
            }
        }

        if (componentTypes.contains(ComponentType.INPUT_PORT)) {
            for (final PortStatus portStatus : status.getInputPortStatus()) {
                inputPortMap.put(portStatus.getId(), portStatus);
            }
        }

        if (componentTypes.contains(ComponentType.OUTPUT_PORT)) {
            for (final PortStatus portStatus : status.getOutputPortStatus()) {
                outputPortMap.put(portStatus.getId(), portStatus);
            }
        }

        for (final ProcessGroupStatus childStatus : status.getProcessGroupStatus()) {
            populate(childStatus, componentTypes);
        }
    }

    public ProcessGroupStatus getProcessGroupStatus(final String groupId) {
        return groupMap.get(groupId);
    }

    public ProcessorStatus getProcessorStatus(final String processorId) {
        return processorMap.get(processorId);
    }

    public ConnectionStatus getConnectionStatus(final String connectionId) {
        return connectionMap.get(connectionId);
    }

    public RemoteProcessGroupStatus getRemoteProcessGroupStatus(final String remoteGroupId) {
        return remoteGroupMap.get(remoteGroupId);
    }

    public PortStatus getInputPortStatus(final String portId) {
        return inputPortMap.get(portId);
    }

    public PortStatus getOutputPortStatus(final String portId) {
        return outputPortMap.get(portId);
    }

    public static enum ComponentType {

        PROCESSOR,
        INPUT_PORT,
        OUTPUT_PORT,
        PROCESS_GROUP,
        CONNECTION,
        REMOTE_PROCESS_GROUP;
    }
}
