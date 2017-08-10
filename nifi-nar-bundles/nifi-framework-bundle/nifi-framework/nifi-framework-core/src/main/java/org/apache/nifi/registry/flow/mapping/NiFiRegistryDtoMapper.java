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

package org.apache.nifi.registry.flow.mapping;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.nifi.registry.flow.BatchSize;
import org.apache.nifi.registry.flow.Bundle;
import org.apache.nifi.registry.flow.ComponentType;
import org.apache.nifi.registry.flow.ConnectableComponent;
import org.apache.nifi.registry.flow.ConnectableComponentType;
import org.apache.nifi.registry.flow.ControllerServiceAPI;
import org.apache.nifi.registry.flow.PortType;
import org.apache.nifi.registry.flow.Position;
import org.apache.nifi.registry.flow.VersionedConnection;
import org.apache.nifi.registry.flow.VersionedControllerService;
import org.apache.nifi.registry.flow.VersionedFunnel;
import org.apache.nifi.registry.flow.VersionedLabel;
import org.apache.nifi.registry.flow.VersionedPort;
import org.apache.nifi.registry.flow.VersionedProcessGroup;
import org.apache.nifi.registry.flow.VersionedProcessor;
import org.apache.nifi.registry.flow.VersionedRemoteGroupPort;
import org.apache.nifi.registry.flow.VersionedRemoteProcessGroup;
import org.apache.nifi.web.api.dto.BatchSettingsDTO;
import org.apache.nifi.web.api.dto.BundleDTO;
import org.apache.nifi.web.api.dto.ConnectableDTO;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.ControllerServiceApiDTO;
import org.apache.nifi.web.api.dto.ControllerServiceDTO;
import org.apache.nifi.web.api.dto.FlowSnippetDTO;
import org.apache.nifi.web.api.dto.FunnelDTO;
import org.apache.nifi.web.api.dto.LabelDTO;
import org.apache.nifi.web.api.dto.PortDTO;
import org.apache.nifi.web.api.dto.PositionDTO;
import org.apache.nifi.web.api.dto.ProcessGroupDTO;
import org.apache.nifi.web.api.dto.ProcessorConfigDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupDTO;
import org.apache.nifi.web.api.dto.RemoteProcessGroupPortDTO;


public class NiFiRegistryDtoMapper {
    // We need to keep a mapping of component id to versionedComponentId as we transform these objects. This way, when
    // we call #mapConnectable, instead of generating a new UUID for the ConnectableComponent, we can lookup the 'versioned'
    // identifier based on the comopnent's actual id. We do connections last, so that all components will already have been
    // created before attempting to create the connection, where the ConnectableDTO is converted.
    private Map<String, String> versionedComponentIds = new HashMap<>();

    public VersionedProcessGroup mapProcessGroup(final ProcessGroupDTO dto) {
        versionedComponentIds.clear();
        return mapGroup(dto);
    }

    private VersionedProcessGroup mapGroup(final ProcessGroupDTO dto) {
        final VersionedProcessGroup versionedGroup = new VersionedProcessGroup();
        versionedGroup.setIdentifier(getId(dto.getVersionedComponentId(), dto.getId()));
        versionedGroup.setGroupIdentifier(getGroupId(dto.getParentGroupId()));
        versionedGroup.setName(dto.getName());
        versionedGroup.setComments(dto.getComments());
        versionedGroup.setPosition(mapPosition(dto.getPosition()));

        final FlowSnippetDTO contents = dto.getContents();

        versionedGroup.setControllerServices(contents.getControllerServices().stream()
            .map(this::mapControllerService)
            .collect(Collectors.toCollection(LinkedHashSet::new)));

        versionedGroup.setFunnels(contents.getFunnels().stream()
            .map(this::mapFunnel)
            .collect(Collectors.toCollection(LinkedHashSet::new)));

        versionedGroup.setInputPorts(contents.getInputPorts().stream()
            .map(this::mapPort)
            .collect(Collectors.toCollection(LinkedHashSet::new)));

        versionedGroup.setOutputPorts(contents.getOutputPorts().stream()
            .map(this::mapPort)
            .collect(Collectors.toCollection(LinkedHashSet::new)));

        versionedGroup.setLabels(contents.getLabels().stream()
            .map(this::mapLabel)
            .collect(Collectors.toCollection(LinkedHashSet::new)));

        versionedGroup.setProcessors(contents.getProcessors().stream()
            .map(this::mapProcessor)
            .collect(Collectors.toCollection(LinkedHashSet::new)));

        versionedGroup.setRemoteProcessGroups(contents.getRemoteProcessGroups().stream()
            .map(this::mapRemoteProcessGroup)
            .collect(Collectors.toCollection(LinkedHashSet::new)));

        versionedGroup.setProcessGroups(contents.getProcessGroups().stream()
            .map(this::mapGroup)
            .collect(Collectors.toCollection(LinkedHashSet::new)));

        versionedGroup.setConnections(contents.getConnections().stream()
            .map(this::mapConnection)
            .collect(Collectors.toCollection(LinkedHashSet::new)));

        return versionedGroup;
    }

    private String getId(final String currentVersionedId, final String componentId) {
        final String versionedId;
        if (currentVersionedId == null) {
            versionedId = UUID.nameUUIDFromBytes(componentId.getBytes(StandardCharsets.UTF_8)).toString();
        } else {
            versionedId = currentVersionedId;
        }

        versionedComponentIds.put(componentId, versionedId);
        return versionedId;
    }

    private String getGroupId(final String groupId) {
        return versionedComponentIds.get(groupId);
    }

    public VersionedConnection mapConnection(final ConnectionDTO dto) {
        final VersionedConnection connection = new VersionedConnection();
        connection.setIdentifier(getId(dto.getVersionedComponentId(), dto.getId()));
        connection.setGroupIdentifier(getGroupId(dto.getParentGroupId()));
        connection.setName(dto.getName());
        connection.setBackPressureDataSizeThreshold(dto.getBackPressureDataSizeThreshold());
        connection.setBackPressureObjectThreshold(dto.getBackPressureObjectThreshold());
        connection.setFlowFileExpiration(dto.getFlowFileExpiration());
        connection.setLabelIndex(dto.getLabelIndex());
        connection.setPosition(mapPosition(dto.getPosition()));
        connection.setPrioritizers(dto.getPrioritizers());
        connection.setSelectedRelationships(dto.getSelectedRelationships());
        connection.setzIndex(dto.getzIndex());

        connection.setBends(dto.getBends().stream()
            .map(this::mapPosition)
            .collect(Collectors.toList()));

        connection.setSource(mapConnectable(dto.getSource()));
        connection.setDestination(mapConnectable(dto.getDestination()));

        return connection;
    }

    public ConnectableComponent mapConnectable(final ConnectableDTO dto) {
        final ConnectableComponent component = new ConnectableComponent();

        final String versionedId = dto.getVersionedComponentId();
        if (versionedId == null) {
            final String resolved = versionedComponentIds.get(dto.getId());
            if (resolved == null) {
                throw new IllegalArgumentException("Unable to map Connectable Component with identifier " + dto.getId() + " to any version-controlled component");
            }

            component.setId(resolved);
        } else {
            component.setId(versionedId);
        }

        component.setComments(dto.getComments());
        component.setGroupId(dto.getGroupId());
        component.setName(dto.getName());
        component.setType(ConnectableComponentType.valueOf(dto.getType()));
        return component;
    }

    public VersionedControllerService mapControllerService(final ControllerServiceDTO dto) {
        final VersionedControllerService service = new VersionedControllerService();
        service.setIdentifier(getId(dto.getVersionedComponentId(), dto.getId()));
        service.setGroupIdentifier(getGroupId(dto.getParentGroupId()));
        service.setName(dto.getName());
        service.setAnnotationData(dto.getAnnotationData());
        service.setBundle(mapBundle(dto.getBundle()));
        service.setComments(dto.getComments());
        service.setControllerServiceApis(dto.getControllerServiceApis().stream()
            .map(this::mapControllerServiceApi)
            .collect(Collectors.toList()));
        service.setProperties(dto.getProperties());
        service.setType(dto.getType());
        return null;
    }

    private Bundle mapBundle(final BundleDTO dto) {
        final Bundle bundle = new Bundle();
        bundle.setGroup(dto.getGroup());
        bundle.setArtifact(dto.getArtifact());
        bundle.setVersion(dto.getVersion());
        return bundle;
    }

    private ControllerServiceAPI mapControllerServiceApi(final ControllerServiceApiDTO dto) {
        final ControllerServiceAPI api = new ControllerServiceAPI();
        api.setBundle(mapBundle(dto.getBundle()));
        api.setType(dto.getType());
        return api;
    }

    public VersionedFunnel mapFunnel(final FunnelDTO dto) {
        final VersionedFunnel funnel = new VersionedFunnel();
        funnel.setIdentifier(getId(dto.getVersionedComponentId(), dto.getId()));
        funnel.setGroupIdentifier(getGroupId(dto.getParentGroupId()));
        funnel.setPosition(mapPosition(dto.getPosition()));
        return funnel;
    }

    public VersionedLabel mapLabel(final LabelDTO dto) {
        final VersionedLabel label = new VersionedLabel();
        label.setIdentifier(getId(dto.getVersionedComponentId(), dto.getId()));
        label.setGroupIdentifier(getGroupId(dto.getParentGroupId()));
        label.setHeight(dto.getHeight());
        label.setWidth(dto.getWidth());
        label.setLabel(dto.getLabel());
        label.setPosition(mapPosition(dto.getPosition()));
        label.setStyle(dto.getStyle());
        return label;
    }

    public VersionedPort mapPort(final PortDTO dto) {
        final VersionedPort port = new VersionedPort();
        port.setIdentifier(getId(dto.getVersionedComponentId(), dto.getId()));
        port.setGroupIdentifier(getGroupId(dto.getParentGroupId()));
        port.setComments(dto.getComments());
        port.setConcurrentlySchedulableTaskCount(dto.getConcurrentlySchedulableTaskCount());
        port.setName(dto.getName());
        port.setPosition(mapPosition(dto.getPosition()));
        port.setType(PortType.valueOf(dto.getType()));
        return port;
    }

    public Position mapPosition(final PositionDTO dto) {
        final Position position = new Position();
        position.setX(dto.getX());
        position.setY(dto.getY());
        return position;
    }

    public VersionedProcessor mapProcessor(final ProcessorDTO dto) {
        final ProcessorConfigDTO config = dto.getConfig();

        final VersionedProcessor processor = new VersionedProcessor();
        processor.setIdentifier(getId(dto.getVersionedComponentId(), dto.getId()));
        processor.setGroupIdentifier(getGroupId(dto.getParentGroupId()));
        processor.setType(dto.getType());
        processor.setAnnotationData(config.getAnnotationData());
        processor.setAutoTerminatedRelationships(config.getAutoTerminatedRelationships());
        processor.setBulletinLevel(config.getBulletinLevel());
        processor.setBundle(mapBundle(dto.getBundle()));
        processor.setComments(config.getComments());
        processor.setConcurrentlySchedulableTaskCount(config.getConcurrentlySchedulableTaskCount());
        processor.setExecutionNode(config.getExecutionNode());
        processor.setName(dto.getName());
        processor.setPenaltyDuration(config.getPenaltyDuration());
        processor.setPosition(mapPosition(dto.getPosition()));
        processor.setProperties(config.getProperties());
        processor.setRunDurationMillis(config.getRunDurationMillis());
        processor.setSchedulingPeriod(config.getSchedulingPeriod());
        processor.setSchedulingStrategy(config.getSchedulingStrategy());
        processor.setStyle(dto.getStyle());
        processor.setYieldDuration(config.getYieldDuration());
        return processor;
    }

    public VersionedRemoteProcessGroup mapRemoteProcessGroup(final RemoteProcessGroupDTO dto) {
        final VersionedRemoteProcessGroup rpg = new VersionedRemoteProcessGroup();
        rpg.setIdentifier(getId(dto.getVersionedComponentId(), dto.getId()));
        rpg.setGroupIdentifier(getGroupId(dto.getParentGroupId()));
        rpg.setComments(dto.getComments());
        rpg.setCommunicationsTimeout(dto.getCommunicationsTimeout());
        rpg.setLocalNetworkInterface(dto.getLocalNetworkInterface());
        rpg.setName(dto.getName());
        rpg.setInputPorts(dto.getContents().getInputPorts().stream()
            .map(port -> mapRemotePort(port, ComponentType.REMOTE_INPUT_PORT))
            .collect(Collectors.toSet()));
        rpg.setOutputPorts(dto.getContents().getOutputPorts().stream()
            .map(port -> mapRemotePort(port, ComponentType.REMOTE_OUTPUT_PORT))
            .collect(Collectors.toSet()));
        rpg.setPosition(mapPosition(dto.getPosition()));
        rpg.setProxyHost(dto.getProxyHost());
        rpg.setProxyPort(dto.getProxyPort());
        rpg.setProxyUser(dto.getProxyUser());
        rpg.setTargetUri(dto.getTargetUri());
        rpg.setTargetUris(dto.getTargetUris());
        rpg.setTransportProtocol(dto.getTransportProtocol());
        rpg.setYieldDuration(dto.getYieldDuration());
        return rpg;
    }

    public VersionedRemoteGroupPort mapRemotePort(final RemoteProcessGroupPortDTO dto, final ComponentType componentType) {
        final VersionedRemoteGroupPort port = new VersionedRemoteGroupPort();
        port.setIdentifier(getId(dto.getVersionedComponentId(), dto.getId()));
        port.setGroupIdentifier(getGroupId(dto.getGroupId()));
        port.setComments(dto.getComments());
        port.setConcurrentlySchedulableTaskCount(dto.getConcurrentlySchedulableTaskCount());
        port.setGroupId(dto.getGroupId());
        port.setName(dto.getName());
        port.setUseCompression(dto.getUseCompression());
        port.setBatchSettings(mapBatchSettings(dto.getBatchSettings()));
        port.setComponentType(componentType);
        return port;
    }

    private BatchSize mapBatchSettings(final BatchSettingsDTO dto) {
        final BatchSize batchSize = new BatchSize();
        batchSize.setCount(dto.getCount());
        batchSize.setDuration(dto.getDuration());
        batchSize.setSize(dto.getSize());
        return batchSize;
    }
}
