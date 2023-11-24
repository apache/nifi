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

import static java.lang.Boolean.TRUE;
import static java.util.Map.entry;
import static java.util.Optional.ofNullable;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.apache.nifi.minifi.commons.api.MiNiFiProperties.NIFI_MINIFI_FLOW_MAX_CONCURRENT_THREADS;
import static org.apache.nifi.util.NiFiProperties.ADMINISTRATIVE_YIELD_DURATION;
import static org.apache.nifi.util.NiFiProperties.BORED_YIELD_DURATION;
import static org.apache.nifi.util.NiFiProperties.COMPONENT_STATUS_SNAPSHOT_FREQUENCY;
import static org.apache.nifi.util.NiFiProperties.CONTENT_ARCHIVE_ENABLED;
import static org.apache.nifi.util.NiFiProperties.CONTENT_ARCHIVE_MAX_RETENTION_PERIOD;
import static org.apache.nifi.util.NiFiProperties.CONTENT_ARCHIVE_MAX_USAGE_PERCENTAGE;
import static org.apache.nifi.util.NiFiProperties.CONTENT_REPOSITORY_IMPLEMENTATION;
import static org.apache.nifi.util.NiFiProperties.FLOWFILE_REPOSITORY_ALWAYS_SYNC;
import static org.apache.nifi.util.NiFiProperties.FLOWFILE_REPOSITORY_CHECKPOINT_INTERVAL;
import static org.apache.nifi.util.NiFiProperties.FLOWFILE_REPOSITORY_IMPLEMENTATION;
import static org.apache.nifi.util.NiFiProperties.FLOW_CONTROLLER_GRACEFUL_SHUTDOWN_PERIOD;
import static org.apache.nifi.util.NiFiProperties.MAX_APPENDABLE_CLAIM_SIZE;
import static org.apache.nifi.util.NiFiProperties.PROVENANCE_INDEX_SHARD_SIZE;
import static org.apache.nifi.util.NiFiProperties.PROVENANCE_MAX_STORAGE_SIZE;
import static org.apache.nifi.util.NiFiProperties.PROVENANCE_MAX_STORAGE_TIME;
import static org.apache.nifi.util.NiFiProperties.PROVENANCE_REPO_IMPLEMENTATION_CLASS;
import static org.apache.nifi.util.NiFiProperties.PROVENANCE_ROLLOVER_TIME;
import static org.apache.nifi.util.NiFiProperties.QUEUE_SWAP_THRESHOLD;
import static org.apache.nifi.util.NiFiProperties.WRITE_DELAY_INTERVAL;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collector;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.controller.flow.VersionedDataflow;
import org.apache.nifi.controller.flow.VersionedFlowEncodingVersion;
import org.apache.nifi.flow.Bundle;
import org.apache.nifi.flow.ComponentType;
import org.apache.nifi.flow.ConnectableComponent;
import org.apache.nifi.flow.PortType;
import org.apache.nifi.flow.Position;
import org.apache.nifi.flow.ScheduledState;
import org.apache.nifi.flow.VersionedComponent;
import org.apache.nifi.flow.VersionedConnection;
import org.apache.nifi.flow.VersionedControllerService;
import org.apache.nifi.flow.VersionedFunnel;
import org.apache.nifi.flow.VersionedPort;
import org.apache.nifi.flow.VersionedProcessGroup;
import org.apache.nifi.flow.VersionedProcessor;
import org.apache.nifi.flow.VersionedRemoteGroupPort;
import org.apache.nifi.flow.VersionedRemoteProcessGroup;
import org.apache.nifi.flow.VersionedReportingTask;
import org.apache.nifi.logging.LogLevel;
import org.apache.nifi.minifi.toolkit.schema.ComponentStatusRepositorySchema;
import org.apache.nifi.minifi.toolkit.schema.ConfigSchema;
import org.apache.nifi.minifi.toolkit.schema.ConnectionSchema;
import org.apache.nifi.minifi.toolkit.schema.ContentRepositorySchema;
import org.apache.nifi.minifi.toolkit.schema.ControllerServiceSchema;
import org.apache.nifi.minifi.toolkit.schema.CorePropertiesSchema;
import org.apache.nifi.minifi.toolkit.schema.FlowFileRepositorySchema;
import org.apache.nifi.minifi.toolkit.schema.FunnelSchema;
import org.apache.nifi.minifi.toolkit.schema.PortSchema;
import org.apache.nifi.minifi.toolkit.schema.ProcessGroupSchema;
import org.apache.nifi.minifi.toolkit.schema.ProcessorSchema;
import org.apache.nifi.minifi.toolkit.schema.ProvenanceRepositorySchema;
import org.apache.nifi.minifi.toolkit.schema.RemotePortSchema;
import org.apache.nifi.minifi.toolkit.schema.RemoteProcessGroupSchema;
import org.apache.nifi.minifi.toolkit.schema.ReportingSchema;
import org.apache.nifi.minifi.toolkit.schema.SwapSchema;
import org.apache.nifi.minifi.toolkit.schema.common.BaseSchemaWithIdAndName;
import org.apache.nifi.scheduling.ExecutionNode;

public class ConfigSchemaToVersionedDataFlowTransformer {

    private static final String DEFAULT_FLOW_FILE_EXPIRATION = "0 sec";
    private static final String DEFAULT_BACK_PRESSURE_DATA_SIZE_THRESHOLD = "1 GB";
    private static final String FLOW_FILE_CONCURRENCY = "UNBOUNDED";
    private static final String FLOW_FILE_OUTBOUND_POLICY = "STREAM_WHEN_AVAILABLE";
    private static final long DEFAULT_BACK_PRESSURE_OBJECT_THRESHOLD = 10000L;
    private static final Position DEFAULT_POSITION = new Position(0, 0);

    private final ConfigSchema configSchema;
    private final ComponentPropertyProvider componentPropertyProvider;

    public ConfigSchemaToVersionedDataFlowTransformer(ConfigSchema configSchema) {
        this.configSchema = configSchema;
        this.componentPropertyProvider = new ComponentPropertyProvider(configSchema);
    }

    public Map<String, String> extractProperties() {
        CorePropertiesSchema coreProperties = configSchema.getCoreProperties();
        FlowFileRepositorySchema flowFileRepositoryProperties = configSchema.getFlowfileRepositoryProperties();
        ContentRepositorySchema contentRepositoryProperties = configSchema.getContentRepositoryProperties();
        ProvenanceRepositorySchema provenanceRepositoryProperties = configSchema.getProvenanceRepositorySchema();
        ComponentStatusRepositorySchema componentStatusRepositoryProperties = configSchema.getComponentStatusRepositoryProperties();
        SwapSchema swapProperties = configSchema.getFlowfileRepositoryProperties().getSwapProperties();

        return Stream.concat(
                Stream.of(
                    entry(NIFI_MINIFI_FLOW_MAX_CONCURRENT_THREADS.getKey(), coreProperties.getMaxConcurrentThreads().toString()),
                    entry(FLOW_CONTROLLER_GRACEFUL_SHUTDOWN_PERIOD, coreProperties.getFlowControllerGracefulShutdownPeriod()),
                    entry(WRITE_DELAY_INTERVAL, coreProperties.getFlowServiceWriteDelayInterval()),
                    entry(ADMINISTRATIVE_YIELD_DURATION, coreProperties.getAdministrativeYieldDuration()),
                    entry(BORED_YIELD_DURATION, coreProperties.getBoredYieldDuration()),
                    entry(FLOWFILE_REPOSITORY_IMPLEMENTATION, flowFileRepositoryProperties.getFlowFileRepository()),
                    entry(FLOWFILE_REPOSITORY_CHECKPOINT_INTERVAL, flowFileRepositoryProperties.getCheckpointInterval()),
                    entry(FLOWFILE_REPOSITORY_ALWAYS_SYNC, Boolean.toString(flowFileRepositoryProperties.getAlwaysSync())),
                    entry(CONTENT_REPOSITORY_IMPLEMENTATION, contentRepositoryProperties.getContentRepository()),
                    entry(MAX_APPENDABLE_CLAIM_SIZE, contentRepositoryProperties.getContentClaimMaxAppendableSize()),
                    entry(CONTENT_ARCHIVE_MAX_RETENTION_PERIOD, contentRepositoryProperties.getContentRepoArchiveMaxRetentionPeriod()),
                    entry(CONTENT_ARCHIVE_MAX_USAGE_PERCENTAGE, contentRepositoryProperties.getContentRepoArchiveMaxUsagePercentage()),
                    entry(CONTENT_ARCHIVE_ENABLED, Boolean.toString(contentRepositoryProperties.getContentRepoArchiveEnabled())),
                    entry(PROVENANCE_REPO_IMPLEMENTATION_CLASS, provenanceRepositoryProperties.getProvenanceRepository()),
                    entry(PROVENANCE_ROLLOVER_TIME, provenanceRepositoryProperties.getProvenanceRepoRolloverTimeKey()),
                    entry(PROVENANCE_INDEX_SHARD_SIZE, provenanceRepositoryProperties.getProvenanceRepoIndexShardSize()),
                    entry(PROVENANCE_MAX_STORAGE_SIZE, provenanceRepositoryProperties.getProvenanceRepoMaxStorageSize()),
                    entry(PROVENANCE_MAX_STORAGE_TIME, provenanceRepositoryProperties.getProvenanceRepoMaxStorageTime()),
                    entry(COMPONENT_STATUS_SNAPSHOT_FREQUENCY, componentStatusRepositoryProperties.getSnapshotFrequency()),
                    entry(QUEUE_SWAP_THRESHOLD, swapProperties.getThreshold().toString())
                ),
                ofNullable(configSchema.getNifiPropertiesOverrides()).map(Map::entrySet).orElse(Set.of()).stream()
            )
            .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public VersionedDataflow convert() {
        VersionedDataflow versionedDataflow = new VersionedDataflow();
        versionedDataflow.setEncodingVersion(new VersionedFlowEncodingVersion(2, 0));
        versionedDataflow.setMaxTimerDrivenThreadCount(configSchema.getCoreProperties().getMaxConcurrentThreads().intValue());

        versionedDataflow.setRegistries(List.of());
        versionedDataflow.setParameterContexts(List.of());
        versionedDataflow.setParameterProviders(List.of());
        versionedDataflow.setControllerServices(List.of());
        versionedDataflow.setReportingTasks(
            convertComponents(configSchema::getReportingTasksSchema, this::toVersionedReportingTask, toList()));

        VersionedProcessGroup versionedProcessGroup = new VersionedProcessGroup();
        versionedProcessGroup.setDefaultFlowFileExpiration(DEFAULT_FLOW_FILE_EXPIRATION);
        versionedProcessGroup.setDefaultBackPressureObjectThreshold(DEFAULT_BACK_PRESSURE_OBJECT_THRESHOLD);
        versionedProcessGroup.setDefaultBackPressureDataSizeThreshold(DEFAULT_BACK_PRESSURE_DATA_SIZE_THRESHOLD);
        versionedProcessGroup.setFlowFileConcurrency(FLOW_FILE_CONCURRENCY);
        versionedProcessGroup.setFlowFileOutboundPolicy(FLOW_FILE_OUTBOUND_POLICY);

        convertProcessGroup(configSchema.getProcessGroupSchema(), versionedProcessGroup);

        // we need to set the instance ids of the components in the end, as at the time of creating the connection the instance id is not available
        Map<String, String> idToInstanceIdMapOfConnectableComponents = getIdToInstanceIdMapOfConnectableComponents(versionedProcessGroup);
        setConnectableComponentsInstanceId(versionedProcessGroup, idToInstanceIdMapOfConnectableComponents);

        versionedDataflow.setRootGroup(versionedProcessGroup);

        return versionedDataflow;
    }

    private Map<String, String> getIdToInstanceIdMapOfConnectableComponents(VersionedProcessGroup versionedProcessGroup) {
        Map<String, String> thisProcessGroupIdToInstanceIdMaps = Stream.of(
                ofNullable(versionedProcessGroup.getProcessors()).orElse(Set.of()),
                ofNullable(versionedProcessGroup.getInputPorts()).orElse(Set.of()),
                ofNullable(versionedProcessGroup.getOutputPorts()).orElse(Set.of()),
                ofNullable(versionedProcessGroup.getFunnels()).orElse(Set.of()),
                ofNullable(versionedProcessGroup.getRemoteProcessGroups()).orElse(Set.of())
                    .stream()
                    .map(VersionedRemoteProcessGroup::getInputPorts)
                    .flatMap(Set::stream)
                    .collect(toSet()),
                ofNullable(versionedProcessGroup.getRemoteProcessGroups()).orElse(Set.of())
                    .stream()
                    .map(VersionedRemoteProcessGroup::getOutputPorts)
                    .flatMap(Set::stream)
                    .collect(toSet())
            )
            .flatMap(Set::stream)
            .collect(toMap(VersionedComponent::getIdentifier, VersionedComponent::getInstanceIdentifier));

        Stream<Map<String, String>> childProcessGroupsIdToInstanceIdMaps = ofNullable(versionedProcessGroup.getProcessGroups()).orElse(Set.of())
            .stream()
            .map(this::getIdToInstanceIdMapOfConnectableComponents);

        return Stream.concat(
                Stream.of(thisProcessGroupIdToInstanceIdMaps),
                childProcessGroupsIdToInstanceIdMaps)
            .map(Map::entrySet)
            .flatMap(Set::stream)
            .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private void setConnectableComponentsInstanceId(VersionedProcessGroup versionedProcessGroup, Map<String, String> idToInstanceIdMapOfConnectableComponents) {
        ofNullable(versionedProcessGroup.getConnections()).orElse(Set.of())
            .forEach(connection -> {
                ConnectableComponent source = connection.getSource();
                source.setInstanceIdentifier(idToInstanceIdMapOfConnectableComponents.get(source.getId()));
                ConnectableComponent destination = connection.getDestination();
                System.err.println(destination.getType() + " - " + destination.getId() + " - " + idToInstanceIdMapOfConnectableComponents.get(destination.getId()));
                destination.setInstanceIdentifier(idToInstanceIdMapOfConnectableComponents.get(destination.getId()));
            });
        ofNullable(versionedProcessGroup.getProcessGroups()).orElse(Set.of())
            .forEach(childProcessGroup -> setConnectableComponentsInstanceId(childProcessGroup, idToInstanceIdMapOfConnectableComponents));
    }

    private void convertProcessGroup(ProcessGroupSchema processGroupSchema, VersionedProcessGroup processGroup) {
        processGroup.setIdentifier(processGroupSchema.getId());
        processGroup.setInstanceIdentifier(processGroupSchema.getId());
        processGroup.setName(getNameOrId(processGroupSchema));
        processGroup.setComments(processGroupSchema.getComment());
        processGroup.setPosition(DEFAULT_POSITION);
        processGroup.setProcessors(
            convertComponents(processGroupSchema::getProcessors, this::toVersionedProcessor, toSet()));
        processGroup.setControllerServices(
            convertComponents(processGroupSchema::getControllerServices, this::toVersionedControllerService, toSet()));
        processGroup.setConnections(
            convertComponents(processGroupSchema::getConnections, this::toVersionedConnection, toSet()));
        processGroup.setFunnels(
            convertComponents(processGroupSchema::getFunnels, this::toVersionedFunnel, toSet()));
        processGroup.setRemoteProcessGroups(
            convertComponents(processGroupSchema::getRemoteProcessGroups, this::toRemoteProcessGroup, toSet()));
        processGroup.setInputPorts(
            convertComponents(processGroupSchema::getInputPortSchemas, this::toInputPort, toSet()));
        processGroup.setOutputPorts(
            convertComponents(processGroupSchema::getOutputPortSchemas, this::toOutputPort, toSet()));
        processGroup.setProcessGroups(
            convertComponents(processGroupSchema::getProcessGroupSchemas, this::toVersionedProcessGroup, toSet()));
    }

    private <T, U, V> V convertComponents(Supplier<List<U>> convertibles, Function<U, T> converter, Collector<T, ?, V> collector) {
        return ofNullable(convertibles.get()).orElse(List.of())
            .stream()
            .map(converter)
            .collect(collector);
    }

    private VersionedReportingTask toVersionedReportingTask(ReportingSchema reportingSchema) {
        VersionedReportingTask reportingTask = new VersionedReportingTask();
        reportingTask.setIdentifier(reportingSchema.getId());
        reportingTask.setInstanceIdentifier(randomUUID().toString());
        reportingTask.setName(getNameOrId(reportingSchema));
        reportingTask.setComments(reportingSchema.getComment());
        reportingTask.setType(reportingSchema.getReportingClass());
        reportingTask.setBundle(bundleFor(reportingSchema.getReportingClass()));
        reportingTask.setSchedulingStrategy(reportingSchema.getSchedulingStrategy());
        reportingTask.setSchedulingPeriod(reportingSchema.getSchedulingPeriod());
        reportingTask.setProperties(toStringStringProperties(reportingSchema.getProperties()));
        reportingTask.setComponentType(ComponentType.REPORTING_TASK);
        reportingTask.setScheduledState(ScheduledState.RUNNING);
        reportingTask.setPropertyDescriptors(Map.of());
        reportingTask.setPosition(DEFAULT_POSITION);
        return reportingTask;
    }

    private VersionedProcessor toVersionedProcessor(ProcessorSchema processorSchema) {
        VersionedProcessor processor = new VersionedProcessor();
        processor.setIdentifier(processorSchema.getId());
        processor.setInstanceIdentifier(randomUUID().toString());
        processor.setGroupIdentifier(componentPropertyProvider.parentId(processorSchema.getId()));
        processor.setName(getNameOrId(processorSchema));
        processor.setType(processorSchema.getProcessorClass());
        processor.setBundle(bundleFor(processorSchema.getProcessorClass()));
        processor.setConcurrentlySchedulableTaskCount(processorSchema.getMaxConcurrentTasks().intValue());
        processor.setSchedulingStrategy(processorSchema.getSchedulingStrategy());
        processor.setSchedulingPeriod(processorSchema.getSchedulingPeriod());
        processor.setPenaltyDuration(processorSchema.getPenalizationPeriod());
        processor.setYieldDuration(processorSchema.getYieldPeriod());
        processor.setRunDurationMillis(NANOSECONDS.toMicros(processorSchema.getRunDurationNanos().longValue()));
        processor.setAutoTerminatedRelationships(Set.copyOf(processorSchema.getAutoTerminatedRelationshipsList()));
        processor.setProperties(toStringStringProperties(processorSchema.getProperties()));
        processor.setAnnotationData(processorSchema.getAnnotationData());
        processor.setComponentType(ComponentType.PROCESSOR);
        processor.setScheduledState(ScheduledState.RUNNING);
        processor.setPropertyDescriptors(Map.of());
        processor.setBulletinLevel(LogLevel.WARN.name());
        processor.setPosition(DEFAULT_POSITION);
        processor.setExecutionNode(ExecutionNode.ALL.name());
        return processor;
    }

    private VersionedControllerService toVersionedControllerService(ControllerServiceSchema controllerServiceSchema) {
        VersionedControllerService controllerService = new VersionedControllerService();
        controllerService.setIdentifier(controllerServiceSchema.getId());
        controllerService.setInstanceIdentifier(randomUUID().toString());
        controllerService.setGroupIdentifier(componentPropertyProvider.parentId(controllerServiceSchema.getId()));
        controllerService.setName(getNameOrId(controllerServiceSchema));
        controllerService.setType(controllerServiceSchema.getServiceClass());
        controllerService.setBundle(bundleFor(controllerServiceSchema.getServiceClass()));
        controllerService.setProperties(toStringStringProperties(controllerServiceSchema.getProperties()));
        controllerService.setAnnotationData(controllerServiceSchema.getAnnotationData());
        controllerService.setComponentType(ComponentType.CONTROLLER_SERVICE);
        controllerService.setScheduledState(ScheduledState.RUNNING);
        controllerService.setPropertyDescriptors(Map.of());
        controllerService.setPosition(DEFAULT_POSITION);
        return controllerService;
    }

    private VersionedConnection toVersionedConnection(ConnectionSchema connectionSchema) {
        VersionedConnection connection = new VersionedConnection();
        connection.setIdentifier(connectionSchema.getId());
        connection.setInstanceIdentifier(randomUUID().toString());
        connection.setName(getNameOrId(connectionSchema));
        connection.setComponentType(ComponentType.CONNECTION);
        connection.setSource(connectableComponent(connectionSchema.getSourceId()));
        connection.setDestination(connectableComponent(connectionSchema.getDestinationId()));
        connection.setSelectedRelationships(Set.copyOf(connectionSchema.getSourceRelationshipNames()));
        connection.setBackPressureDataSizeThreshold(connectionSchema.getMaxWorkQueueDataSize());
        connection.setBackPressureObjectThreshold(connectionSchema.getMaxWorkQueueSize().longValue());
        connection.setFlowFileExpiration(connectionSchema.getFlowfileExpiration());
        connection.setPrioritizers(List.of(connectionSchema.getQueuePrioritizerClass()).stream().filter(StringUtils::isNotBlank).collect(toList()));
        connection.setPosition(DEFAULT_POSITION);
        connection.setLabelIndex(0);
        connection.setzIndex(0L);
        return connection;
    }

    private ConnectableComponent connectableComponent(String componentId) {
        ConnectableComponent component = new ConnectableComponent();
        component.setId(componentId);
        component.setGroupId(componentPropertyProvider.parentId(componentId));
        component.setType(componentPropertyProvider.connectableComponentType(componentId));
        return component;
    }

    private VersionedFunnel toVersionedFunnel(FunnelSchema funnelSchema) {
        VersionedFunnel funnel = new VersionedFunnel();
        funnel.setIdentifier(funnelSchema.getId());
        funnel.setInstanceIdentifier(randomUUID().toString());
        funnel.setGroupIdentifier(componentPropertyProvider.parentId(funnelSchema.getId()));
        funnel.setName(funnelSchema.getWrapperName());
        funnel.setComponentType(ComponentType.FUNNEL);
        funnel.setPosition(DEFAULT_POSITION);
        return funnel;
    }

    private VersionedRemoteProcessGroup toRemoteProcessGroup(RemoteProcessGroupSchema remoteProcessGroupSchema) {
        VersionedRemoteProcessGroup remoteProcessGroup = new VersionedRemoteProcessGroup();
        remoteProcessGroup.setIdentifier(remoteProcessGroupSchema.getId());
        remoteProcessGroup.setInstanceIdentifier(randomUUID().toString());
        remoteProcessGroup.setGroupIdentifier(componentPropertyProvider.parentId(remoteProcessGroupSchema.getId()));
        remoteProcessGroup.setName(getNameOrId(remoteProcessGroupSchema));
        remoteProcessGroup.setComponentType(ComponentType.REMOTE_PROCESS_GROUP);
        remoteProcessGroup.setTargetUris(remoteProcessGroupSchema.getUrls());
        remoteProcessGroup.setComments(remoteProcessGroupSchema.getComment());
        remoteProcessGroup.setCommunicationsTimeout(remoteProcessGroupSchema.getTimeout());
        remoteProcessGroup.setYieldDuration(remoteProcessGroupSchema.getYieldPeriod());
        remoteProcessGroup.setTransportProtocol(remoteProcessGroupSchema.getTransportProtocol());
        ofNullable(remoteProcessGroupSchema.getLocalNetworkInterface())
            .filter(StringUtils::isNotBlank)
            .ifPresent(remoteProcessGroup::setLocalNetworkInterface);
        ofNullable(remoteProcessGroupSchema.getProxyHost())
            .filter(StringUtils::isNotBlank)
            .ifPresent(remoteProcessGroup::setProxyHost);
        ofNullable(remoteProcessGroupSchema.getProxyPort())
            .filter(Objects::nonNull)
            .ifPresent(remoteProcessGroup::setProxyPort);
        ofNullable(remoteProcessGroupSchema.getProxyUser())
            .filter(StringUtils::isNotBlank)
            .ifPresent(remoteProcessGroup::setProxyUser);
        ofNullable(remoteProcessGroupSchema.getProxyPassword())
            .filter(StringUtils::isNotBlank)
            .ifPresent(remoteProcessGroup::setProxyPassword);
        remoteProcessGroup.setPosition(DEFAULT_POSITION);

        remoteProcessGroup.setInputPorts(
            convertComponents(remoteProcessGroupSchema::getInputPorts, this::toRemoteInputPort, toSet()));
        remoteProcessGroup.setOutputPorts(
            convertComponents(remoteProcessGroupSchema::getOutputPorts, this::toRemoteOutputPort, toSet()));

        return remoteProcessGroup;
    }

    private VersionedRemoteGroupPort toRemoteInputPort(RemotePortSchema portSchema) {
        return toVersionedRemoteGroupPort(portSchema, ComponentType.REMOTE_INPUT_PORT);
    }

    private VersionedRemoteGroupPort toRemoteOutputPort(RemotePortSchema portSchema) {
        return toVersionedRemoteGroupPort(portSchema, ComponentType.REMOTE_OUTPUT_PORT);
    }

    private VersionedRemoteGroupPort toVersionedRemoteGroupPort(RemotePortSchema portSchema, ComponentType portType) {
        VersionedRemoteGroupPort port = new VersionedRemoteGroupPort();
        port.setIdentifier(randomUUID().toString());
        port.setInstanceIdentifier(portSchema.getId());
        port.setTargetId(portSchema.getId());
        port.setGroupIdentifier(componentPropertyProvider.parentId(portSchema.getId()));
        port.setName(getNameOrId(portSchema));
        port.setComponentType(portType);
        port.setScheduledState(ScheduledState.RUNNING);
        port.setComments(portSchema.getComment());
        port.setConcurrentlySchedulableTaskCount(portSchema.getMax_concurrent_tasks().intValue());
        port.setUseCompression(portSchema.getUseCompression());
        port.setPosition(DEFAULT_POSITION);
        return port;
    }

    private VersionedPort toInputPort(PortSchema portSchema) {
        return toVersionedPort(portSchema, PortType.INPUT_PORT);
    }

    private VersionedPort toOutputPort(PortSchema portSchema) {
        return toVersionedPort(portSchema, PortType.OUTPUT_PORT);
    }

    private VersionedPort toVersionedPort(PortSchema portSchema, PortType portType) {
        VersionedPort port = new VersionedPort();
        port.setIdentifier(portSchema.getId());
        port.setInstanceIdentifier(randomUUID().toString());
        port.setGroupIdentifier(componentPropertyProvider.parentId(portSchema.getId()));
        port.setName(getNameOrId(portSchema));
        port.setComponentType(portType == PortType.INPUT_PORT ? ComponentType.INPUT_PORT : ComponentType.OUTPUT_PORT);
        port.setScheduledState(ScheduledState.RUNNING);
        port.setType(portType);
        port.setAllowRemoteAccess(TRUE);
        port.setPosition(DEFAULT_POSITION);
        return port;
    }

    private VersionedProcessGroup toVersionedProcessGroup(ProcessGroupSchema childProcessGroupSchema) {
        VersionedProcessGroup childProcessGroup = new VersionedProcessGroup();
        childProcessGroup.setGroupIdentifier(componentPropertyProvider.parentId(childProcessGroupSchema.getId()));

        convertProcessGroup(childProcessGroupSchema, childProcessGroup);

        return childProcessGroup;
    }

    private Map<String, String> toStringStringProperties(Map<String, Object> stringObjectProperties) {
        return stringObjectProperties.entrySet()
            .stream()
            .collect(toMap(Map.Entry::getKey, entry -> ofNullable(entry.getValue()).map(Object::toString).orElse(EMPTY)));
    }

    private Bundle bundleFor(String type) {
        Bundle bundle = new Bundle();
        bundle.setGroup(EMPTY);
        bundle.setArtifact(type);
        bundle.setVersion(EMPTY);
        return bundle;
    }

    private String getNameOrId(BaseSchemaWithIdAndName schema) {
        return ofNullable(schema.getName()).filter(StringUtils::isNotBlank).orElse(schema.getId());
    }
}