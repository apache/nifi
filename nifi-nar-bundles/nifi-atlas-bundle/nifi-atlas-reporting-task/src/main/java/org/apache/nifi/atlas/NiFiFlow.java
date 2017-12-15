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
package org.apache.nifi.atlas;

import org.apache.atlas.model.instance.AtlasEntity;
import org.apache.atlas.model.instance.AtlasObjectId;
import org.apache.nifi.controller.status.ConnectionStatus;
import org.apache.nifi.controller.status.PortStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.controller.status.RemoteProcessGroupStatus;
import org.apache.nifi.util.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.nifi.atlas.AtlasUtils.findIdByQualifiedName;
import static org.apache.nifi.atlas.AtlasUtils.isGuidAssigned;
import static org.apache.nifi.atlas.AtlasUtils.isUpdated;
import static org.apache.nifi.atlas.AtlasUtils.updateMetadata;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_CLUSTER_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_DESCRIPTION;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_INPUTS;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_NIFI_FLOW;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_OUTPUTS;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_URL;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_FLOW;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_FLOW_PATH;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_INPUT_PORT;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_OUTPUT_PORT;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_QUEUE;

public class NiFiFlow {

    private static final Logger logger = LoggerFactory.getLogger(NiFiFlow.class);

    private final String rootProcessGroupId;
    private String flowName;
    private String clusterName;
    private String url;
    private String atlasGuid;
    private AtlasEntity exEntity;
    private AtlasObjectId atlasObjectId;
    private String description;

    /**
     * Track whether this instance has metadata updated and should be updated in Atlas.
     */
    private AtomicBoolean metadataUpdated = new AtomicBoolean(false);
    private List<String> updateAudit = new ArrayList<>();
    private Set<String> updatedEntityGuids = new LinkedHashSet<>();
    private Set<String> stillExistingEntityGuids = new LinkedHashSet<>();
    private Set<String> traversedPathIds = new LinkedHashSet<>();
    private boolean urlUpdated = false;

    private final Map<String, NiFiFlowPath> flowPaths = new HashMap<>();
    private final Map<String, ProcessorStatus> processors = new HashMap<>();
    private final Map<String, RemoteProcessGroupStatus> remoteProcessGroups = new HashMap<>();
    private final Map<String, List<ConnectionStatus>> incomingConnections = new HashMap<>();
    private final Map<String, List<ConnectionStatus>> outGoingConnections = new HashMap<>();

    private final Map<AtlasObjectId, AtlasEntity> queues = new HashMap<>();
    // Any Ports.
    private final Map<String, PortStatus> inputPorts = new HashMap<>();
    private final Map<String, PortStatus> outputPorts = new HashMap<>();
    // Root Group Ports.
    private final Map<String, PortStatus> rootInputPorts = new HashMap<>();
    private final Map<String, PortStatus> rootOutputPorts = new HashMap<>();
    // Root Group Ports Entity.
    private final Map<AtlasObjectId, AtlasEntity> rootInputPortEntities = new HashMap<>();
    private final Map<AtlasObjectId, AtlasEntity> rootOutputPortEntities = new HashMap<>();


    public NiFiFlow(String rootProcessGroupId) {
        this.rootProcessGroupId = rootProcessGroupId;
    }

    public AtlasObjectId getAtlasObjectId() {
        return atlasObjectId;
    }

    public String getRootProcessGroupId() {
        return rootProcessGroupId;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        updateMetadata(metadataUpdated, updateAudit, ATTR_CLUSTER_NAME, this.clusterName, clusterName);
        this.clusterName = clusterName;
        atlasObjectId = createAtlasObjectId();
    }

    private AtlasObjectId createAtlasObjectId() {
        return new AtlasObjectId(atlasGuid, TYPE_NIFI_FLOW, Collections.singletonMap(ATTR_QUALIFIED_NAME, getQualifiedName()));
    }

    public AtlasEntity getExEntity() {
        return exEntity;
    }

    public void setExEntity(AtlasEntity exEntity) {
        this.exEntity = exEntity;
        this.setAtlasGuid(exEntity.getGuid());
    }

    public String getAtlasGuid() {
        return atlasGuid;
    }

    public void setAtlasGuid(String atlasGuid) {
        this.atlasGuid = atlasGuid;
        atlasObjectId = createAtlasObjectId();
    }

    public String getQualifiedName() {
        return toQualifiedName(rootProcessGroupId);
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        updateMetadata(metadataUpdated, updateAudit, ATTR_DESCRIPTION, this.description, description);
        this.description = description;
    }

    public void addConnection(ConnectionStatus c) {
        outGoingConnections.computeIfAbsent(c.getSourceId(), k -> new ArrayList<>()).add(c);
        incomingConnections.computeIfAbsent(c.getDestinationId(), k -> new ArrayList<>()).add(c);
    }

    public void addProcessor(ProcessorStatus p) {
        processors.put(p.getId(), p);
    }

    public Map<String, ProcessorStatus> getProcessors() {
        return processors;
    }

    public void addRemoteProcessGroup(RemoteProcessGroupStatus r) {
        remoteProcessGroups.put(r.getId(), r);
    }

    public void setFlowName(String flowName) {
        updateMetadata(metadataUpdated, updateAudit, ATTR_NAME, this.flowName, flowName);
        this.flowName = flowName;
    }

    public String getFlowName() {
        return flowName;
    }

    public void setUrl(String url) {
        updateMetadata(metadataUpdated, updateAudit, ATTR_URL, this.url, url);
        if (isUpdated(this.url, url)) {
            this.urlUpdated = true;
        }
        this.url = url;
    }

    public String getUrl() {
        return url;
    }

    public List<ConnectionStatus> getIncomingConnections(String componentId) {
        return incomingConnections.get(componentId);
    }

    public List<ConnectionStatus> getOutgoingConnections(String componentId) {
        return outGoingConnections.get(componentId);
    }

    public void addInputPort(PortStatus port) {
        inputPorts.put(port.getId(), port);
    }

    public Map<String, PortStatus> getInputPorts() {
        return inputPorts;
    }

    public void addOutputPort(PortStatus port) {
        outputPorts.put(port.getId(), port);
    }

    public Map<String, PortStatus> getOutputPorts() {
        return outputPorts;
    }

    public void addRootInputPort(PortStatus port) {
        rootInputPorts.put(port.getId(), port);
        createOrUpdateRootGroupPortEntity(true, toQualifiedName(port.getId()), port.getName());
    }

    public Map<String, PortStatus> getRootInputPorts() {
        return rootInputPorts;
    }

    public void addRootOutputPort(PortStatus port) {
        rootOutputPorts.put(port.getId(), port);
        createOrUpdateRootGroupPortEntity(false, toQualifiedName(port.getId()), port.getName());
    }

    public Map<String, PortStatus> getRootOutputPorts() {
        return rootOutputPorts;
    }

    public Map<AtlasObjectId, AtlasEntity> getRootInputPortEntities() {
        return rootInputPortEntities;
    }

    private AtlasEntity createOrUpdateRootGroupPortEntity(boolean isInput, String qualifiedName, String portName) {
        final Map<AtlasObjectId, AtlasEntity> ports = isInput ? rootInputPortEntities : rootOutputPortEntities;
        final Optional<AtlasObjectId> existingPortId = findIdByQualifiedName(ports.keySet(), qualifiedName);

        final String typeName = isInput ? TYPE_NIFI_INPUT_PORT : TYPE_NIFI_OUTPUT_PORT;

        if (existingPortId.isPresent()) {
            final AtlasEntity entity = ports.get(existingPortId.get());
            final String portGuid = entity.getGuid();
            stillExistingEntityGuids.add(portGuid);

            final Object currentName = entity.getAttribute(ATTR_NAME);
            if (isUpdated(currentName, portName)) {
                // Update port name and set updated flag.
                entity.setAttribute(ATTR_NAME, portName);
                updatedEntityGuids.add(portGuid);
                updateAudit.add(String.format("Name of %s %s changed from %s to %s", entity.getTypeName(), portGuid, currentName, portName));
            }
            return entity;
        } else {
            final AtlasEntity entity = new AtlasEntity(typeName);

            entity.setAttribute(ATTR_NIFI_FLOW, getAtlasObjectId());
            entity.setAttribute(ATTR_NAME, portName);
            entity.setAttribute(ATTR_QUALIFIED_NAME, qualifiedName);

            final AtlasObjectId portId = new AtlasObjectId(typeName, ATTR_QUALIFIED_NAME, qualifiedName);
            ports.put(portId, entity);
            return entity;
        }
    }

    public Map<AtlasObjectId, AtlasEntity> getRootOutputPortEntities() {
        return rootOutputPortEntities;
    }

    public Tuple<AtlasObjectId, AtlasEntity> getOrCreateQueue(String destinationComponentId) {
        final String qualifiedName = toQualifiedName(destinationComponentId);
        final Optional<AtlasObjectId> existingQueueId = findIdByQualifiedName(queues.keySet(), qualifiedName);

        if (existingQueueId.isPresent()) {
            final AtlasEntity entity = queues.get(existingQueueId.get());
            stillExistingEntityGuids.add(entity.getGuid());
            return new Tuple<>(existingQueueId.get(), entity);
        } else {
            final AtlasObjectId queueId = new AtlasObjectId(TYPE_NIFI_QUEUE, ATTR_QUALIFIED_NAME, qualifiedName);
            final AtlasEntity queue = new AtlasEntity(TYPE_NIFI_QUEUE);
            queue.setAttribute(ATTR_NIFI_FLOW, getAtlasObjectId());
            queue.setAttribute(ATTR_QUALIFIED_NAME, qualifiedName);
            queue.setAttribute(ATTR_NAME, "queue");
            queue.setAttribute(ATTR_DESCRIPTION, "Input queue for " + destinationComponentId);
            queues.put(queueId, queue);
            return new Tuple<>(queueId, queue);
        }
    }

    public Map<AtlasObjectId, AtlasEntity> getQueues() {
        return queues;
    }

    public Map<String, NiFiFlowPath> getFlowPaths() {
        return flowPaths;
    }

    /**
     * Find a flow_path that contains specified componentId.
     */
    public NiFiFlowPath findPath(String componentId) {
        for (NiFiFlowPath path: flowPaths.values()) {
            if (path.getProcessComponentIds().contains(componentId)){
                return path;
            }
        }
        return null;
    }

    /**
     * Determine if a component should be reported as NiFiFlowPath.
     */
    public boolean isProcessComponent(String componentId) {
        return isProcessor(componentId) || isRootInputPort(componentId) || isRootOutputPort(componentId);
    }

    public boolean isProcessor(String componentId) {
        return processors.containsKey(componentId);
    }

    public boolean isInputPort(String componentId) {
        return inputPorts.containsKey(componentId);
    }

    public boolean isOutputPort(String componentId) {
        return outputPorts.containsKey(componentId);
    }

    public boolean isRootInputPort(String componentId) {
        return rootInputPorts.containsKey(componentId);
    }

    public boolean isRootOutputPort(String componentId) {
        return rootOutputPorts.containsKey(componentId);
    }

    public String getProcessComponentName(String componentId) {
        return getProcessComponentName(componentId, () -> "unknown");
    }

    public String getProcessComponentName(String componentId, Supplier<String> unknown) {
        return isProcessor(componentId) ? getProcessors().get(componentId).getName()
                : isRootInputPort(componentId) ? getRootInputPorts().get(componentId).getName()
                : isRootOutputPort(componentId) ? getRootOutputPorts().get(componentId).getName() : unknown.get();
    }

    /**
     * Start tracking changes from current state.
     */
    public void startTrackingChanges() {
        this.metadataUpdated.set(false);
        this.updateAudit.clear();
        this.updatedEntityGuids.clear();
        this.stillExistingEntityGuids.clear();
        this.urlUpdated = false;
    }

    public boolean isMetadataUpdated() {
        return this.metadataUpdated.get();
    }

    public String toQualifiedName(String componentId) {
        return AtlasUtils.toQualifiedName(clusterName, componentId);
    }

    public enum EntityChangeType {
        AS_IS,
        CREATED,
        UPDATED,
        DELETED;

        public static boolean containsChange(Collection<EntityChangeType> types) {
            return types.contains(CREATED) || types.contains(UPDATED) || types.contains(DELETED);
        }
    }

    private EntityChangeType getEntityChangeType(String guid) {
        if (!isGuidAssigned(guid)) {
            return EntityChangeType.CREATED;
        } else if (updatedEntityGuids.contains(guid)) {
            return EntityChangeType.UPDATED;
        } else if (!stillExistingEntityGuids.contains(guid)) {
            return EntityChangeType.DELETED;
        }
        return EntityChangeType.AS_IS;
    }

    public Map<EntityChangeType, List<AtlasEntity>> getChangedDataSetEntities() {
        final Map<EntityChangeType, List<AtlasEntity>> changedEntities = Stream
                .of(rootInputPortEntities.values().stream(), rootOutputPortEntities.values().stream(), queues.values().stream())
                .flatMap(Function.identity())
                .collect(Collectors.groupingBy(entity -> getEntityChangeType(entity.getGuid())));
        updateAudit.add("CREATED DataSet entities=" + changedEntities.get(EntityChangeType.CREATED));
        updateAudit.add("UPDATED DataSet entities=" + changedEntities.get(EntityChangeType.UPDATED));
        updateAudit.add("DELETED DataSet entities=" + changedEntities.get(EntityChangeType.DELETED));
        return changedEntities;
    }

    public NiFiFlowPath getOrCreateFlowPath(String pathId) {
        traversedPathIds.add(pathId);
        return flowPaths.computeIfAbsent(pathId, k -> new NiFiFlowPath(pathId));
    }

    public boolean isTraversedPath(String pathId) {
        return traversedPathIds.contains(pathId);
    }

    private EntityChangeType getFlowPathChangeType(NiFiFlowPath path) {
        if (path.getExEntity() == null) {
            return EntityChangeType.CREATED;
        } else if (path.isMetadataUpdated() || urlUpdated) {
            return EntityChangeType.UPDATED;
        } else if (!traversedPathIds.contains(path.getId())) {
            return EntityChangeType.DELETED;
        }
        return EntityChangeType.AS_IS;
    }

    private EntityChangeType getFlowPathIOChangeType(AtlasObjectId id) {
        final String guid = id.getGuid();
        if (!isGuidAssigned(guid)) {
            return EntityChangeType.CREATED;
        } else {
            if (TYPE_NIFI_QUEUE.equals(id.getTypeName()) && queues.containsKey(id)) {
                // If an input/output is a queue, and it is owned by this NiFiFlow, then check if it's still needed. NiFiFlow knows active queues.
                if (stillExistingEntityGuids.contains(guid)) {
                    return EntityChangeType.AS_IS;
                } else {
                    return EntityChangeType.DELETED;
                }
            } else {
                // Otherwise, do not need to delete.
                return EntityChangeType.AS_IS;
            }
        }
    }

    private Tuple<EntityChangeType, AtlasEntity> toAtlasEntity(EntityChangeType changeType, final NiFiFlowPath path) {

        final AtlasEntity entity = EntityChangeType.CREATED.equals(changeType) ? new AtlasEntity() : new AtlasEntity(path.getExEntity());
        entity.setTypeName(TYPE_NIFI_FLOW_PATH);
        entity.setVersion(1L);
        entity.setAttribute(ATTR_NIFI_FLOW, getAtlasObjectId());

        final StringBuilder name = new StringBuilder();
        final StringBuilder description = new StringBuilder();
        path.getProcessComponentIds().forEach(pid -> {
            final String componentName = getProcessComponentName(pid);

            if (name.length() > 0) {
                name.append(", ");
                description.append(", ");
            }
            name.append(componentName);
            description.append(String.format("%s::%s", componentName, pid));
        });

        path.setName(name.toString());
        entity.setAttribute(ATTR_NAME, name.toString());
        entity.setAttribute(ATTR_DESCRIPTION, description.toString());

        // Use first processor's id as qualifiedName.
        entity.setAttribute(ATTR_QUALIFIED_NAME, toQualifiedName(path.getId()));

        entity.setAttribute(ATTR_URL, path.createDeepLinkURL(getUrl()));

        final boolean inputsChanged = setChangedIOIds(path, entity, true);
        final boolean outputsChanged = setChangedIOIds(path, entity, false);

        // Even iff there's no flow path metadata changed, if any IO is changed then the pass should be updated.
        EntityChangeType finalChangeType = EntityChangeType.AS_IS.equals(changeType)
                ? (path.isMetadataUpdated() || inputsChanged || outputsChanged ? EntityChangeType.UPDATED : EntityChangeType.AS_IS)
                : changeType;

        return new Tuple<>(finalChangeType, entity);
    }

    /**
     * Set input or output DataSet ids for a NiFiFlowPath.
     * The updated ids only containing active ids.
     * @return True if there is any changed IO reference (create, update, delete).
     */
    private boolean setChangedIOIds(NiFiFlowPath path, AtlasEntity pathEntity, boolean isInput) {
        Set<AtlasObjectId> ids = isInput ? path.getInputs() : path.getOutputs();
        String targetAttribute = isInput ? ATTR_INPUTS : ATTR_OUTPUTS;
        final Map<EntityChangeType, List<AtlasObjectId>> changedIOIds
                = ids.stream().collect(Collectors.groupingBy(this::getFlowPathIOChangeType));
        // Remove DELETED references.
        final Set<AtlasObjectId> remainingFlowPathIOIds = toRemainingFlowPathIOIds(changedIOIds);

        // If references are changed, update it.
        if (path.isDataSetReferenceChanged(remainingFlowPathIOIds, isInput)) {
            pathEntity.setAttribute(targetAttribute, remainingFlowPathIOIds);
            return true;
        }
        return false;
    }

    private Set<AtlasObjectId> toRemainingFlowPathIOIds(Map<EntityChangeType, List<AtlasObjectId>> ids) {
        return ids.entrySet().stream()
                .filter(entry -> !EntityChangeType.DELETED.equals(entry.getKey()))
                .flatMap(entry -> entry.getValue().stream())
                .collect(Collectors.toSet());
    }

    public Map<EntityChangeType, List<AtlasEntity>> getChangedFlowPathEntities() {
        // Convert NiFiFlowPath to AtlasEntity.
        final HashMap<EntityChangeType, List<AtlasEntity>> changedPaths = flowPaths.values().stream()
                .map(path -> {
                    final EntityChangeType changeType = getFlowPathChangeType(path);
                    switch (changeType) {
                        case CREATED:
                        case UPDATED:
                        case AS_IS:
                            return toAtlasEntity(changeType, path);
                        default:
                            return new Tuple<>(changeType, path.getExEntity());
                    }
                }).collect(Collectors.groupingBy(Tuple::getKey, HashMap::new, Collectors.mapping(Tuple::getValue, Collectors.toList())));

        updateAudit.add("CREATED NiFiFlowPath=" + changedPaths.get(EntityChangeType.CREATED));
        updateAudit.add("UPDATED NiFiFlowPath=" + changedPaths.get(EntityChangeType.UPDATED));
        updateAudit.add("DELETED NiFiFlowPath=" + changedPaths.get(EntityChangeType.DELETED));
        return changedPaths;
    }

    public List<String> getUpdateAudit() {
        return updateAudit;
    }
}
