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
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.atlas.processors.Egress;
import org.apache.nifi.atlas.processors.EgressProcessors;
import org.apache.nifi.atlas.processors.Ingress;
import org.apache.nifi.atlas.processors.IngressProcessors;
import org.apache.nifi.web.api.dto.ConnectableDTO;
import org.apache.nifi.web.api.dto.ConnectionDTO;
import org.apache.nifi.web.api.dto.ProcessorDTO;
import org.apache.nifi.web.api.dto.flow.FlowDTO;
import org.apache.nifi.web.api.entity.PortEntity;
import org.apache.nifi.web.api.entity.ProcessGroupEntity;
import org.apache.nifi.web.api.entity.ProcessGroupFlowEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.nifi.atlas.AtlasObjectIdUtils.validObjectId;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_DESCRIPTION;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_NAME;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_NIFI_FLOW;
import static org.apache.nifi.atlas.NiFiTypes.ATTR_QUALIFIED_NAME;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_DATA;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_INPUT_PORT;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_OUTPUT_PORT;
import static org.apache.nifi.atlas.NiFiTypes.TYPE_NIFI_QUEUE;

public class NiFiFlowAnalyzer {

    private static final Logger logger = LoggerFactory.getLogger(NiFiFlowAnalyzer.class);

    private final NiFiApiClient niFiApiClient;

    public NiFiFlowAnalyzer(NiFiApiClient niFiApiClient) {
        this.niFiApiClient = niFiApiClient;
    }

    public NiFiFlow analyzeProcessGroup(AtlasVariables atlasVariables) throws IOException {
        final ProcessGroupFlowEntity rootProcessGroupFlow = niFiApiClient.getProcessGroupFlow();
        if (!rootProcessGroupFlow.getPermissions().getCanRead()) {
            throw new IllegalStateException("Not allowed to read the root ProcessGroup.");
        }


        final String flowName = rootProcessGroupFlow.getProcessGroupFlow().getBreadcrumb().getBreadcrumb().getName();
        final String nifiUrlForAtlasMetadata = atlasVariables.getNifiUrl();
        final NiFiFlow nifiFlow = new NiFiFlow(flowName, rootProcessGroupFlow.getProcessGroupFlow().getId(),
                StringUtils.isBlank(nifiUrlForAtlasMetadata) ? niFiApiClient.getBaseUri() : nifiUrlForAtlasMetadata);

        final FlowDTO rootProcessGroup = rootProcessGroupFlow.getProcessGroupFlow().getFlow();

        final ProcessGroupEntity processGroupEntity = niFiApiClient.getProcessGroupEntity();
        if (processGroupEntity != null) {
            nifiFlow.setDescription(processGroupEntity.getComponent().getComments());
        }

        analyzeProcessGroup(rootProcessGroup, nifiFlow, atlasVariables);

        analyzeRootGroupPorts(nifiFlow, rootProcessGroup);

        return nifiFlow;
    }

    public void analyzeRootGroupPorts(NiFiFlow nifiFlow, FlowDTO rootProcessGroup) {
        BiConsumer<PortEntity, Boolean> portEntityCreator = (port, isInput) -> {
            final String typeName = isInput ? TYPE_NIFI_INPUT_PORT : TYPE_NIFI_OUTPUT_PORT;

            final AtlasEntity entity = new AtlasEntity(typeName);
            final String portName = port.getComponent().getName();

            entity.setAttribute(ATTR_NIFI_FLOW, nifiFlow.getId());
            entity.setAttribute(ATTR_NAME, portName);
            entity.setAttribute(ATTR_QUALIFIED_NAME, port.getId());
            entity.setAttribute(ATTR_DESCRIPTION, port.getComponent().getComments());

            final AtlasObjectId portId = new AtlasObjectId(typeName, ATTR_QUALIFIED_NAME, port.getId());
            final Map<AtlasObjectId, AtlasEntity> ports = isInput ? nifiFlow.getInputPorts() : nifiFlow.getOutputPorts();
            ports.put(portId, entity);
        };

        rootProcessGroup.getInputPorts().forEach(port -> portEntityCreator.accept(port, true));
        rootProcessGroup.getOutputPorts().forEach(port -> portEntityCreator.accept(port, false));
    }

    private List<AtlasObjectId> extractRootGroupPorts(NiFiFlow nifiFlow, boolean isExtractingInputPorts, List<ConnectionDTO> connections) {
        if (connections == null || connections.isEmpty()) {
            return Collections.emptyList();
        }

        final String targetType = isExtractingInputPorts ? "INPUT_PORT" : "OUTPUT_PORT";
        // The direction keyword becomes opposite for remote ports. A processor receives data from REMOTE_OUTPUT_PORT>
        final String targetRemoteType = isExtractingInputPorts ? "REMOTE_OUTPUT_PORT" : "REMOTE_INPUT_PORT";
        final Map<Boolean, List<ConnectableDTO>> connectedPortsAndOthers = connections.stream()
                .map(c -> isExtractingInputPorts ? c.getSource() : c.getDestination())
                .filter(otherEnd -> !"PROCESSOR".equals(otherEnd.getType()))
                .collect(Collectors.groupingBy(otherEnd -> {
                    final String type = otherEnd.getType();
                    return targetType.equals(type) || targetRemoteType.equals(type);
                }));

        final List<AtlasObjectId> rootPortIds = new ArrayList<>();
        final List<ConnectableDTO> connectedOthers = new ArrayList<>();

        connectedPortsAndOthers.entrySet().forEach(po -> {
            if (po.getKey()) {
                // Connected ports
                final Map<Boolean, List<ConnectableDTO>> rootPortAndOthers =  po.getValue().stream().collect(Collectors
                        // If it connects to remote input/output port, or it connects to one in the root process group.
                        .groupingBy(otherEnd -> targetRemoteType.equals(otherEnd.getType()) || otherEnd.getGroupId().equals(nifiFlow.getRootProcessGroupId())));

                rootPortAndOthers.entrySet().forEach(ro -> {
                    if (ro.getKey()) {
                        // Connected root group ports
                        ro.getValue().forEach(rootPort -> {
                            // If type matched with remote, then use opposite port type.
                            // If it is a remote port, Atlas entity should be created by the remote NiFi, only AtlasObjectId link is necessary here.
                            // If it is a local root group port, Atlas entity will be created at later code from this NiFi.
                            final boolean isRemote = targetRemoteType.equals(rootPort.getType());
                            final boolean toInputPort = isRemote ? !isExtractingInputPorts : isExtractingInputPorts;
                            final AtlasObjectId rootGroupPortId = new AtlasObjectId(toInputPort
                                    ? TYPE_NIFI_INPUT_PORT : TYPE_NIFI_OUTPUT_PORT,
                                    ATTR_QUALIFIED_NAME, rootPort.getId());
                            rootPortIds.add(rootGroupPortId);
                        });
                    } else {
                        // Connected ports but not in the root group
                        connectedOthers.addAll(ro.getValue());
                    }
                });

            } else {
                // Connected others
                connectedOthers.addAll(po.getValue());
            }
        });

        // If those are not root group ports, let's dig one level deeper.
        final List<ConnectionDTO> nextLevel = connectedOthers.stream()
                .map(c -> isExtractingInputPorts
                        ? nifiFlow.getIncomingRelationShips(c.getId())
                        : nifiFlow.getOutgoingRelationShips(c.getId()))
                .filter(Objects::nonNull)
                .flatMap(Collection::stream).collect(Collectors.toList());

        if (nextLevel == null || nextLevel.isEmpty()) {
            // There's no more next level. Return those are found so far.
            return rootPortIds;
        }

        // Those are found at one level deeper.
        final List<AtlasObjectId> found = extractRootGroupPorts(nifiFlow, isExtractingInputPorts, nextLevel);

        rootPortIds.addAll(found);

        return rootPortIds;
    }

    public void analyzeProcessGroup(final FlowDTO flow, final NiFiFlow nifiFlow, final AtlasVariables atlasVariables) throws IOException {

        flow.getConnections().stream().map(c -> c.getComponent()).forEach(c -> nifiFlow.addConnection(c));

        flow.getProcessors().forEach(p -> nifiFlow.addProcessor(p));

        flow.getRemoteProcessGroups().forEach(r -> nifiFlow.addRemoteProcessGroup(r));

        // Analyze each processor.
        for (Map.Entry<String, ProcessorDTO> entry : nifiFlow.getProcessors().entrySet()) {

            final ProcessorDTO processor = entry.getValue();
            final String pid = entry.getKey();

            final Map<String, String> properties = processor.getConfig().getProperties();

            final Ingress ingress = IngressProcessors.get(processor.getType());
            final Egress egress = EgressProcessors.get(processor.getType());

            final Consumer<AtlasObjectId> putInput = input -> nifiFlow.putInput(pid, input, ingress, properties);
            final Consumer<AtlasObjectId> putOutput = output -> nifiFlow.putOutput(pid, output, egress, properties);

            extractRootGroupPorts(nifiFlow, true, nifiFlow.getIncomingRelationShips(pid)).forEach(putInput);
            extractRootGroupPorts(nifiFlow, false, nifiFlow.getOutgoingRelationShips(pid)).forEach(putOutput);

            if (ingress != null) {
                final Set<AtlasObjectId> inputs = ingress.getInputs(properties, atlasVariables);
                if (inputs != null && inputs.size() > 0) {
                    inputs.stream().filter(validObjectId).forEach(putInput);
                }
            } else {
                // Even if it doesn't have ingress info registered, treat it as a unknown ingress if it doesn't have any incoming relationship.
                final List<ConnectionDTO> ins = nifiFlow.getIncomingRelationShips(pid);
                if (ins == null || ins.isEmpty()) {
                    final AtlasEntity createdData = new AtlasEntity(TYPE_NIFI_DATA);
                    createdData.setAttribute(ATTR_NIFI_FLOW, nifiFlow.getId());
                    createdData.setAttribute(ATTR_QUALIFIED_NAME, pid);
                    createdData.setAttribute(ATTR_NAME, processor.getName());
                    final AtlasObjectId createdDataId = new AtlasObjectId(TYPE_NIFI_DATA, ATTR_QUALIFIED_NAME, pid);
                    nifiFlow.getCreatedData().put(createdDataId, createdData);
                    putInput.accept(createdDataId);
                }
            }

            if (egress != null) {
                final Set<AtlasObjectId> outputs = egress.getOutputs(properties, atlasVariables);
                if (outputs != null && outputs.size() > 0) {
                    outputs.stream().filter(validObjectId).forEach(putOutput);
                }
            }
        }

        // Analyze child ProcessGroups recursively.
        for (ProcessGroupEntity child : flow.getProcessGroups()) {
            final ProcessGroupFlowEntity processGroupFlow = niFiApiClient.getProcessGroupFlow(child.getId());

            if (!processGroupFlow.getPermissions().getCanRead()) {
                logger.warn("Not allowed to read ProcessGroup:{}. Skipp it.", child.getId());
                continue;
            }

            analyzeProcessGroup(processGroupFlow.getProcessGroupFlow().getFlow(), nifiFlow, atlasVariables);
        }

    }

    private List<String> getIncomingProcessorsIds(NiFiFlow nifiFlow, List<ConnectionDTO> incomingConnections) {
        if (incomingConnections == null) {
            return Collections.emptyList();
        }

        final List<String> ids = new ArrayList<>();

        incomingConnections.forEach(c -> {
            final ConnectableDTO source = c.getSource();
            // Ignore self relationship.
            if (!source.getId().equals(c.getDestination().getId())) {
                if (source.getType().equals("PROCESSOR")) {
                    ids.add(source.getId());
                } else {
                    ids.addAll(getIncomingProcessorsIds(nifiFlow, nifiFlow.getIncomingRelationShips(source.getId())));
                }
            }
        });

        return ids;
    }

    private void traverse(NiFiFlow nifiFlow, List<NiFiFlowPath> paths, NiFiFlowPath path, String pid) {

        // Skipping non-processor outgoing relationships
        if (nifiFlow.getProcessors().containsKey(pid)) {
            path.addProcessor(pid);

            if (nifiFlow.getInputs(pid) != null) {
                path.getInputs().addAll(nifiFlow.getInputs(pid));
            }

            if (nifiFlow.getOutputs(pid) != null) {
                path.getOutputs().addAll(nifiFlow.getOutputs(pid));
            }
        }

        final List<ConnectionDTO> outs = nifiFlow.getOutgoingRelationShips(pid);
        if (outs == null) {
            return;
        }

        // Skip non-processor outputs.
        final Predicate<ConnectionDTO> isProcessor = c -> c.getDestination().getType().equals("PROCESSOR");
        outs.stream().filter(isProcessor.negate())
                .map(c -> c.getDestination().getId())
                .forEach(destId -> traverse(nifiFlow, paths, path, destId));

        // Analyze destination processors.
        outs.stream().filter(isProcessor).forEach(out -> {
            final String destPid = out.getDestination().getId();
            if (pid.equals(destPid)) {
                // Avoid loop.
                return;
            }

            // Count how many incoming relationship the destination has.
            long destIncomingConnectionCount = getIncomingProcessorsIds(nifiFlow, nifiFlow.getIncomingRelationShips(destPid)).size();

            if (destIncomingConnectionCount > 1) {
                // If destination has more than one (except the destination itself), it is an independent flow path.
                final NiFiFlowPath newJointPoint = new NiFiFlowPath(destPid);

                final boolean exists = paths.contains(newJointPoint);
                final NiFiFlowPath jointPoint = exists ? paths.stream()
                        .filter(p -> p.equals(newJointPoint)).findFirst().get() : newJointPoint;

                // Link together.
                path.getOutgoingPaths().add(jointPoint);
                jointPoint.getIncomingPaths().add(path);

                if (exists) {
                    // Link existing incoming queue of the joint point.
                    path.getOutputs().add(jointPoint.getInputs().iterator().next());

                } else {
                    // Add jointPoint only if it doesn't exist, to avoid adding the same jointPoint again.
                    jointPoint.setName("p" + paths.size());
                    paths.add(jointPoint);

                    // Create an input queue DataSet because Atlas doesn't show lineage if it doesn't have in and out.
                    // This DataSet is also useful to link flowPaths together on Atlas lineage graph.
                    final AtlasObjectId queueId = new AtlasObjectId(TYPE_NIFI_QUEUE, ATTR_QUALIFIED_NAME, destPid);

                    final AtlasEntity queue = new AtlasEntity(TYPE_NIFI_QUEUE);
                    queue.setAttribute(ATTR_NIFI_FLOW, nifiFlow.getId());
                    queue.setAttribute(ATTR_QUALIFIED_NAME, destPid);
                    queue.setAttribute(ATTR_NAME, "queue");
                    queue.setAttribute(ATTR_DESCRIPTION, "Input queue for " + jointPoint.getName());

                    nifiFlow.getQueues().put(queueId, queue);
                    newJointPoint.getInputs().add(queueId);
                    path.getOutputs().add(queueId);

                    // Start traversing as a new joint point.
                    traverse(nifiFlow, paths, jointPoint, destPid);
                }

            } else {
                // Normal relation, continue digging.
                traverse(nifiFlow, paths, path, destPid);
            }

        });
    }

    private boolean isOnlyConnectedToRootInputPortOrNone(NiFiFlow nifiFlow, List<ConnectionDTO> ins) {
        if (ins == null || ins.isEmpty()) {
            return true;
        }
        return ins.stream().allMatch(
                in -> {
                    final String sourceType = in.getSource().getType();
                    // If it has incoming relationship from other processor, then return false.
                    final boolean isProcessor = sourceType.equals("PROCESSOR");
                    // RemoteOutputPort does not have any further input.
                    final boolean isRemoteOutput = sourceType.equals("REMOTE_OUTPUT_PORT");
                    // Root Group InputPort does not have any further input.
                    final boolean isRootGroupInput = sourceType.equals("INPUT_PORT")
                            && in.getSource().getGroupId().equals(nifiFlow.getRootProcessGroupId());
                    final boolean checkNext = isOnlyConnectedToRootInputPortOrNone(nifiFlow,
                            nifiFlow.getIncomingRelationShips(in.getSource().getId()));
                    return !isProcessor && (isRootGroupInput || isRemoteOutput || checkNext);
                }
        );
    }

    public List<NiFiFlowPath> analyzePaths(NiFiFlow nifiFlow) {
        // Now let's break it into flow paths.
        final Set<String> headProcessors = nifiFlow.getProcessors().keySet().stream()
                .filter(pid -> {
                    final List<ConnectionDTO> ins = nifiFlow.getIncomingRelationShips(pid);
                    return isOnlyConnectedToRootInputPortOrNone(nifiFlow, ins);
                })
                .collect(Collectors.toSet());

        List<NiFiFlowPath> paths = new ArrayList<>();

        headProcessors.forEach(startPid -> {
            final NiFiFlowPath path = new NiFiFlowPath(startPid);
            path.setName("p" + paths.size());
            paths.add(path);
            traverse(nifiFlow, paths, path, startPid);
        });

        return paths;
    }

}
